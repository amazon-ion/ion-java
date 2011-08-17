// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.streaming;

import com.amazon.ion.BinaryTest;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonTestCase;
import com.amazon.ion.IonType;
import com.amazon.ion.Span;
import com.amazon.ion.impl.IonImplUtils;
import com.amazon.ion.impl.IonReaderOctetPosition;
import com.amazon.ion.impl.IonReaderWithPosition;
import com.amazon.ion.junit.IonAssert;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 */
public class ReaderPositioningTest
    extends IonTestCase
{
    private IonReader in;
    private IonReaderWithPosition p;

    private IonReader read(byte[] binary)
    {
        in = system().newReader(binary);
        p = in.asFacet(IonReaderWithPosition.class);
        return in;
    }

    private IonReader read(String text)
    {
        byte[] binary = encode(text);
        in = read(binary);
        return in;
    }

    private IonReader readAsStream(String text)
    {
        ByteArrayInputStream bytesIn = new ByteArrayInputStream(encode(text));
        in = system().newReader(bytesIn);
        p = in.asFacet(IonReaderWithPosition.class);
        return in;
    }

    private InputStream repeatStream(final String text, final long times)
    {
        final byte[] binary = encode(text);
        return new InputStream()
        {
            private long remainder = times;
            private ByteBuffer buf = ByteBuffer.wrap(binary);

            private boolean isDone() {
                return remainder == 0 && !buf.hasRemaining();
            }

            private void checkBuf() {
                if (!isDone() && !buf.hasRemaining()) {
                    remainder--;
                    buf.clear();
                }
            }

            @Override
            public int read() throws IOException
            {
                if (isDone())
                {
                    return -1;
                }

                int octet = buf.get() & 0xFF;
                checkBuf();
                return octet;
            }
            @Override
            public int read(byte[] b, int off, int len) throws IOException
            {
                if (isDone())
                {
                    return -1;
                }

                int rem = len - off;
                int consumed = 0;
                while (rem > 0 && !isDone())
                {
                    int amount = Math.min(rem, buf.remaining());
                    buf.get(b, off, amount);
                    off += amount;
                    rem -= amount;
                    consumed += amount;
                    checkBuf();
                }
                return consumed;
            }
        };
    }

    @Test
    public void testWalkingBackwards()
    {
        String text =
            "null true 3 4e0 5.0 6666-06-06T '7' \"8\" {{\"\"}} {{}} [] () {}";

        IonDatagram dg = loader().load(text);
        byte[] binary = dg.getBytes();

        Span[] positions = new Span[dg.size()];

        read(binary);
        for (int i = 0; i < dg.size(); i++)
        {
            assertEquals(dg.get(i).getType(), in.next());
            positions[i] = p.currentSpan();
            // TODO test reading the value before calling getPos
        }
        assertEquals(null, in.next());

        for (int i = dg.size() - 1; i >= 0; i--)
        {
            p.hoist(positions[i]);
            assertEquals(dg.get(i).getType(), in.next());
            IonAssert.assertIonEquals(dg.get(i), system().newValue(in));
        }
    }


    @Test
    public void testSeekingIntoContainers()
    {
        read("{f:v,g:[c]} s");

        in.next();
        in.stepIn();
            in.next();
            Span fPos = p.currentSpan();
            assertEquals("v", in.stringValue());
            in.next();
            Span gPos = p.currentSpan();
            in.stepIn();
                in.next();
                assertEquals("c", in.stringValue());
                Span cPos = p.currentSpan();
                assertEquals(null, in.next());
            in.stepOut();
            assertEquals(null, in.next());
        in.stepOut();
        in.next();
        Span sPos = p.currentSpan();
        assertEquals(null, in.next());


        p.hoist(fPos);
        assertEquals(IonType.SYMBOL, in.next());
        assertEquals(null, in.getFieldName());
        assertEquals("v", in.stringValue());
        assertEquals(null, in.next());

        p.hoist(cPos);
        in.next();
        assertEquals("c", in.stringValue());
        assertEquals(null, in.getFieldName());
        assertEquals(null, in.next());

        p.hoist(gPos);
        assertEquals(IonType.LIST, in.next());
        assertEquals(null, in.getFieldName());
        in.stepIn();
            in.next();
            assertEquals("c", in.stringValue());
            assertEquals(null, in.next());
        in.stepOut();
        assertEquals(null, in.next());

        p.hoist(fPos);
        assertEquals(null, in.getFieldName());
        assertEquals(IonType.SYMBOL, in.next());
        assertEquals("v", in.stringValue());
        assertEquals(null, in.next());

        p.hoist(sPos);
        assertEquals(IonType.SYMBOL, in.next());
        assertEquals("s", in.stringValue());
        assertEquals(null, in.next());
    }

    @Test
    public void testSeekingToLongValue()
    {
        // This value is "long" in that it has a length subfield in the prefix.
        String text = " \"123456789012345\" ";
        read(text);

        in.next();
        Span pos = p.currentSpan();
        assertEquals(null, in.next());

        p.hoist(pos);
        assertEquals(IonType.STRING, in.next());
        assertEquals(null, in.next());
    }

    @Test
    public void testSeekingToOrderedStruct()
    throws IOException
    {

        File file = getTestdataFile("good/structOrdered.10n");
        byte[] binary = IonImplUtils.loadFileBytes(file);

        read(binary);

        in.next();
        Span pos = p.currentSpan();
        assertEquals(null, in.next());

        p.hoist(pos);
        assertEquals(IonType.STRUCT, in.next());
        assertEquals(null, in.next());
    }


    // TODO test annotations

    @Test(expected=IllegalStateException.class)
    public void testGetPosBeforeFirstTopLevel()
    {
        read("foo");
        p.currentSpan();
    }

    @Test(expected=IllegalStateException.class)  // TODO similar for list/sexp
    public void testGetPosBeforeFirstStructChild()
    {
        read("{f:v}");
        in.next();
        in.stepIn();
        p.currentSpan();
    }

    @Test(expected=IllegalStateException.class)  // TODO similar for list/sexp
    public void testGetPosAfterLastStructChild()
    {
        read("{f:v}");
        in.next();
        in.stepIn();
        in.next();
        in.next();
        p.currentSpan();
    }


    @Test(expected=IllegalStateException.class)
    public void testGetPosAtEndOfStream()
    {
        read("foo");
        in.next();
        assertEquals(null, in.next());
        p.currentSpan();
    }

    @Test
    public void testGetPosFromStream()
    {
        readAsStream("'''hello''' 1 2 3 4 5 6 7 8 9 10 '''Kumo the fluffy dog! He is so fluffy and yet so happy!'''");
        assertSame(IonType.STRING, in.next());
        IonReaderOctetPosition pos = p.currentSpan().asFacet(IonReaderOctetPosition.class);
        assertNotNull(pos);
        assertEquals( 4, pos.getOffset());
        assertEquals( 4, pos.getStartOffset());
        assertEquals( 6, pos.getLength());
        assertEquals(10, pos.getFinishOffset());
        for (int i = 1; i <= 10; i++) {
            assertSame(IonType.INT, in.next());
            assertEquals(i, in.intValue());
        }

        pos = p.currentSpan().asFacet(IonReaderOctetPosition.class);
        assertNotNull(pos);
        assertEquals(28, pos.getOffset());
        assertEquals(28, pos.getStartOffset());
        assertEquals( 2, pos.getLength());
        assertEquals(30, pos.getFinishOffset());

        // Capture for ION-217
        assertSame(IonType.STRING, in.next());
        pos = p.currentSpan().asFacet(IonReaderOctetPosition.class);
        assertNotNull(pos);
        assertEquals(30, pos.getOffset());
        assertEquals(30, pos.getStartOffset());
        assertEquals(56, pos.getLength());
        assertEquals(86, pos.getFinishOffset());
    }

    // Capture for ION-219
    @Test
    public void testGetPosFromStreamMed() throws IOException {
        final ByteArrayOutputStream buf = new ByteArrayOutputStream();
        final int count = 8000;
        for (int i = 0; i < count; i++) {
            buf.write(BinaryTest.hexToBytes("E0 01 00 EA"));
            buf.write(BinaryTest.hexToBytes("22 03 E8"));
        }

        read(buf.toByteArray());
        int offset = 4;
        for (int i = 0; i < count; i++) {
            assertSame(IonType.INT, in.next());
            IonReaderOctetPosition pos = p.currentSpan().asFacet(IonReaderOctetPosition.class);
            assertEquals(offset, pos.getOffset());
            assertEquals(offset, pos.getStartOffset());
            assertEquals(3, pos.getLength());
            assertEquals(offset+3, pos.getFinishOffset());
            offset += 7;
        }
        assertNull(in.next());
    }

    // FIXME ION-216
    @Ignore
    @Test
    public void testGetPosFromStreamBig() {
        final String text = "'''Kumo the fluffy dog! He is so fluffy and yet so happy!'''";
        final long repeat = 40000000L;
        in = system().newReader(
            repeatStream(text, repeat) // make sure we go past Integer.MAX_VALUE
        );
        p = in.asFacet(IonReaderWithPosition.class);

        long iterLimit = repeat - 10;
        for (long i = 0; i < iterLimit; i++)
        {
            assertSame(IonType.STRING, in.next());
        }
        IonReaderOctetPosition pos = p.currentSpan().asFacet(IonReaderOctetPosition.class);
        assertNotNull(pos);
        assertEquals(iterLimit * 60, pos.getOffset());
        assertEquals(iterLimit * 60, pos.getStartOffset());
        assertEquals(56, pos.getLength());
        assertEquals(iterLimit * 60 + 56, pos.getFinishOffset());
    }
}
