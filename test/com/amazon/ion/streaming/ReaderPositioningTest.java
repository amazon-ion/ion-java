// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.streaming;

import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonTestCase;
import com.amazon.ion.IonType;
import com.amazon.ion.impl.IonImplUtils;
import com.amazon.ion.impl.IonReaderOctetPosition;
import com.amazon.ion.impl.IonReaderPosition;
import com.amazon.ion.impl.IonReaderWithPosition;
import com.amazon.ion.junit.IonAssert;
import java.io.ByteArrayInputStream;
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
    private IonReaderWithPosition read(byte[] binary)
    {
        return (IonReaderWithPosition) system().newReader(binary);
    }

    private IonReaderWithPosition read(String text)
    {
        byte[] binary = encode(text);
        return read(binary);
    }

    private IonReaderWithPosition readAsStream(String text)
    {
        return (IonReaderWithPosition) system().newReader(new ByteArrayInputStream(encode(text)));
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

        IonReaderPosition[] positions = new IonReaderPosition[dg.size()];

        IonReaderWithPosition in = read(binary);
        for (int i = 0; i < dg.size(); i++)
        {
            assertEquals(dg.get(i).getType(), in.next());
            positions[i] = in.getCurrentPosition();
            // TODO test reading the value before calling getPos
        }
        assertEquals(null, in.next());

        for (int i = dg.size() - 1; i >= 0; i--)
        {
            in.seek(positions[i]);
            assertEquals(dg.get(i).getType(), in.next());
            IonAssert.assertIonEquals(dg.get(i), system().newValue(in));
        }
    }


    @Test
    public void testSeekingIntoContainers()
    {
        IonReaderWithPosition in = read("{f:v,g:[c]} s");

        in.next();
        in.stepIn();
            in.next();
            IonReaderPosition fPos = in.getCurrentPosition();
            assertEquals("v", in.stringValue());
            in.next();
            IonReaderPosition gPos = in.getCurrentPosition();
            in.stepIn();
                in.next();
                assertEquals("c", in.stringValue());
                IonReaderPosition cPos = in.getCurrentPosition();
                assertEquals(null, in.next());
            in.stepOut();
            assertEquals(null, in.next());
        in.stepOut();
        in.next();
        IonReaderPosition sPos = in.getCurrentPosition();
        assertEquals(null, in.next());


        in.seek(fPos);
        assertEquals(IonType.SYMBOL, in.next());
        assertEquals(null, in.getFieldName());
        assertEquals("v", in.stringValue());
        assertEquals(null, in.next());

        in.seek(cPos);
        in.next();
        assertEquals("c", in.stringValue());
        assertEquals(null, in.getFieldName());
        assertEquals(null, in.next());

        in.seek(gPos);
        assertEquals(IonType.LIST, in.next());
        assertEquals(null, in.getFieldName());
        in.stepIn();
            in.next();
            assertEquals("c", in.stringValue());
            assertEquals(null, in.next());
        in.stepOut();
        assertEquals(null, in.next());

        in.seek(fPos);
        assertEquals(null, in.getFieldName());
        assertEquals(IonType.SYMBOL, in.next());
        assertEquals("v", in.stringValue());
        assertEquals(null, in.next());

        in.seek(sPos);
        assertEquals(IonType.SYMBOL, in.next());
        assertEquals("s", in.stringValue());
        assertEquals(null, in.next());
    }

    @Test
    public void testSeekingToLongValue()
    {
        // This value is "long" in that it has a length subfield in the prefix.
        String text = " \"123456789012345\" ";
        IonReaderWithPosition in = read(text);

        in.next();
        IonReaderPosition pos = in.getCurrentPosition();
        assertEquals(null, in.next());

        in.seek(pos);
        assertEquals(IonType.STRING, in.next());
        assertEquals(null, in.next());
    }

    @Test
    public void testSeekingToOrderedStruct()
    throws IOException
    {

        File file = getTestdataFile("good/structOrdered.10n");
        byte[] binary = IonImplUtils.loadFileBytes(file);

        IonReaderWithPosition in = read(binary);

        in.next();
        IonReaderPosition pos = in.getCurrentPosition();
        assertEquals(null, in.next());

        in.seek(pos);
        assertEquals(IonType.STRUCT, in.next());
        assertEquals(null, in.next());
    }


    // TODO test annotations

    @Test(expected=IllegalStateException.class)
    public void testGetPosBeforeFirstTopLevel()
    {
        IonReaderWithPosition in = read("foo");
        IonReaderPosition pos = in.getCurrentPosition();
    }

    @Test(expected=IllegalStateException.class)  // TODO similar for list/sexp
    public void testGetPosBeforeFirstStructChild()
    {
        IonReaderWithPosition in = read("{f:v}");
        in.next();
        in.stepIn();
        in.getCurrentPosition();
    }

    @Test(expected=IllegalStateException.class)  // TODO similar for list/sexp
    public void testGetPosAfterLastStructChild()
    {
        IonReaderWithPosition in = read("{f:v}");
        in.next();
        in.stepIn();
        in.next();
        in.next();
        in.getCurrentPosition();
    }


    @Test(expected=IllegalStateException.class)
    public void testGetPosAtEndOfStream()
    {
        IonReaderWithPosition in = read("foo");
        in.next();
        assertEquals(null, in.next());
        IonReaderPosition pos = in.getCurrentPosition();
        // TODO what does this mean?
    }

    @Test
    public void testGetPosFromStream()
    {
        IonReaderWithPosition in = readAsStream("'''hello''' 1 2 3 4 5 6 7 8 9 10 '''Kumo the fluffy dog! He is so fluffy and yet so happy!'''");
        assertSame(IonType.STRING, in.next());
        IonReaderOctetPosition pos = in.getCurrentPosition().asFacet(IonReaderOctetPosition.class);
        assertNotNull(pos);
        assertEquals(4, pos.getOffset());
        assertEquals(6, pos.getLength());
        for (int i = 1; i <= 10; i++) {
            assertSame(IonType.INT, in.next());
            assertEquals(i, in.intValue());
        }

        pos = in.getCurrentPosition().asFacet(IonReaderOctetPosition.class);
        assertNotNull(pos);
        assertEquals(28, pos.getOffset());
        assertEquals(2, pos.getLength());

        // Capture for ION-217
        assertSame(IonType.STRING, in.next());
        pos = in.getCurrentPosition().asFacet(IonReaderOctetPosition.class);
        assertNotNull(pos);
        assertEquals(30, pos.getOffset());
        assertEquals(56, pos.getLength());
    }

    // FIXME ION-216
    @Ignore
    @Test
    public void testGetPosFromStreamBig() {
        final String text = "'''Kumo the fluffy dog! He is so fluffy and yet so happy!'''";
        final long repeat = 40000000L;
        IonReaderWithPosition in = (IonReaderWithPosition) system().newReader(
            repeatStream(text, repeat) // make sure we go past Integer.MAX_VALUE
        );

        long iterLimit = repeat - 10;
        for (long i = 0; i < iterLimit; i++)
        {
            assertSame(IonType.STRING, in.next());
        }
        IonReaderOctetPosition pos = in.getCurrentPosition().asFacet(IonReaderOctetPosition.class);
        assertNotNull(pos);
        assertEquals(iterLimit * 60, pos.getOffset());
        assertEquals(56, pos.getLength());
    }
}
