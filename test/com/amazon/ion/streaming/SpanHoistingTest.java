// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.streaming;

import com.amazon.ion.Facets;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonTestCase;
import com.amazon.ion.IonType;
import com.amazon.ion.ReaderMaker;
import com.amazon.ion.Span;
import com.amazon.ion.SpanReader;
import com.amazon.ion.TestUtils;
import com.amazon.ion.impl.IonImplUtils;
import com.amazon.ion.junit.Injected.Inject;
import com.amazon.ion.junit.IonAssert;
import java.io.File;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class SpanHoistingTest
    extends IonTestCase
{
    private ReaderMaker myReaderMaker;

    public void setReaderMaker(ReaderMaker maker)
    {
        myReaderMaker = maker;
    }

    @Inject("readerMaker") // TODO ION-230 ION-231
    public static final ReaderMaker[] READER_MAKERS =
        ReaderMaker.valuesExcluding(// ReaderMaker.FROM_STRING,
                                    // ReaderMaker.FROM_BYTES_TEXT,
                                    // ReaderMaker.FROM_BYTES_OFFSET_TEXT,
                                    ReaderMaker.FROM_INPUT_STREAM_BINARY,
                                    ReaderMaker.FROM_INPUT_STREAM_TEXT);


    private IonReader in;
    private SpanReader sr;



    private void expectNoCurrentValue()
    {
        IonAssert.assertNoCurrentValue(in);
    }

    private void expectTopLevel()
    {
        IonAssert.assertTopLevel(in);
    }

    private void expectEof()
    {
        IonAssert.assertEof(in);
    }

    private void expectTopEof()
    {
        IonAssert.assertTopEof(in);
    }



    private void read(byte[] ionData)
    {
        in = myReaderMaker.newReader(system(), ionData);
        sr = Facets.assumeFacet(SpanReader.class, in);
    }

    private void read(String ionText)
    {
        in = myReaderMaker.newReader(system(), ionText);
        sr = Facets.assumeFacet(SpanReader.class, in);
    }


    private void hoist(Span s)
    {
        sr.hoist(s);
        expectTopLevel();
        if (! myReaderMaker.sourceIsBinary()) {   // FIXME ION-234
            expectNoCurrentValue();
        }
    }


    private void checkSpans(IonDatagram dg, Span[] positions)
    {
        for (int i = dg.size() - 1; i >= 0; i--)
        {
            hoist(positions[i]);
            IonType dg_type = dg.get(i).getType();
            IonType span_type = in.next();
            if (dg_type.equals(span_type) == false) {
                assertEquals(dg_type, span_type);
            }
            expectTopLevel();
            IonAssert.assertIonEquals(dg.get(i), system().newValue(in));
        }
        expectTopEof();
    }


    @Test
    public void testWalkingBackwards()
    {
        String text =
            "null true 3 4e0 5.0 6666-06-06T '7' \"8\" {{\"\"}} {{}} [] () {}";

        IonDatagram dg = loader().load(text);

        Span[] positions = new Span[dg.size()];

        read(text);
        for (int i = 0; i < dg.size(); i++)
        {
            assertEquals(dg.get(i).getType(), in.next());
            positions[i] = sr.currentSpan();
        }
        expectEof();

        checkSpans(dg, positions);


        // Collect spans *after* extracting scalar body.

        read(text);
        for (int i = 0; i < dg.size(); i++)
        {
            IonType t =  in.next();
            assertEquals(dg.get(i).getType(),t);
            if (! IonType.isContainer(t))
            {
                TestUtils.consumeCurrentValue(in);
            }
            positions[i] = sr.currentSpan();
        }
        expectTopEof();

        checkSpans(dg, positions);
    }


    @Test
    public void testHoistingWithinContainers()
    {
        read("{f:v,g:[c]} s");

        in.next();
        in.stepIn();
            in.next();
            Span fPos = sr.currentSpan();
            assertEquals("v", in.stringValue());
            in.next();
            Span gPos = sr.currentSpan();
            in.stepIn();
                in.next();
                assertEquals("c", in.stringValue());
                Span cPos = sr.currentSpan();
                expectEof();
            in.stepOut();
            expectEof();
        in.stepOut();
        in.next();
        Span sPos = sr.currentSpan();
        expectTopEof();

        hoist(fPos);
        IonType in_type = in.next();
        assertEquals(IonType.SYMBOL, in_type);
        expectTopLevel();
        assertEquals("v", in.stringValue());
        expectTopEof();

        hoist(cPos);
        in.next();
        expectTopLevel();
        assertEquals("c", in.stringValue());
        expectTopEof();

        hoist(gPos);
        assertEquals(IonType.LIST, in.next());
        expectTopLevel();
        in.stepIn();
            in.next();
            assertEquals("c", in.stringValue());
            assertEquals(null, in.next());
            expectEof();
        in.stepOut();
        expectTopEof();

        hoist(fPos);
        assertEquals(null, in.getFieldName());
        assertEquals(IonType.SYMBOL, in.next());
        expectTopLevel();
        assertEquals("v", in.stringValue());
        expectTopEof();

        hoist(sPos);
        assertEquals(IonType.SYMBOL, in.next());
        expectTopLevel();
        assertEquals("s", in.stringValue());
        expectTopEof();
    }

    @Test
    public void testHoistingLongValue()
    {
        // This value is "long" in that it has a length subfield in the prefix.
        String text = " \"123456789012345\" ";
        read(text);

        in.next();
        Span pos = sr.currentSpan();
        expectTopEof();

        hoist(pos);
        assertEquals(IonType.STRING, in.next());
        expectTopEof();
    }

    @Test
    public void testHoistingOrderedStruct()
    throws IOException
    {
        File file = getTestdataFile("good/structOrdered.10n");
        byte[] binary = IonImplUtils.loadFileBytes(file);

        read(binary);

        in.next();
        Span pos = sr.currentSpan();
        expectTopEof();

        hoist(pos);
        assertEquals(IonType.STRUCT, in.next());
        expectTopEof();
    }


    @Test
    public void testHoistingAnnotatedTopLevelValue()
        throws IOException
    {
        if (myReaderMaker.sourceIsBinary()) return; // TODO ION-229

        read("a::v");
        in.next();
        Span span = sr.currentSpan();
        expectTopEof();

        hoist(span);
        assertSame(IonType.SYMBOL, in.next());
        expectTopLevel();
        Assert.assertArrayEquals(new String[]{"a"}, in.getTypeAnnotations());
        expectTopEof();
    }


    @Test
    public void testHoistingAnnotatedContainedValue()
        throws IOException
    {
        if (myReaderMaker.sourceIsBinary()) return; // TODO ION-229

        read("[a::v]");
        in.next();
        in.stepIn();
        in.next();
        Span span = sr.currentSpan();
        in.stepOut();
        in.next();

        hoist(span);
        assertSame(IonType.SYMBOL, in.next());
        expectTopLevel();
        Assert.assertArrayEquals(new String[]{"a"}, in.getTypeAnnotations());
        expectTopEof();
    }


    //========================================================================
    // Failure cases

    @Test(expected=IllegalStateException.class)
    public void testCurrentSpanBeforeFirstTopLevel()
    {
        read("foo");
        sr.currentSpan();
    }


    private void callCurrentSpanBeforeFirstChild(String ionText)
    {
        read(ionText);
        in.next();
        in.stepIn();
        sr.currentSpan();
    }

    @Test(expected=IllegalStateException.class)
    public void testCurrentSpanBeforeFirstListChild()
    {
        callCurrentSpanBeforeFirstChild("[v]");
    }

    @Test(expected=IllegalStateException.class)
    public void testCurrentSpanBeforeFirstSexpChild()
    {
        callCurrentSpanBeforeFirstChild("(v)");
    }

    @Test(expected=IllegalStateException.class)
    public void testCurrentSpanBeforeFirstStructChild()
    {
        callCurrentSpanBeforeFirstChild("{f:v}");
    }


    private void callCurrentSpanAfterLastChild(String ionText)
    {
        read(ionText);
        in.next();
        in.stepIn();
        in.next();
        in.next();
        sr.currentSpan();
    }

    @Test(expected=IllegalStateException.class)
    public void testCurrentSpanAfterLastListChild()
    {
        callCurrentSpanAfterLastChild("[v]");
    }

    @Test(expected=IllegalStateException.class)
    public void testCurrentSpanAfterLastSexpChild()
    {
        callCurrentSpanAfterLastChild("(v)");
    }

    @Test(expected=IllegalStateException.class)
    public void testCurrentSpanAfterLastStructChild()
    {
        callCurrentSpanAfterLastChild("{f:v}");
    }


    @Test(expected=IllegalStateException.class)
    public void testCurrentSpanAtEndOfStream()
    {
        read("foo");
        in.next();
        assertEquals(null, in.next());
        sr.currentSpan();
    }
}
