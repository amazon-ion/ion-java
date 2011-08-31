// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.streaming;

import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonType;
import com.amazon.ion.ReaderMaker;
import com.amazon.ion.Span;
import com.amazon.ion.TestUtils;
import com.amazon.ion.junit.Injected.Inject;
import org.junit.Test;

/**
 *
 */
public class CurrentSpanTest
    extends SpanReaderTestCase
{
    public CurrentSpanTest()
    {
        super(/* spanReaderRequired */ false);
    }


    /**
     * DON'T DISABLE VALUES HERE! All readers must pass these tests if they
     * provide the SpanReader facet.
     */
    @Inject("readerMaker")
    public static final ReaderMaker[] READER_MAKERS = ReaderMaker.values();



    @Test
    public void testCallingCurrentSpan()
    {
        if (sr == null) return;

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

            assertEquals(positions[i], sr.currentSpan());
        }
        expectTopEof();
    }


    @Test
    public void testCurrentSpanWithinContainers()
    {
        if (sr == null) return;

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
    }



    //========================================================================
    // Failure cases

    @Test(expected=IllegalStateException.class)
    public void testCurrentSpanBeforeFirstTopLevel()
    {
        if (sr == null) throw new IllegalStateException("not really");

        read("foo");
        sr.currentSpan();
    }


    private void callCurrentSpanBeforeFirstChild(String ionText)
    {
        if (sr == null) throw new IllegalStateException("not really");

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
        if (sr == null) throw new IllegalStateException("not really");

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
        if (sr == null) throw new IllegalStateException("not really");

        read("foo");
        in.next();
        assertEquals(null, in.next());
        sr.currentSpan();
    }
}
