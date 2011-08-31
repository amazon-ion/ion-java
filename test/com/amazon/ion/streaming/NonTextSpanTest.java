// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.streaming;

import com.amazon.ion.ReaderMaker;
import com.amazon.ion.Span;
import com.amazon.ion.TextSpan;
import com.amazon.ion.junit.Injected.Inject;
import org.junit.Test;

/**
 *
 */
public class NonTextSpanTest
    extends SpanReaderTestCase
{
    public NonTextSpanTest()
    {
        super(/* spanReaderRequired */ false);
    }

    @Inject("readerMaker")
    public static final ReaderMaker[] READER_MAKERS = NON_TEXT_SPAN_READERS;


    @Test
    public void testTrivialSpan()
    {
        read("null");
        in.next();
        if (sr != null)
        {
            Span s = sr.currentSpan();
            TextSpan ts = s.asFacet(TextSpan.class);
            assertSame("TextSpan facet not expected", null, ts);
        }
    }
}
