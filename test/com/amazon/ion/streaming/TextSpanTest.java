// Copyright (c) 2011-2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.streaming;

import static com.amazon.ion.ReaderMaker.valuesExcluding;
import static com.amazon.ion.util.Spans.currentSpan;

import com.amazon.ion.ReaderMaker;
import com.amazon.ion.Span;
import com.amazon.ion.TextSpan;
import com.amazon.ion.facet.Facets;
import com.amazon.ion.junit.Injected.Inject;
import org.junit.Test;

/**
 * @see NonTextSpanTest
 */
public class TextSpanTest
    extends ReaderFacetTestCase
{
    @Inject("readerMaker")
    public static final ReaderMaker[] READER_MAKERS =
        valuesExcluding(NON_TEXT_SPAN_READERS);


    public TextSpanTest()
    {
        mySeekableReaderRequired = false;
    }


    protected void expectNextSpan(int startLine, int startColumn)
    {
        in.next();

        Span s = sp.currentSpan();
        TextSpan ts = Facets.assumeFacet(TextSpan.class, s);
        checkSpan(startLine, startColumn, ts);

        ts = currentSpan(TextSpan.class, in);
        checkSpan(startLine, startColumn, ts);
    }


    @Test
    public void testTrivialSpan()
    {
        read("1 true\n 'hallo'");  // TODO test all types

        expectNextSpan(1, 1);
        expectNextSpan(1, 3);
        expectNextSpan(2, 2);
    }
}
