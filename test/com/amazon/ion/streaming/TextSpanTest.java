// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.streaming;

import static com.amazon.ion.ReaderMaker.valuesExcluding;

import com.amazon.ion.Facets;
import com.amazon.ion.ReaderMaker;
import com.amazon.ion.Span;
import com.amazon.ion.TextSpan;
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


    protected void expectNextStart(int startLine, int startColumn)
    {
        in.next();
        Span s = sp.currentSpan();
        TextSpan ts = Facets.assumeFacet(TextSpan.class, s);
        assertEquals("startLine",   startLine,   ts.getStartLine());
        assertEquals("startColumn", startColumn, ts.getStartColumn());
    }

    @Test
    public void testTrivialSpan()
    {
        read("1 true\n 'hallo'");  // TODO test all types

        expectNextStart(1, 1);
        expectNextStart(1, 3);
        expectNextStart(2, 2);
    }
}
