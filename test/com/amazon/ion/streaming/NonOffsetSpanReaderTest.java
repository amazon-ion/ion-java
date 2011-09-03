// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.streaming;

import com.amazon.ion.OffsetSpan;
import com.amazon.ion.ReaderMaker;
import com.amazon.ion.Span;
import com.amazon.ion.junit.Injected.Inject;
import org.junit.Test;

/**
 * @see OffsetSpanReaderTest
 */
public class NonOffsetSpanReaderTest
    extends ReaderFacetTestCase
{
    @Inject("readerMaker")
    public static final ReaderMaker[] READER_MAKERS =
    {
         ReaderMaker.FROM_STRING,
         ReaderMaker.FROM_BYTES_TEXT,
         ReaderMaker.FROM_BYTES_OFFSET_TEXT,
//         ReaderMaker.FROM_BYTES_OFFSET_BINARY,
         ReaderMaker.FROM_INPUT_STREAM_TEXT,
         ReaderMaker.FROM_DOM
    };


    public NonOffsetSpanReaderTest()
    {
        mySpanProviderRequired = false;
        mySeekableReaderRequired = false;
    }


    @Test
    public void noOffsetSpanFacet()
    {
        read("null");
        in.next();
        if (sp != null)
        {
            Span s = sp.currentSpan();
            expectNoFacet(OffsetSpan.class, s);
        }
    }
}
