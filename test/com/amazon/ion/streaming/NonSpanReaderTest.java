// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.streaming;

import com.amazon.ion.ReaderMaker;
import com.amazon.ion.junit.Injected.Inject;
import org.junit.Test;

/**
 * @see CurrentSpanTest
 */
public class NonSpanReaderTest
    extends SpanReaderTestCase
{
    public NonSpanReaderTest()
    {
        mySpanProviderRequired = false;
        mySeekableReaderRequired = false;
    }

    @Inject("readerMaker")
    public static final ReaderMaker[] READER_MAKERS = NON_SPAN_READERS;


    /**
     * Ensure that we don't get the SpanProvider facet where its not supported.
     */
    @Test
    public void testNoSpanProviderFacet()
    {
        read("something");
        assertNull("SpanProvider facet not expected", sp);
    }
}
