// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.streaming;

import com.amazon.ion.ReaderMaker;
import com.amazon.ion.junit.Injected.Inject;
import org.junit.Test;

/**
 * @see SeekableReaderTest
 */
public class NonSeekableReaderTest
    extends ReaderFacetTestCase
{
    @Inject("readerMaker")
    public static final ReaderMaker[] READER_MAKERS = NON_SEEKABLE_READERS;


    public NonSeekableReaderTest()
    {
        mySpanProviderRequired = false;
        mySeekableReaderRequired = false;
    }


    @Test
    public void seekableReaderFacetNotAvailable()
    {
        read("null");
        assertNull("SeekableReader facet not expected", sr);
    }
}
