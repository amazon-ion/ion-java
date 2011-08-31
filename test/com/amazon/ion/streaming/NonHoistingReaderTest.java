// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.streaming;

import com.amazon.ion.ReaderMaker;
import com.amazon.ion.Span;
import com.amazon.ion.junit.Injected.Inject;
import org.junit.Test;

/**
 *
 */
public class NonHoistingReaderTest
    extends SpanReaderTestCase
{
    public NonHoistingReaderTest()
    {
        super(/* spanReaderRequired */ false);
    }


    /**
     * Test all readers that are NOT covered by {@link SpanHoistingTest}.
     */
    @Inject("readerMaker")
    public static final ReaderMaker[] READER_MAKERS = NON_HOISTING_READERS;


    @Test
    public void testTrivialSpan()
    {
        read("null");
        in.next();
        if (sr != null)
        {
            Span s = sr.currentSpan();
            try {
                sr.hoist(s);
                fail("Expected exception");
            }
            catch (UnsupportedOperationException e) { }
        }
    }

}
