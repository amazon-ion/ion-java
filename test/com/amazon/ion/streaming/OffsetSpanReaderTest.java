// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.streaming;

import com.amazon.ion.BinaryTest;
import com.amazon.ion.IonType;
import com.amazon.ion.ReaderMaker;
import com.amazon.ion.junit.Injected.Inject;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.Test;

/**
 *
 */
public class OffsetSpanReaderTest
    extends ReaderFacetTestCase
{
    /**
     * Test only readers that provide stable octet offsets.
     */
    @Inject("readerMaker")
    public static final ReaderMaker[] READER_MAKERS =
        ReaderMaker.valuesExcluding(NON_OFFSET_SPAN_READERS);


    public OffsetSpanReaderTest()
    {
        mySeekableReaderRequired = false;
    }


    @Test
    public void testCurrentSpan()
    {
        read("'''hello''' 1 2 3 4 5 6 7 8 9 10 '''Kumo the fluffy dog! He is so fluffy and yet so happy!'''");
        assertSame(IonType.STRING, in.next());
        checkCurrentSpan(4, 10);

        for (int i = 1; i <= 10; i++) {
            assertSame(IonType.INT, in.next());
            assertEquals(i, in.intValue());
        }

        checkCurrentSpan(28, 30);

        // Capture for ION-217
        assertSame(IonType.STRING, in.next());
        checkCurrentSpan(30, 86);
    }

    // Capture for ION-219
    @Test
    public void testCurrentSpanFromStreamMed() throws IOException {
        final ByteArrayOutputStream buf = new ByteArrayOutputStream();
        final int count = 8000;
        for (int i = 0; i < count; i++) {
            buf.write(BinaryTest.hexToBytes("E0 01 00 EA"));
            buf.write(BinaryTest.hexToBytes("22 03 E8"));
        }

        read(buf.toByteArray());
        int offset = 4;
        for (int i = 0; i < count; i++) {
            assertSame(IonType.INT, in.next());
            checkCurrentSpan(offset, offset+3);
            offset += 7;
        }
        assertNull(in.next());
    }
}
