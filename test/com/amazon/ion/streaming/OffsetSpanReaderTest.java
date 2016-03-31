// Copyright (c) 2011-2012 Amazon.com, Inc.  All rights reserved.

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


    public void checkCurrentSpan(int binaryStart, int binaryFinish, int textStart)
    {
        if (myReaderMaker.sourceIsBinary()) {
            checkCurrentSpan(binaryStart, binaryFinish);
        }
        else {
            checkCurrentSpan(textStart, -1);
        }
    }


    @Test
    public void testCurrentSpan()
    {
        read("'''hello''' 1 2 3 4 5 6 7 8 9 10 '''Kumo the fluffy dog! He is so fluffy and yet so happy!'''");
        assertSame(IonType.STRING, in.next());
        checkCurrentSpan(4, 10, 0);

        for (int i = 1; i <= 10; i++) {
            assertSame(IonType.INT, in.next());
            assertEquals(i, in.intValue());
        }

        checkCurrentSpan(28, 30, 30);

        // Capture for ION-217
        assertSame(IonType.STRING, in.next());
        checkCurrentSpan(30, 86, 33);
    }

    // Capture for ION-219
    @Test
    public void testCurrentSpanFromStreamMed() throws IOException {
        final ByteArrayOutputStream buf = new ByteArrayOutputStream();
        final int count = 8000;
        for (int i = 0; i < count; i++) {
            // $ion_1_0 1000
            buf.write(BinaryTest.hexToBytes("E0 01 00 EA"));
            buf.write(BinaryTest.hexToBytes("22 03 E8"));
        }

        read(buf.toByteArray());
        int binaryStart = 4;
        int textStart = 9;
        for (int i = 0; i < count; i++) {
            assertSame(IonType.INT, in.next());
            checkCurrentSpan(binaryStart, binaryStart+3, textStart);
            binaryStart += 7;
            textStart += 5;
        }
        assertNull(in.next());
    }
}
