// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.streaming;

import static com.amazon.ion.BinaryTest.hexToBytes;

import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonType;
import com.amazon.ion.ReaderMaker;
import com.amazon.ion.impl._Private_Utils;
import com.amazon.ion.util.RepeatInputStream;
import org.junit.Test;

public class OffsetSpanBinaryReaderTest
    extends ReaderFacetTestCase
{
    public OffsetSpanBinaryReaderTest()
    {
        // This is just to get checkCurrentSpan to work.
        myReaderMaker = ReaderMaker.FROM_INPUT_STREAM_BINARY;
        mySeekableReaderRequired = false;
    }


    /**
     * Creates a stream that repeats a single IonValue numerous times until
     * the octet offsets are larger than Integer.MAX_VALUE
     */
    private void readBeyondMaxInt(byte[] data, IonType valueType)
    {
        final long bvmLength   = 4;
        final long valueLength = data.length - bvmLength;

        // Make sure our offsets get larger than Integer.MAX_VALUE
        final long repeat = (((long)Integer.MAX_VALUE) * 2) / data.length;

        in = system().newReader(new RepeatInputStream(data, repeat));
        initFacets();

        long iterLimit = repeat;
        for (long i = 0; i <= iterLimit; i++)
        {
            assertSame(valueType, in.next());

            long expectedStart = bvmLength + i * (valueLength + bvmLength);
            checkCurrentSpan(expectedStart, expectedStart + valueLength);
        }
    }


    @Test // Traps ION-216
    public void testCurrentSpanBeyondMaxInt()
    {
        IonDatagram dg = system().newDatagram();
        dg.add().newBlob(new byte[2000]);
        byte[] binary = dg.getBytes();
        readBeyondMaxInt(binary, IonType.BLOB);
    }


    /**
     * Tests ordered struct since it has special length handling (and had a
     * bug at time of writing).  This data is hand-coded since we don't have
     * an API to generate ordered structs.
     */
    @Test // Traps ION-216
    public void testCurrentSpanBeyondMaxIntForOrderedStruct()
    {
        // Value is ordered-struct { name:{{ /* 1024 bytes */}} }

        byte[] data = hexToBytes("E0 01 00 EA "
                                 + "D1 08 84 "  // struct, ordered, len=1028
                                 + "84 "        //   name:
                                 + "AE 08 80"   //     blob, len=1024
                                );
        data = _Private_Utils.copyOf(data, data.length + 1024);

        readBeyondMaxInt(data, IonType.STRUCT);
    }
}
