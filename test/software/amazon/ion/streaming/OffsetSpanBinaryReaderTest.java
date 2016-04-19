/*
 * Copyright 2011-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package software.amazon.ion.streaming;

import static software.amazon.ion.BinaryTest.hexToBytes;

import org.junit.Test;
import software.amazon.ion.IonDatagram;
import software.amazon.ion.IonType;
import software.amazon.ion.ReaderMaker;
import software.amazon.ion.impl.PrivateUtils;
import software.amazon.ion.util.RepeatInputStream;

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


    @Test
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
    @Test
    public void testCurrentSpanBeyondMaxIntForOrderedStruct()
    {
        // Value is ordered-struct { name:{{ /* 1024 bytes */}} }

        byte[] data = hexToBytes("E0 01 00 EA "
                                 + "D1 08 84 "  // struct, ordered, len=1028
                                 + "84 "        //   name:
                                 + "AE 08 80"   //     blob, len=1024
                                );
        data = PrivateUtils.copyOf(data, data.length + 1024);

        readBeyondMaxInt(data, IonType.STRUCT);
    }
}
