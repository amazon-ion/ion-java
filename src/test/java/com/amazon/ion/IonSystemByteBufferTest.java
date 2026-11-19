/*
 * Copyright 2007-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.ion;

import com.amazon.ion.impl._Private_Utils;
import java.nio.ByteBuffer;
import java.util.Iterator;
import org.junit.Test;

/**
 * Unit tests for the {@link java.nio.ByteBuffer} overloads on {@link IonSystem}:
 * {@link IonSystem#iterate(ByteBuffer)}, {@link IonSystem#newReader(ByteBuffer)},
 * and {@link IonSystem#singleValue(ByteBuffer)}.
 * <p>
 * The tests exercise all supported buffer kinds (array-backed writable, direct
 * off-heap, read-only, and a sliced sub-range of a larger backing array) for
 * both binary and (UTF-8) text Ion, and verify the documented buffer
 * position/limit contract, snapshot isolation, and error behavior.
 */
public class IonSystemByteBufferTest
    extends IonTestCase
{
    // Three top-level values, used for the iterate/multi-value cases.
    private static final String THREE_VALUES = "1 2 3";
    // A single value, used for the singleValue/newReader cases.
    private static final String ONE_VALUE = "\"hello\"";

    /** How the ByteBuffer under test is constructed from a byte[]. */
    private enum BufferKind
    {
        /** {@link ByteBuffer#wrap(byte[])}: array-backed, writable. */
        ARRAY_BACKED
        {
            @Override
            ByteBuffer wrap(byte[] data)
            {
                return ByteBuffer.wrap(data);
            }
        },
        /** {@link ByteBuffer#allocateDirect(int)}: off-heap, hasArray()==false. */
        DIRECT
        {
            @Override
            ByteBuffer wrap(byte[] data)
            {
                ByteBuffer b = ByteBuffer.allocateDirect(data.length);
                b.put(data);
                b.flip();
                return b;
            }
        },
        /** {@link ByteBuffer#asReadOnlyBuffer()}: isReadOnly()==true. */
        READ_ONLY
        {
            @Override
            ByteBuffer wrap(byte[] data)
            {
                return ByteBuffer.wrap(data).asReadOnlyBuffer();
            }
        },
        /**
         * A {@link ByteBuffer#slice()} whose readable region is exactly
         * {@code data}, but which sits in the middle of a larger backing array
         * (non-zero {@code arrayOffset}). Bytes outside the readable region are
         * filled with a sentinel to catch over-reads.
         */
        SLICED_SUBRANGE
        {
            @Override
            ByteBuffer wrap(byte[] data)
            {
                int prefix = 13;
                int suffix = 17;
                byte[] padded = new byte[prefix + data.length + suffix];
                // Sentinel bytes that must never be read.
                java.util.Arrays.fill(padded, (byte) 0x7F);
                System.arraycopy(data, 0, padded, prefix, data.length);
                ByteBuffer backing = ByteBuffer.wrap(padded);
                backing.position(prefix);
                backing.limit(prefix + data.length);
                return backing.slice();
            }
        };

        abstract ByteBuffer wrap(byte[] data);
    }

    private byte[] binary(String ionText)
    {
        return encode(ionText);
    }

    private byte[] text(String ionText)
    {
        return _Private_Utils.utf8(ionText);
    }

    //========================================================================
    // iterate(ByteBuffer)

    @Test
    public void testIterateBinaryAllKinds()
    {
        for (BufferKind kind : BufferKind.values())
        {
            ByteBuffer buffer = kind.wrap(binary(THREE_VALUES));
            Iterator<IonValue> it = system().iterate(buffer);
            assertThreeInts(it, kind.name());
        }
    }

    @Test
    public void testIterateTextAllKinds()
    {
        for (BufferKind kind : BufferKind.values())
        {
            ByteBuffer buffer = kind.wrap(text(THREE_VALUES));
            Iterator<IonValue> it = system().iterate(buffer);
            assertThreeInts(it, kind.name());
        }
    }

    @Test
    public void testIterateEmptyBuffer()
    {
        Iterator<IonValue> it = system().iterate(ByteBuffer.wrap(new byte[0]));
        assertFalse(it.hasNext());
    }

    @Test(expected = NullPointerException.class)
    public void testIterateNullBuffer()
    {
        system().iterate((ByteBuffer) null);
    }

    //========================================================================
    // newReader(ByteBuffer)

    @Test
    public void testNewReaderBinaryAllKinds()
    {
        for (BufferKind kind : BufferKind.values())
        {
            ByteBuffer buffer = kind.wrap(binary(ONE_VALUE));
            IonReader reader = system().newReader(buffer);
            assertSame("kind=" + kind, IonType.STRING, reader.next());
            assertEquals("kind=" + kind, "hello", reader.stringValue());
            assertNull("kind=" + kind, reader.next());
        }
    }

    @Test
    public void testNewReaderTextAllKinds()
    {
        for (BufferKind kind : BufferKind.values())
        {
            ByteBuffer buffer = kind.wrap(text(ONE_VALUE));
            IonReader reader = system().newReader(buffer);
            assertSame("kind=" + kind, IonType.STRING, reader.next());
            assertEquals("kind=" + kind, "hello", reader.stringValue());
            assertNull("kind=" + kind, reader.next());
        }
    }

    @Test(expected = NullPointerException.class)
    public void testNewReaderNullBuffer()
    {
        system().newReader((ByteBuffer) null);
    }

    //========================================================================
    // singleValue(ByteBuffer)

    @Test
    public void testSingleValueBinaryAllKinds()
    {
        for (BufferKind kind : BufferKind.values())
        {
            ByteBuffer buffer = kind.wrap(binary(ONE_VALUE));
            IonValue v = system().singleValue(buffer);
            assertEquals("kind=" + kind, "hello", ((IonString) v).stringValue());
        }
    }

    @Test
    public void testSingleValueTextAllKinds()
    {
        for (BufferKind kind : BufferKind.values())
        {
            ByteBuffer buffer = kind.wrap(text(ONE_VALUE));
            IonValue v = system().singleValue(buffer);
            assertEquals("kind=" + kind, "hello", ((IonString) v).stringValue());
        }
    }

    @Test(expected = UnexpectedEofException.class)
    public void testSingleValueEmptyBuffer()
    {
        system().singleValue(ByteBuffer.wrap(new byte[0]));
    }

    @Test(expected = IonException.class)
    public void testSingleValueMultipleValues()
    {
        system().singleValue(ByteBuffer.wrap(text(THREE_VALUES)));
    }

    @Test(expected = NullPointerException.class)
    public void testSingleValueNullBuffer()
    {
        system().singleValue((ByteBuffer) null);
    }

    //========================================================================
    // Buffer position/limit contract (Requirement 5)

    @Test
    public void testPositionAdvancedToLimitAfterCall()
    {
        for (BufferKind kind : BufferKind.values())
        {
            ByteBuffer buffer = kind.wrap(binary(ONE_VALUE));
            int limitBefore = buffer.limit();
            int capacityBefore = buffer.capacity();

            system().newReader(buffer);

            assertEquals("remaining, kind=" + kind, 0, buffer.remaining());
            assertEquals("position==limit, kind=" + kind, limitBefore, buffer.position());
            assertEquals("limit unchanged, kind=" + kind, limitBefore, buffer.limit());
            assertEquals("capacity unchanged, kind=" + kind, capacityBefore, buffer.capacity());
        }
    }

    @Test
    public void testMarkPreservedWhenStillValid()
    {
        // A mark set at position 0 remains valid after consuming to the limit,
        // because reset() would move position back to 0 (<= limit).
        ByteBuffer buffer = ByteBuffer.wrap(binary(ONE_VALUE));
        buffer.mark(); // mark at position 0

        system().newReader(buffer);
        assertEquals(0, buffer.remaining());

        buffer.reset(); // must not throw InvalidMarkException
        assertEquals("mark should restore position to 0", 0, buffer.position());
    }

    @Test
    public void testSnapshotIsolationMutateAfterCall()
    {
        // The reader/value must operate over a private copy: mutating the
        // (writable, array-backed) buffer's bytes after the call must not
        // affect the already-extracted value.
        byte[] data = text(ONE_VALUE);
        ByteBuffer buffer = ByteBuffer.wrap(data);

        IonValue v = system().singleValue(buffer);
        assertEquals("hello", ((IonString) v).stringValue());

        // Corrupt the original backing array after the fact.
        java.util.Arrays.fill(data, (byte) 'X');
        assertEquals("value must be unaffected by later buffer mutation",
                     "hello", ((IonString) v).stringValue());
    }

    @Test
    public void testSubRangeReadsOnlyReadableRegion()
    {
        // The sliced buffer is surrounded by 0x7F sentinel bytes; if extraction
        // read outside [position, limit) the parse would fail or produce wrong
        // values. A correct read yields exactly the three ints.
        ByteBuffer buffer = BufferKind.SLICED_SUBRANGE.wrap(binary(THREE_VALUES));
        Iterator<IonValue> it = system().iterate(buffer);
        assertThreeInts(it, "SLICED_SUBRANGE");
        assertEquals(0, buffer.remaining());
    }

    //========================================================================
    // helpers

    private void assertThreeInts(Iterator<IonValue> it, String label)
    {
        for (int expected = 1; expected <= 3; expected++)
        {
            assertTrue(label + ": expected value " + expected, it.hasNext());
            IonValue v = it.next();
            assertEquals(label, expected, ((IonInt) v).intValue());
        }
        assertFalse(label + ": no more values", it.hasNext());
    }
}
