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

package com.amazon.ion.impl.bin;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * A facade over {@link Block} management and low-level Ion encoding concerns for the {@link IonRawBinaryWriter}.
 */
/*package*/ final class WriteBuffer implements Closeable
{

    // A `long` is 8 bytes of data. The VarUInt encoding will add a continuation bit to every byte,
    // growing the data size by 8 more bits. Therefore, the largest encoded size of a `long` is
    // 9 bytes.
//    private static final int MAX_VAR_UINT_LENGTH = 9;
//    private static final int BITS_PER_VAR_UINT_BYTE = 7;

    // A `long` is 8 bytes of data. The VarInt encoding will add one continuation bit per byte
    // as well as a sign bit, for a total of 9 extra bits. Therefore, the largest encoding
    // of a `long` will be just over 9 bytes.
    private static final int MAX_VAR_INT_LENGTH = 10;

    private ByteBuffer byteBuffer;
    // Reusable buffer for encoding VarUInts and VarInts
//    private final byte[] varUIntBuffer;
//    private final byte[] varIntBuffer;

    private Block initialBlock;

    private byte[] intBuffer;

    public WriteBuffer(final BlockAllocator allocator)
    {
        initialBlock = allocator.allocateBlock();
        byteBuffer = ByteBuffer.wrap(initialBlock.data);
//        varUIntBuffer = new byte[MAX_VAR_UINT_LENGTH];
//        varIntBuffer = new byte[MAX_VAR_INT_LENGTH];
        intBuffer = new byte[16];
    }

    /** Resets the write buffer to empty. */
    public void reset()
    {
        close();
    }

    public void close()
    {
        byteBuffer.clear();
        if (initialBlock != null) {
            initialBlock.close();
            initialBlock = null;
        }
    }

    /** Resets the write buffer to a particular point. */
    public void truncate(final long position)
    {
        byteBuffer.position((int) position);
    }

    private int remaining() {
        return byteBuffer.remaining();
    }

    public int position() {
        return byteBuffer.position();
    }

    private void grow() {
        //System.out.println("Growing from " + byteBuffer.capacity() + " to " + (byteBuffer.capacity() * 2));
        ByteBuffer newBuffer = ByteBuffer.allocate(byteBuffer.capacity() * 2);
        byteBuffer.flip();
        newBuffer.put(byteBuffer);
        if (initialBlock != null) {
            // return the block to its pool, we'll make a new one
            initialBlock.close();
            initialBlock = null;
        }
        byteBuffer = newBuffer;
    }

    private static final int OCTET_MASK = 0xFF;

    /** Returns the octet at the logical position given. */
    public int getUInt8At(final long position)
    {
        return byteBuffer.get((int) position) & OCTET_MASK;
    }

    /** Writes a single octet to the buffer, expanding if necessary. */
    public void writeByte(final byte octet)
    {
        if (remaining() < 1) {
            grow();
        }
        byteBuffer.put(octet);
    }


    /** Writes an array of bytes to the buffer expanding if necessary. */
    public void writeBytes(final byte[] bytes, final int off, final int len)
    {
        while (remaining() < len) {
            // TODO: Enforce a maximum size
            grow();
        }
        byteBuffer.put(bytes, off, len);
    }

    /**
     * Shifts the last `length` bytes in the buffer to the left. This can be used when a value's header was
     * preallocated but the value's encoded size proved to be much smaller than anticipated.
     *
     * The caller must guarantee that the buffer contains enough bytes to perform the requested shift.
     *
     * @param length    The number of bytes at the end of the buffer that we'll be shifting to the left.
     * @param shiftBy   The number of bytes to the left that we'll be shifting.
     */
    public void shiftBytesLeft(int length, int shiftBy) {
        if (byteBuffer.position() < length + shiftBy) {
            throw new IndexOutOfBoundsException(
                    "Cannot shift "
                        + length
                        + " bytes to the left by "
                        + shiftBy
                        + "; buffer only contains "
                        + byteBuffer.position()
                        + " bytes."
            );
        }

        int startOfSliceToShift = byteBuffer.position() - length;
        System.arraycopy(
                byteBuffer.array(),
                startOfSliceToShift,
                byteBuffer.array(),
                startOfSliceToShift - shiftBy,
                length
        );
        int newEnd = byteBuffer.position() - shiftBy;
        byteBuffer.position(newEnd);
    }

    /** Writes an array of bytes to the buffer expanding if necessary, defaulting to the entire array. */
    public void writeBytes(byte[] bytes)
    {
        writeBytes(bytes, 0, bytes.length);
    }

    // unsigned fixed integer writes -- does not check sign/bounds

    private static final int UINT_2_OCTET_SHIFT = 8 * 1;
    private static final int UINT_3_OCTET_SHIFT = 8 * 2;
    private static final int UINT_4_OCTET_SHIFT = 8 * 3;
    private static final int UINT_5_OCTET_SHIFT = 8 * 4;
    private static final int UINT_6_OCTET_SHIFT = 8 * 5;
    private static final int UINT_7_OCTET_SHIFT = 8 * 6;
    private static final int UINT_8_OCTET_SHIFT = 8 * 7;


    public void writeUInt8(long value)
    {
        writeByte((byte) value);
    }

    public void writeUInt16(long value)
    {
        if (remaining() < 2) {
            grow();
        }
        byteBuffer.put((byte) (value >> UINT_2_OCTET_SHIFT));
        byteBuffer.put((byte) (value));
    }

    public void writeUInt24(long value)
    {
        if (remaining() < 3) {
            grow();
        }

        byteBuffer.put((byte) (value >> UINT_3_OCTET_SHIFT));
        byteBuffer.put((byte) (value >> UINT_2_OCTET_SHIFT));
        byteBuffer.put((byte) (value));
    }

    public void writeUInt32(long value)
    {
        if (remaining() < 4) {
            grow();
        }

        byteBuffer.put((byte) (value >> UINT_4_OCTET_SHIFT));
        byteBuffer.put((byte) (value >> UINT_3_OCTET_SHIFT));
        byteBuffer.put((byte) (value >> UINT_2_OCTET_SHIFT));
        byteBuffer.put((byte) (value));
    }

    public void writeUInt40(long value)
    {
        if (remaining() < 5) {
            grow();
        }

        byteBuffer.put((byte) (value >> UINT_5_OCTET_SHIFT));
        byteBuffer.put((byte) (value >> UINT_4_OCTET_SHIFT));
        byteBuffer.put((byte) (value >> UINT_3_OCTET_SHIFT));
        byteBuffer.put((byte) (value >> UINT_2_OCTET_SHIFT));
        byteBuffer.put((byte) (value));
    }

    public void writeUInt48(long value)
    {
        if (remaining() < 6) {
            grow();
        }

        byteBuffer.put((byte) (value >> UINT_6_OCTET_SHIFT));
        byteBuffer.put((byte) (value >> UINT_5_OCTET_SHIFT));
        byteBuffer.put((byte) (value >> UINT_4_OCTET_SHIFT));
        byteBuffer.put((byte) (value >> UINT_3_OCTET_SHIFT));
        byteBuffer.put((byte) (value >> UINT_2_OCTET_SHIFT));
        byteBuffer.put((byte) (value));
    }

    public void writeUInt56(long value)
    {
        if (remaining() < 7) {
            grow();
        }

        byteBuffer.put((byte) (value >> UINT_7_OCTET_SHIFT));
        byteBuffer.put((byte) (value >> UINT_6_OCTET_SHIFT));
        byteBuffer.put((byte) (value >> UINT_5_OCTET_SHIFT));
        byteBuffer.put((byte) (value >> UINT_4_OCTET_SHIFT));
        byteBuffer.put((byte) (value >> UINT_3_OCTET_SHIFT));
        byteBuffer.put((byte) (value >> UINT_2_OCTET_SHIFT));
        byteBuffer.put((byte) (value));
    }

    public void writeUInt64(long value)
    {
        if (remaining() < 8) {
            grow();
        }

        byteBuffer.put((byte) (value >> UINT_8_OCTET_SHIFT));
        byteBuffer.put((byte) (value >> UINT_7_OCTET_SHIFT));
        byteBuffer.put((byte) (value >> UINT_6_OCTET_SHIFT));
        byteBuffer.put((byte) (value >> UINT_5_OCTET_SHIFT));
        byteBuffer.put((byte) (value >> UINT_4_OCTET_SHIFT));
        byteBuffer.put((byte) (value >> UINT_3_OCTET_SHIFT));
        byteBuffer.put((byte) (value >> UINT_2_OCTET_SHIFT));
        byteBuffer.put((byte) (value));
    }

    // signed fixed integer writes - does not check bounds (especially important for IntX.MIN_VALUE).

    private static final long INT8_SIGN_MASK  = 1L << ((8 * 1) - 1);
    private static final long INT16_SIGN_MASK = 1L << ((8 * 2) - 1);
    private static final long INT24_SIGN_MASK = 1L << ((8 * 3) - 1);
    private static final long INT32_SIGN_MASK = 1L << ((8 * 4) - 1);
    private static final long INT40_SIGN_MASK = 1L << ((8 * 5) - 1);
    private static final long INT48_SIGN_MASK = 1L << ((8 * 6) - 1);
    private static final long INT56_SIGN_MASK = 1L << ((8 * 7) - 1);
    private static final long INT64_SIGN_MASK = 1L << ((8 * 8) - 1);

    public void writeInt8(long value)
    {
        if (value < 0)
        {
            value = (-value) | INT8_SIGN_MASK;
        }
        writeUInt8(value);
    }


    public void writeInt16(long value)
    {
        if (value < 0)
        {
            value = (-value) | INT16_SIGN_MASK;
        }
        writeUInt16(value);
    }

    public void writeInt24(long value)
    {
        if (value < 0)
        {
            value = (-value) | INT24_SIGN_MASK;
        }
        writeUInt24(value);
    }


    public void writeInt32(long value)
    {
        if (value < 0)
        {
            value = (-value) | INT32_SIGN_MASK;
        }
        writeUInt32(value);
    }


    public void writeInt40(long value)
    {
        if (value < 0)
        {
            value = (-value) | INT40_SIGN_MASK;
        }
        writeUInt40(value);
    }


    public void writeInt48(long value)
    {
        if (value < 0)
        {
            value = (-value) | INT48_SIGN_MASK;
        }
        writeUInt48(value);
    }


    public void writeInt56(long value)
    {
        if (value < 0)
        {
            value = (-value) | INT56_SIGN_MASK;
        }
        writeUInt56(value);
    }


    public void writeInt64(long value)
    {
        if (value < 0)
        {
            value = (-value) | INT64_SIGN_MASK;
        }
        writeUInt64(value);
    }

    // variable length integer writing

    private static final long VAR_INT_BITS_PER_OCTET = 7;
    private static final long LOWER_SEVEN_BITS_MASK = 0x7F;

    private static final long VAR_UINT_9_OCTET_SHIFT = (8 * VAR_INT_BITS_PER_OCTET);
    private static final long VAR_UINT_9_OCTET_MIN_VALUE = (1L << VAR_UINT_9_OCTET_SHIFT);

    private static final long VAR_UINT_8_OCTET_SHIFT = (7 * VAR_INT_BITS_PER_OCTET);
    private static final long VAR_UINT_8_OCTET_MIN_VALUE = (1L << VAR_UINT_8_OCTET_SHIFT);

    private static final long VAR_UINT_7_OCTET_SHIFT = (6 * VAR_INT_BITS_PER_OCTET);
    private static final long VAR_UINT_7_OCTET_MIN_VALUE = (1L << VAR_UINT_7_OCTET_SHIFT);

    private static final long VAR_UINT_6_OCTET_SHIFT = (5 * VAR_INT_BITS_PER_OCTET);
    private static final long VAR_UINT_6_OCTET_MIN_VALUE = (1L << VAR_UINT_6_OCTET_SHIFT);

    private static final long VAR_UINT_5_OCTET_SHIFT = (4 * VAR_INT_BITS_PER_OCTET);
    private static final long VAR_UINT_5_OCTET_MIN_VALUE = (1L << VAR_UINT_5_OCTET_SHIFT);

    private static final long VAR_UINT_4_OCTET_SHIFT = (3 * VAR_INT_BITS_PER_OCTET);
    private static final long VAR_UINT_4_OCTET_MIN_VALUE = (1L << VAR_UINT_4_OCTET_SHIFT);

    private static final long VAR_UINT_3_OCTET_SHIFT = (2 * VAR_INT_BITS_PER_OCTET);
    private static final long VAR_UINT_3_OCTET_MIN_VALUE = (1L << VAR_UINT_3_OCTET_SHIFT);

    private static final long VAR_UINT_2_OCTET_SHIFT = (1 * VAR_INT_BITS_PER_OCTET);
    private static final long VAR_UINT_2_OCTET_MIN_VALUE = (1L << VAR_UINT_2_OCTET_SHIFT);

    private static final long VAR_INT_FINAL_OCTET_SIGNAL_MASK = 0x80;

    // Caller guarantees value is less than VAR_UINT_4_OCTET_MIN_VALUE
    public int writeSmallVarUInt(final long value) {
        if (remaining() < 3) { // todo: constant?
            grow();
        }

        int startPosition = position();
        if (value >= VAR_UINT_3_OCTET_MIN_VALUE)
        {
            intBuffer[0] = (byte) ((value >> VAR_UINT_3_OCTET_SHIFT) & LOWER_SEVEN_BITS_MASK);
            intBuffer[1] = (byte) ((value >> VAR_UINT_2_OCTET_SHIFT) & LOWER_SEVEN_BITS_MASK);
            intBuffer[2] = (byte) ((value & LOWER_SEVEN_BITS_MASK) | VAR_INT_FINAL_OCTET_SIGNAL_MASK);
            byteBuffer.put(intBuffer, 0, 3);
            return 3;
//            byteBuffer.put((byte) ((value >> VAR_UINT_3_OCTET_SHIFT) & LOWER_SEVEN_BITS_MASK));
//            byteBuffer.put((byte) ((value >> VAR_UINT_2_OCTET_SHIFT) & LOWER_SEVEN_BITS_MASK));
//            byteBuffer.put((byte) ((value & LOWER_SEVEN_BITS_MASK) | VAR_INT_FINAL_OCTET_SIGNAL_MASK));
//            return position() - startPosition;
        }

        if (value >= VAR_UINT_2_OCTET_MIN_VALUE)
        {
            intBuffer[0] = (byte) ((value >> VAR_UINT_2_OCTET_SHIFT) & LOWER_SEVEN_BITS_MASK);
            intBuffer[1] = (byte) ((value & LOWER_SEVEN_BITS_MASK) | VAR_INT_FINAL_OCTET_SIGNAL_MASK);
            byteBuffer.put(intBuffer, 0, 2);
            return 2;
//            byteBuffer.put((byte) ((value >> VAR_UINT_2_OCTET_SHIFT) & LOWER_SEVEN_BITS_MASK));
//            byteBuffer.put((byte) ((value & LOWER_SEVEN_BITS_MASK) | VAR_INT_FINAL_OCTET_SIGNAL_MASK));
//            return position() - startPosition;
        }

        byteBuffer.put((byte) ((value & LOWER_SEVEN_BITS_MASK) | VAR_INT_FINAL_OCTET_SIGNAL_MASK));
        return position() - startPosition;
    }

    public int writeVarUInt(final long value)
    {
        if (value < VAR_UINT_4_OCTET_MIN_VALUE) {
            return writeSmallVarUInt(value);
        }

        if (remaining() < 9) { // todo: constant?
            grow();
        }

        int startPosition = position();
        if (value >= VAR_UINT_9_OCTET_MIN_VALUE)
        {
            byteBuffer.put((byte) ((value >> VAR_UINT_9_OCTET_SHIFT) & LOWER_SEVEN_BITS_MASK));
        }
        if (value >= VAR_UINT_8_OCTET_MIN_VALUE)
        {
            byteBuffer.put((byte) ((value >> VAR_UINT_8_OCTET_SHIFT) & LOWER_SEVEN_BITS_MASK));
        }
        if (value >= VAR_UINT_7_OCTET_MIN_VALUE)
        {
            byteBuffer.put((byte) ((value >> VAR_UINT_7_OCTET_SHIFT) & LOWER_SEVEN_BITS_MASK));
        }
        if (value >= VAR_UINT_6_OCTET_MIN_VALUE)
        {
            byteBuffer.put((byte) ((value >> VAR_UINT_6_OCTET_SHIFT) & LOWER_SEVEN_BITS_MASK));
        }
        if (value >= VAR_UINT_5_OCTET_MIN_VALUE)
        {
            byteBuffer.put((byte) ((value >> VAR_UINT_5_OCTET_SHIFT) & LOWER_SEVEN_BITS_MASK));
        }
        if (value >= VAR_UINT_4_OCTET_MIN_VALUE)
        {
            byteBuffer.put((byte) ((value >> VAR_UINT_4_OCTET_SHIFT) & LOWER_SEVEN_BITS_MASK));
        }
        if (value >= VAR_UINT_3_OCTET_MIN_VALUE)
        {
            byteBuffer.put((byte) ((value >> VAR_UINT_3_OCTET_SHIFT) & LOWER_SEVEN_BITS_MASK));
        }
        if (value >= VAR_UINT_2_OCTET_MIN_VALUE)
        {
            byteBuffer.put((byte) ((value >> VAR_UINT_2_OCTET_SHIFT) & LOWER_SEVEN_BITS_MASK));
        }
        byteBuffer.put((byte) ((value & LOWER_SEVEN_BITS_MASK) | VAR_INT_FINAL_OCTET_SIGNAL_MASK));
        return position() - startPosition;
    }

//    public int slow_writeVarUInt(final long value)
//    {
//        // This check is conservative in that it could trigger growth even if a smaller value could still fit.
//        // However, doing a single bounds check for the entire method is nice and simple.
//        if (remaining() < MAX_VAR_UINT_LENGTH) {
//            grow();
//        }
//
//        if (value == 0) {
//            byteBuffer.put((byte) VAR_INT_FINAL_OCTET_SIGNAL_MASK);
//            return 1;
//        }
//
//        // Escape analysis should notice that this byte array 1) is small and 2) doesn't survive the method. That will
//        // allow the JVM to convert it to a stack allocation.
//        final byte[] encodedBytes = varUIntBuffer;
//        Arrays.fill(encodedBytes, (byte) 0);
//
//        // Set the `end` flag bit of the final byte to 1
//        encodedBytes[encodedBytes.length - 1] = (byte) VAR_INT_FINAL_OCTET_SIGNAL_MASK;
//
//        // We'll be writing to the end of the buffer as we go and working backwards. Keep track of the index of the
//        // left-most populated byte in the buffer.
//        int firstByte;
//        // A copy of `value` that we'll consume in the process of encoding.
//        long magnitude = value;
//        for (firstByte = encodedBytes.length - 1; firstByte >= 0; firstByte--) {
//            encodedBytes[firstByte] |= (byte) (magnitude & LOWER_SEVEN_BITS_MASK);
//            magnitude >>= BITS_PER_VAR_UINT_BYTE;
//            if (magnitude == 0) {
//                break;
//            }
//        }

//        int encodedLength = encodedBytes.length - firstByte;
//        if (remaining() < encodedLength) {
//            int remaining = remaining();
//            System.out.println("oh no!");
//            System.out.println("encoded length: " + encodedLength);
//            System.out.println("encoded bytes : " + Arrays.toString(encodedBytes));
//            System.out.println("remaining     : " + remaining);
//        }
//        byteBuffer.put(encodedBytes, firstByte, encodedLength);
//
//        return encodedLength;
//    }

    private static final long VAR_INT_SIGNED_OCTET_MASK = 0x3F;
    private static final long VAR_INT_SIGNBIT_ON_MASK   = 0x40L;
    private static final long VAR_INT_SIGNBIT_OFF_MASK  = 0x00L;

    // note that the highest order bit for signed 64-bit values cannot fit in 9 bytes with the sign
    private static final long VAR_INT_10_OCTET_SHIFT = 62;

    private static final long VAR_INT_10_OCTET_MIN_VALUE = (1L << VAR_INT_10_OCTET_SHIFT);
    private static final long VAR_INT_9_OCTET_MIN_VALUE  = (VAR_UINT_9_OCTET_MIN_VALUE >> 1);
    private static final long VAR_INT_8_OCTET_MIN_VALUE  = (VAR_UINT_8_OCTET_MIN_VALUE >> 1);
    private static final long VAR_INT_7_OCTET_MIN_VALUE  = (VAR_UINT_7_OCTET_MIN_VALUE >> 1);
    private static final long VAR_INT_6_OCTET_MIN_VALUE  = (VAR_UINT_6_OCTET_MIN_VALUE >> 1);
    private static final long VAR_INT_5_OCTET_MIN_VALUE  = (VAR_UINT_5_OCTET_MIN_VALUE >> 1);
    private static final long VAR_INT_4_OCTET_MIN_VALUE  = (VAR_UINT_4_OCTET_MIN_VALUE >> 1);
    private static final long VAR_INT_3_OCTET_MIN_VALUE  = (VAR_UINT_3_OCTET_MIN_VALUE >> 1);
    private static final long VAR_INT_2_OCTET_MIN_VALUE  = (VAR_UINT_2_OCTET_MIN_VALUE >> 1);

    private static final long VAR_INT_BITS_PER_SIGNED_OCTET = 6;
    private static final long VAR_SINT_2_OCTET_SHIFT = VAR_INT_BITS_PER_SIGNED_OCTET + (1 * VAR_INT_BITS_PER_OCTET);
    private static final long VAR_SINT_3_OCTET_SHIFT = VAR_INT_BITS_PER_SIGNED_OCTET + (2 * VAR_INT_BITS_PER_OCTET);
    private static final long VAR_SINT_4_OCTET_SHIFT = VAR_INT_BITS_PER_SIGNED_OCTET + (3 * VAR_INT_BITS_PER_OCTET);
    private static final long VAR_SINT_5_OCTET_SHIFT = VAR_INT_BITS_PER_SIGNED_OCTET + (4 * VAR_INT_BITS_PER_OCTET);

    private void writeVarIntByte(int sizeSoFar, long signMask, long magnitude, long octetShift) {
        final long bits = (magnitude >> octetShift);
        byte next;
        if (sizeSoFar == 1) {
            next = (byte) ((bits & VAR_INT_SIGNED_OCTET_MASK) | signMask);
        } else {
            next = (byte) (bits & LOWER_SEVEN_BITS_MASK);
        }
        byteBuffer.put(next);
    }
    public int writeVarInt(final long value)
    {
        if (remaining() < MAX_VAR_INT_LENGTH) {
            grow();
        }

        long magnitude = Math.abs(value);
        final long signMask = value < 0 ? VAR_INT_SIGNBIT_ON_MASK : VAR_INT_SIGNBIT_OFF_MASK;
        int size = 1;

        if (magnitude >= VAR_INT_10_OCTET_MIN_VALUE)
        {
            byte next = (byte) (((magnitude >> VAR_INT_10_OCTET_SHIFT) & VAR_INT_SIGNED_OCTET_MASK) | signMask);
            byteBuffer.put(next);
            size++;
        }
        if (magnitude >= VAR_INT_9_OCTET_MIN_VALUE)
        {
            writeVarIntByte(size, signMask, magnitude, VAR_UINT_9_OCTET_SHIFT);
            size++;
        }
        if (magnitude >= VAR_INT_8_OCTET_MIN_VALUE)
        {
            writeVarIntByte(size, signMask, magnitude, VAR_UINT_8_OCTET_SHIFT);
            size++;
        }
        if (magnitude >= VAR_INT_7_OCTET_MIN_VALUE)
        {
            writeVarIntByte(size, signMask, magnitude, VAR_UINT_7_OCTET_SHIFT);
            size++;
        }
        if (magnitude >= VAR_INT_6_OCTET_MIN_VALUE)
        {
            writeVarIntByte(size, signMask, magnitude, VAR_UINT_6_OCTET_SHIFT);
            size++;
        }
        if (magnitude >= VAR_INT_5_OCTET_MIN_VALUE)
        {
            writeVarIntByte(size, signMask, magnitude, VAR_UINT_5_OCTET_SHIFT);
            size++;
        }
        if (magnitude >= VAR_INT_4_OCTET_MIN_VALUE)
        {
            writeVarIntByte(size, signMask, magnitude, VAR_UINT_4_OCTET_SHIFT);
            size++;
        }
        if (magnitude >= VAR_INT_3_OCTET_MIN_VALUE)
        {
            writeVarIntByte(size, signMask, magnitude, VAR_UINT_3_OCTET_SHIFT);
            size++;
        }
        if (magnitude >= VAR_INT_2_OCTET_MIN_VALUE)
        {
            writeVarIntByte(size, signMask, magnitude, VAR_UINT_2_OCTET_SHIFT);
            size++;
        }
        byte lastByte;
        if (size == 1) {
            lastByte = (byte) ((magnitude & VAR_INT_SIGNED_OCTET_MASK) | signMask);
        } else {
            lastByte = (byte) (magnitude & LOWER_SEVEN_BITS_MASK);
        }
        lastByte |= VAR_INT_FINAL_OCTET_SIGNAL_MASK;
        byteBuffer.put(lastByte);

        return size;
    }

//    public int slow_writeVarInt(long value)
//    {
//        assert value != Long.MIN_VALUE;
//
//        if (remaining() < MAX_VAR_INT_LENGTH) {
//            grow();
//        }
//
//        if (value == 0) {
//            byteBuffer.put((byte) VAR_INT_FINAL_OCTET_SIGNAL_MASK);
//            return 1;
//        }
//
//        // Escape analysis should notice that this byte array 1) is small and 2) doesn't survive the method. That will
//        // allow the JVM to convert it to a stack allocation.
//        final byte[] encodedBytes = varIntBuffer;
//        Arrays.fill(encodedBytes, (byte) 0);
//        // Set the `end` flag bit of the final byte to 1
//        encodedBytes[encodedBytes.length - 1] = (byte) VAR_INT_FINAL_OCTET_SIGNAL_MASK;
//
//        // Create a copy of `value` that we can consume during encoding. We'll encode the sign separately; we only
//        // need the magnitude.
//        long magnitude = Math.abs(value);
//        int occupiedBits = 64 /* TODO: Const */ - Long.numberOfLeadingZeros(magnitude);
//        int bytesRequired = 1;
//        int remainingBits = Math.max(occupiedBits - 6 /*Mag bits in final byte*/, 0);
//        bytesRequired += (int) Math.ceil(remainingBits / 7.0);
//
//        int bytesRemaining = bytesRequired;
//        int firstByte = encodedBytes.length - bytesRequired;
//        for (int byteIndex = encodedBytes.length - 1; byteIndex >= firstByte; byteIndex--) {
//            bytesRemaining -= 1;
//            if (bytesRemaining > 0) {
//                encodedBytes[byteIndex] |= (byte) (magnitude & LOWER_SEVEN_BITS_MASK);
//                magnitude >>>= 7;
//            } else {
//                encodedBytes[byteIndex] |= (byte) (magnitude & 0x3f); // Lower 6
//                // If the value we're encoding is negative, flip the sign bit in the leftmost
//                // encoded byte.
//                if (value < 0) {
//                    encodedBytes[byteIndex] |= 0x40;
//                }
//            }
//        }
//
//        byteBuffer.put(encodedBytes, firstByte, bytesRequired);
//        return bytesRequired;
//    }

    // write variable integer of specific size at a specified position -- no bounds checking, will not expand the buffer

    public void writeVarUIntDirect1At(final long position, final long value)
    {
        writeUInt8At(position, (value & LOWER_SEVEN_BITS_MASK) | VAR_INT_FINAL_OCTET_SIGNAL_MASK);
    }

    public void writeVarUIntDirect2At(long position, long value)
    {
        if (byteBuffer.remaining() < 2) {
            grow();
        }

        byteBuffer.put((int) position, (byte)((value >> VAR_UINT_2_OCTET_SHIFT) & LOWER_SEVEN_BITS_MASK));
        byteBuffer.put((int) position + 1, (byte)((value & LOWER_SEVEN_BITS_MASK) | VAR_INT_FINAL_OCTET_SIGNAL_MASK));
    }

    public void writeUInt8At(final long position, final long value)
    {
        byteBuffer.put((int) position, (byte) value);
    }

    /** Write the entire buffer to output stream. */
    public void writeTo(final OutputStream out) throws IOException
    {
        out.write(byteBuffer.array(), 0, byteBuffer.position());
    }

    /** Write a specific segment of data from the buffer to a stream. */
    public void writeTo(final OutputStream out, long position, long length) throws IOException
    {
        out.write(byteBuffer.array(), (int) position, (int) length);
    }
}
