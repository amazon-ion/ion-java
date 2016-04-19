/*
 * Copyright 2015-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.ion.impl.bin;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * A facade over {@link Block} management and low-level Ion encoding concerns for the {@link IonRawBinaryWriter}.
 */
/*package*/ final class WriteBuffer implements Closeable
{
    private final BlockAllocator allocator;
    private final List<Block> blocks;
    private Block current;
    private int index;

    public WriteBuffer(final BlockAllocator allocator)
    {
        this.allocator = allocator;
        this.blocks = new ArrayList<Block>();

        // initial seed of the first block
        allocateNewBlock();

        this.index = 0;
        this.current = blocks.get(0);
    }

    private void allocateNewBlock()
    {
        blocks.add(allocator.allocateBlock());
    }

    /** Returns the block index for the given position. */
    private int index(final long position)
    {
        return (int) (position / allocator.getBlockSize());
    }

    /** Returns the offset within the block for a given position. */
    private int offset(final long position)
    {
        return (int) (position % allocator.getBlockSize());
    }

    /** Resets the write buffer to empty. */
    public void reset()
    {
        close();
        allocateNewBlock();
        index = 0;
        current = blocks.get(index);
    }

    public void close()
    {
        // free all the blocks
        for (final Block block : blocks)
        {
            block.close();
        }
        blocks.clear();

        // note--we don't explicitly flag that we're closed for efficiency
    }

    /** Resets the write buffer to a particular point. */
    public void truncate(final long position)
    {
        final int index = index(position);
        final int offset = offset(position);
        final Block block = blocks.get(index);
        this.index = index;
        block.limit = offset;
        current = block;
    }

    /** Returns the amount of capacity left in the current block. */
    public int remaining()
    {
        return current.remaining();
    }

    /** Returns the logical position in the current block. */
    public long position()
    {
        return (((long) index) * allocator.getBlockSize()) + current.limit;
    }

    private static final int OCTET_MASK = 0xFF;

    /** Returns the octet at the logical position given. */
    public int getUInt8At(final long position)
    {
        final int index = index(position);
        final int offset = offset(position);
        final Block block = blocks.get(index);
        return block.data[offset] & OCTET_MASK;
    }

    /** Writes a single octet to the buffer, expanding if necessary. */
    public void writeByte(final byte octet)
    {
        if (remaining() < 1)
        {
            if (index == blocks.size() - 1)
            {
                allocateNewBlock();
            }
            index++;
            current = blocks.get(index);
        }
        final Block block = current;
        block.data[block.limit] = octet;
        block.limit++;
    }

    // slow in the sense that we do all kind of block boundary checking
    private void writeBytesSlow(final byte[] bytes, int off, int len)
    {
        while (len > 0)
        {
            final Block block = current;
            final int amount = Math.min(len, block.remaining());
            System.arraycopy(bytes, off, block.data, block.limit, amount);
            block.limit += amount;
            off += amount;
            len -= amount;
            if (block.remaining() == 0)
            {
                if (index == blocks.size() - 1)
                {
                    allocateNewBlock();
                }
                index++;
                current = blocks.get(index);
            }
        }

    }

    /** Writes an array of bytes to the buffer expanding if necessary. */
    public void writeBytes(final byte[] bytes, final int off, final int len)
    {
        if (len > remaining())
        {
            writeBytesSlow(bytes, off, len);
            return;
        }

        final Block block = current;
        System.arraycopy(bytes, off, block.data, block.limit, len);
        block.limit += len;
    }

    /** Writes an array of bytes to the buffer expanding if necessary, defaulting to the entire array. */
    public void writeBytes(byte[] bytes)
    {
        writeBytes(bytes, 0, bytes.length);
    }

    // UTF-8 character writing

    private static final char HIGH_SURROGATE_FIRST      = 0xD800;
    private static final char HIGH_SURROGATE_LAST       = 0xDBFF;
    private static final char LOW_SURROGATE_FIRST       = 0xDC00;
    private static final char LOW_SURROGATE_LAST        = 0xDFFF;
    private static final int  SURROGATE_BASE            = 0x10000;
    private static final int  BITS_PER_SURROGATE        = 10;

    private static final int  UTF8_FOLLOW_MASK          = 0x3F;

    private static final int  UTF8_FOLLOW_PREFIX_MASK   = 0x80;
    private static final int  UTF8_2_OCTET_PREFIX_MASK  = 0xC0;
    private static final int  UTF8_3_OCTET_PREFIX_MASK  = 0xE0;
    private static final int  UTF8_4_OCTET_PREFIX_MASK  = 0xF0;

    private static final int  UTF8_BITS_PER_FOLLOW_OCTET = 6;
    private static final int  UTF8_2_OCTET_SHIFT         = 1 * UTF8_BITS_PER_FOLLOW_OCTET;
    private static final int  UTF8_3_OCTET_SHIFT         = 2 * UTF8_BITS_PER_FOLLOW_OCTET;
    private static final int  UTF8_4_OCTET_SHIFT         = 3 * UTF8_BITS_PER_FOLLOW_OCTET;

    private static final int UTF8_2_OCTET_MIN_VALUE = 1 << 7;
    private static final int UTF8_3_OCTET_MIN_VALUE = 1 << (5 + (1 * UTF8_BITS_PER_FOLLOW_OCTET));


    // slow in the sense that we deal with any kind of UTF-8 sequence and block boundaries
    private int writeUTF8Slow(final CharSequence chars, int off, int len)
    {
        int octets = 0;
        while (len > 0)
        {
            final char ch = chars.charAt(off);
            if (ch >= LOW_SURROGATE_FIRST && ch <= LOW_SURROGATE_LAST)
            {
                throw new IllegalArgumentException("Unpaired low surrogate: " + (int) ch);
            }
            if ((ch >= HIGH_SURROGATE_FIRST && ch <= HIGH_SURROGATE_LAST))
            {
                // we need to look ahead in this case
                off++;
                len--;
                if (len == 0)
                {
                    throw new IllegalArgumentException("Unpaired low surrogate at end of character sequence: " + ch);
                }

                final int ch2 = chars.charAt(off);
                if (ch2 < LOW_SURROGATE_FIRST || ch2 > LOW_SURROGATE_LAST)
                {
                    throw new IllegalArgumentException("Low surrogate with unpaired high surrogate: " + ch + " + " + ch2);
                }

                // at this point we have a high and low surrogate
                final int codepoint = (((ch - HIGH_SURROGATE_FIRST) << BITS_PER_SURROGATE) | (ch2 - LOW_SURROGATE_FIRST)) + SURROGATE_BASE;
                writeByte((byte) (UTF8_4_OCTET_PREFIX_MASK | ( codepoint >> UTF8_4_OCTET_SHIFT)                    ));
                writeByte((byte) (UTF8_FOLLOW_PREFIX_MASK  | ((codepoint >> UTF8_3_OCTET_SHIFT) & UTF8_FOLLOW_MASK)));
                writeByte((byte) (UTF8_FOLLOW_PREFIX_MASK  | ((codepoint >> UTF8_2_OCTET_SHIFT) & UTF8_FOLLOW_MASK)));
                writeByte((byte) (UTF8_FOLLOW_PREFIX_MASK  | ( codepoint                        & UTF8_FOLLOW_MASK)));

                octets += 4;
            }
            else if (ch < UTF8_2_OCTET_MIN_VALUE)
            {
                writeByte((byte) ch);
                octets++;
            }
            else if (ch < UTF8_3_OCTET_MIN_VALUE)
            {
                writeByte((byte) (UTF8_2_OCTET_PREFIX_MASK | (ch >> UTF8_2_OCTET_SHIFT)                    ));
                writeByte((byte) (UTF8_FOLLOW_PREFIX_MASK  | (ch                        & UTF8_FOLLOW_MASK)));
                octets += 2;
            }
            else
            {
                writeByte((byte) (UTF8_3_OCTET_PREFIX_MASK | ( ch >> UTF8_3_OCTET_SHIFT)                    ));
                writeByte((byte) (UTF8_FOLLOW_PREFIX_MASK  | ((ch >> UTF8_2_OCTET_SHIFT) & UTF8_FOLLOW_MASK)));
                writeByte((byte) (UTF8_FOLLOW_PREFIX_MASK  | ( ch                        & UTF8_FOLLOW_MASK)));
                octets += 3;
            }
            off++;
            len--;
        }
        return octets;
    }

    private int writeUTF8UpTo3Byte(final CharSequence chars, int off, int len)
    {
        // fast path if we fit in the block assuming optimistically for all three-byte
        if ((len * 3) > remaining())
        {
            return writeUTF8Slow(chars, off, len);
        }

        final Block block = current;
        int limit = block.limit;
        int octets = 0;
        while (len > 0)
        {
            final char ch = chars.charAt(off);
            if (ch >= LOW_SURROGATE_FIRST && ch <= LOW_SURROGATE_LAST)
            {
                throw new IllegalArgumentException("Unpaired low surrogate: " + ch);
            }
            if ((ch >= HIGH_SURROGATE_FIRST && ch <= HIGH_SURROGATE_LAST))
            {
                // we lost the 3-byte bet
                break;
            }

            if (ch < UTF8_2_OCTET_MIN_VALUE)
            {
                block.data[limit++] = (byte) ch;
                octets++;
            }
            else if (ch < UTF8_3_OCTET_MIN_VALUE)
            {
                block.data[limit++] = (byte) (UTF8_2_OCTET_PREFIX_MASK | (ch >> UTF8_2_OCTET_SHIFT)                    );
                block.data[limit++] = (byte) (UTF8_FOLLOW_PREFIX_MASK  | (ch                        & UTF8_FOLLOW_MASK));
                octets += 2;
            }
            else
            {
                block.data[limit++] = (byte) (UTF8_3_OCTET_PREFIX_MASK | ( ch >> UTF8_3_OCTET_SHIFT)                    );
                block.data[limit++] = (byte) (UTF8_FOLLOW_PREFIX_MASK  | ((ch >> UTF8_2_OCTET_SHIFT) & UTF8_FOLLOW_MASK));
                block.data[limit++] = (byte) (UTF8_FOLLOW_PREFIX_MASK  | ( ch                        & UTF8_FOLLOW_MASK));
                octets += 3;
            }
            off++;
            len--;
        }
        block.limit = limit;

        if (len > 0)
        {
            // just defer to 'slow' writing for non-BMP characters
            return octets + writeUTF8Slow(chars, off, len);
        }
        return octets;
    }

    private int writeUTF8UpTo2Byte(final CharSequence chars, int off, int len)
    {
        // fast path if we fit in the block assuming optimistically for all two-byte
        if ((len * 2) > remaining())
        {
            return writeUTF8Slow(chars, off, len);
        }

        final Block block = current;
        int limit = block.limit;
        char ch = '\0';
        int octets = 0;
        while (len > 0)
        {
            ch = chars.charAt(off);
            if (ch >= UTF8_3_OCTET_MIN_VALUE)
            {
                // we lost the 2-byte bet
                break;
            }

            if (ch < UTF8_2_OCTET_MIN_VALUE)
            {
                block.data[limit++] = (byte) ch;
                octets++;
            }
            else
            {
                block.data[limit++] = (byte) (UTF8_2_OCTET_PREFIX_MASK | (ch >> UTF8_2_OCTET_SHIFT)                    );
                block.data[limit++] = (byte) (UTF8_FOLLOW_PREFIX_MASK  | (ch                        & UTF8_FOLLOW_MASK));
                octets += 2;
            }
            off++;
            len--;
        }
        block.limit = limit;

        if (len > 0)
        {
            if (ch >= LOW_SURROGATE_FIRST && ch <= LOW_SURROGATE_LAST)
            {
                throw new IllegalArgumentException("Unpaired low surrogate: " + ch);
            }
            if (ch >= HIGH_SURROGATE_FIRST && ch <= HIGH_SURROGATE_LAST)
            {
                // just defer to 'slow' writing for non-BMP characters
                return octets + writeUTF8Slow(chars, off, len);
            }

            // we must be a three byte BMP character
            return octets + writeUTF8UpTo3Byte(chars, off, len);
        }
        return octets;
    }

    /** Returns the number of octets written. */
    public int writeUTF8(final CharSequence chars, int off, int len)
    {
        // fast path if we fit in the block assuming optimistically for all ASCII
        if (len > remaining())
        {
            return writeUTF8Slow(chars, off, len);
        }
        final Block block = current;
        int limit = block.limit;
        char ch = '\0';
        int octets = 0;
        while (len > 0)
        {
            ch = chars.charAt(off);
            if (ch >= UTF8_2_OCTET_MIN_VALUE)
            {
                // we lost the ASCII bet
                break;
            }

            block.data[limit++] = (byte) ch;
            octets++;
            off++;
            len--;
        }
        block.limit = limit;

        if (len > 0)
        {
            if (ch < UTF8_3_OCTET_MIN_VALUE)
            {
                return octets + writeUTF8UpTo2Byte(chars, off, len);
            }
            if (ch >= LOW_SURROGATE_FIRST && ch <= LOW_SURROGATE_LAST)
            {
                throw new IllegalArgumentException("Unpaired low surrogate: " + ch);
            }
            if (ch >= HIGH_SURROGATE_FIRST && ch <= HIGH_SURROGATE_LAST)
            {
                // just defer to 'slow' writing for non-BMP characters
                return octets + writeUTF8Slow(chars, off, len);
            }

            // we must be a three byte BMP character
            return octets + writeUTF8UpTo3Byte(chars, off, len);
        }
        return octets;
    }

    /** Returns the number of octets written. */
    public int writeUTF8(final CharSequence chars)
    {
        return writeUTF8(chars, 0, chars.length());
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

    private void writeUInt16Slow(long value)
    {
        writeByte((byte) (value >> UINT_2_OCTET_SHIFT));
        writeByte((byte) (value                      ));
    }

    public void writeUInt16(long value)
    {
        if (remaining() < 2)
        {
            writeUInt16Slow(value);
            return;
        }

        final Block block = current;
        final byte[] data = block.data;
        int limit = block.limit;
        data[limit++] = (byte) (value >> UINT_2_OCTET_SHIFT);
        data[limit++] = (byte) (value                      );
        block.limit = limit;
    }

    private void writeUInt24Slow(long value)
    {
        writeByte((byte) (value >> UINT_3_OCTET_SHIFT));
        writeByte((byte) (value >> UINT_2_OCTET_SHIFT));
        writeByte((byte) (value                      ));
    }

    public void writeUInt24(long value)
    {
        if (remaining() < 3)
        {
            writeUInt24Slow(value);
            return;
        }

        final Block block = current;
        final byte[] data = block.data;
        int limit = block.limit;
        data[limit++] = (byte) (value >> UINT_3_OCTET_SHIFT);
        data[limit++] = (byte) (value >> UINT_2_OCTET_SHIFT);
        data[limit++] = (byte) (value                      );
        block.limit = limit;
    }

    private void writeUInt32Slow(long value)
    {
        writeByte((byte) (value >> UINT_4_OCTET_SHIFT));
        writeByte((byte) (value >> UINT_3_OCTET_SHIFT));
        writeByte((byte) (value >> UINT_2_OCTET_SHIFT));
        writeByte((byte) (value                      ));
    }

    public void writeUInt32(long value)
    {
        if (remaining() < 4)
        {
            writeUInt32Slow(value);
            return;
        }

        final Block block = current;
        final byte[] data = block.data;
        int limit = block.limit;
        data[limit++] = (byte) (value >> UINT_4_OCTET_SHIFT);
        data[limit++] = (byte) (value >> UINT_3_OCTET_SHIFT);
        data[limit++] = (byte) (value >> UINT_2_OCTET_SHIFT);
        data[limit++] = (byte) (value                      );
        block.limit = limit;
    }

    private void writeUInt40Slow(long value)
    {
        writeByte((byte) (value >> UINT_5_OCTET_SHIFT));
        writeByte((byte) (value >> UINT_4_OCTET_SHIFT));
        writeByte((byte) (value >> UINT_3_OCTET_SHIFT));
        writeByte((byte) (value >> UINT_2_OCTET_SHIFT));
        writeByte((byte) (value                      ));
    }

    public void writeUInt40(long value)
    {
        if (remaining() < 5)
        {
            writeUInt40Slow(value);
            return;
        }

        final Block block = current;
        final byte[] data = block.data;
        int limit = block.limit;
        data[limit++] = (byte) (value >> UINT_5_OCTET_SHIFT);
        data[limit++] = (byte) (value >> UINT_4_OCTET_SHIFT);
        data[limit++] = (byte) (value >> UINT_3_OCTET_SHIFT);
        data[limit++] = (byte) (value >> UINT_2_OCTET_SHIFT);
        data[limit++] = (byte) (value                      );
        block.limit = limit;
    }

    private void writeUInt48Slow(long value)
    {
        writeByte((byte) (value >> UINT_6_OCTET_SHIFT));
        writeByte((byte) (value >> UINT_5_OCTET_SHIFT));
        writeByte((byte) (value >> UINT_4_OCTET_SHIFT));
        writeByte((byte) (value >> UINT_3_OCTET_SHIFT));
        writeByte((byte) (value >> UINT_2_OCTET_SHIFT));
        writeByte((byte) (value                      ));
    }

    public void writeUInt48(long value)
    {
        if (remaining() < 6)
        {
            writeUInt48Slow(value);
            return;
        }

        final Block block = current;
        final byte[] data = block.data;
        int limit = block.limit;
        data[limit++] = (byte) (value >> UINT_6_OCTET_SHIFT);
        data[limit++] = (byte) (value >> UINT_5_OCTET_SHIFT);
        data[limit++] = (byte) (value >> UINT_4_OCTET_SHIFT);
        data[limit++] = (byte) (value >> UINT_3_OCTET_SHIFT);
        data[limit++] = (byte) (value >> UINT_2_OCTET_SHIFT);
        data[limit++] = (byte) ( value                     );
        block.limit = limit;
    }

    private void writeUInt56Slow(long value)
    {
        writeByte((byte) (value >> UINT_7_OCTET_SHIFT));
        writeByte((byte) (value >> UINT_6_OCTET_SHIFT));
        writeByte((byte) (value >> UINT_5_OCTET_SHIFT));
        writeByte((byte) (value >> UINT_4_OCTET_SHIFT));
        writeByte((byte) (value >> UINT_3_OCTET_SHIFT));
        writeByte((byte) (value >> UINT_2_OCTET_SHIFT));
        writeByte((byte) (value                      ));
    }

    public void writeUInt56(long value)
    {
        if (remaining() < 7)
        {
            writeUInt56Slow(value);
            return;
        }

        final Block block = current;
        final byte[] data = block.data;
        int limit = block.limit;
        data[limit++] = (byte) (value >> UINT_7_OCTET_SHIFT);
        data[limit++] = (byte) (value >> UINT_6_OCTET_SHIFT);
        data[limit++] = (byte) (value >> UINT_5_OCTET_SHIFT);
        data[limit++] = (byte) (value >> UINT_4_OCTET_SHIFT);
        data[limit++] = (byte) (value >> UINT_3_OCTET_SHIFT);
        data[limit++] = (byte) (value >> UINT_2_OCTET_SHIFT);
        data[limit++] = (byte) (value                      );
        block.limit = limit;
    }

    private void writeUInt64Slow(long value)
    {
        writeByte((byte) (value >> UINT_8_OCTET_SHIFT));
        writeByte((byte) (value >> UINT_7_OCTET_SHIFT));
        writeByte((byte) (value >> UINT_6_OCTET_SHIFT));
        writeByte((byte) (value >> UINT_5_OCTET_SHIFT));
        writeByte((byte) (value >> UINT_4_OCTET_SHIFT));
        writeByte((byte) (value >> UINT_3_OCTET_SHIFT));
        writeByte((byte) (value >> UINT_2_OCTET_SHIFT));
        writeByte((byte) ( value                     ));
    }

    public void writeUInt64(long value)
    {
        if (remaining() < 8)
        {
            writeUInt64Slow(value);
            return;
        }

        final Block block = current;
        final byte[] data = block.data;
        int limit = block.limit;
        data[limit++] = (byte) (value >> UINT_8_OCTET_SHIFT);
        data[limit++] = (byte) (value >> UINT_7_OCTET_SHIFT);
        data[limit++] = (byte) (value >> UINT_6_OCTET_SHIFT);
        data[limit++] = (byte) (value >> UINT_5_OCTET_SHIFT);
        data[limit++] = (byte) (value >> UINT_4_OCTET_SHIFT);
        data[limit++] = (byte) (value >> UINT_3_OCTET_SHIFT);
        data[limit++] = (byte) (value >> UINT_2_OCTET_SHIFT);
        data[limit++] = (byte) ( value                      );
        block.limit = limit;


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
    private static final long VAR_INT_MASK = 0x7F;

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

    private int writeVarUIntSlow(final long value)
    {
        int size = 1;
        if (value >= VAR_UINT_9_OCTET_MIN_VALUE)
        {
            writeUInt8((value >> VAR_UINT_9_OCTET_SHIFT) & VAR_INT_MASK);
            size++;
        }
        if (value >= VAR_UINT_8_OCTET_MIN_VALUE)
        {
            writeUInt8((value >> VAR_UINT_8_OCTET_SHIFT) & VAR_INT_MASK);
            size++;
        }
        if (value >= VAR_UINT_7_OCTET_MIN_VALUE)
        {
            writeUInt8((value >> VAR_UINT_7_OCTET_SHIFT) & VAR_INT_MASK);
            size++;
        }
        if (value >= VAR_UINT_6_OCTET_MIN_VALUE)
        {
            writeUInt8((value >> VAR_UINT_6_OCTET_SHIFT) & VAR_INT_MASK);
            size++;
        }
        if (value >= VAR_UINT_5_OCTET_MIN_VALUE)
        {
            writeUInt8((value >> VAR_UINT_5_OCTET_SHIFT) & VAR_INT_MASK);
            size++;
        }
        if (value >= VAR_UINT_4_OCTET_MIN_VALUE)
        {
            writeUInt8((value >> VAR_UINT_4_OCTET_SHIFT) & VAR_INT_MASK);
            size++;
        }
        if (value >= VAR_UINT_3_OCTET_MIN_VALUE)
        {
            writeUInt8((value >> VAR_UINT_3_OCTET_SHIFT) & VAR_INT_MASK);
            size++;
        }
        if (value >= VAR_UINT_2_OCTET_MIN_VALUE)
        {
            writeUInt8((value >> VAR_UINT_2_OCTET_SHIFT) & VAR_INT_MASK);
            size++;
        }
        writeUInt8((value & VAR_INT_MASK) | VAR_INT_FINAL_OCTET_SIGNAL_MASK);
        return size;
    }

    private int writeVarUIntDirect2(final long value)
    {
        final Block block = current;
        final byte[] data = block.data;
        int limit = block.limit;
        data[limit++] = (byte)  ((value >> VAR_UINT_2_OCTET_SHIFT) & VAR_INT_MASK);
        data[limit++] = (byte) (((value)                           & VAR_INT_MASK) | VAR_INT_FINAL_OCTET_SIGNAL_MASK);

        block.limit = limit;
        return 2;
    }

    private int writeVarUIntDirect3(final long value)
    {
        final Block block = current;
        final byte[] data = block.data;
        int limit = block.limit;
        data[limit++] = (byte)  ((value >> VAR_UINT_3_OCTET_SHIFT) & VAR_INT_MASK);
        data[limit++] = (byte)  ((value >> VAR_UINT_2_OCTET_SHIFT) & VAR_INT_MASK);
        data[limit++] = (byte) (((value)                           & VAR_INT_MASK) | VAR_INT_FINAL_OCTET_SIGNAL_MASK);

        block.limit = limit;
        return 3;
    }

    private int writeVarUIntDirect4(final long value)
    {
        final Block block = current;
        final byte[] data = block.data;
        int limit = block.limit;
        data[limit++] = (byte)  ((value >> VAR_UINT_4_OCTET_SHIFT) & VAR_INT_MASK);
        data[limit++] = (byte)  ((value >> VAR_UINT_3_OCTET_SHIFT) & VAR_INT_MASK);
        data[limit++] = (byte)  ((value >> VAR_UINT_2_OCTET_SHIFT) & VAR_INT_MASK);
        data[limit++] = (byte) (((value)                           & VAR_INT_MASK) | VAR_INT_FINAL_OCTET_SIGNAL_MASK);

        block.limit = limit;
        return 4;
    }

    private int writeVarUIntDirect5(final long value)
    {
        final Block block = current;
        final byte[] data = block.data;
        int limit = block.limit;
        data[limit++] = (byte)  ((value >> VAR_UINT_5_OCTET_SHIFT) & VAR_INT_MASK);
        data[limit++] = (byte)  ((value >> VAR_UINT_4_OCTET_SHIFT) & VAR_INT_MASK);
        data[limit++] = (byte)  ((value >> VAR_UINT_3_OCTET_SHIFT) & VAR_INT_MASK);
        data[limit++] = (byte)  ((value >> VAR_UINT_2_OCTET_SHIFT) & VAR_INT_MASK);
        data[limit++] = (byte) (((value)                           & VAR_INT_MASK) | VAR_INT_FINAL_OCTET_SIGNAL_MASK);

        block.limit = limit;
        return 5;
    }

    public int writeVarUInt(final long value)
    {
        if (value < VAR_UINT_2_OCTET_MIN_VALUE)
        {
            writeUInt8((value & 0x7F) | 0x80);
            return 1;
        }
        if (value < VAR_UINT_3_OCTET_MIN_VALUE)
        {
            if (remaining() < 2)
            {
                return writeVarUIntSlow(value);
            }
            return writeVarUIntDirect2(value);
        }
        if (value < VAR_UINT_4_OCTET_MIN_VALUE)
        {
            if (remaining() < 3)
            {
                return writeVarUIntSlow(value);
            }
            return writeVarUIntDirect3(value);
        }
        if (value < VAR_UINT_5_OCTET_MIN_VALUE)
        {
            if (remaining() < 4)
            {
                return writeVarUIntSlow(value);
            }
            return writeVarUIntDirect4(value);
        }
        if (value < VAR_UINT_6_OCTET_MIN_VALUE)
        {
            if (remaining() < 5)
            {
                return writeVarUIntSlow(value);
            }
            return writeVarUIntDirect5(value);

        }
        // TODO determine if it is worth doing the fast path beyond 2**35 - 1

        // we give up--go to the 'slow' path
        return writeVarUIntSlow(value);
    }

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

    private int writeVarIntSlow(final long magnitude, final long signMask)
    {
        int size = 1;
        if (magnitude >= VAR_INT_10_OCTET_MIN_VALUE)
        {
            writeUInt8(((magnitude >> VAR_INT_10_OCTET_SHIFT) & VAR_INT_SIGNED_OCTET_MASK) | signMask);
            size++;
        }
        if (magnitude >= VAR_INT_9_OCTET_MIN_VALUE)
        {
            final long bits = (magnitude >> VAR_UINT_9_OCTET_SHIFT);
            writeUInt8(size == 1 ? ((bits & VAR_INT_SIGNED_OCTET_MASK) | signMask) : (bits & VAR_INT_MASK));
            size++;
        }
        if (magnitude >= VAR_INT_8_OCTET_MIN_VALUE)
        {
            final long bits = (magnitude >> VAR_UINT_8_OCTET_SHIFT);
            writeUInt8(size == 1 ? ((bits & VAR_INT_SIGNED_OCTET_MASK) | signMask) : (bits & VAR_INT_MASK));
            size++;
        }
        if (magnitude >= VAR_INT_7_OCTET_MIN_VALUE)
        {
            final long bits = (magnitude >> VAR_UINT_7_OCTET_SHIFT);
            writeUInt8(size == 1 ? ((bits & VAR_INT_SIGNED_OCTET_MASK) | signMask) : (bits & VAR_INT_MASK));
            size++;
        }
        if (magnitude >= VAR_INT_6_OCTET_MIN_VALUE)
        {
            final long bits = (magnitude >> VAR_UINT_6_OCTET_SHIFT);
            writeUInt8(size == 1 ? ((bits & VAR_INT_SIGNED_OCTET_MASK) | signMask) : (bits & VAR_INT_MASK));
            size++;
        }
        if (magnitude >= VAR_INT_5_OCTET_MIN_VALUE)
        {
            final long bits = (magnitude >> VAR_UINT_5_OCTET_SHIFT);
            writeUInt8(size == 1 ? ((bits & VAR_INT_SIGNED_OCTET_MASK) | signMask) : (bits & VAR_INT_MASK));
            size++;
        }
        if (magnitude >= VAR_INT_4_OCTET_MIN_VALUE)
        {
            final long bits = (magnitude >> VAR_UINT_4_OCTET_SHIFT);
            writeUInt8(size == 1 ? ((bits & VAR_INT_SIGNED_OCTET_MASK) | signMask) : (bits & VAR_INT_MASK));
            size++;
        }
        if (magnitude >= VAR_INT_3_OCTET_MIN_VALUE)
        {
            final long bits = (magnitude >> VAR_UINT_3_OCTET_SHIFT);
            writeUInt8(size == 1 ? ((bits & VAR_INT_SIGNED_OCTET_MASK) | signMask) : (bits & VAR_INT_MASK));
            size++;
        }
        if (magnitude >= VAR_INT_2_OCTET_MIN_VALUE)
        {
            final long bits = (magnitude >> VAR_UINT_2_OCTET_SHIFT);
            writeUInt8(size == 1 ? ((bits & VAR_INT_SIGNED_OCTET_MASK) | signMask) : (bits & VAR_INT_MASK));
            size++;
        }
        writeUInt8((size == 1 ? ((magnitude & VAR_INT_SIGNED_OCTET_MASK) | signMask) : (magnitude & VAR_INT_MASK)) | VAR_INT_FINAL_OCTET_SIGNAL_MASK);

        return size;
    }

    private static final long VAR_INT_BITS_PER_SIGNED_OCTET = 6;
    private static final long VAR_SINT_2_OCTET_SHIFT = VAR_INT_BITS_PER_SIGNED_OCTET + (1 * VAR_INT_BITS_PER_OCTET);
    private static final long VAR_SINT_3_OCTET_SHIFT = VAR_INT_BITS_PER_SIGNED_OCTET + (2 * VAR_INT_BITS_PER_OCTET);
    private static final long VAR_SINT_4_OCTET_SHIFT = VAR_INT_BITS_PER_SIGNED_OCTET + (3 * VAR_INT_BITS_PER_OCTET);
    private static final long VAR_SINT_5_OCTET_SHIFT = VAR_INT_BITS_PER_SIGNED_OCTET + (4 * VAR_INT_BITS_PER_OCTET);

    public int writeVarInt(long value)
    {
        assert value != Long.MIN_VALUE;

        final long signMask = value < 0 ? VAR_INT_SIGNBIT_ON_MASK : VAR_INT_SIGNBIT_OFF_MASK;
        final long magnitude = value < 0 ? -value : value;
        if (magnitude < VAR_INT_2_OCTET_MIN_VALUE)
        {
            writeUInt8((magnitude & VAR_INT_SIGNED_OCTET_MASK) | VAR_INT_FINAL_OCTET_SIGNAL_MASK | signMask);
            return 1;
        }
        final long signBit = value < 0 ? 1 : 0;
        final int remaining = remaining();
        if (magnitude < VAR_INT_3_OCTET_MIN_VALUE && remaining >= 2)
        {
            return writeVarUIntDirect2(magnitude | (signBit << VAR_SINT_2_OCTET_SHIFT));
        }
        else if (magnitude < VAR_INT_4_OCTET_MIN_VALUE && remaining >= 3)
        {
            return writeVarUIntDirect3(magnitude | (signBit << VAR_SINT_3_OCTET_SHIFT));
        }
        else if (magnitude < VAR_INT_5_OCTET_MIN_VALUE && remaining >= 4)
        {
            return writeVarUIntDirect4(magnitude | (signBit << VAR_SINT_4_OCTET_SHIFT));
        }
        else if (magnitude < VAR_INT_6_OCTET_MIN_VALUE && remaining >= 5)
        {
            return writeVarUIntDirect5(magnitude | (signBit << VAR_SINT_5_OCTET_SHIFT));
        }
        // TODO determine if it is worth doing the fast path beyond 2**34 - 1

        // we give up--go to the slow path
        return writeVarIntSlow(magnitude, signMask);
    }

    // write variable integer of specific size at a specified position -- no bounds checking, will not expand the buffer

    public void writeVarUIntDirect1At(final long position, final long value)
    {
        writeUInt8At(position, (value & VAR_INT_MASK) | VAR_INT_FINAL_OCTET_SIGNAL_MASK);
    }

    private void writeVarUIntDirect2StraddlingAt(final int index, final int offset, final long value)
    {
        // XXX we're stradling a block
        final Block block1 = blocks.get(index);
        block1.data[offset] = (byte) ((value >> VAR_UINT_2_OCTET_SHIFT) & VAR_INT_MASK);
        final Block block2 = blocks.get(index + 1);
        block2.data[0]      = (byte) ((value                            & VAR_INT_MASK) | VAR_INT_FINAL_OCTET_SIGNAL_MASK);
    }

    public void writeVarUIntDirect2At(long position, long value)
    {
        final int index = index(position);
        final int offset = offset(position);

        if (offset + 2 > allocator.getBlockSize())
        {
            writeVarUIntDirect2StraddlingAt(index, offset, value);
            return;
        }

        final Block block = blocks.get(index);
        block.data[offset    ] = (byte) ((value >> VAR_UINT_2_OCTET_SHIFT) & VAR_INT_MASK);
        block.data[offset + 1] = (byte) ((value                            & VAR_INT_MASK) | VAR_INT_FINAL_OCTET_SIGNAL_MASK);
    }

    public void writeUInt8At(final long position, final long value)
    {
        final int index = index(position);
        final int offset = offset(position);

        // XXX we'll never overrun a block unless we're given a position past our block array
        final Block block = blocks.get(index);
        block.data[offset] = (byte) value;
    }

    /** Write the entire buffer to output stream. */
    public void writeTo(final OutputStream out) throws IOException
    {
        for (final Block block : blocks)
        {
            out.write(block.data, 0, block.limit);
        }
    }

    /** Write a specific segment of data from the buffer to a stream. */
    public void writeTo(final OutputStream out, long position, long length) throws IOException
    {
        while (length > 0)
        {
            final int index = index(position);
            final int offset = offset(position);
            final Block block = blocks.get(index);
            final int amount = (int) Math.min(block.data.length - offset, length);
            out.write(block.data, offset, amount);

            position += amount;
            length -= amount;
        }
    }
}
