// Copyright (c) 2015 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.bin;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Simple buffer interface for writing bytes to a set of {@link Block} instances from a {@link BlockAllocator}.
 */
public final class WriteBuffer implements Closeable
{
    private final BlockAllocator allocator;
    private final List<Block> blocks;
    private int index;

    public WriteBuffer(final BlockAllocator allocator)
    {
        this.allocator = allocator;
        this.blocks = new ArrayList<Block>();
        this.index = 0;

        // initial seed of the first block
        allocateNewBlock();
    }

    private void allocateNewBlock()
    {
        blocks.add(allocator.allocateBlock());
    }

    private Block current()
    {
        return blocks.get(index);
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
    }

    /** Returns the amount of capacity left in the current block. */
    public int remaining()
    {
        return current().remaining();
    }

    /** Returns the logical position in the current block. */
    public long position()
    {
        return (((long) index) * allocator.getBlockSize()) + current().limit;
    }

    /** Returns the octet at the logical position given. */
    public int getUInt8At(final long position)
    {
        final int index = index(position);
        final int offset = offset(position);
        final Block block = blocks.get(index);
        return block.data[offset] & 0xFF;
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
        }
        final Block block = current();
        block.data[block.limit] = octet;
        block.limit++;
    }

    // slow in the sense that we do all kind of block boundary checking
    private void writeBytesSlow(final byte[] bytes, int off, int len)
    {
        while (len > 0)
        {
            final Block block = current();
            final int amount = Math.min(len, block.remaining());
            System.arraycopy(bytes, off, block.data, block.limit, amount);
            block.limit += amount;
            off += amount;
            len -= amount;
            if (block.remaining() == 0)
            {
                allocateNewBlock();
                index++;
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

        final Block block = current();
        System.arraycopy(bytes, off, block.data, block.limit, len);
        block.limit += len;
    }

    /** Writes an array of bytes to the buffer expanding if necessary, defaulting to the entire array. */
    public void writeBytes(byte[] bytes)
    {
        writeBytes(bytes, 0, bytes.length);
    }

    // UTF-8 character writing

    private static final char HIGH_SURROGATE_START      = 0xD800;
    private static final char HIGH_SURROGATE_END        = 0xDBFF;
    private static final char LOW_SURROGATE_START       = 0xDC00;
    private static final char LOW_SURROGATE_END         = 0xDFFF;

    // slow in the sense that we deal with any kind of UTF-8 sequence and block boundaries
    private int writeUTF8Slow(final CharSequence chars, int off, int len)
    {
        int octets = 0;
        while (len > 0)
        {
            final char ch = chars.charAt(off);
            if (ch >= LOW_SURROGATE_START && ch <= LOW_SURROGATE_END)
            {
                throw new IllegalArgumentException("Unpaired low surrogate: " + (int) ch);
            }
            if ((ch >= HIGH_SURROGATE_START && ch <= HIGH_SURROGATE_END))
            {
                // we need to look ahead in this case
                off++;
                len--;
                if (len == 0)
                {
                    throw new IllegalArgumentException("Unpaired low surrogate at end of character sequence: " + ch);
                }

                final int ch2 = chars.charAt(off);
                if (ch2 < LOW_SURROGATE_START || ch2 > LOW_SURROGATE_END)
                {
                    throw new IllegalArgumentException("Unpaired high surrogate: " + ch2);
                }

                // at this point we have a high and low surrogate
                final int codepoint = (((ch - HIGH_SURROGATE_START) << 10) | (ch2 - LOW_SURROGATE_START)) + 0x10000;
                writeByte((byte) ((0xF0 | ( codepoint >> 18)        ) & 0xFF));
                writeByte((byte) ((0x80 | ((codepoint >> 12) & 0x3F)) & 0xFF));
                writeByte((byte) ((0x80 | ((codepoint >>  6) & 0x3F)) & 0xFF));
                writeByte((byte) ((0x80 | ( codepoint        & 0x3F)) & 0xFF));

                octets += 4;
            }
            else if (ch < 0x80)
            {
                writeByte((byte) (ch & 0xFF));
                octets++;
            }
            else if (ch < 0x800)
            {
                writeByte((byte) ((0xC0 | (ch >> 6)        ) & 0xFF));
                writeByte((byte) ((0x80 | (ch       & 0x3F)) & 0xFF));
                octets += 2;
            }
            else
            {
                writeByte((byte) ((0xE0 | ( ch >> 12)        ) & 0xFF));
                writeByte((byte) ((0x80 | ((ch >>  6) & 0x3F)) & 0xFF));
                writeByte((byte) ((0x80 | ( ch        & 0x3F)) & 0xFF));
                octets += 3;
            }
            off++;
            len--;
        }
        return octets;
    }

    private int writeUTF8As3Byte(final CharSequence chars, int off, int len)
    {
        // fast path if we fit in the block assuming optimistically for all three-byte
        if ((len * 3) > remaining())
        {
            return writeUTF8Slow(chars, off, len);
        }

        final Block block = current();
        int limit = block.limit;
        char ch = '\0';
        int octets = 0;
        while (len > 0)
        {
            ch = chars.charAt(off);
            if (ch >= LOW_SURROGATE_START && ch <= LOW_SURROGATE_END)
            {
                throw new IllegalArgumentException("Unpaired low surrogate: " + ch);
            }
            if ((ch >= LOW_SURROGATE_START && ch <= LOW_SURROGATE_END))
            {
                // we lost the 3-byte bet
                break;
            }

            if (ch < 0x80)
            {
                block.data[limit++] = (byte) (ch & 0xFF);
                octets++;
            }
            else if (ch < 0x800)
            {
                block.data[limit++] = (byte) ((0xC0 | (ch >> 6)        ) & 0xFF);
                block.data[limit++] = (byte) ((0x80 | (ch       & 0x3F)) & 0xFF);
                octets += 2;
            }
            else
            {
                block.data[limit++] = (byte) ((0xE0 | ( ch >> 12)        ) & 0xFF);
                block.data[limit++] = (byte) ((0x80 | ((ch >>  6) & 0x3F)) & 0xFF);
                block.data[limit++] = (byte) ((0x80 | ( ch        & 0x3F)) & 0xFF);
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

    private int writeUTF8As2Byte(final CharSequence chars, int off, int len)
    {
        // fast path if we fit in the block assuming optimistically for all two-byte
        if ((len * 2) > remaining())
        {
            return writeUTF8Slow(chars, off, len);
        }

        final Block block = current();
        int limit = block.limit;
        char ch = '\0';
        int octets = 0;
        while (len > 0)
        {
            ch = chars.charAt(off);
            if (ch >= 0x800)
            {
                // we lost the 2-byte bet
                break;
            }

            if (ch < 0x80)
            {
                block.data[limit++] = (byte) (ch & 0xFF);
                octets++;
            }
            else
            {
                block.data[limit++] = (byte) ((0xC0 | (ch >> 6)        ) & 0xFF);
                block.data[limit++] = (byte) ((0x80 | (ch       & 0x3F)) & 0xFF);
                octets += 2;
            }
            off++;
            len--;
        }
        block.limit = limit;

        if (len > 0)
        {
            if (ch >= LOW_SURROGATE_START && ch <= LOW_SURROGATE_END)
            {
                throw new IllegalArgumentException("Unpaired low surrogate: " + ch);
            }
            if (ch >= HIGH_SURROGATE_START && ch <= HIGH_SURROGATE_END)
            {
                // just defer to 'slow' writing for non-BMP characters
                return octets + writeUTF8Slow(chars, off, len);
            }

            // we must be a three byte BMP character
            return octets + writeUTF8As3Byte(chars, off, len);
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
        final Block block = current();
        int limit = block.limit;
        char ch = '\0';
        int octets = 0;
        while (len > 0)
        {
            ch = chars.charAt(off);
            if (ch >= 0x80)
            {
                // we lost the ASCII bet
                break;
            }

            block.data[limit++] = (byte) (ch & 0xFF);
            octets++;
            off++;
            len--;
        }
        block.limit = limit;

        if (len > 0)
        {
            if (ch < 0x800)
            {
                return octets + writeUTF8As2Byte(chars, off, len);
            }
            if (ch >= LOW_SURROGATE_START && ch <= LOW_SURROGATE_END)
            {
                throw new IllegalArgumentException("Unpaired low surrogate: " + ch);
            }
            if (ch >= HIGH_SURROGATE_START && ch <= HIGH_SURROGATE_END)
            {
                // just defer to 'slow' writing for non-BMP characters
                return octets + writeUTF8Slow(chars, off, len);
            }

            // we must be a three byte BMP character
            return octets + writeUTF8As3Byte(chars, off, len);
        }
        return octets;
    }

    /** Returns the number of octets written. */
    public int writeUTF8(final CharSequence chars)
    {
        return writeUTF8(chars, 0, chars.length());
    }

    // unsigned fixed integer writes -- does not check sign/bounds

    public void writeUInt8(long value)
    {
        writeByte((byte) (value & 0xFF));
    }

    private void writeUInt16Slow(long value)
    {
        writeByte((byte) ((value >> 8) & 0xFF));
        writeByte((byte) ( value       & 0xFF));
    }

    public void writeUInt16(long value)
    {
        if (remaining() < 2)
        {
            writeUInt16Slow(value);
            return;
        }

        final Block block = current();
        final byte[] data = block.data;
        int limit = block.limit;
        data[limit++] = (byte) ((value >> 8) & 0xFF);
        data[limit++] = (byte) ( value       & 0xFF);
        block.limit = limit;
    }

    private void writeUInt24Slow(long value)
    {
        writeByte((byte) ((value >> 16) & 0xFF));
        writeByte((byte) ((value >> 8)  & 0xFF));
        writeByte((byte) ( value        & 0xFF));
    }

    public void writeUInt24(long value)
    {
        if (remaining() < 3)
        {
            writeUInt24Slow(value);
            return;
        }

        final Block block = current();
        final byte[] data = block.data;
        int limit = block.limit;
        data[limit++] = (byte) ((value >> 16) & 0xFF);
        data[limit++] = (byte) ((value >>  8) & 0xFF);
        data[limit++] = (byte) ( value        & 0xFF);
        block.limit = limit;
    }

    private void writeUInt32Slow(long value)
    {
        writeByte((byte) ((value >> 24) & 0xFF));
        writeByte((byte) ((value >> 16) & 0xFF));
        writeByte((byte) ((value >> 8)  & 0xFF));
        writeByte((byte) ( value        & 0xFF));
    }

    public void writeUInt32(long value)
    {
        if (remaining() < 4)
        {
            writeUInt32Slow(value);
            return;
        }

        final Block block = current();
        final byte[] data = block.data;
        int limit = block.limit;
        data[limit++] = (byte) ((value >> 24) & 0xFF);
        data[limit++] = (byte) ((value >> 16) & 0xFF);
        data[limit++] = (byte) ((value >> 8)  & 0xFF);
        data[limit++] = (byte) ( value        & 0xFF);
        block.limit = limit;
    }

    private void writeUInt40Slow(long value)
    {
        writeByte((byte) ((value >> 32) & 0xFF));
        writeByte((byte) ((value >> 24) & 0xFF));
        writeByte((byte) ((value >> 16) & 0xFF));
        writeByte((byte) ((value >> 8)  & 0xFF));
        writeByte((byte) ( value        & 0xFF));
    }

    public void writeUInt40(long value)
    {
        if (remaining() < 5)
        {
            writeUInt40Slow(value);
            return;
        }

        final Block block = current();
        final byte[] data = block.data;
        int limit = block.limit;
        data[limit++] = (byte) ((value >> 32) & 0xFF);
        data[limit++] = (byte) ((value >> 24) & 0xFF);
        data[limit++] = (byte) ((value >> 16) & 0xFF);
        data[limit++] = (byte) ((value >> 8)  & 0xFF);
        data[limit++] = (byte) ( value        & 0xFF);
        block.limit = limit;
    }

    private void writeUInt48Slow(long value)
    {
        writeByte((byte) ((value >> 40) & 0xFF));
        writeByte((byte) ((value >> 32) & 0xFF));
        writeByte((byte) ((value >> 24) & 0xFF));
        writeByte((byte) ((value >> 16) & 0xFF));
        writeByte((byte) ((value >> 8)  & 0xFF));
        writeByte((byte) ( value        & 0xFF));
    }

    public void writeUInt48(long value)
    {
        if (remaining() < 6)
        {
            writeUInt48Slow(value);
            return;
        }

        final Block block = current();
        final byte[] data = block.data;
        int limit = block.limit;
        data[limit++] = (byte) ((value >> 40) & 0xFF);
        data[limit++] = (byte) ((value >> 32) & 0xFF);
        data[limit++] = (byte) ((value >> 24) & 0xFF);
        data[limit++] = (byte) ((value >> 16) & 0xFF);
        data[limit++] = (byte) ((value >> 8)  & 0xFF);
        data[limit++] = (byte) ( value        & 0xFF);
        block.limit = limit;
    }

    private void writeUInt56Slow(long value)
    {
        writeByte((byte) ((value >> 48) & 0xFF));
        writeByte((byte) ((value >> 40) & 0xFF));
        writeByte((byte) ((value >> 32) & 0xFF));
        writeByte((byte) ((value >> 24) & 0xFF));
        writeByte((byte) ((value >> 16) & 0xFF));
        writeByte((byte) ((value >> 8)  & 0xFF));
        writeByte((byte) ( value        & 0xFF));
    }

    public void writeUInt56(long value)
    {
        if (remaining() < 7)
        {
            writeUInt56Slow(value);
            return;
        }

        final Block block = current();
        final byte[] data = block.data;
        int limit = block.limit;
        data[limit++] = (byte) ((value >> 48) & 0xFF);
        data[limit++] = (byte) ((value >> 40) & 0xFF);
        data[limit++] = (byte) ((value >> 32) & 0xFF);
        data[limit++] = (byte) ((value >> 24) & 0xFF);
        data[limit++] = (byte) ((value >> 16) & 0xFF);
        data[limit++] = (byte) ((value >> 8)  & 0xFF);
        data[limit++] = (byte) ( value        & 0xFF);
        block.limit = limit;
    }

    private void writeUInt64Slow(long value)
    {
        writeByte((byte) ((value >> 56) & 0xFF));
        writeByte((byte) ((value >> 48) & 0xFF));
        writeByte((byte) ((value >> 40) & 0xFF));
        writeByte((byte) ((value >> 32) & 0xFF));
        writeByte((byte) ((value >> 24) & 0xFF));
        writeByte((byte) ((value >> 16) & 0xFF));
        writeByte((byte) ((value >> 8)  & 0xFF));
        writeByte((byte) ( value        & 0xFF));
    }

    public void writeUInt64(long value)
    {
        if (remaining() < 8)
        {
            writeUInt64Slow(value);
            return;
        }

        final Block block = current();
        final byte[] data = block.data;
        int limit = block.limit;
        data[limit++] = (byte) ((value >> 56) & 0xFF);
        data[limit++] = (byte) ((value >> 48) & 0xFF);
        data[limit++] = (byte) ((value >> 40) & 0xFF);
        data[limit++] = (byte) ((value >> 32) & 0xFF);
        data[limit++] = (byte) ((value >> 24) & 0xFF);
        data[limit++] = (byte) ((value >> 16) & 0xFF);
        data[limit++] = (byte) ((value >> 8)  & 0xFF);
        data[limit++] = (byte) ( value        & 0xFF);
        block.limit = limit;


    }

    // signed fixed integer writes - does not check bounds (especially important for IntX.MIN_VALUE).

    public void writeInt8(long value)
    {
        if (value < 0)
        {
            value = (-value) | 0x80L;
        }
        writeUInt8(value);
    }

    public void writeInt16(long value)
    {
        if (value < 0)
        {
            value = (-value) | 0x8000L;
        }
        writeUInt16(value);
    }

    public void writeInt24(long value)
    {
        if (value < 0)
        {
            value = (-value) | 0x800000;
        }
        writeUInt24(value);
    }

    public void writeInt32(long value)
    {
        if (value < 0)
        {
            value = (-value) | 0x80000000L;
        }
        writeUInt32(value);
    }

    public void writeInt40(long value)
    {
        if (value < 0)
        {
            value = (-value) | 0x8000000000L;
        }
        writeUInt40(value);
    }

    public void writeInt48(long value)
    {
        if (value < 0)
        {
            value = (-value) | 0x800000000000L;
        }
        writeUInt48(value);
    }

    public void writeInt56(long value)
    {
        if (value < 0)
        {
            value = (-value) | 0x80000000000000L;
        }
        writeUInt56(value);
    }

    public void writeInt64(long value)
    {
        if (value < 0)
        {
            value = (-value) | 0x8000000000000000L;
        }
        writeUInt64(value);
    }

    // variable length integer writing

    // TODO deal with full 64-bit magnitude if we need it (this is 2**63 - 1)
    private int writeVarUIntSlow(final long value)
    {
        int size = 1;
        if (value >= 0x100000000000000L)
        {
            writeUInt8((value >> 56) & 0x7F);
            size++;
        }
        if (value >= 0x2000000000000L)
        {
            writeUInt8((value >> 49) & 0x7F);
            size++;
        }
        if (value >= 0x40000000000L)
        {
            writeUInt8((value >> 42) & 0x7F);
            size++;
        }
        if (value >= 0x800000000L)
        {
            writeUInt8((value >> 35) & 0x7F);
            size++;
        }
        if (value >= 0x10000000L)
        {
            writeUInt8((value >> 28) & 0x7F);
            size++;
        }
        if (value >= 0x200000L)
        {
            writeUInt8((value >> 21) & 0x7F);
            size++;
        }
        if (value >= 0x4000L)
        {
            writeUInt8((value >> 14) & 0x7F);
            size++;
        }
        if (value >= 0x80L)
        {
            writeUInt8((value >> 7) & 0x7F);
            size++;
        }
        writeUInt8((value & 0x7F) | 0x80);
        return size;
    }

    private int writeVarUIntDirect2(final long value)
    {
        final Block block = current();
        final byte[] data = block.data;
        int limit = block.limit;
        data[limit++] = (byte)  ((value >> 7) & 0x7F);
        data[limit++] = (byte) (((value)      & 0x7F) | 0x80);

        block.limit = limit;
        return 2;
    }

    private int writeVarUIntDirect3(final long value)
    {
        final Block block = current();
        final byte[] data = block.data;
        int limit = block.limit;
        data[limit++] = (byte)  ((value >> 14) & 0x7F);
        data[limit++] = (byte)  ((value >> 7)  & 0x7F);
        data[limit++] = (byte) (((value)       & 0x7F) | 0x80);

        block.limit = limit;
        return 3;
    }

    private int writeVarUIntDirect4(final long value)
    {
        final Block block = current();
        final byte[] data = block.data;
        int limit = block.limit;
        data[limit++] = (byte)  ((value >> 21) & 0x7F);
        data[limit++] = (byte)  ((value >> 14) & 0x7F);
        data[limit++] = (byte)  ((value >> 7)  & 0x7F);
        data[limit++] = (byte) (((value)       & 0x7F) | 0x80);

        block.limit = limit;
        return 4;
    }

    private int writeVarUIntDirect5(final long value)
    {
        final Block block = current();
        final byte[] data = block.data;
        int limit = block.limit;
        data[limit++] = (byte)  ((value >> 28) & 0x7F);
        data[limit++] = (byte)  ((value >> 21) & 0x7F);
        data[limit++] = (byte)  ((value >> 14) & 0x7F);
        data[limit++] = (byte)  ((value >> 7)  & 0x7F);
        data[limit++] = (byte) (((value)       & 0x7F) | 0x80);

        block.limit = limit;
        return 5;
    }

    public int writeVarUInt(final long value)
    {
        if (value < 0x80L)
        {
            writeUInt8((value & 0x7F) | 0x80);
            return 1;
        }
        if (value < 0x4000L)
        {
            if (remaining() < 2)
            {
                return writeVarUIntSlow(value);
            }
            return writeVarUIntDirect2(value);
        }
        if (value < 0x200000L)
        {
            if (remaining() < 3)
            {
                return writeVarUIntSlow(value);
            }
            return writeVarUIntDirect3(value);
        }
        if (value < 0x10000000L)
        {
            if (remaining() < 4)
            {
                return writeVarUIntSlow(value);
            }
            return writeVarUIntDirect4(value);
        }
        if (value < 0x800000000L)
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

    private int writeVarIntSlow(final long magnitude, final long signMask)
    {
        int size = 1;
        if (magnitude >= 0x4000000000000000L)
        {
            writeUInt8(((magnitude >> 62) & 0x3F) | signMask);
            size++;
        }
        if (magnitude >= 0x80000000000000L)
        {
            final long bits = (magnitude >> 56);
            writeUInt8(size == 1 ? ((bits & 0x3F) | signMask) : (bits & 0x7F));
            size++;
        }
        if (magnitude >= 0x1000000000000L)
        {
            final long bits = (magnitude >> 49);
            writeUInt8(size == 1 ? ((bits & 0x3F) | signMask) : (bits & 0x7F));
            size++;
        }
        if (magnitude >= 0x20000000000L)
        {
            final long bits = (magnitude >> 42);
            writeUInt8(size == 1 ? ((bits & 0x3F) | signMask) : (bits & 0x7F));
            size++;
        }
        if (magnitude >= 0x400000000L)
        {
            final long bits = (magnitude >> 35);
            writeUInt8(size == 1 ? ((bits & 0x3F) | signMask) : (bits & 0x7F));
            size++;
        }
        if (magnitude >= 0x8000000L)
        {
            final long bits = (magnitude >> 28);
            writeUInt8(size == 1 ? ((bits & 0x3F) | signMask) : (bits & 0x7F));
            size++;
        }
        if (magnitude >= 0x100000L)
        {
            final long bits = (magnitude >> 21);
            writeUInt8(size == 1 ? ((bits & 0x3F) | signMask) : (bits & 0x7F));
            size++;
        }
        if (magnitude >= 0x2000L)
        {
            final long bits = (magnitude >> 14);
            writeUInt8(size == 1 ? ((bits & 0x3F) | signMask) : (bits & 0x7F));
            size++;
        }
        if (magnitude >= 0x40L)
        {
            final long bits = (magnitude >> 7);
            writeUInt8(size == 1 ? ((bits & 0x3F) | signMask) : (bits & 0x7F));
            size++;
        }
        writeUInt8((size == 1 ? ((magnitude & 0x3F) | signMask) : (magnitude & 0x7F)) | 0x80);

        return size;
    }

    public int writeVarInt(long value)
    {
        final long signMask = value < 0 ? 0x40 : 0x00;
        final long magnitude = value < 0 ? -value : value;
        if (magnitude < 0x40L)
        {
            writeUInt8((magnitude & 0x3F) | 0x80 | signMask);
            return 1;
        }
        final long signBit = value < 0 ? 0x1 : 0x0;
        final int remaining = remaining();
        if (magnitude < 0x2000L && remaining >= 2)
        {
            return writeVarUIntDirect2(magnitude | (signBit << 13));
        }
        else if (magnitude < 0x100000L && remaining >= 3)
        {
            return writeVarUIntDirect3(magnitude | (signBit << 20));
        }
        else if (magnitude < 0x8000000L && remaining >= 4)
        {
            return writeVarUIntDirect4(magnitude | (signBit << 27));
        }
        else if (magnitude < 0x400000000L && remaining >= 5)
        {
            return writeVarUIntDirect5(magnitude | (signBit << 34));
        }
        // TODO determine if it is worth doing the fast path beyond 2**34 - 1

        // we give up--go to the slow path
        return writeVarIntSlow(magnitude, signMask);
    }

    // write variable integer of specific size at a specified position -- no bounds checking, will not expand the buffer

    public void writeVarUIntDirect1At(final long position, final long value)
    {
        writeUInt8At(position, (value & 0x7F) | 0x80);
    }

    private void writeVarUIntDirect2SlowAt(final int index, final int offset, final long value)
    {
        // XXX we're stradling a block
        final Block block1 = blocks.get(index);
        block1.data[offset] = (byte) ((value >> 7) & 0x7F);
        final Block block2 = blocks.get(index + 1);
        block2.data[0]      = (byte) ((value       & 0x7F) | 0x80);
    }

    public void writeVarUIntDirect2At(long position, long value)
    {
        final int index = index(position);
        final int offset = offset(position);

        if (offset + 2 > allocator.getBlockSize())
        {
            writeVarUIntDirect2SlowAt(index, offset, value);
            return;
        }

        final Block block = blocks.get(index);
        block.data[offset    ] = (byte) ((value >> 7) & 0x7F);
        block.data[offset + 1] = (byte) ((value       & 0x7F) | 0x80);
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

    public void close()
    {
        // free all the blocks
        for (final Block block : blocks)
        {
            block.close();
        }
        blocks.clear();

        // XXX we don't explicitly flag that we're closed for efficiency
    }
}
