// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl

import com.amazon.ion.impl.bin.Block
import com.amazon.ion.impl.bin.BlockAllocator
import com.amazon.ion.util._Private_FastAppendable
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import java.io.OutputStream

/**
 * A [_Private_FastAppendable] that buffers data to blocks of memory which are managed by a [BlockAllocator]. Only when
 * [flush] is called are the blocks written to the wrapped [OutputStream].
 *
 * This is necessary for cases where an [IonManagedWriter_1_1] over Ion text needs to emit encoding directives that are
 * not known in advance. The [OutputStreamFastAppendable] class only buffers a fixed amount of data, so it is not safe
 * to use if there are system values to be written. For a sufficiently large user value, an [OutputStreamFastAppendable]
 * can end up flushing partial or whole user values flushing to the [OutputStream] before the [IonManagedWriter_1_1] can
 * write the system value that it depends on.
 *
 * Once [IonManagedWriter_1_1] supports an auto-flush feature, then this class will have very little practical
 * difference from [OutputStreamFastAppendable] for the case where no system values are needed.
 *
 * TODO:
 *   - Add proper tests
 *
 * @see BufferedAppendableFastAppendable
 * @see OutputStreamFastAppendable
 */
internal class BufferedOutputStreamFastAppendable(
    @SuppressFBWarnings("EI_EXPOSE_REP2", justification = "We're intentionally storing a reference to a mutable object because we need to write to it.")
    private val out: OutputStream,
    private val allocator: BlockAllocator,
    /**
     * The minimum utilization of a block before a longer value
     * can skip the end of a block and just start a new block.
     */
    minBlockUtilization: Double = 1.0,
) : OutputStream(), _Private_FastAppendable {

    init {
        // 0.0 would have the possibility of wasting entire blocks.
        // 0.5 is somewhat arbitrary, but at least sensible that you should use at least
        // half of a block before moving on to the next block.
        require(minBlockUtilization in 0.5..1.0) { "minBlockUtilization must be between 0.5 and 1" }
        require(allocator.blockSize > 10)
    }

    private val maxBlockWaste: Int = (allocator.blockSize * (1.0 - minBlockUtilization)).toInt()

    private var index = -1
    private val blocks = mutableListOf<Block>()
    private var current: Block = nextBlock()

    private fun nextBlock(): Block {
        index++
        if (index < 0) throw IllegalStateException("This output stream is closed.")
        if (index >= blocks.size) blocks.add(allocator.allocateBlock())
        return blocks[index]
    }

    override fun close() {
        try {
            flush()
        } finally {
            blocks.onEach { it.close() }.clear()
            index = Int.MIN_VALUE
        }
    }

    override fun flush() {
        blocks.forEach { block ->
            out.write(block.data, 0, block.limit)
            block.reset()
        }
        index = 0
        current = blocks[index]
        out.flush()
    }

    override fun write(b: Int) {
        if (current.remaining() < 1) current = nextBlock()
        val block = current
        block.data[block.limit] = b.toByte()
        block.limit++
    }

    override fun write(b: ByteArray, off: Int, len: Int) {
        if (len > current.remaining()) {
            if (current.remaining() < maxBlockWaste && len < allocator.blockSize) {
                current = nextBlock()
            } else {
                writeBytesSlow(b, off, len)
                return
            }
        }
        val block = current
        System.arraycopy(b, off, block.data, block.limit, len)
        block.limit += len
    }

    // slow in the sense that we do all kind of block boundary checking
    private fun writeBytesSlow(bytes: ByteArray, _off: Int, _len: Int) {
        var off = _off
        var len = _len
        while (len > 0) {
            val block = current
            val amount = Math.min(len, block.remaining())
            System.arraycopy(bytes, off, block.data, block.limit, amount)
            block.limit += amount
            off += amount
            len -= amount
            if (block.remaining() == 0) {
                current = nextBlock()
            }
        }
    }

    override fun append(c: Char): Appendable = apply { if (c.code < 0x80) appendAscii(c) else appendUtf16(c) }

    override fun append(csq: CharSequence): Appendable = apply { append(csq, 0, csq.length) }

    override fun append(csq: CharSequence, start: Int, end: Int): Appendable {
        for (i in start until end) {
            append(csq[i])
        }
        return this
    }

    override fun appendAscii(c: Char) {
        assert(c.code < 0x80)
        write(c.code)
    }

    override fun appendAscii(csq: CharSequence) = appendAscii(csq, 0, csq.length)

    override fun appendAscii(csq: CharSequence, start: Int, end: Int) {
        if (csq is String) {
            var pos = start
            val len = end - start
            if (len > current.remaining() && current.remaining() < maxBlockWaste && len < allocator.blockSize) {
                current = nextBlock()
            }
            while (true) {
                val copyAmount = minOf(current.remaining(), end - pos)
                csq.copyAsciiBytes(pos, pos + copyAmount, current.data, current.limit)
                current.limit += copyAmount
                pos += copyAmount
                if (pos >= end) return
                current = nextBlock()
            }
        } else {
            append(csq, start, end)
        }
    }

    override fun appendUtf16(c: Char) {
        assert(c.code >= 0x80)
        if (current.remaining() < 3) {
            current = nextBlock()
        }
        if (c.code < 0x800) {
            current.data[current.limit++] = (0xff and (0xC0 or (c.code shr 6))).toByte()
            current.data[current.limit++] = (0xff and (0x80 or (c.code and 0x3F))).toByte()
        } else if (c.code < 0x10000) {
            current.data[current.limit++] = (0xff and (0xE0 or (c.code shr 12))).toByte()
            current.data[current.limit++] = (0xff and (0x80 or (c.code shr 6 and 0x3F))).toByte()
            current.data[current.limit++] = (0xff and (0x80 or (c.code and 0x3F))).toByte()
        }
    }

    override fun appendUtf16Surrogate(leadSurrogate: Char, trailSurrogate: Char) {
        // Here we must convert a UTF-16 surrogate pair to UTF-8 bytes.
        val c = _Private_IonConstants.makeUnicodeScalar(leadSurrogate.code, trailSurrogate.code)
        assert(c >= 0x10000)
        if (current.remaining() < 4) {
            current = nextBlock()
        }
        current.data[current.limit++] = (0xff and (0xF0 or (c shr 18))).toByte()
        current.data[current.limit++] = (0xff and (0x80 or (c shr 12 and 0x3F))).toByte()
        current.data[current.limit++] = (0xff and (0x80 or (c shr 6 and 0x3F))).toByte()
        current.data[current.limit++] = (0xff and (0x80 or (c and 0x3F))).toByte()
    }

    /** Helper function to wrap [java.lang.String.getBytes]. */
    private fun String.copyAsciiBytes(srcBegin: Int, srcEnd: Int, dst: ByteArray, dstBegin: Int) {
        // Using deprecated String.getBytes intentionally, since it is
        // correct behavior in this case, and much faster.
        (this as java.lang.String).getBytes(srcBegin, srcEnd, dst, dstBegin)
    }
}
