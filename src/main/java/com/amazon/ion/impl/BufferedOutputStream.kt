// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl

import com.amazon.ion.impl.bin.*
import java.io.OutputStream

/**
 * TODO:
 *   - Add proper tests
 */
internal class BufferedOutputStream(
    private val out: OutputStream,
    private val allocator: BlockAllocator,
    /**
     * The minimum utilization of a block before a longer value
     * can skip the end of a block and just start a new block.
     */
    minBlockUtilization: Double = 1.0,
) : OutputStream() {

    init {
        // 0.0 would have the possibility of wasting entire blocks.
        // 0.5 is somewhat arbitrary, but at least sensible that you should use at least
        // half of a block before moving on to the next block.
        require(minBlockUtilization in 0.5..1.0) { "minBlockUtilization must be between 0.5 and 1" }
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
        blocks.onEach { it.close() }.clear()
        index = Int.MIN_VALUE
    }

    override fun flush() {
        blocks.forEach { block ->
            out.write(block.data, 0, block.limit)
            block.reset()
        }
        index = 0
        current = blocks[index]
    }

    override fun write(b: Int) {
        if (current.remaining() < 1) current = nextBlock()
        val block = current
        block.data[block.limit] = b.toByte()
        block.limit++
    }

    override fun write(b: ByteArray, off: Int, len: Int) {
        if (len > current.remaining()) {
            if (len < maxBlockWaste) {
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
}
