// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode

import com.amazon.ion.IonException
import java.math.BigInteger
import java.nio.ByteBuffer

/**
 * Helper class containing methods for reading FixedInts, FlexInts, FixedUInts, and FlexUInts.
 *
 * TODO: Right now, this class is hijacking the test cases for [FlexInt][com.amazon.ion.impl.bin.FlexInt].
 *       This needs to be cleaned up before any of this is merged.
 *
 *
 * Some methods in this class use a technique to reduce the number of branches and minimize the number of calls to
 * get data from the ByteBuffer. We know that any FlexInt or FlexUint must have at least 4 bytes preceding it (because
 * the IVM is 4 bytes), so rather than reading bytes one at a time, we'll read one byte to figure out how many bytes to
 * read, and then we'll read the entire FlexUInt (plus zero or more preceding bytes) in one call to [ByteBuffer.getInt]
 * or [ByteBuffer.getLong]. This puts all the bytes we want into the _most_ significant bits of the `int` or `long`.
 * Then we can remove the extra bytes and the continuation bits by using a single right-shift operation (signed for
 * FlexInt or unsigned for FlexUInt). This technique significantly reduces the number of operations required to read a
 * Flex(U)Int as compared to reading bytes one at a time.
 *
 * A similar technique is also used for reading FixedInts and FixedUInts.
 *
 * Examples:
 * ```
 * aaaa_aaaa bbbb_bbbb cccc_cccc dddd_dddd eeee_eee1 ffff_ffff gggg_gggg hhhh_hhhh iiii_iiii
 * ```
 * - read B-E... `eeee_eee1 dddd_dddd cccc_cccc bbbb_bbbb`
 * - shift right by (8 * 3 + 1) = 25 = 4 + 7 * 3
 * - unsigned shift right for FlexUint; signed shift right for FlexInt.
 *
 * ```
 * aaaa_aaaa bbbb_bbbb cccc_cccc dddd_dddd eeee_ee10 ffff_ffff gggg_gggg hhhh_hhhh iiii_iiii
 * ```
 * - read C-F... `ffff_ffff eeee_ee10 dddd_dddd cccc_cccc`
 * - shift right by (8 * 2 + 2) = 18 = 4 + 7 * 2
 *
 * ```
 * aaaa_aaaa bbbb_bbbb cccc_cccc dddd_dddd eeee_e100 ffff_ffff gggg_gggg hhhh_hhhh iiii_iiii
 * ```
 * - read D-G... `gggg_gggg ffff_ffff eeee_e100 dddd_dddd`
 * - shift right by (8 * 1 + 3) = 11 = 4 + 7 * 1
 *
 * ```
 * aaaa_aaaa bbbb_bbbb cccc_cccc dddd_dddd eeee_1000 ffff_ffff gggg_gggg hhhh_hhhh iiii_iiii
 * ```
 * - read E-H... `hhhh_hhhh gggg_gggg ffff_ffff eeee_1000`
 * - shift right by (8 * 0 + 4) = 4
 */
object BinaryPrimitiveReader {

    @JvmStatic
    private fun ByteArray.getInt(position: Int): Int {
        return (this[position].toInt() and 0xFF) or
            ((this[position + 1].toInt() and 0xFF) shl 8) or
            ((this[position + 2].toInt() and 0xFF) shl 16) or
            ((this[position + 3].toInt() and 0xFF) shl 24)
    }
    @JvmStatic
    private fun ByteArray.getLong(position: Int): Long {
        return (this[position].toLong() and 0xFF) or
            ((this[position + 1].toLong() and 0xFF) shl 8) or
            ((this[position + 2].toLong() and 0xFF) shl 16) or
            ((this[position + 3].toLong() and 0xFF) shl 24) or
            ((this[position + 4].toLong() and 0xFF) shl 32) or
            ((this[position + 5].toLong() and 0xFF) shl 40) or
            ((this[position + 6].toLong() and 0xFF) shl 48) or
            ((this[position + 7].toLong() and 0xFF) shl 56)
    }

    @JvmStatic
    fun readFixedIntAt(source: ByteArray, start: Int, length: Int): Long {
        if (source.size < start + length) throw IonException("Incomplete data: start=$start, length=$length, limit=${source.size}")
        if (length > 4) {
            // TODO: See if we can simplify some of the calculations
            return source.getLong(start - 8 + length) shr ((8 - length) * 8)
        } else {
            return (source.getInt(start - 4 + length) shr ((4 - length) * 8)).toLong()
        }
    }

    @JvmStatic
    fun readFixedUIntAt(source: ByteArray, start: Int, length: Int): Long {
        if (source.size < start + length) throw IonException("Incomplete data: start=$start, length=$length, limit=${source.size}")
        if (length > 4) {
            // TODO: See if we can simplify some of the calculations
            return (source.getLong(start - 8 + length) shr ((8 - length) * 8))
        } else {
            return (source.getInt(start - 4 + length) shr ((4 - length) * 8)).toLong()
        }
    }

    @JvmStatic
    fun readFlexUIntAsLong(source: ByteBuffer): Long {
        val position = source.position()
        // TODO: Rewrite this as a relative get() so that we don't have to set the position
        //       again in the 1-byte case.
        val firstByte = source.get()
        val numBytes = firstByte.countTrailingZeroBits() + 1
        if (source.remaining() < (numBytes - 1)) throw IonException("Incomplete data")
        when (numBytes) {
            1 -> {
                return ((firstByte.toInt() and 0xFF) ushr 1).toLong()
            }
            2, 3, 4 -> {
                source.position(position + numBytes)
                // TODO: We could probably simplify some of these calculations.
                val backtrack = 4 - numBytes
                val data = source.getInt(position - backtrack)
                val shiftAmount = 8 * backtrack + numBytes
                return (data ushr shiftAmount).toLong()
            }
            5, 6, 7, 8 -> {
                source.position(position + numBytes)
                val backtrack = 8 - numBytes
                val data = source.getLong(position - backtrack)
                return data ushr (8 * backtrack + numBytes)
            }
            9 -> {
                // The first byte was entirely `0`. We'll assume that the least significant bit of the next byte is 1
                // which would mean that the FlexUInt is 9 bytes long. In this case, we can read a long to get the
                // remaining 8 bytes, and shift out the single bit.
                val value = source.getLong(position + 1)
                source.position(position + 9)
                // Our assumption that it is a 9 byte flex uint is incorrect.
                if (value and 0x1L == 0L) throw IonException("FlexInt value too large to find in a Long")
                return value ushr 1
            }
            else -> throw IonException("FlexInt value too large to find in a Long")
        }
    }

    @JvmStatic
    fun readFlexUInt(source: ByteBuffer): Int {
        // TODO: Consider writing a specialized implementation that is optimized for Ints.
        val value = readFlexUIntAsLong(source)
        if (value < 0 || value > Int.MAX_VALUE) {
            throw IonException("FlexUInt is too large to fit in an int")
        }
        return value.toInt()
    }

    /**
     * This employs some hackery. The result contains both the value and the length.
     * The 8 low order bits are the length, and the other bits are the value.
     */
    @JvmStatic
    fun readFlexUIntValueAndLengthAt(source: ByteBuffer, position: Int): Long {
        val firstByte = source.get(position)
        val numBytes = firstByte.countTrailingZeroBits() + 1
        val value = when (numBytes) {
            1 -> {
                ((firstByte.toInt() and 0xFF) ushr 1)
            }
            2 -> {
                val secondByte = source.get(position + 1).toInt() and 0xFF
                (secondByte shl 6) or ((firstByte.toInt() and 0xFF) ushr 2)
            }
            3, 4 -> {
                // TODO: We could probably simplify some of these calculations.
                // FIXME: This might be broken.
                val backtrack = 4 - numBytes
                val data = source.getInt(position - backtrack)
                val shiftAmount = 8 * backtrack + numBytes
                (data ushr shiftAmount)
            }
            5 -> {
                val data = source.getInt(position + 1)
                val data1 = (data shl 3) ushr 3
                if (data != data1) throw IonException("FlexUInt value too large to find in an Int")
                (data shl 3) + (firstByte.toInt() ushr 5)
            }
            else -> throw IonException("FlexUInt value too large to find in an Int")
        }
        return (value.toLong() shl 8) or numBytes.toLong()
    }

    /**
     * This employs some hackery. The result contains both the value and the length.
     * The 8 low order bits are the length, and the other bits are the value.
     */
    @JvmStatic
    fun readFlexUIntValueAndLengthAt(source: ByteArray, position: Int): Long {
        val firstByte = source.get(position)
        val numBytes = firstByte.countTrailingZeroBits() + 1
        val value = when (numBytes) {
            1 -> {
                ((firstByte.toInt() and 0xFF) ushr 1)
            }
            2 -> {
                val secondByte = source.get(position + 1).toInt() and 0xFF
                (secondByte shl 6) or ((firstByte.toInt() and 0xFF) ushr 2)
            }

            3, 4 -> {
                // TODO: We could probably simplify some of these calculations.
                val backtrack = 4 - numBytes
                val data = source.getInt(position - backtrack)
                val shiftAmount = 8 * backtrack + numBytes
                (data ushr shiftAmount)
            }
            5 -> {
                val data = source.getInt(position + 1)
                val data1 = (data shl 3) ushr 3
                if (data != data1) throw IonException("FlexUInt value too large to find in an Int")
                (data shl 3) + (firstByte.toInt() ushr 5)
            }
            else -> throw IonException("FlexUInt value too large to find in an Int")
        }
        return (value.toLong() shl 8) or numBytes.toLong()
    }

    /**
     * Returns the length of the flexuint + the value of the flexuint. Useful for skipping length-prefixed values.
     */
    @JvmStatic
    fun getLengthPlusValueOfFlexUIntAt(source: ByteBuffer, position: Int): Int {
        val firstByte = source.get(position)
        val numBytes = firstByte.countTrailingZeroBits() + 1
        when (numBytes) {
            1 -> {
                return ((firstByte.toInt() and 0xFF) ushr 1) + numBytes
            }
            2, 3, 4 -> {
                // TODO: We could probably simplify some of these calculations.
                val backtrack = 4 - numBytes
                val data = source.getInt(position - backtrack)
                val shiftAmount = 8 * backtrack + numBytes
                return (data ushr shiftAmount) + numBytes
            }
            5 -> {
                val data = source.getInt(position + 1)
                val data1 = (data shl 3) ushr 3
                if (data != data1) throw IonException("FlexUInt value too large to find in an Int")
                return (data shl 3) + (firstByte.toInt() ushr 5) + numBytes
            }
            else -> throw IonException("FlexUInt value too large to find in an Int")
        }
    }

    /**
     * Returns the length of the flexuint + the value of the flexuint. Useful for skipping length-prefixed values.
     */
    @JvmStatic
    fun getLengthPlusValueOfFlexUIntAt(source: ByteArray, position: Int): Int {
        val firstByte = source.get(position)
        val numBytes = firstByte.countTrailingZeroBits() + 1
        when (numBytes) {
            1 -> {
                return ((firstByte.toInt() and 0xFF) ushr 1) + numBytes
            }
            2, 3, 4 -> {
                // TODO: We could probably simplify some of these calculations.
                val backtrack = 4 - numBytes
                val data = source.getInt(position - backtrack)
                val shiftAmount = 8 * backtrack + numBytes
                return (data ushr shiftAmount) + numBytes
            }
            5 -> {
                val data = source.getInt(position + 1)
                val data1 = (data shl 3) ushr 3
                if (data != data1) throw IonException("FlexUInt value too large to find in an Int")
                return (data shl 3) + (firstByte.toInt() ushr 5) + numBytes
            }
            else -> throw IonException("FlexUInt value too large to find in an Int")
        }
    }

    @JvmStatic
    fun lengthOfFlexUIntAt(source: ByteBuffer, position: Int): Int {
        var firstByte = source.get(position)
        var i = 0
        while (firstByte == 0.toByte()) {
            firstByte = source.get(position + i)
            i++
        }
        return firstByte.countTrailingZeroBits() + 1 + (i * 8)
    }

    @JvmStatic
    fun lengthOfFlexUIntAt(source: ByteArray, position: Int): Int {
        var firstByte = source[position]
        var i = 0
        while (firstByte == 0.toByte()) {
            firstByte = source[position + i]
            i++
        }
        return firstByte.countTrailingZeroBits() + 1 + (i * 8)
    }

    @JvmStatic
    fun readFlexIntAt(source: ByteBuffer, position: Int): Int {
        val firstByte = source.get(position)
        val numBytes = firstByte.countTrailingZeroBits() + 1
        when (numBytes) {
            1 -> {
                return (firstByte.toInt() shr 1)
            }
            2, 3, 4 -> {
                // TODO: We could probably simplify some of these calculations.
                val backtrack = 4 - numBytes
                val data = source.getInt(position - backtrack)
                val shiftAmount = 8 * backtrack + numBytes
                return (data shr shiftAmount)
            }
            5 -> {
                val data = source.getInt(position + 1)
                val data1 = (data shl 4) shr 4
                if (data != data1) throw IonException("FlexUInt value too large to find in an Int")
                return (data shl 3) + (firstByte.toInt() ushr 5)
            }
            else -> throw IonException("FlexUInt value too large to find in an Int")
        }
    }

    /**
     * This employs some hackery. The result contains both the value and the length.
     * The 8 low order bits are the length, and the other bits are the value.
     */
    @JvmStatic
    fun readFlexIntValueAndLengthAt(source: ByteArray, position: Int): Long {
        val firstByte = source.get(position)
        val numBytes = firstByte.countTrailingZeroBits() + 1
        val value = when (numBytes) {
            1 -> {
                (firstByte.toInt() shr 1)
            }
            2, 3, 4 -> {
                // TODO: We could probably simplify some of these calculations.
                val backtrack = 4 - numBytes
                val data = source.getInt(position - backtrack)
                val shiftAmount = 8 * backtrack + numBytes
                (data shr shiftAmount)
            }
            5 -> {
                val data = source.getInt(position + 1)
                val dataWithout3HighOrderBits = (data shl 3) shr 3
                if (data != dataWithout3HighOrderBits) throw IonException("FlexUInt value too large to find in an Int")
                (data shl 3) + (firstByte.toInt().and(0xFF) ushr 5)
            }
            else -> throw IonException("FlexInt value too large to find in an Int")
        }
        return ((value.toLong() shl 32) shr 24) or numBytes.toLong()
    }

    @JvmStatic
    fun readFlexIntLongValue(source: ByteArray, position: Int): Long {
        val firstByte = source.get(position)
        val numBytes = firstByte.countTrailingZeroBits() + 1
        val value = when (numBytes) {
            1 -> {
                (firstByte.toInt() shr 1).toLong()
            }
            2, 3, 4 -> {
                // TODO: We could probably simplify some of these calculations.
                val backtrack = 4 - numBytes
                val data = source.getInt(position - backtrack)
                val shiftAmount = 8 * backtrack + numBytes
                (data shr shiftAmount).toLong()
            }
            5, 6, 7, 8 -> {
                val backtrack = 8 - numBytes
                val data = source.getLong(position - backtrack)
                val shiftAmount = 8 * backtrack + numBytes
                (data shr shiftAmount)
            }
            else -> {
                val numBytes = source[position + 1].countTrailingZeroBits() + 9
                when (numBytes) {
                    9 -> {
                        val value = readFixedIntAt(source, position + 1, 8)
                        value shr 1
                    }
                    10 -> {
                        // This could fit, but maybe we don't need it yet.
                        TODO("10")
                    }
                    else -> throw IonException("FlexInt value too large to find in a Long")
                }
            }
        }
        return value
    }

    @JvmStatic
    fun readFlexIntBigIntegerValue(source: ByteArray, position: Int): BigInteger {
        // FIXME
        var p = position
        var firstByte = 0.toByte()
        var i = 0
        while (firstByte == 0.toByte()) {
            firstByte = source[p++]
            i++
        }
        val length = firstByte.countTrailingZeroBits() + 1 + ((i - 1) * 8)
        var numberOfDataBytesRemaining = length - i
        val dataBytes = ByteArray(numberOfDataBytesRemaining + 1)
        val rightShiftAmount = (length.mod(8))
        val leftShiftAmount = (8 - rightShiftAmount)
        val mask = (1 shl leftShiftAmount) - 1

        dataBytes[numberOfDataBytesRemaining] = firstByte.toInt().shr(rightShiftAmount).toByte()
        while (numberOfDataBytesRemaining > 0) {
            val currentByte = source[p++]
            dataBytes[numberOfDataBytesRemaining] = dataBytes[numberOfDataBytesRemaining].toInt().and(mask).or(currentByte.toInt().shl(leftShiftAmount)).toByte()
            dataBytes[numberOfDataBytesRemaining - 1] = currentByte.toInt().shr(rightShiftAmount).toByte()
            numberOfDataBytesRemaining--
        }
        return BigInteger(dataBytes)
    }

    @JvmStatic
    fun readFlexInt(source: ByteBuffer): Int {
        val position = source.position()
        val firstByte = source.get().toInt() and 0xFF
        val numBytes = firstByte.countTrailingZeroBits() + 1
        if (source.remaining() < numBytes) { throw IonException("Incomplete data at $position") }
        source.position(position + numBytes)
        val value = when (numBytes) {
            1 -> {
                (firstByte.toInt() shr 1)
            }
            2 -> {
                (firstByte.toInt() shr 2) or
                    source.get(position + 1).toInt().and(0xFF).shl(6)
            }
            3 -> {
                (firstByte.toInt() shr 3) or
                    source.get(position + 1).toInt().and(0xFF).shl(5) or
                    source.get(position + 2).toInt().and(0xFF).shl(13)
            }
            4 -> {
                (firstByte.toInt() shr 4) or
                    source.get(position + 1).toInt().and(0xFF).shl(4) or
                    source.get(position + 2).toInt().and(0xFF).shl(12) or
                    source.get(position + 3).toInt().and(0xFF).shl(20)
            }
            5 -> {
                val data = source.getInt(position + 1)
                val dataWithout3HighOrderBits = (data shl 3) shr 3
                if (data != dataWithout3HighOrderBits) throw IonException("FlexUInt value too large to find in an Int")
                (data shl 3) + (firstByte.toInt().and(0xFF) ushr 5)
            }
            else -> throw IonException("FlexInt value too large to find in an Int")
        }
        return value
    }
}
