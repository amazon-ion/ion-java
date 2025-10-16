// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11.bytearray

import com.amazon.ion.IonException
import java.math.BigInteger

/**
 * Helper class containing methods for reading `FixedInt`, `FixedUInt`, `FlexInt`, and `FlexUInt` from [ByteArray]s.
 */
internal object PrimitiveDecoder {

    /** Constant for shifting by one byte */
    private const val ONE_BYTE = 8

    // Masks
    private const val BYTE_BIT_MASK = 0xFF
    private const val BYTE_BIT_MASK_L = 0xFFL
    private const val INT_BIT_MASK = 0xFF_FF_FF_FFL
    private const val BYTE_BIT_MASK_UL = 0xFFuL

    @JvmStatic
    fun readFixedInt8AsShort(source: ByteArray, start: Int): Short {
        // TODO: ion-java#1114
        if (source.size < start + 1) throw IonException("Incomplete data: start=$start, length=1, limit=${source.size}")
        return source[start].toShort()
    }

    @JvmStatic
    fun readFixedInt16(source: ByteArray, start: Int): Short {
        // TODO: ion-java#1114
        if (source.size < start + 2) throw IonException("Incomplete data: start=$start, length=2, limit=${source.size}")
        return read2IntBytes(source[start], source, start).shr(16).toShort()
    }

    @JvmStatic
    fun readFixedInt24AsInt(source: ByteArray, start: Int): Int {
        // TODO: ion-java#1114
        if (source.size < start + 3) throw IonException("Incomplete data: start=$start, length=3, limit=${source.size}")
        return read3IntBytes(source[start], source, start).shr(8)
    }

    @JvmStatic
    fun readFixedInt32(source: ByteArray, start: Int): Int {
        // TODO: ion-java#1114
        if (source.size < start + 4) throw IonException("Incomplete data: start=$start, length=4, limit=${source.size}")
        return read4IntBytes(source[start], source, start)
    }

    @JvmStatic
    fun readFixedInt64(source: ByteArray, start: Int): Long {
        // TODO: ion-java#1114
        if (source.size < start + 4) throw IonException("Incomplete data: start=$start, length=4, limit=${source.size}")
        return read8IntBytes(source[start], source, start)
    }

    @JvmStatic
    fun readFixedIntAsLong(source: ByteArray, start: Int, length: Int): Long {
        // TODO: ion-java#1114
        if (source.size < start + length) throw IonException("Incomplete data: start=$start, length=$length, limit=${source.size}")
        val firstByte = source[start]
        return when (length) {
            1 -> firstByte.toLong()
            2 -> read2IntBytes(firstByte, source, start).shr(16).toLong()
            3 -> read3IntBytes(firstByte, source, start).shr(8).toLong()
            4 -> read4IntBytes(firstByte, source, start).toLong()
            5 -> read5IntBytes(firstByte, source, start).shr(24)
            6 -> read6IntBytes(firstByte, source, start).shr(16)
            7 -> read7IntBytes(firstByte, source, start).shr(8)
            8 -> read8IntBytes(firstByte, source, start)
            // TODO: Technically, it's possible that the FixedInt is over-padded with 0-bytes, but we can deal with that later.
            else -> throw IonException("FixedInt with length $length is too large to fit in a Long")
        }
    }

    @JvmStatic
    fun readFixedUInt16(source: ByteArray, position: Int): UShort {
        return read2IntBytes(source[position], source, position).shr(16).toUShort()
    }

    @JvmStatic
    fun readFixedUInt32(source: ByteArray, position: Int): UInt {
        return read4IntBytes(source[position], source, position).toUInt()
    }

    @JvmStatic
    fun readFixedUInt64(source: ByteArray, position: Int): ULong {
        return read8IntBytes(source[position], source, position).toULong()
    }

    // ==== FLEX INT AND UINT FUNCTIONS ==== //

    @JvmStatic
    fun lengthOfFlexIntOrUIntAt(source: ByteArray, position: Int): Int {
        var i = position
        var continuationByte = source[i++]
        var length = 1
        while (continuationByte == 0.toByte()) {
            length += 8
            continuationByte = source[i++]
        }
        return length + continuationByte.countTrailingZeroBits()
    }

    /**
     * Returns both the [Int] value and the length of the FlexInt, packed into a [Long].
     *
     * ```
     *      ┌─ Length   ┌── Value
     * ┌────┴────┐ ┌────┴────┐
     * 00 00 00 00 00 00 00 00
     * ```
     *
     * _Why are we doing this?_ So that we can return both the value and the length from one method call without having to
     * allocate an object to do so.
     *
     * To get the value and length:
     * ```
     * val valueAndLength = readFlexIntValueAndLength(source, position)
     * val value = valueAndLength.toInt()
     * val length = valueAndLength.shr(Int.SIZE_BITS).toInt()
     * ```
     * If you only need the value:
     * ```
     * readFlexIntValueAndLength(source, position).toInt()
     * ```
     */
    @JvmStatic
    fun readFlexIntValueAndLength(source: ByteArray, position: Int): Long {
        val firstByte = source[position]
        var numBytes = firstByte.countTrailingZeroBits() + 1
        val value = when (numBytes) {
            1 -> firstByte.toInt().shr(1)
            // These `shr` amounts are not arbitrary. See the `read*IntBytes` method documentation.
            2 -> read2IntBytes(firstByte, source, position).shr(18)
            3 -> read3IntBytes(firstByte, source, position).shr(11)
            4 -> read4IntBytes(firstByte, source, position).shr(4)
            5 -> {
                val longValue = read5IntBytes(firstByte, source, position).shr(29)
                val intValue = longValue.toInt()
                if (intValue.toLong() != longValue) {
                    throw IonException("FlexInt value too large to find in an Int")
                }
                intValue
            }
            else -> {
                // This is only possible if the FlexInt is over-padded or can't fit in an Int,
                // so we don't care about the performance here.
                numBytes = lengthOfFlexIntOrUIntAt(source, position)
                val bigInt = readFlexIntAsBigInteger(source, position)
                if (bigInt.bitLength() >= Int.SIZE_BITS) throw IonException("FlexInt value too large to find in an Int")
                bigInt.intValueExact()
            }
        }
        return value.toLong().and(INT_BIT_MASK) or numBytes.toLong().shl(Int.SIZE_BITS)
    }

    @JvmStatic
    fun readFlexIntAsLong(source: ByteArray, position: Int): Long {
        val firstByte = source.get(position)
        val numBytes = firstByte.countTrailingZeroBits() + 1
        val value = when (numBytes) {
            1 -> firstByte.toLong().shr(1)
            // These `shr` amounts are not arbitrary. See the `read*IntBytes` method documentation.
            2 -> read2IntBytes(firstByte, source, position).shr(18).toLong()
            3 -> read3IntBytes(firstByte, source, position).shr(11).toLong()
            4 -> read4IntBytes(firstByte, source, position).shr(4).toLong()
            5 -> read5IntBytes(firstByte, source, position).shr(29)
            6 -> read6IntBytes(firstByte, source, position).shr(22)
            7 -> read7IntBytes(firstByte, source, position).shr(15)
            8 -> read8IntBytes(firstByte, source, position).shr(8)
            else -> {
                val bigInt = readFlexIntAsBigInteger(source, position)
                // bitLength() does not include a sign bit.
                if (bigInt.bitLength() >= Long.SIZE_BITS) throw IonException("FlexInt value too large to find in an Long")
                bigInt.longValueExact()
            }
        }
        return value
    }

    /**
     * Does exactly what the function name says. This is slow. This method should rarely, if ever, be invoked.
     */
    @JvmStatic
    fun readFlexIntAsBigInteger(source: ByteArray, position: Int): BigInteger {
        val length = lengthOfFlexIntOrUIntAt(source, position)
        val bytes = readExtraLongFlexIntOrUIntBytes(source, position)
        return BigInteger(bytes).shiftRight(length)
    }

    /**
     * Returns both the [Int] value and the length of the FlexUInt, packed into a [Long].
     *
     * ```
     *      ┌─ Length   ┌── Value
     * ┌────┴────┐ ┌────┴────┐
     * 00 00 00 00 00 00 00 00
     * ```
     *
     * _Why are we doing this?_ So that we can return both the value and the length from one method call without having to
     * allocate an object to do so.
     *
     * To get the value and length:
     * ```
     * val valueAndLength = readFlexUIntValueAndLength(source, position)
     * val value = valueAndLength.toInt()
     * val length = valueAndLength.shr(Int.SIZE_BITS).toInt()
     * ```
     * If you only need the value:
     * ```
     * readFlexUIntValueAndLength(source, position).toInt()
     * ```
     */
    @JvmStatic
    fun readFlexUIntValueAndLength(source: ByteArray, position: Int): Long {
        val firstByte = source[position]
        var numBytes = firstByte.countTrailingZeroBits() + 1
        val value = when (numBytes) {
            1 -> firstByte.toInt().and(BYTE_BIT_MASK).ushr(1)
            // These `shr` amounts are not arbitrary. See the `read*IntBytes` method documentation.
            2 -> read2IntBytes(firstByte, source, position).ushr(18)
            3 -> read3IntBytes(firstByte, source, position).ushr(11)
            4 -> read4IntBytes(firstByte, source, position).ushr(4)
            5 -> {
                val longValue = read5IntBytes(firstByte, source, position).ushr(29)
                val intValue = longValue.toInt()
                if (intValue.toLong() != longValue) {
                    throw IonException("FlexUInt value too large to find in an Int")
                }
                intValue
            }
            else -> {
                // This is only possible if the FlexUInt is over-padded or can't fit in an Int,
                // so we don't care about the performance here.
                numBytes = lengthOfFlexIntOrUIntAt(source, position)
                val bigInt = readFlexUIntAsBigInteger(source, position)
                if (bigInt.bitLength() >= Int.SIZE_BITS) throw IonException("FlexUInt value too large to find in an Int")
                bigInt.intValueExact()
            }
        }
        return value.toLong().and(INT_BIT_MASK) or numBytes.toLong().shl(Int.SIZE_BITS)
    }

    /**
     * Reads a FlexUInt as a ULong. Throws if value is too large for a ULong.
     */
    @JvmStatic
    fun readFlexUIntAsULong(source: ByteArray, position: Int): ULong {
        val firstByte = source[position]
        val numBytes = firstByte.countTrailingZeroBits() + 1
        val value = when (numBytes) {
            1 -> firstByte.toULong().and(BYTE_BIT_MASK_UL).shr(1)
            // These `shr` amounts are not arbitrary. See the `read*IntBytes` method documentation.
            2 -> read2IntBytes(firstByte, source, position).ushr(18).toULong()
            3 -> read3IntBytes(firstByte, source, position).ushr(11).toULong()
            4 -> read4IntBytes(firstByte, source, position).ushr(4).toULong()
            5 -> read5IntBytes(firstByte, source, position).ushr(29).toULong()
            6 -> read6IntBytes(firstByte, source, position).ushr(22).toULong()
            7 -> read7IntBytes(firstByte, source, position).ushr(15).toULong()
            8 -> read8IntBytes(firstByte, source, position).ushr(8).toULong()
            else -> {
                val bigInt = readFlexUIntAsBigInteger(source, position)
                // bitLength() does not include a sign bit.
                if (bigInt.bitLength() > Long.SIZE_BITS) throw IonException("FlexInt value too large to find in a ULong")
                bigInt.toLong().toULong()
            }
        }
        return value
    }

    /**
     * Does exactly what the function name says. This is slow. This method should rarely, if ever, be invoked.
     */
    @JvmStatic
    fun readFlexUIntAsBigInteger(source: ByteArray, position: Int): BigInteger {
        val length = lengthOfFlexIntOrUIntAt(source, position)
        val bytes = readExtraLongFlexIntOrUIntBytes(source, position)
        return BigInteger(1, bytes).shiftRight(length)
    }

    // ==== READ HELPERS ==== //

    /**
     * If reading a FlexInt or FlexUInt, the result is `DDDDDDDD DDDDDDCC XXXXXXXX XXXXXXXX`,
     * where `D`, `C`, and `X` denote data bits, continuation bits, and unused bits (respectively).
     * For a FixedInt or FixedUInt, both `D` and `C` represent data bits.
     *
     * To get the final value, shift right (either signed or unsigned) so that only the data bits are remaining.
     *
     * To convert the result to:
     * - FlexInt `shr 18`
     * - FlexUInt `ushr 18`
     * - FixedInt `shr 16`
     * - FixedUInt `ushr 16`
     */
    @JvmStatic
    private fun read2IntBytes(firstByte: Byte, source: ByteArray, position: Int): Int {
        var result = firstByte.toInt().and(BYTE_BIT_MASK).shl(2 * ONE_BYTE)
        result = result.or(source[position + 1].toInt().and(BYTE_BIT_MASK).shl(3 * ONE_BYTE))
        return result
    }

    /**
     * If reading a FlexInt or FlexUInt, the result is `DDDDDDDD DDDDDDDD DDDDDCCC XXXXXXXX`,
     * where `D`, `C`, and `X` denote data bits, continuation bits, and unused bits (respectively).
     * For a FixedInt or FixedUInt, both `D` and `C` represent data bits.
     *
     * To get the final value, shift right (either signed or unsigned) so that only the data bits are remaining.
     * - FlexInt `shr 11`
     * - FlexUInt `ushr 11`
     * - FixedInt `shr 8`
     * - FixedUInt `ushr 8`
     */
    @JvmStatic
    private fun read3IntBytes(firstByte: Byte, source: ByteArray, position: Int): Int {
        var result = firstByte.toInt().and(BYTE_BIT_MASK).shl(1 * ONE_BYTE)
        result = result.or(source[position + 1].toInt().and(BYTE_BIT_MASK).shl(2 * ONE_BYTE))
        result = result.or(source[position + 2].toInt().and(BYTE_BIT_MASK).shl(3 * ONE_BYTE))
        return result
    }

    /**
     * If reading a FlexInt or FlexUInt, the result is `DDDDDDDD DDDDDDDD DDDDDDDD DDDDCCCC`,
     * where `D`, `C`, and `X` denote data bits, continuation bits, and unused bits (respectively).
     * For a FixedInt or FixedUInt, both `D` and `C` represent data bits.
     *
     * To get the final value, shift right (either signed or unsigned) so that only the data bits are remaining.
     * - FlexInt `shr 4`
     * - FlexUInt `ushr 4`
     * - FixedInt: do nothing
     * - FixedUInt: convert to Long and mask to 4 bytes
     */
    @JvmStatic
    private fun read4IntBytes(firstByte: Byte, source: ByteArray, position: Int): Int {
        var result = firstByte.toInt().and(BYTE_BIT_MASK)
        result = result.or(source[position + 1].toInt().and(BYTE_BIT_MASK).shl(1 * ONE_BYTE))
        result = result.or(source[position + 2].toInt().and(BYTE_BIT_MASK).shl(2 * ONE_BYTE))
        result = result.or(source[position + 3].toInt().and(BYTE_BIT_MASK).shl(3 * ONE_BYTE))
        return result
    }

    /**
     * If reading a FlexInt or FlexUInt, the result is `DDDDDDDD DDDDDDDD DDDDDDDD DDDDDDDD DDDCCCCC XXXXXXXX XXXXXXXX XXXXXXXX`,
     * where `D`, `C`, and `X` denote data bits, continuation bits, and unused bits (respectively).
     * For a FixedInt or FixedUInt, both `D` and `C` represent data bits.
     *
     * To get the final value, shift right (either signed or unsigned) so that only the data bits are remaining.
     * - FlexInt `shr 29`
     * - FlexUInt `ushr 29`
     * - FixedInt: `shr 24`
     * - FixedUInt: `ushr 24`
     */
    @JvmStatic
    private fun read5IntBytes(firstByte: Byte, source: ByteArray, position: Int): Long {
        var result = firstByte.toLong().and(BYTE_BIT_MASK_L).shl(3 * ONE_BYTE)
        result = result.or(source[position + 1].toLong().and(BYTE_BIT_MASK_L).shl(4 * ONE_BYTE))
        result = result.or(source[position + 2].toLong().and(BYTE_BIT_MASK_L).shl(5 * ONE_BYTE))
        result = result.or(source[position + 3].toLong().and(BYTE_BIT_MASK_L).shl(6 * ONE_BYTE))
        result = result.or(source[position + 4].toLong().and(BYTE_BIT_MASK_L).shl(7 * ONE_BYTE))
        return result
    }

    /**
     * If reading a FlexInt or FlexUInt, the result is `DDDDDDDD DDDDDDDD DDDDDDDD DDDDDDDD DDDDDDDD DDCCCCCC XXXXXXXX XXXXXXXX`,
     * where `D`, `C`, and `X` denote data bits, continuation bits, and unused bits (respectively).
     * For a FixedInt or FixedUInt, both `D` and `C` represent data bits.
     *
     * To get the final value, shift right (either signed or unsigned) so that only the data bits are remaining.
     * - FlexInt `shr 22`
     * - FlexUInt `ushr 22`
     * - FixedInt: `shr 16`
     * - FixedUInt: `ushr 16`
     */
    @JvmStatic
    private fun read6IntBytes(firstByte: Byte, source: ByteArray, position: Int): Long {
        var result = firstByte.toLong().and(BYTE_BIT_MASK_L).shl(2 * ONE_BYTE)
        result = result.or(source[position + 1].toLong().and(BYTE_BIT_MASK_L).shl(3 * ONE_BYTE))
        result = result.or(source[position + 2].toLong().and(BYTE_BIT_MASK_L).shl(4 * ONE_BYTE))
        result = result.or(source[position + 3].toLong().and(BYTE_BIT_MASK_L).shl(5 * ONE_BYTE))
        result = result.or(source[position + 4].toLong().and(BYTE_BIT_MASK_L).shl(6 * ONE_BYTE))
        result = result.or(source[position + 5].toLong().and(BYTE_BIT_MASK_L).shl(7 * ONE_BYTE))
        return result
    }

    /**
     * If reading a FlexInt or FlexUInt, the result is `DDDDDDDD DDDDDDDD DDDDDDDD DDDDDDDD DDDDDDDD DDDDDDDD DCCCCCCC XXXXXXXX`,
     * where `D`, `C`, and `X` denote data bits, continuation bits, and unused bits (respectively).
     * For a FixedInt or FixedUInt, both `D` and `C` represent data bits.
     *
     * To get the final value, shift right (either signed or unsigned) so that only the data bits are remaining.
     * - FlexInt `shr 15`
     * - FlexUInt `ushr 15`
     * - FixedInt: `shr 8`
     * - FixedUInt: `ushr 8`
     */
    @JvmStatic
    private fun read7IntBytes(firstByte: Byte, source: ByteArray, position: Int): Long {
        var result = firstByte.toLong().and(BYTE_BIT_MASK_L).shl(1 * ONE_BYTE)
        result = result.or(source[position + 1].toLong().and(BYTE_BIT_MASK_L).shl(2 * ONE_BYTE))
        result = result.or(source[position + 2].toLong().and(BYTE_BIT_MASK_L).shl(3 * ONE_BYTE))
        result = result.or(source[position + 3].toLong().and(BYTE_BIT_MASK_L).shl(4 * ONE_BYTE))
        result = result.or(source[position + 4].toLong().and(BYTE_BIT_MASK_L).shl(5 * ONE_BYTE))
        result = result.or(source[position + 5].toLong().and(BYTE_BIT_MASK_L).shl(6 * ONE_BYTE))
        result = result.or(source[position + 6].toLong().and(BYTE_BIT_MASK_L).shl(7 * ONE_BYTE))
        return result
    }

    /**
     * If reading a FlexInt or FlexUInt, the result is `DDDDDDDD DDDDDDDD DDDDDDDD DDDDDDDD DDDDDDDD DDDDDDDD DDDDDDDD CCCCCCCC`,
     * where `D`, `C`, and `X` denote data bits, continuation bits, and unused bits (respectively).
     * For a FixedInt or FixedUInt, both `D` and `C` represent data bits.
     *
     * To get the final value, shift right (either signed or unsigned) so that only the data bits are remaining.
     * - FlexInt `shr 8`
     * - FlexUInt `ushr 8`
     * - FixedInt: do nothing, it's already fine.
     * - FixedUInt: if the most significant but is `1`, convert to [ULong] or [BigInteger]
     */
    @JvmStatic
    private fun read8IntBytes(firstByte: Byte, source: ByteArray, position: Int): Long {
        var result = firstByte.toLong().and(BYTE_BIT_MASK_L)
        result = result.or(source[position + 1].toLong().and(BYTE_BIT_MASK_L).shl(1 * ONE_BYTE))
        result = result.or(source[position + 2].toLong().and(BYTE_BIT_MASK_L).shl(2 * ONE_BYTE))
        result = result.or(source[position + 3].toLong().and(BYTE_BIT_MASK_L).shl(3 * ONE_BYTE))
        result = result.or(source[position + 4].toLong().and(BYTE_BIT_MASK_L).shl(4 * ONE_BYTE))
        result = result.or(source[position + 5].toLong().and(BYTE_BIT_MASK_L).shl(5 * ONE_BYTE))
        result = result.or(source[position + 6].toLong().and(BYTE_BIT_MASK_L).shl(6 * ONE_BYTE))
        result = result.or(source[position + 7].toLong().and(BYTE_BIT_MASK_L).shl(7 * ONE_BYTE))
        return result
    }

    /**
     * Reads all the bytes of a FlexInt or FlexUInt into a ByteArray. Caller must choose whether to interpret the result
     * as signed or unsigned and construct a BigInteger accordingly. Then caller must shift right by the number of bytes
     * in the result from this method.
     *
     * See [readFlexIntAsBigInteger] and [readFlexUIntAsBigInteger] for how it is used in practice.
     */
    @JvmStatic
    private fun readExtraLongFlexIntOrUIntBytes(source: ByteArray, position: Int): ByteArray {
        var i = position
        var continuationByte = source[i++]
        var length = 1
        while (continuationByte == 0.toByte()) {
            length += 8
            continuationByte = source[i++]
        }
        length += continuationByte.countTrailingZeroBits()

        val bytes = ByteArray(length)
        var numRemainingBytes = length - (i - position)
        bytes[numRemainingBytes--] = continuationByte
        while (numRemainingBytes >= 0) {
            bytes[numRemainingBytes--] = source[i++]
        }
        return bytes
    }
}
