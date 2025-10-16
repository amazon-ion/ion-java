// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.bin

import com.amazon.ion.IonException
import java.math.BigInteger

/**
 * Functions for encoding and decoding FlexInts and FlexUInts.
 *
 * TODO(perf): See if performance can be improved with specialized implementations for writing FlexInt and FlexUInt
 *             that accept an `Int` instead of a `Long`.
 */
object FlexInt {

    /** Constant for shifting by one byte */
    private const val ONE_BYTE = 8

    // Masks
    private const val BYTE_BIT_MASK = 0xFF
    private const val BYTE_BIT_MASK_L = 0xFFL
    private const val INT_BIT_MASK = 0xFF_FF_FF_FFL

    /**
     * A byte representing zero, encoded as a FlexInt (or FlexUInt).
     */
    const val ZERO: Byte = 0x01

    /** Determine the length of FlexUInt for the provided value.  */
    @JvmStatic
    fun flexUIntLength(value: Long): Int {
        val numLeadingZeros = java.lang.Long.numberOfLeadingZeros(value)
        val numMagnitudeBitsRequired = 64 - numLeadingZeros
        return (numMagnitudeBitsRequired - 1) / 7 + 1
    }

    /** Determine the length of FlexInt for the provided value.  */
    @JvmStatic
    fun flexIntLength(value: Long): Int {
        val numMagnitudeBitsRequired: Int = if (value < 0) {
            val numLeadingOnes = java.lang.Long.numberOfLeadingZeros(value.inv())
            64 - numLeadingOnes
        } else {
            val numLeadingZeros = java.lang.Long.numberOfLeadingZeros(value)
            64 - numLeadingZeros
        }
        return numMagnitudeBitsRequired / 7 + 1
    }

    @JvmStatic
    private fun writeFlexIntOrUInt3Into(data: ByteArray, offset: Int, value: Long) {
        data[offset] = (0x04L or (value shl 3)).toByte()
        data[offset + 1] = (value shr 5).toByte()
        data[offset + 2] = (value shr 13).toByte()
    }

    @JvmStatic
    private fun writeFlexIntOrUInt4Into(data: ByteArray, offset: Int, value: Long) {
        data[offset] = (0x08L or (value shl 4)).toByte()
        data[offset + 1] = (value shr 4).toByte()
        data[offset + 2] = (value shr 12).toByte()
        data[offset + 3] = (value shr 20).toByte()
    }

    @JvmStatic
    private fun writeFlexIntOrUInt5Into(data: ByteArray, offset: Int, value: Long) {
        data[offset] = (0x10L or (value shl 5)).toByte()
        data[offset + 1] = (value shr 3).toByte()
        data[offset + 2] = (value shr 11).toByte()
        data[offset + 3] = (value shr 19).toByte()
        data[offset + 4] = (value shr 27).toByte()
    }

    @JvmStatic
    private fun writeFlexIntOrUInt6Into(data: ByteArray, offset: Int, value: Long) {
        data[offset] = (0x20L or (value shl 6)).toByte()
        data[offset + 1] = (value shr 2).toByte()
        data[offset + 2] = (value shr 10).toByte()
        data[offset + 3] = (value shr 18).toByte()
        data[offset + 4] = (value shr 26).toByte()
        data[offset + 5] = (value shr 34).toByte()
    }

    @JvmStatic
    private fun writeFlexIntOrUInt7Into(data: ByteArray, offset: Int, value: Long) {
        data[offset] = (0x40L or (value shl 7)).toByte()
        data[offset + 1] = (value shr 1).toByte()
        data[offset + 2] = (value shr 9).toByte()
        data[offset + 3] = (value shr 17).toByte()
        data[offset + 4] = (value shr 25).toByte()
        data[offset + 5] = (value shr 33).toByte()
        data[offset + 6] = (value shr 41).toByte()
    }

    @JvmStatic
    private fun writeFlexIntOrUInt8Into(data: ByteArray, offset: Int, value: Long) {
        data[offset] = 0x80.toByte()
        data[offset + 1] = (value shr 0).toByte()
        data[offset + 2] = (value shr 8).toByte()
        data[offset + 3] = (value shr 16).toByte()
        data[offset + 4] = (value shr 24).toByte()
        data[offset + 5] = (value shr 32).toByte()
        data[offset + 6] = (value shr 40).toByte()
        data[offset + 7] = (value shr 48).toByte()
    }

    @JvmStatic
    private fun writeFlexIntOrUInt9Into(data: ByteArray, offset: Int, value: Long) {
        data[offset] = 0
        data[offset + 1] = (0x01L or (value shl 1)).toByte()
        data[offset + 2] = (value shr 7).toByte()
        data[offset + 3] = (value shr 15).toByte()
        data[offset + 4] = (value shr 23).toByte()
        data[offset + 5] = (value shr 31).toByte()
        data[offset + 6] = (value shr 39).toByte()
        data[offset + 7] = (value shr 47).toByte()
        data[offset + 8] = (value shr 55).toByte()
    }

    @JvmStatic
    private fun writeFlexIntOrUInt10Into(data: ByteArray, offset: Int, value: Long) {
        data[offset] = 0
        data[offset + 1] = (0x02L or (value shl 2)).toByte()
        data[offset + 2] = (value shr 6).toByte()
        data[offset + 3] = (value shr 14).toByte()
        data[offset + 4] = (value shr 22).toByte()
        data[offset + 5] = (value shr 30).toByte()
        data[offset + 6] = (value shr 38).toByte()
        data[offset + 7] = (value shr 46).toByte()
        data[offset + 8] = (value shr 54).toByte()
        data[offset + 9] = (value shr 62).toByte()
    }

    /**
     * Writes a FlexInt or FlexUInt encoding of [value] into [data] starting at [offset].
     * Use [flexIntLength] or [flexUIntLength] to get the value for the [numBytes] parameter.
     *
     * The length and write functions are separate so that callers can make decisions or
     * compute other values based on the encoded size of the value.
     */
    @JvmStatic
    fun writeFlexIntOrUIntInto(data: ByteArray, offset: Int, value: Long, numBytes: Int) {
        when (numBytes) {
            1 -> data[offset] = (0x01L or (value shl 1)).toByte()
            2 -> {
                data[offset] = (0x02L or (value shl 2)).toByte()
                data[offset + 1] = (value shr 6).toByte()
            }
            3 -> writeFlexIntOrUInt3Into(data, offset, value)
            4 -> writeFlexIntOrUInt4Into(data, offset, value)
            5 -> writeFlexIntOrUInt5Into(data, offset, value)
            6 -> writeFlexIntOrUInt6Into(data, offset, value)
            7 -> writeFlexIntOrUInt7Into(data, offset, value)
            8 -> writeFlexIntOrUInt8Into(data, offset, value)
            9 -> writeFlexIntOrUInt9Into(data, offset, value)
            10 -> writeFlexIntOrUInt10Into(data, offset, value)
            else -> throw IllegalArgumentException("Long values can always be encoded as a FlexInt using 10 bytes or fewer")
        }
    }

    /** Determine the length of FlexUInt for the provided value.  */
    @JvmStatic
    fun flexUIntLength(value: BigInteger): Int {
        return (value.bitLength() - 1) / 7 + 1
    }

    /** Determine the length of FlexInt for the provided value.  */
    @JvmStatic
    fun flexIntLength(value: BigInteger): Int {
        return value.bitLength() / 7 + 1
    }

    /**
     * Writes a FlexInt or FlexUInt encoding of [value] into [data] starting at [offset].
     * Use [flexIntLength] or [flexUIntLength] to get the value for the [numBytes] parameter.
     */
    @JvmStatic
    fun writeFlexIntOrUIntInto(data: ByteArray, offset: Int, value: BigInteger, numBytes: Int) {
        // TODO: Should we branch to the implementation for long if the number is small enough?
        // https://github.com/amazon-ion/ion-java/issues/614
        val valueBytes = value.toByteArray()
        var i = 0 // `i` gets incremented for every byte written.

        // Start with leading zero bytes.
        // If there's 1-8 total bytes, we need no leading zero-bytes.
        // If there's 9-16 total bytes, we need one zero-byte
        // If there's 17-24 total bytes, we need two zero-bytes, etc.
        while (i < (numBytes - 1) / 8) {
            data[offset + i] = 0
            i++
        }

        // Write the last length bits, possibly also containing some value bits.
        val remainingLengthBits = (numBytes - 1) % 8
        val lengthPart = (0x01 shl remainingLengthBits).toByte()
        val valueBitOffset = remainingLengthBits + 1
        val valuePart = (valueBytes[valueBytes.size - 1].toInt() shl valueBitOffset).toByte()
        data[offset + i] = (valuePart.toInt() or lengthPart.toInt()).toByte()
        i++
        for (valueByteOffset in valueBytes.size - 1 downTo 1) {
            // Technically it's only a nibble if the bitOffset is 4, so we call it nibble-ish
            val highNibbleIsh = (valueBytes[valueByteOffset - 1].toInt() shl valueBitOffset).toByte()
            val lowNibbleIsh = (valueBytes[valueByteOffset].toInt() and 0xFF shr 8 - valueBitOffset).toByte()
            data[offset + i] = (highNibbleIsh.toInt() or lowNibbleIsh.toInt()).toByte()
            i++
        }
        if (i < numBytes) {
            data[offset + i] = (valueBytes[0].toInt() shr 8 - valueBitOffset).toByte()
        }
    }

    // ==== READ METHODS ==== //

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
