// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.bin

import java.math.BigInteger

/**
 * Functions for encoding Ion 1.1 primitives (FlexInts, FlexUInts, FlexSym, etc.) to [ByteArray]s.
 *
 * TODO(perf): See if performance can be improved with specialized implementations for writing FlexInt and FlexUInt
 *             that accept an `Int` instead of a `Long`.
 */
object PrimitiveEncoder {

    /**
     * A byte representing zero, encoded as a FlexInt (or FlexUInt).
     */
    const val FLEX_ZERO: Byte = 0x01

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
        val numLeadingSignBits = java.lang.Long.numberOfLeadingZeros(value.shr(63).xor(value))
        val numMagnitudeBitsRequired: Int = 64 - numLeadingSignBits
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
}
