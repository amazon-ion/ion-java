// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.bin

import java.math.BigInteger

/**
 * Functions for encoding FlexInts and FlexUInts.
 *
 * Expected usage is calling one of the `___length` functions, and then using the result as the input for
 * [writeFlexIntOrUIntInto]. The length and write functions are separate so that callers can make decisions or
 * compute other values based on the encoded size of the value.
 */
object FlexInt {

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
        val numMagnitudeBitsRequired: Int
        numMagnitudeBitsRequired = if (value < 0) {
            val numLeadingOnes = java.lang.Long.numberOfLeadingZeros(value.inv())
            64 - numLeadingOnes
        } else {
            val numLeadingZeros = java.lang.Long.numberOfLeadingZeros(value)
            64 - numLeadingZeros
        }
        return numMagnitudeBitsRequired / 7 + 1
    }

    /**
     * Writes a FlexInt or FlexUInt encoding of [value] into [data] starting at [offset].
     * Use [flexIntLength] or [flexUIntLength] to get the value for the [numBytes] parameter.
     */
    @JvmStatic
    fun writeFlexIntOrUIntInto(data: ByteArray, offset: Int, value: Long, numBytes: Int) {
        var i = offset

        when (numBytes) {
            1 -> {
                data[i] = (0x01L or (value shl 1)).toByte()
            }
            2 -> {
                data[i] = (0x02L or (value shl 2)).toByte()
                data[++i] = (value shr 6).toByte()
            }
            3 -> {
                data[i] = (0x04L or (value shl 3)).toByte()
                data[++i] = (value shr 5).toByte()
                data[++i] = (value shr 13).toByte()
            }
            4 -> {
                data[i] = (0x08L or (value shl 4)).toByte()
                data[++i] = (value shr 4).toByte()
                data[++i] = (value shr 12).toByte()
                data[++i] = (value shr 20).toByte()
            }
            5 -> {
                data[i] = (0x10L or (value shl 5)).toByte()
                data[++i] = (value shr 3).toByte()
                data[++i] = (value shr 11).toByte()
                data[++i] = (value shr 19).toByte()
                data[++i] = (value shr 27).toByte()
            }
            6 -> {
                data[i] = (0x20L or (value shl 6)).toByte()
                data[++i] = (value shr 2).toByte()
                data[++i] = (value shr 10).toByte()
                data[++i] = (value shr 18).toByte()
                data[++i] = (value shr 26).toByte()
                data[++i] = (value shr 34).toByte()
            }
            7 -> {
                data[i] = (0x40L or (value shl 7)).toByte()
                data[++i] = (value shr 1).toByte()
                data[++i] = (value shr 9).toByte()
                data[++i] = (value shr 17).toByte()
                data[++i] = (value shr 25).toByte()
                data[++i] = (value shr 33).toByte()
                data[++i] = (value shr 41).toByte()
            }
            8 -> {
                data[i] = 0x80.toByte()
                data[++i] = (value shr 0).toByte()
                data[++i] = (value shr 8).toByte()
                data[++i] = (value shr 16).toByte()
                data[++i] = (value shr 24).toByte()
                data[++i] = (value shr 32).toByte()
                data[++i] = (value shr 40).toByte()
                data[++i] = (value shr 48).toByte()
            }
            9 -> {
                data[i] = 0
                data[++i] = (0x01L or (value shl 1)).toByte()
                data[++i] = (value shr 7).toByte()
                data[++i] = (value shr 15).toByte()
                data[++i] = (value shr 23).toByte()
                data[++i] = (value shr 31).toByte()
                data[++i] = (value shr 39).toByte()
                data[++i] = (value shr 47).toByte()
                data[++i] = (value shr 55).toByte()
            }
            10 -> {
                data[i] = 0
                data[++i] = (0x02L or (value shl 2)).toByte()
                data[++i] = (value shr 6).toByte()
                data[++i] = (value shr 14).toByte()
                data[++i] = (value shr 22).toByte()
                data[++i] = (value shr 30).toByte()
                data[++i] = (value shr 38).toByte()
                data[++i] = (value shr 46).toByte()
                data[++i] = (value shr 54).toByte()
                data[++i] = (value shr 62).toByte()
            }
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
