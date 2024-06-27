// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.bin

import java.lang.Long.numberOfLeadingZeros

/**
 * Functions for encoding FixedInts and FixedUInts.
 *
 * Expected usage is calling one of the `___length` functions, and then using the result as the input for
 * [writeFixedIntOrUIntInto]. The length and write functions are separate so that callers can make decisions or
 * compute other values based on the encoded size of the value.
 */
object FixedInt {

    /**
     * Writes a FixedInt or FixedUInt encoding of [value] into [data] starting at [offset].
     * Use [fixedIntLength] or [fixedUIntLength] to get the value for the [numBytes] parameter.
     */
    @JvmStatic
    inline fun writeFixedIntOrUIntInto(data: ByteArray, offset: Int, value: Long, numBytes: Int) {
        when (numBytes) {
            1 -> data[offset] = value.toByte()
            2 -> {
                data[offset] = value.toByte()
                data[offset + 1] = (value shr 8).toByte()
            }
            3 -> {
                data[offset] = value.toByte()
                data[offset + 1] = (value shr 8).toByte()
                data[offset + 2] = (value shr 16).toByte()
            }
            4 -> {
                data[offset] = value.toByte()
                data[offset + 1] = (value shr 8).toByte()
                data[offset + 2] = (value shr 16).toByte()
                data[offset + 3] = (value shr 24).toByte()
            }
            else -> {
                for (i in 0 until numBytes) {
                    data[offset + i] = (value shr 8 * i).toByte()
                }
            }
        }
    }

    /** Determine the length of FixedUInt for the provided value.  */
    @JvmStatic
    fun fixedUIntLength(value: Long): Int {
        val numLeadingZeros = numberOfLeadingZeros(value)
        val numMagnitudeBitsRequired = 64 - numLeadingZeros
        return (numMagnitudeBitsRequired - 1) / 8 + 1
    }

    /** Determine the length of FixedInt for the provided value.  */
    @JvmStatic
    fun fixedIntLength(value: Long): Int {
        val numMagnitudeBitsRequired: Int
        if (value < 0) {
            val numLeadingOnes = numberOfLeadingZeros(value.inv())
            numMagnitudeBitsRequired = 64 - numLeadingOnes
        } else {
            val numLeadingZeros = numberOfLeadingZeros(value)
            numMagnitudeBitsRequired = 64 - numLeadingZeros
        }
        return numMagnitudeBitsRequired / 8 + 1
    }
}
