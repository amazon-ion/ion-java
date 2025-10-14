// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin10

import com.amazon.ion.IonException

object VarIntHelper {

    private const val TERMINATION_BIT_MASK = 0b10000000
    private const val MASK_7_BITS = 0b01111111

    /**
     * Returns an unsigned integer up to 7 bytes, with an 1 byte integer signifying how many varuint bytes were used in its encoding.
     */
    @JvmStatic
    fun readVarUIntValueAndLength(source: ByteArray, position: Int): Long {
        val currentByte: Int = source[position].toInt()
        val result = (currentByte and MASK_7_BITS).toLong()
        return if (currentByte and TERMINATION_BIT_MASK != 0) {
            (result shl 8) or 1L
        } else {
            readVarUIntValueAndLength2(source, position + 1, result)
        }
    }

    @JvmStatic
    private fun readVarUIntValueAndLength2(source: ByteArray, position: Int, partialResult: Long): Long {
        val currentByte: Int = source.get(position).toInt()
        val result = (partialResult shl 7) or (currentByte and MASK_7_BITS).toLong()
        if (currentByte and TERMINATION_BIT_MASK != 0) {
            return (result shl 8) or 2
        } else {
            return readVarUIntValueAndLength3Plus(source, position + 1, result)
        }
    }

    @JvmStatic
    private fun readVarUIntValueAndLength3Plus(source: ByteArray, position: Int, partialResult: Long): Long {
        var currentByte: Int
        var result = partialResult
        var p = position
        var length = 2
        do {
            length++
            currentByte = source.get(p++).toInt()
            result = (result shl 7) or (currentByte and MASK_7_BITS).toLong()
        } while (currentByte and TERMINATION_BIT_MASK == 0)

        return (result shl 8) or length.toLong()
    }

    /**
     * Returns a signed integer up to 7 bytes, with an 1 byte integer signifying how many varuint bytes were used in its encoding.
     */
    @JvmStatic
    fun readVarIntValueAndLength(source: ByteArray, position: Int): Long {
        var p = position

        var length = 1
        try {
            var currentByte = source[p++].toInt() and 0xFF
            var result = (currentByte and 0b00111111).toLong()
            val sign = getSignumValueFromVarIntSignBit(currentByte)
            while (currentByte and TERMINATION_BIT_MASK == 0) {
                length++
                currentByte = source[p++].toInt()
                result = (result shl 7) or (currentByte and MASK_7_BITS).toLong()
            }
            return ((sign * result) shl 8) or length.toLong()
        } catch (e: ArrayIndexOutOfBoundsException) {
            throw IonException("Incomplete VarInt at position $position", e)
        }
    }

    /**
     * Return either -1 or 1 based on the sign bit of the given byte. This uses some bit manipulation to avoid any branching.
     *
     * Visible only for testing.
     */
    @JvmStatic
    internal fun getSignumValueFromVarIntSignBit(byte: Int): Int = byte.shl(25).shr(31).shl(1) + 1
}
