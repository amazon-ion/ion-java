// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode

import com.amazon.ion.IonException

/**
 * Helper class containing methods for reading FixedInts, and (TODO) FlexInts, FixedUInts, and FlexUInts.
 */
object BinaryPrimitiveReader {

    @JvmStatic
    private fun ByteArray.getShort(position: Int): Short {
        return (
            (this[position].toInt() and 0xFF) or
                ((this[position + 1].toInt() and 0xFF) shl 8)
            ).toShort()
    }
    @JvmStatic
    private fun ByteArray.getInt24(position: Int): Int {
        return (this[position].toInt() and 0xFF) or
            ((this[position + 1].toInt() and 0xFF) shl 8) or
            // Shift left into 4th byte and then back down a byte here spreads the sign
            // across high byte, which is needed for negatives
            ((this[position + 2].toInt() and 0xFF) shl 24 shr 8)
    }
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
    fun ByteArray.readFixedInt8AsShort(start: Int): Short {
        // TODO: ion-java#1114
        if (this.size < start + 1) throw IonException("Incomplete data: start=$start, length=${1}, limit=${this.size}")
        return this[start].toShort()
    }

    @JvmStatic
    fun ByteArray.readFixedInt16AsShort(start: Int): Short {
        // TODO: ion-java#1114
        if (this.size < start + 2) throw IonException("Incomplete data: start=$start, length=${2}, limit=${this.size}")
        return this.getShort(start)
    }

    @JvmStatic
    fun ByteArray.readFixedInt24AsInt(start: Int): Int {
        // TODO: ion-java#1114
        if (this.size < start + 3) throw IonException("Incomplete data: start=$start, length=${3}, limit=${this.size}")
        return this.getInt24(start)
    }

    @JvmStatic
    fun ByteArray.readFixedInt32AsInt(start: Int): Int {
        // TODO: ion-java#1114
        if (this.size < start + 4) throw IonException("Incomplete data: start=$start, length=${4}, limit=${this.size}")
        return this.getInt(start)
    }

    @JvmStatic
    fun ByteArray.readFixedIntAsInt(start: Int, length: Int): Int {
        // TODO: ion-java#1114
        if (this.size < start + length) throw IonException("Incomplete data: start=$start, length=$length, limit=${this.size}")
        return (this.getInt(start - 4 + length) shr ((4 - length) * 8))
    }

    @JvmStatic
    fun ByteArray.readFixedIntAsLong(start: Int, length: Int): Long {
        // TODO: ion-java#1114
        if (this.size < start + length) throw IonException("Incomplete data: start=$start, length=$length, limit=${this.size}")
        if (length > 4) {
            // TODO: See if we can simplify some of the calculations
            return this.getLong(start - 8 + length) shr ((8 - length) * 8)
        } else {
            return (this.getInt(start - 4 + length) shr ((4 - length) * 8)).toLong()
        }
    }

    // TODO: add helpers for reading FixedUInts, FlexInts, and FlexUInts once needed. These needed more cleanup
    //  and did not have test coverage so I did not include them yet.
}
