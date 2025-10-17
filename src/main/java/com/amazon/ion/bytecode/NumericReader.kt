// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode

import com.amazon.ion.IonException

/**
 * Helper class containing methods for reading numerics of various forms off of a [ByteArray], such as floats,
 * doubles, etc.
 */
internal object NumericReader {
    /**
     * Returns the 2-byte int at the given position in the array. Does NOT validate array bounds.
     */
    @JvmStatic
    fun ByteArray.getShort(position: Int): Short {
        return (
            (this[position].toInt() and 0xFF) or
                ((this[position + 1].toInt() and 0xFF) shl 8)
            ).toShort()
    }

    /**
     * Returns the 3-byte int at the given position in the array. Does NOT validate array bounds.
     */
    @JvmStatic
    fun ByteArray.getInt24(position: Int): Int {
        return (this[position].toInt() and 0xFF) or
            ((this[position + 1].toInt() and 0xFF) shl 8) or
            // Shift left into 4th byte and then back down a byte here spreads the sign
            // across high byte, which is needed for negatives
            ((this[position + 2].toInt() and 0xFF) shl 24 shr 8)
    }

    /**
     * Returns the 4-byte int at the given position in the array. Does NOT validate array bounds.
     */
    @JvmStatic
    fun ByteArray.getInt(position: Int): Int {
        return (this[position].toInt() and 0xFF) or
            ((this[position + 1].toInt() and 0xFF) shl 8) or
            ((this[position + 2].toInt() and 0xFF) shl 16) or
            ((this[position + 3].toInt() and 0xFF) shl 24)
    }

    /**
     * Returns the 8-byte int at the given position in the array. Does NOT validate array bounds.
     */
    @JvmStatic
    fun ByteArray.getLong(position: Int): Long {
        return (this[position].toLong() and 0xFF) or
            ((this[position + 1].toLong() and 0xFF) shl 8) or
            ((this[position + 2].toLong() and 0xFF) shl 16) or
            ((this[position + 3].toLong() and 0xFF) shl 24) or
            ((this[position + 4].toLong() and 0xFF) shl 32) or
            ((this[position + 5].toLong() and 0xFF) shl 40) or
            ((this[position + 6].toLong() and 0xFF) shl 48) or
            ((this[position + 7].toLong() and 0xFF) shl 56)
    }

    /**
     * Reads the single-precision float at the given position in the array.
     *
     * @throws IonException if there is not enough data in the array at the specified position to read a complete value
     */
    @JvmStatic
    fun ByteArray.readShort(start: Int): Short {
        // TODO: ion-java#1114
        if (this.size < start + 2) throw IonException("Incomplete data: start=$start, length=${2}, limit=${this.size}")
        return this.getShort(start)
    }

    /**
     * Reads the single-precision float at the given position in the array.
     *
     * @throws IonException if there is not enough data in the array at the specified position to read a complete value
     */
    @JvmStatic
    fun ByteArray.readFloat(start: Int): Float {
        // TODO: ion-java#1114
        if (this.size < start + 4) throw IonException("Incomplete data: start=$start, length=${4}, limit=${this.size}")
        return Float.fromBits(this.getInt(start))
    }

    /**
     * Reads the double-precision float at the given position in the array.
     *
     * @throws IonException if there is not enough data in the array at the specified position to read a complete value
     */
    @JvmStatic
    fun ByteArray.readDouble(start: Int): Double {
        // TODO: ion-java#1114
        if (this.size < start + 8) throw IonException("Incomplete data: start=$start, length=${8}, limit=${this.size}")
        return Double.fromBits(this.getLong(start))
    }
}
