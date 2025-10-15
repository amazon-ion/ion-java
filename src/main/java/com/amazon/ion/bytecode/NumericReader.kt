// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode

import com.amazon.ion.IonException

/**
 * Helper class containing methods for reading numerics of various forms off of a [ByteArray], such as floats,
 * doubles, etc.
 */
internal object NumericReader {
    private const val NAN_EXPONENT_FP16 = 0b01111100_00000000
    private const val NAN_EXPONENT_FP32 = 0b01111111_10000000_00000000_00000000
    private const val EXPONENT_BIAS_FP16 = 15
    private const val EXPONENT_BIAS_FP32 = 127
    private const val MANTISSA_SIZE_FP16 = 10
    private const val MANTISSA_SIZE_FP32 = 23
    private const val MANTISSA_MASK_FP32 = 0b01111111_11111111_11111111
    private const val BIT_LEFT_OF_MANTISSA_MASK_FP32 = 0b10000000_00000000_00000000

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
     * Reads the half-precision float at the given position in the array.
     *
     * @throws IonException if there is not enough data in the array at the specified position to read a complete value
     */
    @JvmStatic
    fun ByteArray.readFloat16(start: Int): Float {
        if (this.size < start + 2) throw IonException("Incomplete data: start=$start, length=${2}, limit=${this.size}")

        val bits16 = this.getShort(start).toInt()
        val sign16 = bits16 and 0b10000000_00000000
        val exponent16 = bits16 and 0b01111100_00000000
        val mantissa16 = bits16 and 0b00000011_11111111

        // Move sign into position
        val sign32 = sign16 shl 16
        // Exponent needs determined based on branching unfortunately
        var exponent32: Int
        // Move 10 mantissa bits to upper portion of the new 23 mantissa bits
        // TODO: we don't need to do this specific operation for 0e0 or -0e0, and we already do the branching to detect
        //  these cases anyway. We can move assignment to mantissa32 into the when branches and set to zero in these
        //  cases.
        var mantissa32 = mantissa16 shl (MANTISSA_SIZE_FP32 - MANTISSA_SIZE_FP16)

        // TODO: these operations can probably be simplified. Avoiding the branching/looping would be nice, but I could
        //  not find a way to normalize subnormal values without it.
        when (exponent16) {
            0 -> {
                if (mantissa16 != 0) {
                    /*
                     * The float16 is subnormal; it needs normalization. To normalize, we need to shift the mantissa
                     * left until its first non-zero bit leaves the mantissa field. This is because normalized
                     * floats have an implied leading digit of 1 before the decimal point, whereas non-normalized ones
                     * have an implied leading digit of 0. We also have to decrement the exponent while we do this.
                     */
                    exponent32 = (1 + EXPONENT_BIAS_FP32 - EXPONENT_BIAS_FP16)
                    while (mantissa32 and BIT_LEFT_OF_MANTISSA_MASK_FP32 == 0) {
                        mantissa32 = mantissa32 shl 1
                        exponent32--
                    }
                    // Remove the extra bit to the left of the mantissa
                    mantissa32 = (mantissa32 and MANTISSA_MASK_FP32)
                    // Shift the exponent into place
                    exponent32 = exponent32 shl MANTISSA_SIZE_FP32
                } else {
                    // exponent zero and mantissa zero = 0e0 or -0e0. Nothing more needed.
                    exponent32 = 0
                    mantissa32 = 0
                }
            }
            NAN_EXPONENT_FP16 -> {
                // Value is NaN or Inf. New exponent has to have all bits set.
                // The mantissa is already set correctly. The 10 bytes from the f16 are at the high end of the mantissa
                // for the f32, so the MSB with NaN signaling semantics is the same. There will be 13 zero bits at the
                // low end of the new mantissa.
                exponent32 = NAN_EXPONENT_FP32
            }
            else -> {
                // This is a normal number. We can just adjust the exponent to use the correct bias and be done.
                exponent32 = (((exponent16 shr MANTISSA_SIZE_FP16) - EXPONENT_BIAS_FP16 + EXPONENT_BIAS_FP32) and 0xFF) shl MANTISSA_SIZE_FP32
            }
        }

        val bits32 = sign32 or exponent32 or mantissa32

        return Float.fromBits(bits32)
    }

    /**
     * Reads the single-precision float at the given position in the array.
     *
     * @throws IonException if there is not enough data in the array at the specified position to read a complete value
     */
    @JvmStatic
    fun ByteArray.readFloat(start: Int): Float {
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
        if (this.size < start + 8) throw IonException("Incomplete data: start=$start, length=${8}, limit=${this.size}")
        return Double.fromBits(this.getLong(start))
    }
}
