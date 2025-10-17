// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.util

/* === Constants related to converting half-precision floats to single-precision === */
private const val NAN_EXPONENT_FP16 = 0b01111100_00000000
private const val NAN_EXPONENT_FP32 = 0b01111111_10000000_00000000_00000000
private const val EBIAS_FP16 = 15
private const val EBIAS_FP32 = 127
private const val F16_EBIAS_CORRECTION = EBIAS_FP32 - EBIAS_FP16
private const val MANTISSA_SIZE_FP16 = 10
private const val MANTISSA_SIZE_FP32 = 23
private const val F16_MANTISSA_CORRECTION = MANTISSA_SIZE_FP32 - MANTISSA_SIZE_FP16
private const val MANT_MASK_FP32 = 0b01111111_11111111_11111111
private const val SIGN_MASK_FP16 = 0b10000000_00000000
private const val EXPONENT_MASK_FP16 = 0b01111100_00000000
private const val MANTISSA_MASK_FP16 = 0b00000011_11111111

/** Converts this [Byte] to an [Int], treating this [Byte] as if it is an _unsigned_ number. */
internal fun Byte.unsignedToInt(): Int = this.toInt() and 0xFF

/**
 * Converts this [Short] to a [Float], interpreting its bit layout as half-precision (binary16) floating
 * point. The resulting [Float] has a different bit layout but identical value and NaN semantics. */
internal fun Short.asHalfToFloat(): Float {
    val bits16 = this.toInt()
    val sign16 = bits16 and SIGN_MASK_FP16
    val exponent16 = bits16 and EXPONENT_MASK_FP16
    val mantissa16 = bits16 and MANTISSA_MASK_FP16

    // Move sign into position
    val sign32 = sign16 shl 16
    // Exponent needs determined based on branching unfortunately
    var exponent32: Int
    var mantissa32: Int

    // TODO: these operations can probably be simplified further.
    when (exponent16) {
        0 -> {
            if (mantissa16 != 0) {
                /*
                 * The float16 is subnormal; it needs normalization. To normalize, we need to shift the mantissa
                 * left until its first non-zero bit leaves the mantissa field. This is because normalized
                 * floats have an implied leading digit of 1 before the decimal point, whereas non-normalized ones
                 * have an implied leading digit of 0. We also have to decrement the exponent.
                 */
                val leadingZeros = Integer.numberOfLeadingZeros(mantissa16.shl(Int.SIZE_BITS - MANTISSA_SIZE_FP16))
                val shiftSize = leadingZeros + 1
                mantissa32 = mantissa16.shl(F16_MANTISSA_CORRECTION + shiftSize)
                    .and(MANT_MASK_FP32) // Remove the extra bit to the left of the mantissa
                exponent32 = (1 + F16_EBIAS_CORRECTION - shiftSize).shl(MANTISSA_SIZE_FP32)
            } else {
                // exponent zero and mantissa zero = 0e0 or -0e0. Nothing more needed.
                exponent32 = 0
                mantissa32 = 0
            }
        }
        NAN_EXPONENT_FP16 -> {
            // Value is NaN or Inf. New exponent has to have all bits set.
            exponent32 = NAN_EXPONENT_FP32
            // Move the 10 mantissa bits to upper portion of the new 23 mantissa bits. The 10 bytes from the f16 are
            // now at the high end of the mantissa for the f32, so the MSB with NaN signaling semantics is the same.
            // There will be 13 zero bits at the low end of the new mantissa.
            mantissa32 = mantissa16.shl(F16_MANTISSA_CORRECTION)
        }
        else -> {
            // This is a normal number. We can just adjust the exponent to use the correct bias and be done.
            exponent32 = (exponent16.shr(MANTISSA_SIZE_FP16) + F16_EBIAS_CORRECTION)
                .shl(MANTISSA_SIZE_FP32)
            // Move 10 mantissa bits to upper portion of the new 23 mantissa bits.
            mantissa32 = mantissa16.shl(F16_MANTISSA_CORRECTION)
        }
    }

    val bits32 = sign32 or exponent32 or mantissa32
    return Float.fromBits(bits32)
}
