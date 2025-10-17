// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.util

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

class NumericConversionsTest {

    /**
     * Tests that half-precision floats packed into a [Short] are converted to [Float] correctly;
     * that is, the resulting float has identical value and NaN semantics (but different bit layout).
     */
    @ParameterizedTest
    @CsvSource(
        "1,     0 01100111 00000000000000000000000, 0.000000059604645", // smallest positive subnormal number
        "1023,  0 01110000 11111111100000000000000, 0.000060975552", // largest subnormal number
        "1024,  0 01110001 00000000000000000000000, 0.00006103515625", // smallest positive normal number
        "31743, 0 10001110 11111111110000000000000, 65504", // largest normal number
        "15359, 0 01111110 11111111110000000000000, 0.99951172", // largest number less than one
        "15360, 0 01111111 00000000000000000000000, 1",
        "15361, 0 01111111 00000000010000000000000, 1.00097656", // smallest number larger than one

        // Same as above, but negative
        "-32767, 1 01100111 00000000000000000000000, -0.000000059604645",
        "-31745, 1 01110000 11111111100000000000000, -0.000060975552",
        "-31744, 1 01110001 00000000000000000000000, -0.00006103515625",
        "-1025,  1 10001110 11111111110000000000000, -65504",
        "-17409, 1 01111110 11111111110000000000000, -0.99951172",
        "-17408, 1 01111111 00000000000000000000000, -1",
        "-17407, 1 01111111 00000000010000000000000, -1.00097656",

        "0,      0 00000000 00000000000000000000000, 0",
        "-32768, 1 00000000 00000000000000000000000, -0",
        "31744,  0 11111111 00000000000000000000000, Infinity",
        "-1024,  1 11111111 00000000000000000000000, -Infinity",
        "32257,  0 11111111 10000000010000000000000, NaN", // quiet NaN
        "31745,  0 11111111 00000000010000000000000, NaN", // signaling NaN
        "-511,   1 11111111 10000000010000000000000, NaN", // negative quiet NaN
        "-1023,  1 11111111 00000000010000000000000, NaN", // negative signaling NaN
        "32595,  0 11111111 11010100110000000000000, NaN", // another quiet NaN
        "-173,   1 11111111 11010100110000000000000, NaN", // another negative quiet NaN

        "-16384, 1 10000000 00000000000000000000000, -2",
        "13653,  0 01111101 01010101010000000000000, 0.33325195",
        "16968,  0 10000000 10010010000000000000000, 3.140625"
    )
    fun `half-precision floats packed into a short are converted to single-precision correctly`(
        input: Short,
        expectedBytes: String,
        expectedValue: Float
    ) {
        val value = input.asHalfToFloat()

        val expectedRawBits = expectedBytes.replace(" ", "").toUInt(2).toInt()

        assertEquals(expectedRawBits, value.toRawBits())
        assertEquals(expectedValue, value)
    }
}
