// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode

import com.amazon.ion.TextToBinaryUtils.hexStringToByteArray
import com.amazon.ion.bytecode.NumericReader.readDouble
import com.amazon.ion.bytecode.NumericReader.readFloat
import com.amazon.ion.bytecode.NumericReader.readShort
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

class NumericReaderTest {

    @ParameterizedTest
    @CsvSource(
        "FE FF, -2",
        "FF FF, -1",
        "00 00, 0",
        "01 00, 1",
        "02 00, 2",
        "03 00, 3",
        "04 00, 4",
        "05 00, 5",
        "FF 03, 1023",
        "00 04, 1024",
        "FF 7B, 31743",
        "FF 3B, 15359",
        "00 3C, 15360",
        "01 3C, 15361",
        "01 80, -32767",
        "FF 83, -31745",
        "00 84, -31744",
        "FF FB, -1025",
        "FF BB, -17409",
        "00 BC, -17408",
        "01 BC, -17407",
        "00 80, ${Short.MIN_VALUE}",
        "FF 7F, ${Short.MAX_VALUE}"
    )
    fun testReadShort(input: String, expectedValue: Short) {
        val data = input.hexStringToByteArray()

        val value = data.readShort(0)
        assertEquals(expectedValue, value)
    }

    @ParameterizedTest
    @CsvSource(
        "01 00 00 00, 1.4012984643e-45", // smallest positive subnormal number
        "FF FF 7F 00, 1.1754942107e-38", // largest subnormal number
        "00 00 80 00, 1.1754943508e-38", // smallest positive normal number
        "FF FF 7F 7F, 3.4028234664e38", // largest normal number
        "FF FF 7F 3F, 0.999999940395355225", // largest number less than one
        "00 00 80 3F, 1",
        "01 00 80 3F, 1.00000011920928955", // smallest number larger than one

        // Same as above, but negative
        "01 00 00 80, -1.4012984643e-45",
        "FF FF 7F 80, -1.1754942107e-38",
        "00 00 80 80, -1.1754943508e-38",
        "FF FF 7F FF, -3.4028234664e38",
        "FF FF 7F BF, -0.999999940395355225",
        "00 00 80 BF, -1",
        "01 00 80 BF, -1.00000011920928955",

        "00 00 00 00, 0",
        "00 00 00 80, -0",
        "00 00 80 7F, Infinity",
        "00 00 80 FF, -Infinity",
        "01 00 C0 7F, NaN", // quiet NaN
        "01 00 80 7F, NaN", // signaling NaN
        "01 00 C0 FF, NaN", // negative quiet NaN
        "01 00 80 FF, NaN", // negative signaling NaN

        "00 00 00 C0, -2",
        "AB AA AA 3E, 0.333333343267440796",
        "DB 0F 49 40, 3.14159274101257324"
    )
    fun testReadFloat(input: String, expectedValue: Float) {
        val data = input.hexStringToByteArray()
        val value = data.readFloat(0)
        assertEquals(expectedValue, value)
    }

    @ParameterizedTest
    @CsvSource(
        "01 00 00 00 00 00 00 00, 4.9406564584124654e-324", // smallest positive subnormal number
        "FF FF FF FF FF FF 0F 00, 2.2250738585072009e-308", // largest subnormal number
        "00 00 00 00 00 00 10 00, 2.2250738585072014e-308", // smallest positive normal number
        "FF FF FF FF FF FF EF 7F, 1.7976931348623157e308", // largest normal number
        "FF FF FF FF FF FF EF 3F, 0.99999999999999988898", // largest number less than one
        "00 00 00 00 00 00 F0 3F, 1",
        "01 00 00 00 00 00 F0 3F, 1.0000000000000002220", // smallest number larger than one
        "02 00 00 00 00 00 F0 3F, 1.0000000000000004441", // the second smallest number greater than 1

        // Same as above, but negative
        "01 00 00 00 00 00 00 80, -4.9406564584124654e-324",
        "FF FF FF FF FF FF 0F 80, -2.2250738585072009e-308",
        "00 00 00 00 00 00 10 80, -2.2250738585072014e-308",
        "FF FF FF FF FF FF EF FF, -1.7976931348623157e308",
        "FF FF FF FF FF FF EF BF, -0.99999999999999988898",
        "00 00 00 00 00 00 F0 BF, -1",
        "01 00 00 00 00 00 F0 BF, -1.0000000000000002220",
        "02 00 00 00 00 00 F0 BF, -1.0000000000000004441",

        "00 00 00 00 00 00 00 00, 0",
        "00 00 00 00 00 00 00 80, -0",
        "00 00 00 00 00 00 F0 7F, Infinity",
        "00 00 00 00 00 00 F0 FF, -Infinity",
        "01 00 00 00 00 00 F8 7F, NaN", // quiet NaN
        "01 00 00 00 00 00 F0 7F, NaN", // signaling NaN
        "01 00 00 00 00 00 F8 FF, NaN", // negative quiet NaN
        "01 00 00 00 00 00 F0 FF, NaN", // negative signaling NaN
        "FF FF FF FF FF FF FF 7F, NaN", // another quiet NaN
        "FF FF FF FF FF FF FF FF, NaN", // another negative quiet NaN

        "00 00 00 00 00 00 00 C0, -2",
        "55 55 55 55 55 55 D5 3F, 0.33333333333333331483",
        "18 2D 44 54 FB 21 09 40, 3.141592653589793116"
    )
    fun testReadDouble(input: String, expectedValue: Double) {
        val data = input.hexStringToByteArray()
        val value = data.readDouble(0)
        assertEquals(expectedValue, value)
    }
}
