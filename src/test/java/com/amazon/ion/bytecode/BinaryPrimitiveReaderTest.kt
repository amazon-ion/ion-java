// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode

import com.amazon.ion.TextToBinaryUtils.hexStringToByteArray
import com.amazon.ion.bytecode.BinaryPrimitiveReader.readFixedInt16AsShort
import com.amazon.ion.bytecode.BinaryPrimitiveReader.readFixedInt24AsInt
import com.amazon.ion.bytecode.BinaryPrimitiveReader.readFixedInt32AsInt
import com.amazon.ion.bytecode.BinaryPrimitiveReader.readFixedInt8AsShort
import com.amazon.ion.bytecode.BinaryPrimitiveReader.readFixedIntAsInt
import com.amazon.ion.bytecode.BinaryPrimitiveReader.readFixedIntAsLong
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

class BinaryPrimitiveReaderTest {

    @ParameterizedTest
    @CsvSource(
        "  64, 40",
        "  78, 4E",
        "   0, 00",
        "   1, 01",
        "   2, 02",
        "   3, 03",
        "   4, 04",
        "   5, 05",
        "  14, 0E",
        " 127, 7F", // max value
        "  -1, FF",
        "  -2, FE",
        "  -3, FD",
        " -14, F2",
        "-128, 80", // min value
    )
    fun testReadFixedInt8AsShort(expectedValue: Short, input: String) {
        val data = "00 00 00 00 $input".hexStringToByteArray()

        val value = data.readFixedInt8AsShort(4)

        assertEquals(expectedValue, value)
    }

    @ParameterizedTest
    @CsvSource(
        "  3257, B9 0C",
        " -3257, 47 F3",
        " -6407, F9 E6",
        "   128, 80 00", // min positive
        "   729, D9 02",
        " 32767, FF 7F", // max value
        "  -129, 7F FF", // max negative
        "  -729, 27 FD",
        "-32768, 00 80", // min value
    )
    fun testReadFixedInt16AsShort(expectedValue: Short, input: String) {
        val data = "00 00 00 00 $input".hexStringToByteArray()

        val value = data.readFixedInt16AsShort(4)

        assertEquals(expectedValue, value)
    }

    @ParameterizedTest
    @CsvSource(
        "    32768, 00 80 00", // min positive
        "  8388607, FF FF 7F", // max value
        "   -32769, FF 7F FF", // max negative
        " -8388608, 00 00 80", // min value
        "  7123462, 06 B2 6C",
        " -7123462, FA 4D 93"
    )
    fun testReadFixedInt24AsInt(expectedValue: Int, input: String) {
        val data = "00 00 00 00 $input".hexStringToByteArray()

        val value = data.readFixedInt24AsInt(4)

        assertEquals(expectedValue, value)
    }

    @ParameterizedTest
    @CsvSource(
        "          8388608, 00 00 80 00", // min positive
        " ${Int.MAX_VALUE}, FF FF FF 7F", // max value
        "         -8388609, FF FF 7F FF", // max negative
        " ${Int.MIN_VALUE}, 00 00 00 80", // min value
        "       1931532212, B4 D7 20 73",
        "      -1931532212, 4C 28 DF 8C"
    )
    fun testReadFixedInt32AsInt(expectedValue: Int, input: String) {
        val data = "00 00 00 00 $input".hexStringToByteArray()

        val value = data.readFixedInt32AsInt(4)

        assertEquals(expectedValue, value)
    }

    @ParameterizedTest
    @CsvSource(
        "                  64, 1, 40",
        "                3257, 2, B9 0C",
        "               -3257, 2, 47 F3",
        "                  78, 1, 4E",
        "               -6407, 2, F9 E6",
        "                   0, 1, 00",
        "                   1, 1, 01",
        "                   2, 1, 02",
        "                   3, 1, 03",
        "                   4, 1, 04",
        "                   5, 1, 05",
        "                  14, 1, 0E",
        "                 127, 1, 7F",
        "                 128, 2, 80 00", // length boundary
        "                 729, 2, D9 02",
        "               32767, 2, FF 7F",
        "               32768, 3, 00 80 00", // length boundary
        "             8388607, 3, FF FF 7F",
        "             8388608, 4, 00 00 80 00", // length boundary
        "    ${Int.MAX_VALUE}, 4, FF FF FF 7F",

        "                  -1, 1, FF",
        "                  -2, 1, FE",
        "                  -3, 1, FD",
        "                 -14, 1, F2",
        "                -128, 1, 80",
        "                -129, 2, 7F FF", // length boundary
        "                -729, 2, 27 FD",
        "              -32768, 2, 00 80",
        "              -32769, 3, FF 7F FF", // length boundary
        "            -8388608, 3, 00 00 80",
        "            -8388609, 4, FF FF 7F FF", // length boundary
        "    ${Int.MIN_VALUE}, 4, 00 00 00 80"
    )
    fun testReadFixedIntAsInt(expectedValue: Int, length: Int, input: String) {
        val data = "00 00 00 00 $input".hexStringToByteArray()

        val value = data.readFixedIntAsInt(4, length)

        assertEquals(expectedValue, value)
    }

    @ParameterizedTest
    @CsvSource(
        "                  64, 1, 40",
        "                3257, 2, B9 0C",
        "               -3257, 2, 47 F3",
        "                  78, 1, 4E",
        "               -6407, 2, F9 E6",
        "                   0, 1, 00",
        "                   1, 1, 01",
        "                   2, 1, 02",
        "                   3, 1, 03",
        "                   4, 1, 04",
        "                   5, 1, 05",
        "                  14, 1, 0E",
        "                 127, 1, 7F",
        "                 128, 2, 80 00", // length boundary
        "                 729, 2, D9 02",
        "               32767, 2, FF 7F",
        "               32768, 3, 00 80 00", // length boundary
        "             8388607, 3, FF FF 7F",
        "             8388608, 4, 00 00 80 00", // length boundary
        "    ${Int.MAX_VALUE}, 4, FF FF FF 7F",
        "          2147483648, 5, 00 00 00 80 00", // length boundary
        "        549755813887, 5, FF FF FF FF 7F",
        "        549755813888, 6, 00 00 00 00 80 00", // length boundary
        "     140737488355327, 6, FF FF FF FF FF 7F",
        "     140737488355328, 7, 00 00 00 00 00 80 00", // length boundary
        "   36028797018963967, 7, FF FF FF FF FF FF 7F",
        "   36028797018963968, 8, 00 00 00 00 00 00 80 00", // length boundary
        "   ${Long.MAX_VALUE}, 8, FF FF FF FF FF FF FF 7F",

        "                  -1, 1, FF",
        "                  -2, 1, FE",
        "                  -3, 1, FD",
        "                 -14, 1, F2",
        "                -128, 1, 80",
        "                -129, 2, 7F FF", // length boundary
        "                -729, 2, 27 FD",
        "              -32768, 2, 00 80",
        "              -32769, 3, FF 7F FF", // length boundary
        "            -8388608, 3, 00 00 80",
        "            -8388609, 4, FF FF 7F FF", // length boundary
        "    ${Int.MIN_VALUE}, 4, 00 00 00 80",
        "         -2147483649, 5, FF FF FF 7F FF", // length boundary
        "       -549755813888, 5, 00 00 00 00 80",
        "       -549755813889, 6, FF FF FF FF 7F FF", // length boundary
        "    -140737488355328, 6, 00 00 00 00 00 80",
        "    -140737488355329, 7, FF FF FF FF FF 7F FF", // length boundary
        "  -36028797018963968, 7, 00 00 00 00 00 00 80",
        "  -36028797018963969, 8, FF FF FF FF FF FF 7F FF", // length boundary
        "   ${Long.MIN_VALUE}, 8, 00 00 00 00 00 00 00 80",
    )
    fun testReadFixedIntAsLong(expectedValue: Long, length: Int, input: String) {
        val data = "00 00 00 00 $input".hexStringToByteArray()

        val value = data.readFixedIntAsLong(4, length)

        assertEquals(expectedValue, value)
    }

    // TODO: add tests for reading the rest of the binary encoding primitives once implemented
}
