// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin10

import com.amazon.ion.TextToBinaryUtils.binaryStringToByteArray
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

class VarIntHelperTest {

    @ParameterizedTest
    @CsvSource(
        "0,      10000000",
        "0,      00000000 10000000",
        "0,      00000000 00000000 10000000",
        "0,      00000000 00000000 00000000 10000000",
        "1,      10000001",
        "1,      00000000 10000001",
        "1,      00000000 00000000 10000001",
        "1,      00000000 00000000 00000000 10000001",
        "2,      10000010",
        "127,    11111111",
        "128,    00000001 10000000",
        "129,    00000001 10000001",
        "16383,  01111111 11111111",
        "16384,  00000001 00000000 10000000",
        "16513,  00000001 00000001 10000001",
    )
    fun testReadVarUIntValueAndLength_ByteArray(expectedValue: Long, binaryString: String) {
        val bytes = binaryString.binaryStringToByteArray()
        val expectedLength = bytes.size

        val result = VarIntHelper.readVarUIntValueAndLength(bytes, 0)
        val actualLength = result.toInt() and 0xFF
        val actualValue = result ushr 8

        assertEquals(expectedValue, actualValue)
        assertEquals(expectedLength, actualLength)
    }

    @ParameterizedTest
    @CsvSource(

        "0,      10000000",
        "0,      00000000 10000000",
        "0,      00000000 00000000 10000000",
        "0,      00000000 00000000 00000000 10000000",
        "1,      10000001",
        "1,      00000000 10000001",
        "1,      00000000 00000000 10000001",
        "1,      00000000 00000000 00000000 10000001",
        "-1,     11000001",
        "-1,     01000000 10000001",
        "-1,     01000000 00000000 10000001",
        "-1,     01000000 00000000 00000000 10000001",
        "63,     10111111",
        "64,     00000000 11000000",
        "-63,    11111111",
        "-64,    01000000 11000000",
        "128,    00000001 10000000",
        "129,    00000001 10000001",
        "-128,   01000001 10000000",
        "-129,   01000001 10000001",
    )
    fun testReadVarIntValueAndLength_ByteArray(expectedValue: Long, binaryString: String) {
        val bytes = binaryString.binaryStringToByteArray()
        val expectedLength = bytes.size

        val result = VarIntHelper.readVarIntValueAndLength(bytes, 0)
        val actualLength = result.toInt() and 0xFF
        val actualValue = result shr 8

        assertEquals(expectedValue, actualValue)
        assertEquals(expectedLength, actualLength)
    }

    @ParameterizedTest
    @CsvSource(
        // The values in the comments assume the use of the Int encoding given at:
        // https://amazon-ion.github.io/ion-docs/docs/binary.html#varuint-and-varint-fields
        "00000000, 1", // 0
        "00111111, 1", // 63
        "01000000, -1", // -0
        "01111111, -1", // -63
        "10000000, 1", // 0
        "10111111, 1", // 63
        "11000000, -1", // -0
        "11111111, -1", // -63
    )
    fun testGetSignumValueFromVarIntSignBit(byteValue: String, expectedSignum: Int) {
        val result = VarIntHelper.getSignumValueFromVarIntSignBit(byteValue.toInt(radix = 2))
        assertEquals(expectedSignum, result)
    }
}
