// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin10

import com.amazon.ion.Decimal
import com.amazon.ion.IonException
import com.amazon.ion.TextToBinaryUtils.hexStringToByteArray
import com.amazon.ion.Timestamp
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

class ValueHelpersTest {

    @ParameterizedTest
    @CsvSource(
        "0, 0, 0",
        "0, 1, 0x01",
        "1, 1, 0x02",
        "1, 2, 0x0203",
        "1, 4, 0x02030405",
        "2, 3, 0x030405",
        "2, 8, 0x030405060708090A",
        "4, 4, 0x05060708",
        "4, 7, 0x05060708090A0B",
        "8, 2, 0x090A",
    )
    fun testReadUInt(startIndex: Int, length: Int, expected: Long) {
        val bytes = byteArrayOf(0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xA, 0xB)
        val result = readUInt(bytes, startIndex, length)
        assertEquals(expected, result)
    }

    @ParameterizedTest
    @CsvSource(
        "0x20, 1",
        "0x21, 1",
        "0x2E, 1",
        "0x31, -1",
        "0x32, -1",
        "0x3E, -1",
    )
    fun testSignForIntTypeId(typeId: Int, expectedSign: Int) {
        val result = signForIntTypeId(typeId)
        assertEquals(expectedSign, result)
    }

    @ParameterizedTest
    @CsvSource(
        // The values in the comments assume the use of the Int encoding given at:
        // https://amazon-ion.github.io/ion-docs/docs/binary.html#uint-and-int-fields
        "00000000, 1", // 0
        "01111111, 1", // 127
        "10000000, -1", // -0
        "11111111, -1" // -127
    )
    fun testGetSignumValueFromLeadingSignBit(byteValue: String, expectedSignum: Int) {
        val result = getSignumValueFromLeadingSignBit(byteValue.toInt(radix = 2).toByte())
        assertEquals(expectedSignum, result)
    }

    @ParameterizedTest
    @CsvSource(
        "2001T,                   C0 0F D1",
        "2001T,                   80 0F D1",
        "2001-01T,                81 0F D1 81",
        "2001-01-01T,             80 0F D1 81 81",
        "2001-01-01T00:00Z,       80 0F D1 81 81 80 80",
        "2000-01-01T00:00:00Z,    80 0F D0 81 81 80 80 80", // 2000-01-01T00:00:00Z with no fractional seconds
        "2000-01-01T00:00:00Z,    80 0F D0 81 81 80 80 80 80", // The same instant with 0d0 fractional seconds and implicit zero coefficient
        "2000-01-01T00:00:00Z,    80 0F D0 81 81 80 80 80 80 00", // The same instant with 0d0 fractional seconds and explicit zero coefficient
        "2000-01-01T00:00:00Z,    80 0F D0 81 81 80 80 80 C0", // The same instant with 0d-0 fractional seconds
        "2000-01-01T00:00:00Z,    80 0F D0 81 81 80 80 80 81", // The same instant with 0d1 fractional seconds
        "2000-01-01T00:00:00.0Z,  80 0F D0 81 81 80 80 80 C1",
        "2000-01-01T00:00:00.00Z, 80 0F D0 81 81 80 80 80 C2",
        "2000-01-02T03:04:05.06Z, 80 0F D0 81 82 83 84 85 C2 06",
    )
    fun testReadTimestampReference(expectedTimestamp: String, hexString: String) {
        val bytes = hexString.hexStringToByteArray()
        val expected = Timestamp.valueOf(expectedTimestamp)
        val actual = readTimestampReference(bytes, 0, bytes.size)
        println(actual.toString())
        assertEquals(expected, actual)
    }

    @Test
    fun testReadTimestampReference_ThrowsOnHourWithoutMinute() {
        // 80 = offset 0, 0F D1 = year 2001 (VarUInt encoded), 81 = month 1, 81 = day 1, 80 = hour 0 (but no minute)
        val bytes = "80 0F D1 81 81 80".hexStringToByteArray()

        assertThrows(IonException::class.java) {
            readTimestampReference(bytes, 0, 6)
        }
    }

    @ParameterizedTest
    @CsvSource(
        "0.00,  C2",
        "0.0,   C1",
        "0.,    80",
        "0e1,   81",
        "0.00,  C2 00",
        "0.0,   C1 00",
        "0.,    80 00",
        "0e1,   81 00",
        "-0.00, C2 80",
        "-0.0,  C1 80",
        "-0.,   80 80",
        "-0e1,  81 80",
        "0.01,  C2 01",
        "0.1,   C1 01",
        "1.,    80 01",
        "1e1,   81 01",
        "-0.01, C2 81",
        "-0.1,  C1 81",
        "-1.,   80 81",
        "-1e1,  81 81",
    )
    fun testReadDecimalReference(expectedDecimalString: String, hexString: String) {
        val bytes = hexString.hexStringToByteArray()
        val expected = Decimal.valueOf(expectedDecimalString)
        val actual = readDecimalReference(bytes, 0, bytes.size)
        assertEquals(expected, actual)
        assertEquals(expected.isNegativeZero, actual.isNegativeZero())
    }
}
