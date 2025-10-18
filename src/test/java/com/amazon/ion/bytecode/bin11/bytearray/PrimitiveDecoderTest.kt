// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11.bytearray

import com.amazon.ion.IonException
import com.amazon.ion.PrimitiveTestCases_1_1.FIXED_INT_16_CASES
import com.amazon.ion.PrimitiveTestCases_1_1.FIXED_INT_24_CASES
import com.amazon.ion.PrimitiveTestCases_1_1.FIXED_INT_32_CASES
import com.amazon.ion.PrimitiveTestCases_1_1.FIXED_INT_64_CASES
import com.amazon.ion.PrimitiveTestCases_1_1.FIXED_INT_8_CASES
import com.amazon.ion.PrimitiveTestCases_1_1.FIXED_UINT_16_CASES
import com.amazon.ion.PrimitiveTestCases_1_1.FIXED_UINT_32_CASES
import com.amazon.ion.PrimitiveTestCases_1_1.FIXED_UINT_64_CASES
import com.amazon.ion.PrimitiveTestCases_1_1.FLEX_INT_READ_ONLY_CASES
import com.amazon.ion.PrimitiveTestCases_1_1.FLEX_INT_READ_WRITE_CASES
import com.amazon.ion.PrimitiveTestCases_1_1.FLEX_UINT_READ_ONLY_CASES
import com.amazon.ion.PrimitiveTestCases_1_1.FLEX_UINT_READ_WRITE_CASES
import com.amazon.ion.TextToBinaryUtils.binaryStringToByteArray
import com.amazon.ion.TextToBinaryUtils.hexStringToByteArray
import com.amazon.ion.bytecode.bin11.bytearray.PrimitiveDecoder.lengthOfFlexIntOrUIntAt
import com.amazon.ion.bytecode.bin11.bytearray.PrimitiveDecoder.readFixedInt16
import com.amazon.ion.bytecode.bin11.bytearray.PrimitiveDecoder.readFixedInt24AsInt
import com.amazon.ion.bytecode.bin11.bytearray.PrimitiveDecoder.readFixedInt32
import com.amazon.ion.bytecode.bin11.bytearray.PrimitiveDecoder.readFixedInt8AsShort
import com.amazon.ion.bytecode.bin11.bytearray.PrimitiveDecoder.readFixedIntAsLong
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.MethodSource
import java.math.BigInteger

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PrimitiveDecoderTest {

    @ParameterizedTest
    @MethodSource(FIXED_INT_8_CASES)
    fun testReadFixedInt8AsShort(expectedValue: Short, input: String) {
        val data = input.binaryStringToByteArray()
        val value = readFixedInt8AsShort(data, 0)
        assertEquals(expectedValue, value)
    }

    @ParameterizedTest
    @MethodSource(FIXED_INT_16_CASES)
    fun testReadFixedInt16(expectedValue: Short, input: String) {
        val data = input.binaryStringToByteArray()
        val value = readFixedInt16(data, 0)
        assertEquals(expectedValue, value)
    }

    @ParameterizedTest
    @MethodSource(FIXED_INT_24_CASES)
    fun testReadFixedInt24AsInt(expectedValue: Int, input: String) {
        val data = input.binaryStringToByteArray()
        val value = readFixedInt24AsInt(data, 0)
        assertEquals(expectedValue, value)
    }

    @ParameterizedTest
    @MethodSource(FIXED_INT_32_CASES)
    fun testReadFixedInt32(expectedValue: Int, input: String) {
        val data = input.binaryStringToByteArray()
        val value = readFixedInt32(data, 0)
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
        val data = if (input.all { it == '0' || it == '1' }) input.binaryStringToByteArray() else input.hexStringToByteArray()

        val value = readFixedIntAsLong(data, 0, data.size)

        assertEquals(expectedValue, value)
    }

    @ParameterizedTest
    @MethodSource(FIXED_INT_8_CASES, FIXED_INT_16_CASES, FIXED_INT_24_CASES, FIXED_INT_32_CASES, FIXED_INT_64_CASES)
    @CsvSource(
        // Additional cases for 40, 48, and 56 bit FixedInt values
        "-36028797018963968, 00000000 00000000 00000000 00000000 00000000 00000000 10000000",
        "  -140737488355328, 00000000 00000000 00000000 00000000 00000000 10000000",
        "     -549755813888, 00000000 00000000 00000000 00000000 10000000",
        "                -1, 11111111 11111111 11111111 11111111 11111111",
        "                -1, 11111111 11111111 11111111 11111111 11111111 11111111",
        "                -1, 11111111 11111111 11111111 11111111 11111111 11111111 11111111",
        "                 0, 00000000 00000000 00000000 00000000 00000000",
        "                 0, 00000000 00000000 00000000 00000000 00000000 00000000",
        "                 0, 00000000 00000000 00000000 00000000 00000000 00000000 00000000",
        "      274877906944, 00000000 00000000 00000000 00000000 01000000",
        "      549755813887, 11111111 11111111 11111111 11111111 01111111",
        "    70368744177664, 00000000 00000000 00000000 00000000 00000000 01000000",
        "   140737488355327, 11111111 11111111 11111111 11111111 11111111 01111111",
        " 18014398509481984, 00000000 00000000 00000000 00000000 00000000 00000000 01000000",
        " 36028797018963967, 11111111 11111111 11111111 11111111 11111111 11111111 01111111",
    )
    fun testReadFixedIntAsLong(expectedValue: Long, input: String) {
        val data = input.binaryStringToByteArray()
        val value = readFixedIntAsLong(data, 0, data.size)
        assertEquals(expectedValue, value)
    }

    @ParameterizedTest
    @MethodSource(FIXED_INT_64_CASES)
    fun testReadFixedInt64(expected: Long, bits: String) {
        val data = bits.binaryStringToByteArray()
        val actual = PrimitiveDecoder.readFixedInt64(data, 0)
        assertEquals(expected, actual)
    }

    @ParameterizedTest
    @MethodSource(FIXED_UINT_16_CASES)
    fun testReadFixedUInt16(expected: Int, bits: String) {
        val data = bits.binaryStringToByteArray()
        val actual = PrimitiveDecoder.readFixedUInt16(data, 0)
        assertEquals(expected, actual.toInt())
    }

    @ParameterizedTest
    @MethodSource(FIXED_UINT_32_CASES)
    fun testReadFixedUInt32(expected: Long, bits: String) {
        val data = bits.binaryStringToByteArray()
        val actual = PrimitiveDecoder.readFixedUInt32(data, 0)
        assertEquals(expected, actual.toLong())
    }

    @ParameterizedTest
    @MethodSource(FIXED_UINT_64_CASES)
    fun testReadFixedUInt64(expectedBigInt: BigInteger, bits: String) {
        val data = bits.binaryStringToByteArray()
        val actual = PrimitiveDecoder.readFixedUInt64(data, 0)
        val expected = expectedBigInt.toULong()
        assertEquals(expected, actual)
    }

    // ==== FLEX INT AND UINT TESTS ==== //

    @ParameterizedTest
    @MethodSource(
        FLEX_INT_READ_WRITE_CASES,
        FLEX_INT_READ_ONLY_CASES,
        FLEX_UINT_READ_WRITE_CASES,
        FLEX_UINT_READ_ONLY_CASES,
    )
    fun testLengthOfFlexIntOrUIntAt(unused: BigInteger, bits: String) {
        val bytes = bits.binaryStringToByteArray()
        val expectedLength = bytes.size
        val actualLength = lengthOfFlexIntOrUIntAt(bytes, 0)
        assertEquals(expectedLength, actualLength)
    }

    @ParameterizedTest
    @MethodSource(FLEX_INT_READ_WRITE_CASES, FLEX_INT_READ_ONLY_CASES)
    fun testReadFlexIntValueAndLength(expectedBigInteger: BigInteger, bits: String) {
        val bytes = bits.binaryStringToByteArray()
        if (expectedBigInteger.isIntValue()) {
            val expectedValue = expectedBigInteger.toInt()
            val expectedLength = bytes.size
            val actual = PrimitiveDecoder.readFlexIntValueAndLength(bytes, 0)
            val actualValue = actual.toInt()
            val actualLength = actual.shr(Int.SIZE_BITS).toInt()
            assertEquals(expectedValue, actualValue)
            assertEquals(expectedLength, actualLength)
        } else {
            assertThrows<IonException> {
                PrimitiveDecoder.readFlexIntValueAndLength(bytes, 0)
            }
        }
    }

    @ParameterizedTest
    @MethodSource(FLEX_INT_READ_WRITE_CASES, FLEX_INT_READ_ONLY_CASES,)
    fun testReadFlexIntAsLong(expectedBigInteger: BigInteger, bits: String) {
        val bytes = bits.binaryStringToByteArray()
        if (expectedBigInteger.isLongValue()) {
            val expected = expectedBigInteger.toLong()
            val actual = PrimitiveDecoder.readFlexIntAsLong(bytes, 0)
            assertEquals(expected, actual, "Unexpected result reading a FlexInt that is ${bytes.size} bytes")
        } else {
            assertThrows<IonException> {
                PrimitiveDecoder.readFlexIntAsLong(bytes, 0)
            }
        }
    }

    @ParameterizedTest
    @MethodSource(FLEX_INT_READ_WRITE_CASES, FLEX_INT_READ_ONLY_CASES,)
    fun testReadFlexIntAsBigInteger(expected: BigInteger, bits: String) {
        val bytes = bits.binaryStringToByteArray()
        val actual = PrimitiveDecoder.readFlexIntAsBigInteger(bytes, 0)
        assertEquals(expected, actual, "Unexpected result reading a FlexInt that is ${bytes.size} bytes")
    }

    @ParameterizedTest
    @MethodSource(FLEX_UINT_READ_WRITE_CASES, FLEX_UINT_READ_ONLY_CASES,)
    fun testReadFlexUIntValueAndLength(expectedBigInteger: BigInteger, bits: String) {
        val bytes = bits.binaryStringToByteArray()
        if (expectedBigInteger.isIntValue()) {
            val expectedValue = expectedBigInteger.toInt()
            val expectedLength = bytes.size
            val actual = PrimitiveDecoder.readFlexUIntValueAndLength(bytes, 0)
            val actualValue = actual.toInt()
            val actualLength = actual.shr(Int.SIZE_BITS).toInt()
            assertEquals(expectedValue, actualValue)
            assertEquals(expectedLength, actualLength)
        } else {
            assertThrows<IonException> {
                PrimitiveDecoder.readFlexUIntValueAndLength(bytes, 0)
            }
        }
    }

    @ParameterizedTest
    @MethodSource(FLEX_UINT_READ_WRITE_CASES, FLEX_UINT_READ_ONLY_CASES,)
    fun testReadFlexUIntAsULong(expectedBigInteger: BigInteger, bits: String) {
        val bytes = bits.binaryStringToByteArray()
        if (expectedBigInteger.isULongValue()) {
            val expected = expectedBigInteger.toULong()
            val actual = PrimitiveDecoder.readFlexUIntAsULong(bytes, 0)
            assertEquals(expected, actual, "Unexpected result reading a FlexUInt that is ${bytes.size} bytes")
        } else {
            assertThrows<IonException> {
                PrimitiveDecoder.readFlexUIntAsULong(bytes, 0)
            }
        }
    }

    @ParameterizedTest
    @MethodSource(FLEX_UINT_READ_WRITE_CASES, FLEX_UINT_READ_ONLY_CASES,)
    fun testReadFlexUIntAsBigInteger(expected: BigInteger, bits: String) {
        val bytes = bits.binaryStringToByteArray()
        val actual = PrimitiveDecoder.readFlexUIntAsBigInteger(bytes, 0)
        assertEquals(expected, actual, "Unexpected result reading a FlexUInt that is ${bytes.size} bytes")
    }

    private fun BigInteger.isLongValue(): Boolean = this == this.toLong().toBigInteger()
    private fun BigInteger.isULongValue(): Boolean = bitLength() <= 64 && signum() >= 0
    private fun BigInteger.isIntValue(): Boolean = this == this.toInt().toBigInteger()
    private fun BigInteger.toULong(): ULong = this.toLong().toULong()
}
