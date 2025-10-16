// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.bin

import com.amazon.ion.IonException
import com.amazon.ion.TextToBinaryUtils.binaryStringToByteArray
import com.amazon.ion.TextToBinaryUtils.byteArrayToBitString
import com.amazon.ion.impl.bin.FlexInt.lengthOfFlexIntOrUIntAt
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.math.BigInteger

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class FlexIntTest {

    fun flexIntReadWriteCases() = listOf(
        "                   0, 00000001",
        "                   1, 00000011",
        "                   2, 00000101",
        "                   3, 00000111",
        "                   4, 00001001",
        "                   5, 00001011",
        "                  14, 00011101",
        "                  63, 01111111",
        "                  64, 00000010 00000001",
        "                 729, 01100110 00001011",
        "                3257, 11100110 00110010",
        "                8191, 11111110 01111111",
        "                8192, 00000100 00000000 00000001",
        "             1048575, 11111100 11111111 01111111",
        "             1048576, 00001000 00000000 00000000 00000001",
        "           134217727, 11111000 11111111 11111111 01111111",
        "           134217728, 00010000 00000000 00000000 00000000 00000001",
        "         17179869184, 00100000 00000000 00000000 00000000 00000000 00000001",
        "       2199023255552, 01000000 00000000 00000000 00000000 00000000 00000000 00000001",
        "     281474976710655, 11000000 11111111 11111111 11111111 11111111 11111111 01111111",
        "     281474976710656, 10000000 00000000 00000000 00000000 00000000 00000000 00000000 00000001",
        "   36028797018963967, 10000000 11111111 11111111 11111111 11111111 11111111 11111111 01111111",
        "   36028797018963968, 00000000 00000001 00000000 00000000 00000000 00000000 00000000 00000000 00000001",
        "   72624976668147840, 00000000 00000001 10000001 01000000 00100000 00010000 00001000 00000100 00000010",
        " 4611686018427387903, 00000000 11111111 11111111 11111111 11111111 11111111 11111111 11111111 01111111",
        " 4611686018427387904, 00000000 00000010 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000001",
        "   ${Long.MAX_VALUE}, 00000000 11111110 11111111 11111111 11111111 11111111 11111111 11111111 11111111 00000001",
        " 9223372036854775808, 00000000 00000010 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000010",
        "                  -1, 11111111",
        "                  -2, 11111101",
        "                  -3, 11111011",
        "                 -14, 11100101",
        "                 -64, 10000001",
        "                 -65, 11111110 11111110",
        "                -729, 10011110 11110100",
        "               -3257, 00011110 11001101",
        "               -6407, 11100110 10011011",
        "               -8192, 00000010 10000000",
        "               -8193, 11111100 11111111 11111110",
        "            -1048576, 00000100 00000000 10000000",
        "            -1048577, 11111000 11111111 11111111 11111110",
        "          -134217728, 00001000 00000000 00000000 10000000",
        "          -134217729, 11110000 11111111 11111111 11111111 11111110",
        "        -17179869184, 00010000 00000000 00000000 00000000 10000000",
        "        -17179869185, 11100000 11111111 11111111 11111111 11111111 11111110",
        "    -281474976710656, 01000000 00000000 00000000 00000000 00000000 00000000 10000000",
        "    -281474976710657, 10000000 11111111 11111111 11111111 11111111 11111111 11111111 11111110",
        "  -36028797018963968, 10000000 00000000 00000000 00000000 00000000 00000000 00000000 10000000",
        "  -36028797018963969, 00000000 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111110",
        "  -72624976668147841, 00000000 11111111 01111110 10111111 11011111 11101111 11110111 11111011 11111101",
        "-4611686018427387904, 00000000 00000001 00000000 00000000 00000000 00000000 00000000 00000000 10000000",
        "-4611686018427387905, 00000000 11111110 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111110",
        "   ${Long.MIN_VALUE}, 00000000 00000010 00000000 00000000 00000000 00000000 00000000 00000000 00000000 11111110",
        "-9223372036854775809, 00000000 11111110 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111101"
    ).map {
        val (bigInt, bits) = it.split(',')
        Arguments.of(bigInt.trim(), bits.trim())
    }

    fun flexIntReadOnlyCases() = listOf(
        // Mostly just over-padded numbers that we should be able to read, but we'll never write them this way.
        "  1, 00000000 00000011 00000000 00000000 00000000 00000000 00000000 00000000 00000000",
        "  1, 00000000 00000110 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000",
        "  1, 00000000 00001100 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000",
        "  1, 00000000 00011000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000",
        " -1, 00000000 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111",
        " -1, 00000000 11111110 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111",
        " -1, 00000000 11111100 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111",
        " -1, 00000000 11111000 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111",
    ).map {
        val (bigInt, bits) = it.split(',')
        Arguments.of(bigInt.trim(), bits.trim())
    }

    fun flexUIntReadWriteCases() = listOf(
        "                   0, 00000001",
        "                   1, 00000011",
        "                   2, 00000101",
        "                   3, 00000111",
        "                   4, 00001001",
        "                   5, 00001011",
        "                  14, 00011101",
        "                  63, 01111111",
        "                  64, 10000001",
        "                 127, 11111111",
        "                 128, 00000010 00000010",
        "                 729, 01100110 00001011",
        "               16383, 11111110 11111111",
        "               16384, 00000100 00000000 00000010",
        "             2097151, 11111100 11111111 11111111",
        "             2097152, 00001000 00000000 00000000 00000010",
        "           268435455, 11111000 11111111 11111111 11111111",
        "           268435456, 00010000 00000000 00000000 00000000 00000010",
        "    ${Int.MAX_VALUE}, 11110000 11111111 11111111 11111111 00001111",
        "         34359738368, 00100000 00000000 00000000 00000000 00000000 00000010",
        "       4398046511104, 01000000 00000000 00000000 00000000 00000000 00000000 00000010",
        "     562949953421311, 11000000 11111111 11111111 11111111 11111111 11111111 11111111",
        "     562949953421312, 10000000 00000000 00000000 00000000 00000000 00000000 00000000 00000010",
        "   72057594037927935, 10000000 11111111 11111111 11111111 11111111 11111111 11111111 11111111",
        "   72057594037927936, 00000000 00000001 00000000 00000000 00000000 00000000 00000000 00000000 00000010",
        "   72624976668147840, 00000000 00000001 10000001 01000000 00100000 00010000 00001000 00000100 00000010",
        "   ${Long.MAX_VALUE}, 00000000 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111",
        " 9223372036854775808, 00000000 00000010 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000010"
    ).map {
        val (bigInt, bits) = it.split(',')
        Arguments.of(bigInt.trim(), bits.trim())
    }

    fun flexUIntReadOnlyCases() = listOf(
        // Mostly just over-padded numbers that we should be able to read, but we'll never write them this way.
        "  1, 00000000 00000011 00000000 00000000 00000000 00000000 00000000 00000000 00000000",
        "  1, 00000000 00000110 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000",
        "  1, 00000000 00001100 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000",
        "  1, 00000000 00011000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000",
    ).map {
        val (bigInt, bits) = it.split(',')
        Arguments.of(bigInt.trim(), bits.trim())
    }

    @ParameterizedTest
    @MethodSource("flexIntReadWriteCases", "flexIntReadOnlyCases", "flexUIntReadWriteCases", "flexUIntReadOnlyCases")
    fun testLengthOfFlexIntOrUIntAt(unused: BigInteger, bits: String) {
        val bytes = bits.binaryStringToByteArray()
        val expectedLength = bytes.size
        val actualLength = lengthOfFlexIntOrUIntAt(bytes, 0)
        assertEquals(expectedLength, actualLength)
    }

    @ParameterizedTest
    @MethodSource("flexIntReadWriteCases")
    fun testWriteFlexInt(bigIntegerValue: BigInteger, expectedBits: String) {
        assumeTrue(bigIntegerValue.isLongValue())
        val value = bigIntegerValue.longValueExact()
        val numBytes: Int = FlexInt.flexIntLength(value)
        val bytes = ByteArray(numBytes)
        FlexInt.writeFlexIntOrUIntInto(bytes, 0, value, numBytes)
        assertEquals(expectedBits, bytes.byteArrayToBitString())
        assertEquals((expectedBits.length + 1) / 9, numBytes)
    }

    @ParameterizedTest
    @MethodSource("flexUIntReadWriteCases")
    fun testWriteFlexUInt(bigIntegerValue: BigInteger, expectedBits: String) {
        assumeTrue(bigIntegerValue.isLongValue())
        val value = bigIntegerValue.longValueExact()
        val numBytes: Int = FlexInt.flexUIntLength(value)
        val bytes = ByteArray(numBytes)
        FlexInt.writeFlexIntOrUIntInto(bytes, 0, value, numBytes)
        assertEquals(expectedBits, bytes.byteArrayToBitString())
        assertEquals((expectedBits.length + 1) / 9, numBytes)
    }

    @ParameterizedTest
    @MethodSource("flexIntReadWriteCases")
    fun testWriteFlexIntForBigInteger(value: BigInteger, expectedBits: String) {
        val numBytes: Int = FlexInt.flexIntLength(value)
        val bytes = ByteArray(numBytes)
        FlexInt.writeFlexIntOrUIntInto(bytes, 0, value, numBytes)
        assertEquals(expectedBits, bytes.byteArrayToBitString())
        assertEquals((expectedBits.length + 1) / 9, numBytes)
    }

    @ParameterizedTest
    @MethodSource("flexUIntReadWriteCases")
    fun testWriteFlexUIntForBigInteger(value: BigInteger, expectedBits: String) {
        val numBytes: Int = FlexInt.flexUIntLength(value)
        val bytes = ByteArray(numBytes)
        FlexInt.writeFlexIntOrUIntInto(bytes, 0, value, numBytes)
        assertEquals(expectedBits, bytes.byteArrayToBitString())
        assertEquals((expectedBits.length + 1) / 9, numBytes)
    }

    @ParameterizedTest
    @MethodSource("flexIntReadWriteCases", "flexIntReadOnlyCases")
    fun testReadFlexIntValueAndLength(expectedBigInteger: BigInteger, bits: String) {
        val bytes = bits.binaryStringToByteArray()
        if (expectedBigInteger.isIntValue()) {
            val expectedValue = expectedBigInteger.toInt()
            val expectedLength = bytes.size
            val actual = FlexInt.readFlexIntValueAndLength(bytes, 0)
            val actualValue = actual.toInt()
            val actualLength = actual.shr(Int.SIZE_BITS).toInt()
            assertEquals(expectedValue, actualValue)
            assertEquals(expectedLength, actualLength)
        } else {
            assertThrows<IonException> {
                FlexInt.readFlexIntValueAndLength(bytes, 0)
            }
        }
    }

    @ParameterizedTest
    @MethodSource("flexIntReadWriteCases", "flexIntReadOnlyCases")
    fun testReadFlexIntAsLong(expectedBigInteger: BigInteger, bits: String) {
        val bytes = bits.binaryStringToByteArray()
        if (expectedBigInteger.isLongValue()) {
            val expected = expectedBigInteger.toLong()
            val actual = FlexInt.readFlexIntAsLong(bytes, 0)
            assertEquals(expected, actual, "Unexpected result reading a FlexInt that is ${bytes.size} bytes")
        } else {
            assertThrows<IonException> {
                FlexInt.readFlexIntAsLong(bytes, 0)
            }
        }
    }

    @ParameterizedTest
    @MethodSource("flexIntReadWriteCases", "flexIntReadOnlyCases")
    fun testReadFlexIntAsBigInteger(expected: BigInteger, bits: String) {
        val bytes = bits.binaryStringToByteArray()
        val actual = FlexInt.readFlexIntAsBigInteger(bytes, 0)
        assertEquals(expected, actual, "Unexpected result reading a FlexInt that is ${bytes.size} bytes")
    }

    @ParameterizedTest
    @MethodSource("flexUIntReadWriteCases", "flexUIntReadOnlyCases")
    fun testReadFlexUIntValueAndLength(expectedBigInteger: BigInteger, bits: String) {
        val bytes = bits.binaryStringToByteArray()
        if (expectedBigInteger.isIntValue()) {
            val expectedValue = expectedBigInteger.toInt()
            val expectedLength = bytes.size
            val actual = FlexInt.readFlexUIntValueAndLength(bytes, 0)
            val actualValue = actual.toInt()
            val actualLength = actual.shr(Int.SIZE_BITS).toInt()
            assertEquals(expectedValue, actualValue)
            assertEquals(expectedLength, actualLength)
        } else {
            assertThrows<IonException> {
                FlexInt.readFlexUIntValueAndLength(bytes, 0)
            }
        }
    }

    @ParameterizedTest
    @MethodSource("flexUIntReadWriteCases", "flexUIntReadOnlyCases")
    fun testReadFlexUIntAsBigInteger(expected: BigInteger, bits: String) {
        val bytes = bits.binaryStringToByteArray()
        val actual = FlexInt.readFlexUIntAsBigInteger(bytes, 0)
        assertEquals(expected, actual, "Unexpected result reading a FlexUInt that is ${bytes.size} bytes")
    }

    private fun BigInteger.isLongValue(): Boolean = this == this.toLong().toBigInteger()
    private fun BigInteger.isIntValue(): Boolean = this == this.toInt().toBigInteger()
}
