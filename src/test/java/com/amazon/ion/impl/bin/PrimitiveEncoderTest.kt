// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.bin

import com.amazon.ion.PrimitiveTestCases_1_1
import com.amazon.ion.TextToBinaryUtils.byteArrayToBitString
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import java.math.BigInteger

class PrimitiveEncoderTest {

    @ParameterizedTest
    @MethodSource(PrimitiveTestCases_1_1.FLEX_INT_READ_WRITE_CASES)
    fun testWriteFlexInt(bigIntegerValue: BigInteger, expectedBits: String) {
        assumeTrue(bigIntegerValue.isLongValue())
        val value = bigIntegerValue.longValueExact()
        val numBytes: Int = PrimitiveEncoder.flexIntLength(value)
        val bytes = ByteArray(numBytes)
        PrimitiveEncoder.writeFlexIntOrUIntInto(bytes, 0, value, numBytes)
        assertEquals(expectedBits, bytes.byteArrayToBitString())
        assertEquals((expectedBits.length + 1) / 9, numBytes)
    }

    @ParameterizedTest
    @MethodSource(PrimitiveTestCases_1_1.FLEX_UINT_READ_WRITE_CASES)
    fun testWriteFlexUInt(bigIntegerValue: BigInteger, expectedBits: String) {
        assumeTrue(bigIntegerValue.isLongValue())
        val value = bigIntegerValue.longValueExact()
        val numBytes: Int = PrimitiveEncoder.flexUIntLength(value)
        val bytes = ByteArray(numBytes)
        PrimitiveEncoder.writeFlexIntOrUIntInto(bytes, 0, value, numBytes)
        assertEquals(expectedBits, bytes.byteArrayToBitString())
        assertEquals((expectedBits.length + 1) / 9, numBytes)
    }

    @ParameterizedTest
    @MethodSource(PrimitiveTestCases_1_1.FLEX_INT_READ_WRITE_CASES)
    fun testWriteFlexIntForBigInteger(value: BigInteger, expectedBits: String) {
        val numBytes: Int = PrimitiveEncoder.flexIntLength(value)
        val bytes = ByteArray(numBytes)
        PrimitiveEncoder.writeFlexIntOrUIntInto(bytes, 0, value, numBytes)
        assertEquals(expectedBits, bytes.byteArrayToBitString())
        assertEquals((expectedBits.length + 1) / 9, numBytes)
    }

    @ParameterizedTest
    @MethodSource(PrimitiveTestCases_1_1.FLEX_UINT_READ_WRITE_CASES)
    fun testWriteFlexUIntForBigInteger(value: BigInteger, expectedBits: String) {
        val numBytes: Int = PrimitiveEncoder.flexUIntLength(value)
        val bytes = ByteArray(numBytes)
        PrimitiveEncoder.writeFlexIntOrUIntInto(bytes, 0, value, numBytes)
        assertEquals(expectedBits, bytes.byteArrayToBitString())
        assertEquals((expectedBits.length + 1) / 9, numBytes)
    }

    private fun BigInteger.isLongValue(): Boolean = this == this.toLong().toBigInteger()
}
