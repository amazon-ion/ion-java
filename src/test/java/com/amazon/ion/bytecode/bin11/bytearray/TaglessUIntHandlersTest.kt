// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11.bytearray

import com.amazon.ion.PrimitiveTestCases_1_1
import com.amazon.ion.TextToBinaryUtils.binaryStringToByteArray
import com.amazon.ion.bytecode.GeneratorTestUtil.assertEqualBytecode
import com.amazon.ion.bytecode.bin11.OpCode
import com.amazon.ion.bytecode.ir.Instructions
import com.amazon.ion.bytecode.ir.Instructions.packInstructionData
import com.amazon.ion.bytecode.util.BytecodeBuffer
import com.amazon.ion.bytecode.util.ConstantPool
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import java.math.BigInteger

class TaglessUIntHandlersTest {

    @ParameterizedTest
    @MethodSource(PrimitiveTestCases_1_1.FLEX_UINT_READ_WRITE_CASES, PrimitiveTestCases_1_1.FLEX_UINT_READ_ONLY_CASES)
    fun testFlexUIntHandler(expectedBigInt: BigInteger, bits: String) {
        val source = bits.binaryStringToByteArray()
        val dest = BytecodeBuffer()
        val cp = ConstantPool()
        cp.add("dummy value")

        val bytesRead = TAGLESS_FLEX_UINT.convertOpcodeToBytecode(OpCode.TE_FLEX_UINT, source, 0, dest, cp, intArrayOf(), intArrayOf(), arrayOf())

        if (bytesRead > 9) {
            assertEqualBytecode(
                intArrayOf(Instructions.I_INT_CP.packInstructionData(1)),
                dest.toArray()
            )
            assertEquals(expectedBigInt, cp.toArray()[1])
        } else if (expectedBigInt <= Int.MAX_VALUE.toBigInteger()) {
            assertEqualBytecode(
                intArrayOf(Instructions.I_INT_I32, expectedBigInt.toInt()),
                dest.toArray()
            )
        } else {
            val expectedLong = expectedBigInt.toLong()
            assertEqualBytecode(
                intArrayOf(Instructions.I_INT_I64, expectedLong.shr(32).toInt(), expectedLong.toInt()),
                dest.toArray()
            )
        }
        assertEquals(source.size, bytesRead)
    }

    @ParameterizedTest
    @MethodSource(PrimitiveTestCases_1_1.FIXED_UINT_8_CASES,)
    fun testFixedUInt8Handler(expectedInteger: Int, bits: String) {
        val source = bits.binaryStringToByteArray()
        val dest = BytecodeBuffer()
        val cp = ConstantPool()

        val bytesRead = TAGLESS_FIXED_UINT_8.convertOpcodeToBytecode(OpCode.TE_UINT_8, source, 0, dest, cp, intArrayOf(), intArrayOf(), arrayOf())

        assertEquals(1, bytesRead)
        assertEqualBytecode(
            intArrayOf(Instructions.I_INT_I16.packInstructionData(expectedInteger)),
            dest.toArray()
        )
    }

    @ParameterizedTest
    @MethodSource(PrimitiveTestCases_1_1.FIXED_UINT_16_CASES,)
    fun testFixedUInt16Handler(expectedInteger: Int, bits: String) {
        val source = bits.binaryStringToByteArray()
        val dest = BytecodeBuffer()
        val cp = ConstantPool()

        val bytesRead = TAGLESS_FIXED_UINT_16.convertOpcodeToBytecode(OpCode.TE_UINT_16, source, 0, dest, cp, intArrayOf(), intArrayOf(), arrayOf())

        assertEquals(2, bytesRead)
        assertEqualBytecode(
            intArrayOf(Instructions.I_INT_I32, expectedInteger),
            dest.toArray()
        )
    }

    @ParameterizedTest
    @MethodSource(PrimitiveTestCases_1_1.FIXED_UINT_32_CASES,)
    fun testFixedUInt32Handler(expectedInteger: Long, bits: String) {
        val source = bits.binaryStringToByteArray()
        val dest = BytecodeBuffer()
        val cp = ConstantPool()

        val bytesRead = TAGLESS_FIXED_UINT_32.convertOpcodeToBytecode(OpCode.TE_UINT_32, source, 0, dest, cp, intArrayOf(), intArrayOf(), arrayOf())

        assertEquals(4, bytesRead)
        if (expectedInteger > Int.MAX_VALUE) {
            assertEqualBytecode(
                intArrayOf(Instructions.I_INT_I64, 0, expectedInteger.toInt()),
                dest.toArray()
            )
        } else {
            assertEqualBytecode(
                intArrayOf(Instructions.I_INT_I32, expectedInteger.toInt()),
                dest.toArray()
            )
        }
    }

    @ParameterizedTest
    @MethodSource(PrimitiveTestCases_1_1.FIXED_UINT_64_CASES,)
    fun testFixedUInt64Handler(expectedBigInteger: BigInteger, bits: String) {
        val source = bits.binaryStringToByteArray()
        val dest = BytecodeBuffer()
        val cp = ConstantPool()
        cp.add("dummy value")

        val bytesRead = TAGLESS_FIXED_UINT_64.convertOpcodeToBytecode(OpCode.TE_UINT_64, source, 0, dest, cp, intArrayOf(), intArrayOf(), arrayOf())

        assertEquals(8, bytesRead)

        try {
            val expectedLong = expectedBigInteger.longValueExact()
            assertEqualBytecode(
                intArrayOf(Instructions.I_INT_I64, expectedLong.shr(32).toInt(), expectedLong.toInt()),
                dest.toArray()
            )
        } catch (e: ArithmeticException) {
            // Larger than Long.MAX_VALUE, so it's in the constant pool
            assertEqualBytecode(
                intArrayOf(Instructions.I_INT_CP.packInstructionData(1)),
                dest.toArray()
            )
            assertEquals(expectedBigInteger, cp.toArray()[1])
        }
    }
}
