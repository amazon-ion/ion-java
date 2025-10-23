// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11.bytearray

import com.amazon.ion.bytecode.bin11.OpcodeTestCases.FLOAT0_OPCODE_CASES
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.FLOAT16_OPCODE_CASES
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.FLOAT32_OPCODE_CASES
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.FLOAT64_OPCODE_CASES
import com.amazon.ion.bytecode.bin11.bytearray.OpcodeHandlerTestUtil.shouldCompile
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import kotlin.String

class FloatOpcodeHandlerTest {

    @ParameterizedTest
    @MethodSource(FLOAT0_OPCODE_CASES)
    fun `float0 opcode handler emits correct bytecode`(input: String, bytecode: String, expectedValue: String) {
        val buffer = Float0OpcodeHandler.shouldCompile(input, bytecode)
        val expectedFloat = expectedValue.toFloat()
        val representedFloat = Float.fromBits(buffer.get(1))
        assertEquals(expectedFloat, representedFloat)
    }

    @ParameterizedTest
    @MethodSource(FLOAT16_OPCODE_CASES)
    fun `float16 opcode handler emits correct bytecode`(input: String, bytecode: String, expectedValue: String) {
        val buffer = Float16OpcodeHandler.shouldCompile(input, bytecode)
        val expectedFloat = expectedValue.toFloat()
        val representedFloat = Float.fromBits(buffer.get(1))
        assertEquals(expectedFloat, representedFloat)
    }

    @ParameterizedTest
    @MethodSource(FLOAT32_OPCODE_CASES)
    fun `float32 opcode handler emits correct bytecode`(input: String, bytecode: String, expectedValue: String) {
        val buffer = Float32OpcodeHandler.shouldCompile(input, bytecode)
        val expectedFloat = expectedValue.toFloat()
        val representedFloat = Float.fromBits(buffer.get(1))
        assertEquals(expectedFloat, representedFloat)
    }

    @ParameterizedTest
    @MethodSource(FLOAT64_OPCODE_CASES)
    fun `float64 opcode handler emits correct bytecode`(input: String, bytecode: String, expectedValue: String) {
        val buffer = DoubleOpcodeHandler.shouldCompile(input, bytecode)
        val expectedFloat = expectedValue.toDouble()
        val representedFloat = Double.fromBits(
            buffer.get(1).toLong().shl(32)
                .or(buffer.get(2).toLong().and(0xFFFF_FFFF))
        )
        assertEquals(expectedFloat, representedFloat)
    }
}
