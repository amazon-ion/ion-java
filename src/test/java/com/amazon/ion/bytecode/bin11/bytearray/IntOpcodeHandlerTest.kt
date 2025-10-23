// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11.bytearray

import com.amazon.ion.bytecode.bin11.OpcodeTestCases.INT0_OPCODE_CASES
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.INT16_OPCODE_CASES
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.INT24_OPCODE_CASES
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.INT32_OPCODE_CASES
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.INT64_EMITTING_OPCODE_CASES
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.INT8_OPCODE_CASES
import com.amazon.ion.bytecode.bin11.bytearray.OpcodeHandlerTestUtil.shouldCompile
import com.amazon.ion.bytecode.ir.Instructions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import kotlin.String

class IntOpcodeHandlerTest {

    @ParameterizedTest
    @MethodSource(INT0_OPCODE_CASES)
    fun `int0 opcode handler emits correct bytecode`(input: String, bytecode: String, expectedValue: String) {
        val buffer = Int0OpcodeHandler.shouldCompile(input, bytecode)
        val expectedShort = expectedValue.toShort()
        val representedShort = Instructions.getData(buffer.get(0)).toShort()
        assertEquals(expectedShort, representedShort)
    }

    @ParameterizedTest
    @MethodSource(INT8_OPCODE_CASES)
    fun `int8 opcode handler emits correct bytecode`(input: String, bytecode: String, expectedValue: String) {
        val buffer = Int8OpcodeHandler.shouldCompile(input, bytecode)
        val expectedShort = expectedValue.toShort()
        val representedShort = Instructions.getData(buffer.get(0)).toShort()
        assertEquals(expectedShort, representedShort)
    }

    @ParameterizedTest
    @MethodSource(INT16_OPCODE_CASES)
    fun `int16 opcode handler emits correct bytecode`(input: String, bytecode: String, expectedValue: String) {
        val buffer = Int16OpcodeHandler.shouldCompile(input, bytecode)
        val expectedShort = expectedValue.toShort()
        val representedShort = Instructions.getData(buffer.get(0)).toShort()
        assertEquals(expectedShort, representedShort)
    }

    @ParameterizedTest
    @MethodSource(INT24_OPCODE_CASES)
    fun `int24 opcode handler emits correct bytecode`(input: String, bytecode: String, expectedValue: String) {
        val buffer = Int24OpcodeHandler.shouldCompile(input, bytecode)
        val expectedInt = expectedValue.toInt()
        val representedInt = buffer.get(1)
        assertEquals(expectedInt, representedInt)
    }

    @ParameterizedTest
    @MethodSource(INT32_OPCODE_CASES)
    fun `int32 opcode handler emits correct bytecode`(input: String, bytecode: String, expectedValue: String) {
        val buffer = Int32OpcodeHandler.shouldCompile(input, bytecode)
        val expectedInt = expectedValue.toInt()
        val representedInt = buffer.get(1)
        assertEquals(expectedInt, representedInt)
    }

    @ParameterizedTest
    @MethodSource(INT64_EMITTING_OPCODE_CASES)
    fun `long int opcode handler emits correct bytecode`(input: String, bytecode: String, expectedValue: String) {
        val buffer = LongIntOpcodeHandler.shouldCompile(input, bytecode)
        val expectedLong = expectedValue.toLong()
        val representedLong = (buffer.get(1).toLong() shl 32) or (buffer.get(2).toLong() and 0xFFFF_FFFF)
        assertEquals(expectedLong, representedLong)
    }
}
