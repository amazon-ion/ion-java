// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11

import com.amazon.ion.TextToBinaryUtils.decimalStringToIntArray
import com.amazon.ion.TextToBinaryUtils.hexStringToByteArray
import com.amazon.ion.Timestamp
import com.amazon.ion.bytecode.GeneratorTestUtil.shouldGenerate
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.BOOLEAN_OPCODE_CASES
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.FLOAT0_OPCODE_CASES
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.FLOAT16_OPCODE_CASES
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.FLOAT32_OPCODE_CASES
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.FLOAT64_OPCODE_CASES
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.INT0_OPCODE_CASES
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.INT16_OPCODE_CASES
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.INT24_OPCODE_CASES
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.INT32_OPCODE_CASES
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.INT64_EMITTING_OPCODE_CASES
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.INT8_OPCODE_CASES
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.LOB_REFERENCE_OPCODE_CASES
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.NULL_OPCODE_CASES
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.REFERENCE_OPCODE_CASES
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.SHORT_TIMESTAMP_OPCODE_CASES
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.STRING_REFERENCE_OPCODE_CASES
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.TYPED_NULL_OPCODE_CASES
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.replacePositionTemplates
import com.amazon.ion.bytecode.bin11.bytearray.PrimitiveDecoder
import com.amazon.ion.bytecode.ir.Instructions
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

class ByteArrayBytecodeGenerator11Test {

    @ParameterizedTest
    @MethodSource(
        BOOLEAN_OPCODE_CASES, NULL_OPCODE_CASES, TYPED_NULL_OPCODE_CASES, FLOAT0_OPCODE_CASES,
        FLOAT16_OPCODE_CASES, FLOAT32_OPCODE_CASES, FLOAT64_OPCODE_CASES, SHORT_TIMESTAMP_OPCODE_CASES,
        REFERENCE_OPCODE_CASES, INT0_OPCODE_CASES, INT8_OPCODE_CASES, INT16_OPCODE_CASES, INT24_OPCODE_CASES,
        INT32_OPCODE_CASES, INT64_EMITTING_OPCODE_CASES, STRING_REFERENCE_OPCODE_CASES, LOB_REFERENCE_OPCODE_CASES
    )
    fun `generator produces correct bytecode for all supported opcodes`(inputBytesString: String, expectedBytecodeString: String) {
        val inputData = inputBytesString.hexStringToByteArray()
        val generator = ByteArrayBytecodeGenerator11(inputData, 0)

        generator.shouldGenerate(
            intArrayOf(
                *replacePositionTemplates(expectedBytecodeString, 0).decimalStringToIntArray(),
                Instructions.I_END_OF_INPUT
            )
        )
    }

    @ParameterizedTest
    @MethodSource(SHORT_TIMESTAMP_OPCODE_CASES)
    fun `generator can read short timestamp references`(encodedTimestampBytes: String, expectedBytecodeString: String, expectedTimestampString: String) {
        val bytes = encodedTimestampBytes.hexStringToByteArray()
        val generator = ByteArrayBytecodeGenerator11(bytes, 0)
        val opcode = bytes[0].toInt().and(0xFF)
        val expectedTimestamp = Timestamp.valueOf(expectedTimestampString)
        val readTimestamp = generator.readShortTimestampReference(1, opcode)
        assertEquals(expectedTimestamp, readTimestamp)
    }

    @ParameterizedTest
    @MethodSource(STRING_REFERENCE_OPCODE_CASES)
    fun `generator can read string references`(encodedStringBytes: String, expectedBytecodeString: String, expectedString: String) {
        val bytes = encodedStringBytes.hexStringToByteArray()
        val generator = ByteArrayBytecodeGenerator11(bytes, 0)
        // Size of input minus the opcode and FlexUInt length prefix
        val length = bytes.size - PrimitiveDecoder.lengthOfFlexIntOrUIntAt(bytes, 1) - 1
        val position = bytes.size - length
        val readString = generator.readTextReference(position, length)
        assertEquals(expectedString, readString)
    }

    @ParameterizedTest
    @MethodSource(LOB_REFERENCE_OPCODE_CASES)
    fun `generator can read lob references`(encodedLobBytes: String, expectedBytecodeString: String, expectedLobBytes: String) {
        val bytes = encodedLobBytes.hexStringToByteArray()
        val generator = ByteArrayBytecodeGenerator11(bytes, 0)
        val length = bytes.size - PrimitiveDecoder.lengthOfFlexIntOrUIntAt(bytes, 1) - 1
        val position = bytes.size - length
        val expectedLob = expectedLobBytes.hexStringToByteArray()
        val readLob = generator.readBytesReference(position, length).newByteArray()
        assertArrayEquals(expectedLob, readLob)
    }
}
