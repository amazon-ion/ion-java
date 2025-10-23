// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11

import com.amazon.ion.TextToBinaryUtils.decimalStringToIntArray
import com.amazon.ion.TextToBinaryUtils.hexStringToByteArray
import com.amazon.ion.Timestamp
import com.amazon.ion.bytecode.GeneratorTestUtil.assertEqualBytecode
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
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.booleanOpcodeCases
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.float0OpcodeCases
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.float16OpcodeCases
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.float32OpcodeCases
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.float64OpcodeCases
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.int0OpcodeCases
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.int16OpcodeCases
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.int32OpcodeCases
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.int64EmittingOpcodeCases
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.int8OpcodeCases
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.lobReferenceOpcodeCases
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.nullOpcodeCases
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.referenceOpcodeCases
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.replacePositionTemplates
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.shortTimestampOpcodeCases
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.stringReferenceOpcodeCases
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.typedNullOpcodeCases
import com.amazon.ion.bytecode.ir.Instructions
import com.amazon.ion.bytecode.util.BytecodeBuffer
import com.amazon.ion.bytecode.util.ConstantPool
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
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

    /**
     * Concatenates all the tests for all supported non-system opcodes together into a single test string. This
     * validates that reference instructions that appear in the middle of the input are handled correctly.
     */
    @Test
    fun `generator produces correct bytecode for sequence of all supported opcodes`() {
        var inputData = byteArrayOf()
        var expectedBytecode = intArrayOf()

        val opcodeTests = booleanOpcodeCases() +
            nullOpcodeCases() +
            typedNullOpcodeCases() +
            float0OpcodeCases() +
            float16OpcodeCases() +
            float32OpcodeCases() +
            float64OpcodeCases() +
            shortTimestampOpcodeCases() +
            referenceOpcodeCases() +
            int0OpcodeCases() +
            int8OpcodeCases() +
            int16OpcodeCases() +
            int32OpcodeCases() +
            int64EmittingOpcodeCases() +
            stringReferenceOpcodeCases() +
            lobReferenceOpcodeCases()

        // Build up the input bytes and expected bytecode from the individual opcode tests.
        var bytesRead = 0
        opcodeTests.forEach { args ->
            val (inputBytesString: String, expectedBytecodeString) = args.get().map { it as String }
            val nextBytes = inputBytesString.hexStringToByteArray()
            inputData = inputData.plus(nextBytes)
            val nextBytecode = replacePositionTemplates(expectedBytecodeString, bytesRead)
                .decimalStringToIntArray()
            bytesRead += nextBytes.size
            expectedBytecode = expectedBytecode.plus(nextBytecode)
        }

        expectedBytecode = expectedBytecode.plus(Instructions.I_END_OF_INPUT)

        val generator = ByteArrayBytecodeGenerator11(inputData, 0)
        val bytecodeBuffer = BytecodeBuffer()
        val constantPool = ConstantPool()
        val macroSrc = intArrayOf()
        val macroIndices = intArrayOf()
        val symbolTable = arrayOf<String?>()
        var isEOF: Boolean
        do {
            generator.refill(bytecodeBuffer, constantPool, macroSrc, macroIndices, symbolTable)
            isEOF = bytecodeBuffer.get(bytecodeBuffer.size() - 1) == Instructions.I_END_OF_INPUT
        } while (!isEOF)

        assertEqualBytecode(expectedBytecode, bytecodeBuffer.toArray())
    }

    @ParameterizedTest
    @MethodSource(SHORT_TIMESTAMP_OPCODE_CASES)
    fun `generator can read short timestamp references`(encodedTimestampBytes: String, expectedBytecodeString: String, expectedTimestampString: String) {
        val timestampReferenceBytes = encodedTimestampBytes.hexStringToByteArray()
        val generator = ByteArrayBytecodeGenerator11(timestampReferenceBytes, 0)
        val bytecode = BytecodeBuffer()
        generator.refill(bytecode, ConstantPool(), intArrayOf(), intArrayOf(), arrayOf())

        val timestampPrecisionAndOffsetMode = Instructions.getData(bytecode.get(0))
        val timestampPosition = bytecode.get(1)
        val expectedTimestamp = Timestamp.valueOf(expectedTimestampString)
        val readTimestamp = generator.readShortTimestampReference(timestampPosition, timestampPrecisionAndOffsetMode)
        assertEquals(expectedTimestamp, readTimestamp)
    }

    @ParameterizedTest
    @MethodSource(STRING_REFERENCE_OPCODE_CASES)
    fun `generator can read string references`(encodedStringBytes: String, expectedBytecodeString: String, expectedString: String) {
        val stringReferenceBytes = encodedStringBytes.hexStringToByteArray()
        val generator = ByteArrayBytecodeGenerator11(stringReferenceBytes, 0)
        val bytecode = BytecodeBuffer()
        generator.refill(bytecode, ConstantPool(), intArrayOf(), intArrayOf(), arrayOf())

        val stringLength = Instructions.getData(bytecode.get(0))
        val stringPosition = bytecode.get(1)
        val readString = generator.readTextReference(stringPosition, stringLength)
        assertEquals(expectedString, readString)
    }

    @ParameterizedTest
    @MethodSource(LOB_REFERENCE_OPCODE_CASES)
    fun `generator can read lob references`(encodedLobBytes: String, expectedBytecodeString: String, expectedLobBytes: String) {
        val lobReferenceBytes = encodedLobBytes.hexStringToByteArray()
        val generator = ByteArrayBytecodeGenerator11(lobReferenceBytes, 0)
        val bytecode = BytecodeBuffer()
        generator.refill(bytecode, ConstantPool(), intArrayOf(), intArrayOf(), arrayOf())

        val lobLength = Instructions.getData(bytecode.get(0))
        val lobPosition = bytecode.get(1)
        val expectedLob = expectedLobBytes.hexStringToByteArray()
        val readLob = generator.readBytesReference(lobPosition, lobLength).newByteArray()
        assertArrayEquals(expectedLob, readLob)
    }
}
