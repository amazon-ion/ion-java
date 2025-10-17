// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11.bytearray

import com.amazon.ion.TextToBinaryUtils.hexStringToByteArray
import com.amazon.ion.bytecode.GeneratorTestUtil.assertEqualBytecode
import com.amazon.ion.bytecode.ir.Instructions
import com.amazon.ion.bytecode.ir.Instructions.packInstructionData
import com.amazon.ion.bytecode.util.BytecodeBuffer
import com.amazon.ion.bytecode.util.ConstantPool
import com.amazon.ion.bytecode.util.unsignedToInt
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import kotlin.random.Random

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class GenericReferenceOpcodeHandlerTest {

    /**
     * Generates test cases in the form:
     * - Instance of [GenericReferenceOpcodeHandler] to test
     * - Input [ByteArray]
     * - Expected bytecode array as [IntArray]
     * - Expected end position as [Int]
     */
    private fun referenceOpcodeHandlerTestCases(): List<Arguments> {
        val arguments = mutableListOf<Arguments>()

        val instructions = arrayOf(
            Pair(Instructions.I_INT_REF, 0xF5),
            Pair(Instructions.I_DECIMAL_REF, 0xF6),
            Pair(Instructions.I_TIMESTAMP_REF, 0xF7),
            Pair(Instructions.I_STRING_REF, 0xF8),
            Pair(Instructions.I_SYMBOL_REF, 0xF9),
            Pair(Instructions.I_BLOB_REF, 0xFE),
            Pair(Instructions.I_CLOB_REF, 0xFF),
        )

        val z = listOf(
            // FlexUInt representation of payload length, decimal payload length, payload start position
            "03, 1, 2",
            "05, 2, 2",
            "07, 3, 2",
            "09, 4, 2",
            "0B, 5, 2",
            "1D, 14, 2",
            "7F, 63, 2",
            "81, 64, 2",
            "FF, 127, 2",
            "02 02, 128, 3",
            "FE FF, 16383, 3",
            "04 00 02, 16384, 4",
            "FC FF FF, 2097151, 4",
            "08 00 00 02, 2097152, 5",
            "F8 FF FF 03, 4194303, 5", // maximum length of a payload
            "00 18 00 00 00 00 00 00 00 00 00 00, 1, 13", // overlong encoding on the FlexUInt
            "01, 0, 2", // zero-length payload  TODO: is this legal?
        )

        val random = Random(100) // Seed so we get the same values every time
        instructions.forEach { (instruction, opcode) ->
            z.forEach {
                val (flexUIntStr, payloadLengthStr, expectedEndPositionStr) = it.split(',')
                val payloadLength = payloadLengthStr.trim().toInt()
                val expectedEndPosition = expectedEndPositionStr.trim().toInt()

                val payload = Array(payloadLength) { random.nextBits(8).toByte() }.toByteArray()
                arguments.add(
                    Arguments.of(
                        GenericReferenceOpcodeHandler(instruction),
                        byteArrayOf(
                            opcode.toByte(), // write the opcode
                            *flexUIntStr.hexStringToByteArray(), // then the FlexUInt
                            *payload // then the payload bytes
                        ),
                        intArrayOf(
                            instruction.packInstructionData(payloadLength),
                            expectedEndPosition
                        ),
                        expectedEndPosition
                    )
                )
            }
        }

        return arguments
    }

    /**
     * Test that variable-length payload opcodes generate the correct *_REF bytecode.
     * Does not validate the actual payload in any way.
     */
    @ParameterizedTest
    @MethodSource("referenceOpcodeHandlerTestCases")
    fun `opcodes with variable-length payloads emit correct bytecode`(
        handler: GenericReferenceOpcodeHandler,
        inputByteArray: ByteArray,
        expectedBytecode: IntArray,
        expectedEndPosition: Int
    ) {
        val buffer = BytecodeBuffer()

        var position = 0
        val opcode = inputByteArray[position++].unsignedToInt()
        position += handler.convertOpcodeToBytecode(
            opcode,
            inputByteArray,
            position,
            buffer,
            ConstantPool(0),
            intArrayOf(),
            intArrayOf(),
            arrayOf()
        )

        assertEqualBytecode(expectedBytecode, buffer.toArray())
        assertEquals(expectedEndPosition, position)
    }
}
