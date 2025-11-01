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
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import kotlin.String

class ShortTimestampOpcodeHandlerTest {

    @ParameterizedTest
    @CsvSource(
        "80 35,                          2", // 2023T
        "81 35 05,                       3", // 2023-10T
        "82 35 7D,                       3", // 2023-10-15T
        "83 35 7D CB 0A,                 5", // 2023-10-15T11:22Z
        "84 35 7D CB 1A 02,              6", // 2023-10-15T11:22:33Z
        "84 35 7D CB 12 02,              6", // 2023-10-15T11:22:33-00:00
        "85 35 7D CB 12 F2 06,           7", // 2023-10-15T11:22:33.444-00:00
        "86 35 7D CB 12 2E 22 1B,        8", // 2023-10-15T11:22:33.444555-00:00
        "87 35 7D CB 12 4A 86 FD 69,     9", // 2023-10-15T11:22:33.444555666-00:00
        "88 35 7D CB EA 01,              6", // 2023-10-15T11:22+01:15
        "89 35 7D CB EA 85,              6", // 2023-10-15T11:22:33+01:15
        "8A 35 7D CB EA 85 BC 01,        8", // 2023-10-15T11:22:33.444+01:15
        "8B 35 7D CB EA 85 8B C8 06,     9", // 2023-10-15T11:22:33.444555+01:15
        "8C 35 7D CB EA 85 92 61 7F 1A, 10", // 2023-10-15T11:22:33.444555666+01:15
    )
    fun `short timestamp opcode handler emits correct bytecode`(
        inputString: String,
        expectedEndPosition: Int
    ) {
        val inputByteArray = inputString.hexStringToByteArray()
        val buffer = BytecodeBuffer()

        var position = 0
        val opcode = inputByteArray[position++].unsignedToInt()
        position += ShortTimestampOpcodeHandler.convertOpcodeToBytecode(
            opcode,
            inputByteArray,
            position,
            buffer,
            ConstantPool(0),
            intArrayOf(),
            intArrayOf(),
            arrayOf()
        )

        val expectedPayloadStartPosition = 1
        val expectedBytecode = intArrayOf(
            Instructions.I_SHORT_TIMESTAMP_REF.packInstructionData(inputByteArray[0].unsignedToInt()),
            expectedPayloadStartPosition
        )
        assertEqualBytecode(expectedBytecode, buffer.toArray())
        assertEquals(expectedEndPosition, position)
    }
}
