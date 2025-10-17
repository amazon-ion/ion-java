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

    // TODO: not all opcodes are checked here
    @ParameterizedTest
    @CsvSource(
        "80 35, 0", // 2023T
        "81 35 05, 1", // 2023-10T
        "82 35 7D, 2", // 2023-10-15T
        "83 35 7D CB 0A, 3", // 2023-10-15T11:22Z
        "84 35 7D CB 1A 02, 4", // 2023-10-15T11:22:33Z
        "84 35 7D CB 12 02, 4", // 2023-10-15T11:22:33-00:00
        "85 35 7D CB 12 F2 06, 5", // 2023-10-15T11:22:33.444-00:00
        "88 35 7D CB 2A 00, 8", // 2023-10-15T11:22+01:15
        "89 35 7D CB 2A 84, 9", // 2023-10-15T11:22:33+01:15
        "8C 35 7D CB 2A 84 92 61 7F 1A, 12", // 2023-10-15T11:22:33.444555666+01:15
    )
    fun `short timestamp opcode handler emits correct bytecode`(
        inputString: String,
        expectedPrecisionAndOffsetMode: Int
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

        val expectedPosition = 1
        val expectedBytecode = intArrayOf(
            Instructions.I_SHORT_TIMESTAMP_REF.packInstructionData(expectedPrecisionAndOffsetMode),
            expectedPosition
        )
        assertEqualBytecode(expectedBytecode, buffer.toArray())
        assertEquals(expectedPosition, position)
    }
}
