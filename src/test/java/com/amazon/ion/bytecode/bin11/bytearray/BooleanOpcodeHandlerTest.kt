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
import org.junit.jupiter.api.Test

class BooleanOpcodeHandlerTest {

    @Test
    fun `handler emits true bytecode for true opcode`() {
        val byteArray: ByteArray = "6E".hexStringToByteArray()
        val buffer = BytecodeBuffer()

        var position = 0
        val opcode = byteArray[position++].unsignedToInt()
        position += BooleanOpcodeHandler.convertOpcodeToBytecode(
            opcode,
            byteArray,
            position,
            buffer,
            ConstantPool(0),
            intArrayOf(),
            intArrayOf(),
            arrayOf()
        )

        val expectedInstruction = Instructions.I_BOOL.packInstructionData(1)
        assertEqualBytecode(intArrayOf(expectedInstruction), buffer.toArray())
        assertEquals(1, position)
    }

    @Test
    fun `handler emits false bytecode for false opcode`() {
        val byteArray: ByteArray = "6F".hexStringToByteArray()
        val buffer = BytecodeBuffer()

        var position = 0
        val opcode = byteArray[position++].unsignedToInt()
        position += BooleanOpcodeHandler.convertOpcodeToBytecode(
            opcode,
            byteArray,
            position,
            buffer,
            ConstantPool(0),
            intArrayOf(),
            intArrayOf(),
            arrayOf()
        )

        val expectedInstruction = Instructions.I_BOOL.packInstructionData(0)
        assertEqualBytecode(intArrayOf(expectedInstruction), buffer.toArray())
        assertEquals(1, position)
    }
}
