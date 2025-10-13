package com.amazon.ion.bytecode.bin11.opcodes

import com.amazon.ion.bytecode.ir.Instructions
import com.amazon.ion.bytecode.ir.Instructions.packInstructionData
import com.amazon.ion.bytecode.util.BytecodeBuffer
import com.amazon.ion.bytecode.util.byteToInt
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

private const val OPCODE_TRUE: Byte = 0x6e
private const val OPCODE_FALSE: Byte = 0x6f

class BooleanOpcodeHandlerTest {

    @Test
    fun `handler emits true bytecode for true opcode`() {
        val byteArray: ByteArray = byteArrayOf(OPCODE_TRUE)
        val buffer = BytecodeBuffer()
        val bytesRead = BooleanOpcodeHandler.convertOpcodeToBytecode(
            byteToInt(OPCODE_TRUE),
            byteArray,
            0,
            buffer
        )
        val trueInstruction = Instructions.I_BOOL.packInstructionData(1)
        assertEquals(trueInstruction, buffer.get(0))
        assertEquals(1, bytesRead)
    }

    @Test
    fun `handler emits false bytecode for false opcode`() {
        val byteArray: ByteArray = byteArrayOf(OPCODE_FALSE)
        val buffer = BytecodeBuffer()
        val bytesRead = BooleanOpcodeHandler.convertOpcodeToBytecode(
            byteToInt(OPCODE_FALSE),
            byteArray,
            0,
            buffer
        )
        val falseInstruction = Instructions.I_BOOL.packInstructionData(0)
        assertEquals(falseInstruction, buffer.get(0))
        assertEquals(1, bytesRead)
    }

}
