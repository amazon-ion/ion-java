package com.amazon.ion.bytecode.bin11.opcodes

import com.amazon.ion.bytecode.ir.Operation
import com.amazon.ion.bytecode.util.BytecodeBuffer
import com.amazon.ion.bytecode.util.byteToInt
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

private const val OPCODE_NULL: Byte = 0x8e.toByte()

class NullOpcodeHandlerTest {

    @Test
    fun `handler emits null bytecode for null opcode`() {
        val byteArray: ByteArray = byteArrayOf(OPCODE_NULL)
        val buffer = BytecodeBuffer()
        val bytesRead = NullOpcodeHandler.convertOpcodeToBytecode(
            byteToInt(OPCODE_NULL),
            byteArray,
            0,
            buffer
        )
        assertEquals(Operation.OP_NULL_NULL, buffer.get(0))
        assertEquals(1, bytesRead)
    }

}
