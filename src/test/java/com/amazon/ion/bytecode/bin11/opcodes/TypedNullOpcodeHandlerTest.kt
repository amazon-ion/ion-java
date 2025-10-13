package com.amazon.ion.bytecode.bin11.opcodes

import com.amazon.ion.bytecode.ir.Operation
import com.amazon.ion.bytecode.util.BytecodeBuffer
import com.amazon.ion.bytecode.util.byteToInt
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

private const val OPCODE_TYPED_NULL: Byte = 0x8f.toByte()

class TypedNullOpcodeHandlerTest {

    @Test
    fun `handler emits correct bytecode for null bool opcode`() {
        typedNullTest(0x01, Operation.OP_NULL_BOOL)
    }

    @Test
    fun `handler emits correct bytecode for null int opcode`() {
        typedNullTest(0x02, Operation.OP_NULL_INT)
    }

    @Test
    fun `handler emits correct bytecode for null float opcode`() {
        typedNullTest(0x03, Operation.OP_NULL_FLOAT)
    }

    @Test
    fun `handler emits correct bytecode for null decimal opcode`() {
        typedNullTest(0x04, Operation.OP_NULL_DECIMAL)
    }

    @Test
    fun `handler emits correct bytecode for null timestamp opcode`() {
        typedNullTest(0x05, Operation.OP_NULL_TIMESTAMP)
    }

    @Test
    fun `handler emits correct bytecode for null string opcode`() {
        typedNullTest(0x06, Operation.OP_NULL_STRING)
    }

    @Test
    fun `handler emits correct bytecode for null symbol opcode`() {
        typedNullTest(0x07, Operation.OP_NULL_SYMBOL)
    }

    @Test
    fun `handler emits correct bytecode for null blob opcode`() {
        typedNullTest(0x08, Operation.OP_NULL_BLOB)
    }

    @Test
    fun `handler emits correct bytecode for null clob opcode`() {
        typedNullTest(0x09, Operation.OP_NULL_CLOB)
    }

    @Test
    fun `handler emits correct bytecode for null list opcode`() {
        typedNullTest(0x0a, Operation.OP_NULL_LIST)
    }

    @Test
    fun `handler emits correct bytecode for null sexp opcode`() {
        typedNullTest(0x0b, Operation.OP_NULL_SEXP)
    }

    @Test
    fun `handler emits correct bytecode for null struct opcode`() {
        typedNullTest(0x0c, Operation.OP_NULL_STRUCT)
    }


    fun typedNullTest(nextByte: Byte, expectedOperation: Int) {
        val byteArray: ByteArray = byteArrayOf(OPCODE_TYPED_NULL, nextByte)
        val buffer = BytecodeBuffer()
        val bytesRead = TypedNullOpcodeHandler.convertOpcodeToBytecode(
            byteToInt(OPCODE_TYPED_NULL),
            byteArray,
            0,
            buffer
        )
        assertEquals(expectedOperation, buffer.get(0))
        assertEquals(2, bytesRead)
    }

}
