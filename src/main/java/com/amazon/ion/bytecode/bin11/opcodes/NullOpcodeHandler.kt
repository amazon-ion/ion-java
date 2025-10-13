package com.amazon.ion.bytecode.bin11.opcodes

import com.amazon.ion.IonType
import com.amazon.ion.bytecode.BytecodeEmitter
import com.amazon.ion.bytecode.util.BytecodeBuffer

/**
 * Writes an untyped null bytecode to the bytecode buffer. Handles opcode `0x8E`.
 */
internal object NullOpcodeHandler : OpcodeToBytecodeHandler {
    override fun convertOpcodeToBytecode(
        opcode: Int,
        source: ByteArray,
        position: Int,
        destination: BytecodeBuffer
    ): Int {
        BytecodeEmitter.emitNullValue(destination, IonType.NULL)
        return 1
    }
}
