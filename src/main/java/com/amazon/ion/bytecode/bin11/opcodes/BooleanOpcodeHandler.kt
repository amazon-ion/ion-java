package com.amazon.ion.bytecode.bin11.opcodes

import com.amazon.ion.bytecode.BytecodeEmitter
import com.amazon.ion.bytecode.util.BytecodeBuffer

/**
 * Writes a boolean bytecode to the bytecode buffer. Handles opcodes `0x6E` and `0x6F`.
 */
internal object BooleanOpcodeHandler : OpcodeToBytecodeHandler {
    override fun convertOpcodeToBytecode(
        opcode: Int,
        source: ByteArray,
        position: Int,
        destination: BytecodeBuffer
    ): Int {
        BytecodeEmitter.emitBoolValue(destination, opcode == 0x6E)
        return 1
    }
}
