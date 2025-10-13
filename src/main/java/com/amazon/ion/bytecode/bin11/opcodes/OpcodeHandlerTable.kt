package com.amazon.ion.bytecode.bin11.opcodes

import com.amazon.ion.bytecode.util.BytecodeBuffer

/**
 * Functional interface defining a handler that, given an opcode read off of an input containing binary Ion data and
 * the surrounding context, writes the representative bytecode to a [BytecodeBuffer].
 */
internal fun interface OpcodeToBytecodeHandler {
    /**
     * Given an opcode read off of an input containing binary Ion data and the surrounding context,
     * writes the representative bytecode to a [BytecodeBuffer].
     *
     * @return The number of bytes that had to be read to handle this opcode, including the opcode itself
     * and any additional bytes read off of [source]. Should always be at least 1.
     */
    fun convertOpcodeToBytecode(
        /** The opcode to handle */
        opcode: Int,
        /** Byte buffer containing the source binary data, or a part of it */
        source: ByteArray,
        /** The position in [source] of the current opcode - that is, `source[position] == opcode` */
        position: Int,
        /** The destination bytecode buffer this handler is expected to write to */
        destination: BytecodeBuffer
    ): Int
}

/**
 * Table mapping numeric opcodes to the appropriate [OpcodeToBytecodeHandler], allowing for array-based access to the
 * appropriate handler.
 */
internal object OpcodeHandlerTable {
    private val table = Array<OpcodeToBytecodeHandler>(256) { opcode ->
        when (opcode) {
            0x8e        -> NullOpcodeHandler
            0x8f        -> TypedNullOpcodeHandler
            0x6e, 0x6f  -> BooleanOpcodeHandler
            else        -> OpcodeToBytecodeHandler { _, _, _, _ ->
                TODO("Opcode $opcode not yet implemented")
            }
        }
    }

    /**
     * Retrieves the appropriate [OpcodeToBytecodeHandler] for a given opcode.
     */
    fun handler(opcode: Int): OpcodeToBytecodeHandler = table[opcode]
}
