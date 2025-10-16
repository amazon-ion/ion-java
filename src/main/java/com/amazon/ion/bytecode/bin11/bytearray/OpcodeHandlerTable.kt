// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11.bytearray

import com.amazon.ion.bytecode.util.AppendableConstantPoolView
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
     * @return The number of additional bytes that had to be read off of [source] to handle this opcode. Should be zero
     * if it was possible to handle the opcode without reading any additional data.
     */
    fun convertOpcodeToBytecode(
        /** The opcode to handle */
        opcode: Int,
        /** Byte buffer containing the source binary data, or a part of it */
        source: ByteArray,
        /** The position of the next unread byte in [source] */
        position: Int,
        /** The destination bytecode buffer this handler is expected to write to */
        destination: BytecodeBuffer,
        /** Container holding instances of eagerly materialized values, such as those in template definitions */
        constantPool: AppendableConstantPoolView,
        /** Bytecode for each macro in the effective macro table */
        macroSrc: IntArray,
        /**
         * A lookup table indicating for each macro address, where to find the first
         * instruction for that macro in [macroSrc]. That is, for the macro with address `i`,
         * `macroSrc[macroIndices[i]]` is its first instruction. For example, to read the bytecode
         * for the macro, you would do something like this:
         * ```
         * var i = macroIndices[macroAddress]
         * var currentInstruction = macroSrc[i++]
         * // ...
         * ```
         */
        macroIndices: IntArray,
        /** The current symbol table */
        symbolTable: Array<String?>,
    ): Int
}

/**
 * Table mapping numeric opcodes to the appropriate [OpcodeToBytecodeHandler], allowing for array-based access to the
 * appropriate handler.
 */
internal object OpcodeHandlerTable {
    private val table = Array<OpcodeToBytecodeHandler>(256) { opcode ->
        when (opcode) {
            0x60 -> Int0OpcodeHandler
            0x61 -> Int8OpcodeHandler
            0x62 -> Int16OpcodeHandler
            0x63 -> Int24OpcodeHandler
            0x64 -> Int32OpcodeHandler
            in 0x65..0x68 -> LongIntOpcodeHandler
            0x6a -> Float0OpcodeHandler
            0x6b -> Float16OpcodeHandler
            0x6c -> Float32OpcodeHandler
            0x6d -> DoubleOpcodeHandler
            0x8e -> NullOpcodeHandler
            0x8f -> TypedNullOpcodeHandler
            0x6e, 0x6f -> BooleanOpcodeHandler
            else -> OpcodeToBytecodeHandler { _, _, _, _, _, _, _, _ ->
                TODO("Opcode $opcode not yet implemented")
            }
        }
    }

    /**
     * Retrieves the appropriate [OpcodeToBytecodeHandler] for a given opcode.
     *
     * TODO: this costs an unnecessary function call for every opcode handled. The performance of this
     *  vs. exposing the lookup table itself and accessing directly by index should be investigated.
     */
    fun handler(opcode: Int): OpcodeToBytecodeHandler = table[opcode]
}
