// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11.bytearray

import com.amazon.ion.bytecode.ir.Instructions
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
     * if it was possible to handle the opcode without reading any additional data. Skipping this many bytes in the
     * input plus one will position the caller at the start of the next expression (the opcode for an opcode-prefixed
     * value, or the first byte of a tagless value payload, etc.).
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
            0x59 -> GenericReferenceOpcodeHandler(Instructions.I_ANNOTATION_REF)
            0x60 -> Int0OpcodeHandler
            0x61 -> Int8OpcodeHandler
            0x62 -> Int16OpcodeHandler
            0x63 -> Int24OpcodeHandler
            0x64 -> Int32OpcodeHandler
            in 0x65..0x68 -> LongIntOpcodeHandler
            in 0x80..0x8c -> ShortTimestampOpcodeHandler
            0x8e -> NullOpcodeHandler
            0x8f -> TypedNullOpcodeHandler
            0x6e, 0x6f -> BooleanOpcodeHandler
            0xf5 -> GenericReferenceOpcodeHandler(Instructions.I_INT_REF)
            0xf6 -> GenericReferenceOpcodeHandler(Instructions.I_DECIMAL_REF)
            0xf7 -> GenericReferenceOpcodeHandler(Instructions.I_TIMESTAMP_REF)
            0xf8 -> GenericReferenceOpcodeHandler(Instructions.I_STRING_REF)
            0xf9 -> GenericReferenceOpcodeHandler(Instructions.I_SYMBOL_REF)
            0xfe -> GenericReferenceOpcodeHandler(Instructions.I_BLOB_REF)
            0xff -> GenericReferenceOpcodeHandler(Instructions.I_CLOB_REF)
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
