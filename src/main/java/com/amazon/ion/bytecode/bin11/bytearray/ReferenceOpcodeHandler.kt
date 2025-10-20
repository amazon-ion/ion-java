// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11.bytearray

import com.amazon.ion.bytecode.BytecodeEmitter
import com.amazon.ion.bytecode.bin11.bytearray.PrimitiveDecoder.readFlexUIntValueAndLength
import com.amazon.ion.bytecode.util.AppendableConstantPoolView
import com.amazon.ion.bytecode.util.BytecodeBuffer

/**
 * Writes a bytecode representing a reference to a variable-length payload of a particular data type.
 * Handles similar opcodes followed by a FlexUInt length and simple payload (`0x59`, `0xF5`-`0xF9`, `0xFE`, `0xFF`).
 */
// TODO: potentially refactor to be consistent with TaglessUIntHandlers
internal class ReferenceOpcodeHandler(
    /**
     * The instruction to write when the handler is invoked. Should be a `I_*_REF` type.
     */
    private val instruction: Int
) : OpcodeToBytecodeHandler {
    override fun convertOpcodeToBytecode(
        opcode: Int,
        source: ByteArray,
        position: Int,
        destination: BytecodeBuffer,
        constantPool: AppendableConstantPoolView,
        macroSrc: IntArray,
        macroIndices: IntArray,
        symbolTable: Array<String?>
    ): Int {
        val payloadSizeFlexUIntValueAndLength = readFlexUIntValueAndLength(source, position)
        val payloadSize = payloadSizeFlexUIntValueAndLength.toInt()
        val payloadSizeFlexUIntLength = payloadSizeFlexUIntValueAndLength.shr(Int.SIZE_BITS).toInt()
        BytecodeEmitter.emitReference(
            destination,
            instruction,
            payloadSize,
            position + payloadSizeFlexUIntLength
        )
        return payloadSizeFlexUIntLength + payloadSize
    }
}
