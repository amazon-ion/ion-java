// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11.bytearray

import com.amazon.ion.bytecode.BytecodeEmitter
import com.amazon.ion.bytecode.util.AppendableConstantPoolView
import com.amazon.ion.bytecode.util.BytecodeBuffer
import com.amazon.ion.impl.bin.FlexInt.readFlexUIntValueAndLength

/**
 * Writes a bytecode representing a reference to a particular dat.
 */
// TODO(perf): Could it be more efficient to not use a class here and copy-paste separate objects for each
//  instruction type? Does accessing the class field add overhead?
internal class GenericReferenceOpcodeHandler(
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
        return payloadSizeFlexUIntLength
    }
}
