// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11.bytearray

import com.amazon.ion.bytecode.ir.Instructions
import com.amazon.ion.bytecode.ir.Instructions.packInstructionData
import com.amazon.ion.bytecode.util.AppendableConstantPoolView
import com.amazon.ion.bytecode.util.BytecodeBuffer

/**
 * Writes an annotation with symbol address to the bytecode buffer. Handles opcode `0x58`.
 */
internal object AnnotationSIDOpcodeHandler : OpcodeToBytecodeHandler {
    @OptIn(ExperimentalStdlibApi::class)
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
        assert(opcode == 0x58) { "Handler cannot compile opcode ${opcode.toHexString()}" }

        val sidValueAndLength = PrimitiveDecoder.readFlexUIntValueAndLength(source, position)
        val sid = sidValueAndLength.toInt()
        val length = sidValueAndLength.shr(Int.SIZE_BITS).toInt()
        destination.add(Instructions.I_ANNOTATION_SID.packInstructionData(sid))
        return length
    }
}
