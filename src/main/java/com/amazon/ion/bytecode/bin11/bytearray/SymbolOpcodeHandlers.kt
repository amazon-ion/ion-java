// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11.bytearray

import com.amazon.ion.bytecode.ir.Instructions
import com.amazon.ion.bytecode.ir.Instructions.packInstructionData
import com.amazon.ion.bytecode.util.AppendableConstantPoolView
import com.amazon.ion.bytecode.util.BytecodeBuffer

/**
 * Writes a symbol with symbol address to the bytecode buffer. Handles opcodes `0x50`-`0x57`.
 */
internal object SymbolSIDOpcodeHandler : OpcodeToBytecodeHandler {
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
        assert(opcode in 0x50..0x57) { "Handler cannot compile opcode ${opcode.toHexString()}" }

        val lsb = opcode and 0b111
        val msbValueAndLength = PrimitiveDecoder.readFlexUIntValueAndLength(source, position)
        val msb = msbValueAndLength.toInt().shl(3)
        val msbLength = msbValueAndLength.shr(Int.SIZE_BITS).toInt()
        val sid = msb or lsb
        destination.add(Instructions.I_SYMBOL_SID.packInstructionData(sid))
        return msbLength
    }
}

/**
 * Writes a single-char string to the bytecode buffer. Handles opcode `0xA1`.
 */
internal object SingleCharSymbolOpcodeHandler : OpcodeToBytecodeHandler {
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
        assert(opcode == 0xa1) { "Handler cannot compile opcode ${opcode.toHexString()}" }

        val char = source[position].toInt()
        destination.add(Instructions.I_SYMBOL_CHAR.packInstructionData(char))
        return 1
    }
}
