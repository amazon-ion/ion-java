// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11.bytearray

import com.amazon.ion.bytecode.BinaryPrimitiveReader.readFixedInt16AsShort
import com.amazon.ion.bytecode.BinaryPrimitiveReader.readFixedInt24AsInt
import com.amazon.ion.bytecode.BinaryPrimitiveReader.readFixedInt32AsInt
import com.amazon.ion.bytecode.BinaryPrimitiveReader.readFixedInt8AsShort
import com.amazon.ion.bytecode.BinaryPrimitiveReader.readFixedIntAsLong
import com.amazon.ion.bytecode.BytecodeEmitter
import com.amazon.ion.bytecode.util.AppendableConstantPoolView
import com.amazon.ion.bytecode.util.BytecodeBuffer

/**
 * Writes a zero-to-8-byte integer bytecode to the bytecode buffer. Handles opcodes `0x60`-`0x68`.
 */
internal object FixedIntOpcodeHandler : OpcodeToBytecodeHandler {
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
        val fixedIntLength = opcode and 0xF
        when (fixedIntLength) {
            // We are already doing the branching here to figure out which kind of instruction to write, we might
            // as well have some helper functions to read the exact length of the FixedInt. Note that for short,
            // `1 -> ..., 2 -> ...` is the same amount of conditionals as `in 1..2 -> ...` and similar for int.
            0 -> BytecodeEmitter.emitInt16Value(destination, 0)
            1 -> BytecodeEmitter.emitInt16Value(
                destination,
                readFixedInt8AsShort(source, position)
            )
            2 -> BytecodeEmitter.emitInt16Value(
                destination,
                readFixedInt16AsShort(source, position)
            )
            3 -> BytecodeEmitter.emitInt32Value(
                destination,
                readFixedInt24AsInt(source, position)
            )
            4 -> BytecodeEmitter.emitInt32Value(
                destination,
                readFixedInt32AsInt(source, position)
            )
            else -> BytecodeEmitter.emitInt64Value(
                destination,
                readFixedIntAsLong(source, position, fixedIntLength)
            )
        }
        return fixedIntLength
    }
}
