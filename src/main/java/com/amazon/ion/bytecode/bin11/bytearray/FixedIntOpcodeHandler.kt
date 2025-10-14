// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11.bytearray

import com.amazon.ion.bytecode.BinaryPrimitiveReader.readFixedIntAt
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
            0 -> BytecodeEmitter.emitInt16Value(destination, 0)
            in 1..2 -> BytecodeEmitter.emitInt16Value(
                destination,
                readFixedIntAt(source, position, fixedIntLength).toShort()
            )
            in 3..4 -> BytecodeEmitter.emitInt32Value(
                destination,
                readFixedIntAt(source, position, fixedIntLength).toInt()
            )
            in 5..8 -> BytecodeEmitter.emitInt64Value(
                destination,
                readFixedIntAt(source, position, fixedIntLength)
            )
        }
        return fixedIntLength
    }
}
