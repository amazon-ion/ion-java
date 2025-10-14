// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11.bytearray.fixedint

import com.amazon.ion.bytecode.BinaryPrimitiveReader.readFixedIntAsLong
import com.amazon.ion.bytecode.BytecodeEmitter
import com.amazon.ion.bytecode.bin11.bytearray.OpcodeToBytecodeHandler
import com.amazon.ion.bytecode.util.AppendableConstantPoolView
import com.amazon.ion.bytecode.util.BytecodeBuffer

/**
 * Writes a 5-to-8-byte integer bytecode to the bytecode buffer. Handles opcodes `0x65`-`0x68`.
 */
internal object FixedLongIntOpcodeHandler : OpcodeToBytecodeHandler {
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
        BytecodeEmitter.emitInt64Value(
            destination,
            readFixedIntAsLong(source, position, fixedIntLength)
        )
        return fixedIntLength
    }
}
