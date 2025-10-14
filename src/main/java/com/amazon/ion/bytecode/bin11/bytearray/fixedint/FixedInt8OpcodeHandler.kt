// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11.bytearray.fixedint

import com.amazon.ion.bytecode.BinaryPrimitiveReader.readFixedInt8AsShort
import com.amazon.ion.bytecode.BytecodeEmitter
import com.amazon.ion.bytecode.bin11.bytearray.OpcodeToBytecodeHandler
import com.amazon.ion.bytecode.util.AppendableConstantPoolView
import com.amazon.ion.bytecode.util.BytecodeBuffer

/**
 * Writes an 8-bit integer bytecode to the bytecode buffer. Handles opcode `0x61`.
 */
internal object FixedInt8OpcodeHandler : OpcodeToBytecodeHandler {
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
        BytecodeEmitter.emitInt16Value(
            destination,
            readFixedInt8AsShort(source, position)
        )
        return 1
    }
}
