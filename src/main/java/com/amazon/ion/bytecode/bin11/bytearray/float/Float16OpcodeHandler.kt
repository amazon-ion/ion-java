// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11.bytearray.float

import com.amazon.ion.bytecode.BytecodeEmitter
import com.amazon.ion.bytecode.NumericReader.readFloat16
import com.amazon.ion.bytecode.bin11.bytearray.OpcodeToBytecodeHandler
import com.amazon.ion.bytecode.util.AppendableConstantPoolView
import com.amazon.ion.bytecode.util.BytecodeBuffer

/**
 * Writes bytecode representing float 0e0 to the bytecode buffer. Handles opcode `0x6A`.
 */
internal object Float16OpcodeHandler : OpcodeToBytecodeHandler {
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
        BytecodeEmitter.emitFloatValue(
            destination,
            source.readFloat16(position)
        )
        return 2
    }
}
