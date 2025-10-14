// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11.bytearray.fixedint

import com.amazon.ion.bytecode.BytecodeEmitter
import com.amazon.ion.bytecode.bin11.bytearray.OpcodeToBytecodeHandler
import com.amazon.ion.bytecode.util.AppendableConstantPoolView
import com.amazon.ion.bytecode.util.BytecodeBuffer

/**
 * Writes bytecode representing integer zero to the bytecode buffer. Handles opcode `0x60`.
 */
internal object FixedInt0OpcodeHandler : OpcodeToBytecodeHandler {
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
            0
        )
        return 0
    }
}
