// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11.bytearray

import com.amazon.ion.bytecode.BytecodeEmitter
import com.amazon.ion.bytecode.ir.Instructions
import com.amazon.ion.bytecode.ir.Instructions.packInstructionData
import com.amazon.ion.bytecode.util.AppendableConstantPoolView
import com.amazon.ion.bytecode.util.BytecodeBuffer
import com.amazon.ion.impl.bin.FlexInt.readFlexUIntValueAndLength

/**
 * Writes a bytecode representing a reference to a short-form timestamp. Handles opcodes `0x80`-`0x8C`.
 */
internal object ShortTimestampOpcodeHandler : OpcodeToBytecodeHandler {
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
        val precisionAndOffsetMode = opcode and 0xF
        BytecodeEmitter.emitShortTimestampReference(
            destination,
            precisionAndOffsetMode,
            position
        )
        return 0
    }
}
