// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11.bytearray

import com.amazon.ion.bytecode.BytecodeEmitter
import com.amazon.ion.bytecode.util.AppendableConstantPoolView
import com.amazon.ion.bytecode.util.BytecodeBuffer

/**
 * Writes a bytecode representing a reference to a short-form timestamp. Handles opcodes `0x80`-`0x8C`.
 */
// TODO: this handler might be worth moving into the same file as either the (eventual) long timestamp handler
internal object ShortTimestampOpcodeHandler : OpcodeToBytecodeHandler {
    /**
     * Maps the precision and offset mode (low nibble of the opcode) to the length of the timestamp's payload
     * in bytes.
     *
     * Note that this mapping can also be expressed with a pretty tight conditional:
     *
     * ```kotlin
     * when (precisionAndOffsetMode) {
     *      0x2 -> 2
     *      0x9 -> 5
     *      in 0x0..0x7 -> precisionAndOffsetMode + 1
     *      else -> precisionAndOffsetMode - 3
     * }
     * ```
     */
    private val serializedSizeByOpcodeTable = intArrayOf(
        1, 2, 2, 4, 5, 6, 7, 8, 5, 5, 7, 8, 9
    )

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
        BytecodeEmitter.emitShortTimestampReference(
            destination,
            opcode,
            position
        )
        val precisionAndOffsetMode = opcode and 0xF
        return serializedSizeByOpcodeTable[precisionAndOffsetMode]
    }
}
