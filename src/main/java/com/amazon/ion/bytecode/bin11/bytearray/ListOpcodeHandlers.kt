// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11.bytearray

import com.amazon.ion.bytecode.BytecodeEmitter
import com.amazon.ion.bytecode.bin11.OpCode
import com.amazon.ion.bytecode.util.AppendableConstantPoolView
import com.amazon.ion.bytecode.util.BytecodeBuffer
import com.amazon.ion.bytecode.util.unsignedToInt

// TODO: much of the logic here is shared between lists and sexps. It might be worthwhile to do something like
//   "SequenceOpcodeHandlers" and pass the start instruction (Instructions.I_LIST_START vs .I_SEXP_START) to a helper
//   BytecodeEmitter.emitSequence() or similar so this logic is not duplicated in a set of `*SexpOpcodeHandler`s.

/**
 * Writes a length prefixed list to the bytecode buffer. Handles opcode `0xB0`-`0xBF`.
 */
internal object ShortLengthPrefixedListOpcodeHandler : OpcodeToBytecodeHandler {
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
        assert(opcode in 0xb0..0xbf) { "Handler cannot compile opcode ${opcode.toHexString()}" }

        val length = opcode and 0xF
        BytecodeEmitter.emitList(destination) {
            var p = position
            val end = p + length
            while (p < end) {
                val opcode = source[p++].unsignedToInt()
                p += OpcodeHandlerTable.handler(opcode).convertOpcodeToBytecode(
                    opcode,
                    source,
                    p,
                    destination,
                    constantPool,
                    macroSrc,
                    macroIndices,
                    symbolTable,
                )
            }
        }
        return length
    }
}

/**
 * Writes a length prefixed list to the bytecode buffer. Handles opcode `0xFA`.
 */
internal object LongLengthPrefixedListOpcodeHandler : OpcodeToBytecodeHandler {
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
        assert(opcode == 0xfa) { "Handler cannot compile opcode ${opcode.toHexString()}" }

        val containerSizeUIntValueAndLength = PrimitiveDecoder.readFlexUIntValueAndLength(source, position)
        val containerLength = containerSizeUIntValueAndLength.toInt()
        val prefixLength = containerSizeUIntValueAndLength.shr(Int.SIZE_BITS).toInt()
        BytecodeEmitter.emitList(destination) {
            var p = position + prefixLength
            val end = p + containerLength
            while (p < end) {
                val opcode = source[p++].unsignedToInt()
                p += OpcodeHandlerTable.handler(opcode).convertOpcodeToBytecode(
                    opcode,
                    source,
                    p,
                    destination,
                    constantPool,
                    macroSrc,
                    macroIndices,
                    symbolTable,
                )
            }
        }
        return containerLength + prefixLength
    }
}

/**
 * Writes a delimited list to the bytecode buffer. Handles opcode `0xF0`.
 */
internal object DelimitedListOpcodeHandler : OpcodeToBytecodeHandler {
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
        assert(opcode == 0xf0) { "Handler cannot compile opcode ${opcode.toHexString()}" }

        var p = position
        BytecodeEmitter.emitList(destination) {
            while (true) {
                val opcode = source[p++].unsignedToInt()
                if (opcode == OpCode.DELIMITED_CONTAINER_END) {
                    break
                }
                p += OpcodeHandlerTable.handler(opcode).convertOpcodeToBytecode(
                    opcode,
                    source,
                    p,
                    destination,
                    constantPool,
                    macroSrc,
                    macroIndices,
                    symbolTable,
                )
            }
        }
        val bytesRead = p - position
        return bytesRead
    }
}

/**
 * Writes a tagless-element list to the bytecode buffer. Handles opcode `0x5B`.
 */
internal object TaglessElementListOpcodeHandler : OpcodeToBytecodeHandler {
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
        assert(opcode == 0x5b) { "Handler cannot compile opcode ${opcode.toHexString()}" }

        var p = position
        val childOpcode = source[p++].unsignedToInt()
        val macroAddress = when (childOpcode) {
            in 0x00..0x47 -> childOpcode
            in 0x48..0x4f, 0xf4 -> {
                val addressValueAndLength = PrimitiveDecoder.readFlexUIntValueAndLength(source, p)
                val addressValue = addressValueAndLength.toInt()
                val addressLength = addressValueAndLength.shr(Int.SIZE_BITS).toInt()
                p += addressLength
                addressValue
            }
            else -> -1
        }

        val containerSizeValueAndLength = PrimitiveDecoder.readFlexUIntValueAndLength(source, p)
        val containerLength = containerSizeValueAndLength.toInt()
        val prefixLength = containerSizeValueAndLength.shr(Int.SIZE_BITS).toInt()
        p += prefixLength

        // If macroAddress > -1, then it is the address of the macro-shaped values,
        // and childOpcode should be ignored.
        // If macroAddress is -1, then childOpcode is the opcode of the values.
        if (macroAddress < 0) {
            val handler = TaglessOpcodeHandlerTable.handler(childOpcode)
            BytecodeEmitter.emitList(destination) {
                for (i in 0 until containerLength) {
                    p += handler.convertOpcodeToBytecode(
                        childOpcode,
                        source,
                        p,
                        destination,
                        constantPool,
                        macroSrc,
                        macroIndices,
                        symbolTable,
                    )
                }
            }
        } else {
            TODO("Macro evaluation not yet implemented")
        }

        return p - position
    }
}
