// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11.bytearray

import com.amazon.ion.bytecode.BytecodeEmitter
import com.amazon.ion.bytecode.bin11.bytearray.PrimitiveDecoder.readFixedInt16AsShort
import com.amazon.ion.bytecode.bin11.bytearray.PrimitiveDecoder.readFixedInt24AsInt
import com.amazon.ion.bytecode.bin11.bytearray.PrimitiveDecoder.readFixedInt32AsInt
import com.amazon.ion.bytecode.bin11.bytearray.PrimitiveDecoder.readFixedInt8AsShort
import com.amazon.ion.bytecode.bin11.bytearray.PrimitiveDecoder.readFixedIntAsLong
import com.amazon.ion.bytecode.util.AppendableConstantPoolView
import com.amazon.ion.bytecode.util.BytecodeBuffer

/**
 * Writes bytecode representing integer zero to the bytecode buffer. Handles opcode `0x60`.
 */
internal object Int0OpcodeHandler : OpcodeToBytecodeHandler {
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

/**
 * Writes an 8-bit integer bytecode to the bytecode buffer. Handles opcode `0x61`.
 */
internal object Int8OpcodeHandler : OpcodeToBytecodeHandler {
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
            source.readFixedInt8AsShort(position)
        )
        return 1
    }
}

/**
 * Writes a 16-bit integer bytecode to the bytecode buffer. Handles opcode `0x62`.
 */
internal object Int16OpcodeHandler : OpcodeToBytecodeHandler {
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
            source.readFixedInt16AsShort(position)
        )
        return 2
    }
}

/**
 * Writes a 24-bit integer bytecode to the bytecode buffer. Handles opcode `0x63`.
 */
internal object Int24OpcodeHandler : OpcodeToBytecodeHandler {
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
        BytecodeEmitter.emitInt32Value(
            destination,
            source.readFixedInt24AsInt(position)
        )
        return 3
    }
}

/**
 * Writes a 32-bit integer bytecode to the bytecode buffer. Handles opcode `0x64`.
 */
internal object Int32OpcodeHandler : OpcodeToBytecodeHandler {
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
        BytecodeEmitter.emitInt32Value(
            destination,
            source.readFixedInt32AsInt(position)
        )
        return 4
    }
}

/**
 * Writes a 5-to-8-byte integer bytecode to the bytecode buffer. Handles opcodes `0x65`-`0x68`.
 */
internal object LongIntOpcodeHandler : OpcodeToBytecodeHandler {
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
            source.readFixedIntAsLong(position, fixedIntLength)
        )
        return fixedIntLength
    }
}
