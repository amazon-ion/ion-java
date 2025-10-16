// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11.bytearray

import com.amazon.ion.bytecode.BytecodeEmitter
import com.amazon.ion.bytecode.bin11.bytearray.PrimitiveDecoder.readFixedInt16
import com.amazon.ion.bytecode.bin11.bytearray.PrimitiveDecoder.readFixedInt24AsInt
import com.amazon.ion.bytecode.bin11.bytearray.PrimitiveDecoder.readFixedInt32
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
            readFixedInt8AsShort(source, position)
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
            readFixedInt16(source, position)
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
            readFixedInt24AsInt(source, position)
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
            readFixedInt32(source, position)
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
            readFixedIntAsLong(source, position, fixedIntLength)
        )
        return fixedIntLength
    }
}
