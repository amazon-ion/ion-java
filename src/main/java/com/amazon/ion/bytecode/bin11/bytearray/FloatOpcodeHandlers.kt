package com.amazon.ion.bytecode.bin11.bytearray

import com.amazon.ion.bytecode.BytecodeEmitter
import com.amazon.ion.bytecode.NumericReader.readDouble
import com.amazon.ion.bytecode.NumericReader.readFloat
import com.amazon.ion.bytecode.NumericReader.readFloat16
import com.amazon.ion.bytecode.util.AppendableConstantPoolView
import com.amazon.ion.bytecode.util.BytecodeBuffer

/**
 * Writes bytecode representing float 0e0 to the bytecode buffer. Handles opcode `0x6A`.
 */
internal object Float0OpcodeHandler : OpcodeToBytecodeHandler {
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
            0f
        )
        return 0
    }
}

/**
 * Writes bytecode for a half-precision float to the bytecode buffer. Handles opcode `0x6A`.
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

/**
 * Writes bytecode for a single-precision float to the bytecode buffer. Handles opcode `0x6A`.
 */
internal object Float32OpcodeHandler : OpcodeToBytecodeHandler {
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
            source.readFloat(position)
        )
        return 4
    }
}

/**
 * Writes bytecode for a double-precision float to the bytecode buffer. Handles opcode `0x6A`.
 */
internal object DoubleOpcodeHandler : OpcodeToBytecodeHandler {
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
        BytecodeEmitter.emitDoubleValue(
            destination,
            source.readDouble(position)
        )
        return 8
    }
}
