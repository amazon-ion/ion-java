// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode

import com.amazon.ion.IonType
import com.amazon.ion.bytecode.ir.Instructions
import com.amazon.ion.bytecode.ir.Instructions.packInstructionData
import com.amazon.ion.bytecode.util.BytecodeBuffer

/**
 * Defines helper functions for emitting individual instructions to a BytecodeBuffer.
 */
internal object BytecodeEmitter {

    /**
     * Table of null instructions indexed by the ordinal of the corresponding [IonType] enum.
     * That is, for example, `nullInstructionTable[IonType.TIMESTAMP.ordinal] == Instructions.I_NULL_TIMESTAMP`
     * and similar. */
    private val nullInstructionTable = arrayOf(
        Instructions.I_NULL_NULL,
        Instructions.I_NULL_BOOL,
        Instructions.I_NULL_INT,
        Instructions.I_NULL_FLOAT,
        Instructions.I_NULL_DECIMAL,
        Instructions.I_NULL_TIMESTAMP,
        Instructions.I_NULL_SYMBOL,
        Instructions.I_NULL_STRING,
        Instructions.I_NULL_CLOB,
        Instructions.I_NULL_BLOB,
        Instructions.I_NULL_LIST,
        Instructions.I_NULL_SEXP,
        Instructions.I_NULL_STRUCT
    )

    @JvmStatic
    fun emitNullValue(destination: BytecodeBuffer, nullType: IonType) {
        val instruction = nullInstructionTable[nullType.ordinal]
        destination.add(instruction)
    }

    @JvmStatic
    fun emitBoolValue(destination: BytecodeBuffer, bool: Boolean) {
        destination.add(Instructions.I_BOOL.packInstructionData(if (bool) 1 else 0))
    }

    @JvmStatic
    fun emitInt16Value(destination: BytecodeBuffer, int16: Short) {
        // The unused portion of the data region of the instruction will be filled with the sign
        // of this int16.
        // For positive int16, the high 6 bits of the data region of the instruction will be 0.
        // For negative int16, the high 6 bits of the data region of the instruction will be 1.
        // This does not matter, since a call to .toShort() on the int value of the instruction data
        // will truncate the high 16 bits and return the correct value either way. Therefore, we don't
        // have to do `int16.toInt() and 0xFFFF` or similar here, or when we read the short back out of the
        // instruction with Instructions.getData(...).toShort().
        destination.add(Instructions.I_INT_I16.packInstructionData(int16.toInt()))
    }

    @JvmStatic
    fun emitInt32Value(destination: BytecodeBuffer, int32: Int) {
        destination.add2(Instructions.I_INT_I32, int32)
    }

    @JvmStatic
    fun emitInt64Value(destination: BytecodeBuffer, int64: Long) {
        // Stores high 4 bytes in first operand, low 4 bytes in the second operand
        destination.add3(Instructions.I_INT_I64, (int64 ushr 32).toInt(), int64.toInt())
    }
}
