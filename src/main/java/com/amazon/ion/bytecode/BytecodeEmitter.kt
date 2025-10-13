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
}
