package com.amazon.ion.bytecode

import com.amazon.ion.IonException
import com.amazon.ion.IonType
import com.amazon.ion.bytecode.ir.Instructions
import com.amazon.ion.bytecode.ir.Instructions.packInstructionData
import com.amazon.ion.bytecode.util.BytecodeBuffer

/**
 * Defines helper functions for emitting individual instructions to a BytecodeBuffer.
 */
internal object BytecodeEmitter {

    @JvmStatic
    fun emitNullValue(destination: BytecodeBuffer, nullType: IonType) {
        val instruction = when (nullType) {
            IonType.NULL      -> Instructions.I_NULL_NULL
            IonType.BOOL      -> Instructions.I_NULL_BOOL
            IonType.INT       -> Instructions.I_NULL_INT
            IonType.FLOAT     -> Instructions.I_NULL_FLOAT
            IonType.DECIMAL   -> Instructions.I_NULL_DECIMAL
            IonType.TIMESTAMP -> Instructions.I_NULL_TIMESTAMP
            IonType.SYMBOL    -> Instructions.I_NULL_SYMBOL
            IonType.STRING    -> Instructions.I_NULL_STRING
            IonType.CLOB      -> Instructions.I_NULL_CLOB
            IonType.BLOB      -> Instructions.I_NULL_BLOB
            IonType.LIST      -> Instructions.I_NULL_LIST
            IonType.SEXP      -> Instructions.I_NULL_SEXP
            IonType.STRUCT    -> Instructions.I_NULL_STRUCT
            IonType.DATAGRAM  -> throw IonException("Unexpected DATAGRAM as IonType")
        }
        destination.add(instruction)
    }

    @JvmStatic
    fun emitBoolValue(destination: BytecodeBuffer, bool: Boolean) {
        destination.add(Instructions.I_BOOL.packInstructionData(if (bool) 1 else 0));
    }
    
}
