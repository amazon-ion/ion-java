package com.amazon.ion.bytecode

import com.amazon.ion.*
import com.amazon.ion.bytecode.ir.Instructions.packInstructionData
import com.amazon.ion.bytecode.ir.Operation
import com.amazon.ion.bytecode.util.BytecodeBuffer

/**
 * Defines helper functions for emitting individual instructions to a BytecodeBuffer.
 */
internal object BytecodeEmitter {

    @JvmStatic
    fun emitNullValue(destination: BytecodeBuffer, nullType: IonType) {
        val instruction = when (nullType) {
            IonType.NULL      -> Operation.OP_NULL_NULL
            IonType.BOOL      -> Operation.OP_NULL_BOOL
            IonType.INT       -> Operation.OP_NULL_INT
            IonType.FLOAT     -> Operation.OP_NULL_FLOAT
            IonType.DECIMAL   -> Operation.OP_NULL_DECIMAL
            IonType.TIMESTAMP -> Operation.OP_NULL_TIMESTAMP
            IonType.SYMBOL    -> Operation.OP_NULL_SYMBOL
            IonType.STRING    -> Operation.OP_NULL_STRING
            IonType.CLOB      -> Operation.OP_NULL_CLOB
            IonType.BLOB      -> Operation.OP_NULL_BLOB
            IonType.LIST      -> Operation.OP_NULL_LIST
            IonType.SEXP      -> Operation.OP_NULL_SEXP
            IonType.STRUCT    -> Operation.OP_NULL_STRUCT
            IonType.DATAGRAM  -> throw IonException("Unexpected DATAGRAM as IonType")
        }
        destination.add(instruction)
    }

    @JvmStatic
    fun emitBoolValue(destination: BytecodeBuffer, bool: Boolean) {
        destination.add(Operation.OP_BOOL.packInstructionData(if (bool) 1 else 0));
    }
    
}
