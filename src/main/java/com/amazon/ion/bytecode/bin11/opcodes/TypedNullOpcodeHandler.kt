package com.amazon.ion.bytecode.bin11.opcodes

import com.amazon.ion.IonException
import com.amazon.ion.IonType
import com.amazon.ion.bytecode.BytecodeEmitter
import com.amazon.ion.bytecode.util.BytecodeBuffer
import com.amazon.ion.bytecode.util.byteToInt

/**
 * Writes a typed null bytecode to the bytecode buffer. Handles opcode `0x8F`.
 */
internal object TypedNullOpcodeHandler : OpcodeToBytecodeHandler {
    private val typeTable = arrayOf(
        IonType.BOOL,
        IonType.INT,
        IonType.FLOAT,
        IonType.DECIMAL,
        IonType.TIMESTAMP,
        IonType.STRING,
        IonType.SYMBOL,
        IonType.BLOB,
        IonType.CLOB,
        IonType.LIST,
        IonType.SEXP,
        IonType.STRUCT
    )

    override fun convertOpcodeToBytecode(
        opcode: Int,
        source: ByteArray,
        position: Int,
        destination: BytecodeBuffer
    ): Int {
        val typeOperand = byteToInt(source[position + 1]) - 1
        if(typeOperand < 0 || typeOperand >= typeTable.size) {
            throw IonException("Unsupported typed null")
        }
        val ionType = typeTable[typeOperand]
        BytecodeEmitter.emitNullValue(destination, ionType)
        return 2
    }
}
