// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.ir

/**
 * Enumeration of all supported bytecode instructions with their metadata.
 *
 * Each instruction entry contains the operation code, data type information,
 * and operand requirements. This enum serves as the central registry for
 * instruction definitions used in Ion bytecode generation and execution.
 *
 * See `com/amazon/ion/bytecode/ir/instruction_reference.md` for more details about the instruction set.
 *
 * @property operation The operation code identifier for this instruction
 * @property dataType Information about the data format and encoding for this instruction
 * @property operands Information about the operands this instruction expects
 */
internal enum class InstructionInfo(
    val operation: Int,
    val dataType: DataInfo,
    val operands: OperandInfo = OperandInfo.NO_OPERANDS,
) {
    NULL_NULL(Operation.OP_NULL_NULL, DataInfo.NO_DATA),
    BOOL(Operation.OP_BOOL, DataInfo.BOOLEAN),
    NULL_BOOL(Operation.OP_NULL_BOOL, DataInfo.NO_DATA),
    INT_I16(Operation.OP_INT_I16, DataInfo.I16),
    INT_I32(Operation.OP_INT_I32, DataInfo.NO_DATA, OperandInfo.I32),
    INT_I64(Operation.OP_INT_I64, DataInfo.NO_DATA, OperandInfo.I64),
    INT_CP(Operation.OP_INT_CP, DataInfo.CP_INDEX),
    INT_REF(Operation.OP_INT_REF, DataInfo.REF_LENGTH, OperandInfo.OFFSET),
    NULL_INT(Operation.OP_NULL_INT, DataInfo.NO_DATA),
    FLOAT_F32(Operation.OP_FLOAT_F32, DataInfo.NO_DATA, OperandInfo.F32),
    FLOAT_F64(Operation.OP_FLOAT_F64, DataInfo.NO_DATA, OperandInfo.F64),
    NULL_FLOAT(Operation.OP_NULL_FLOAT, DataInfo.NO_DATA),
    DECIMAL_CP(Operation.OP_DECIMAL_CP, DataInfo.CP_INDEX),
    DECIMAL_REF(Operation.OP_DECIMAL_REF, DataInfo.REF_LENGTH, OperandInfo.OFFSET),
    NULL_DECIMAL(Operation.OP_NULL_DECIMAL, DataInfo.NO_DATA),
    TIMESTAMP_CP(Operation.OP_TIMESTAMP_CP, DataInfo.CP_INDEX),
    SHORT_TIMESTAMP_REF(Operation.OP_SHORT_TIMESTAMP_REF, DataInfo.OPCODE, OperandInfo.OFFSET),
    TIMESTAMP_REF(Operation.OP_TIMESTAMP_REF, DataInfo.REF_LENGTH, OperandInfo.OFFSET),
    NULL_TIMESTAMP(Operation.OP_NULL_TIMESTAMP, DataInfo.NO_DATA),
    STRING_CP(Operation.OP_STRING_CP, DataInfo.CP_INDEX),
    STRING_REF(Operation.OP_STRING_REF, DataInfo.REF_LENGTH, OperandInfo.OFFSET),
    NULL_STRING(Operation.OP_NULL_STRING, DataInfo.NO_DATA),
    SYMBOL_CP(Operation.OP_SYMBOL_CP, DataInfo.CP_INDEX),
    SYMBOL_REF(Operation.OP_SYMBOL_REF, DataInfo.REF_LENGTH, OperandInfo.OFFSET),
    SYMBOL_SID(Operation.OP_SYMBOL_SID, DataInfo.SID),
    SYMBOL_CHAR(Operation.OP_SYMBOL_CHAR, DataInfo.CHAR),
    NULL_SYMBOL(Operation.OP_NULL_SYMBOL, DataInfo.NO_DATA),
    BLOB_CP(Operation.OP_BLOB_CP, DataInfo.CP_INDEX),
    BLOB_REF(Operation.OP_BLOB_REF, DataInfo.REF_LENGTH, OperandInfo.OFFSET),
    NULL_BLOB(Operation.OP_NULL_BLOB, DataInfo.NO_DATA),
    CLOB_CP(Operation.OP_CLOB_CP, DataInfo.CP_INDEX),
    CLOB_REF(Operation.OP_CLOB_REF, DataInfo.REF_LENGTH, OperandInfo.OFFSET),
    NULL_CLOB(Operation.OP_NULL_CLOB, DataInfo.NO_DATA),
    LIST_START(Operation.OP_LIST_START, DataInfo.BYTECODE_LENGTH),
    NULL_LIST(Operation.OP_NULL_LIST, DataInfo.NO_DATA),
    SEXP_START(Operation.OP_SEXP_START, DataInfo.BYTECODE_LENGTH),
    NULL_SEXP(Operation.OP_NULL_SEXP, DataInfo.NO_DATA),
    STRUCT_START(Operation.OP_STRUCT_START, DataInfo.BYTECODE_LENGTH),
    NULL_STRUCT(Operation.OP_NULL_STRUCT, DataInfo.NO_DATA),
    FIELD_NAME_CP(Operation.OP_FIELD_NAME_CP, DataInfo.CP_INDEX),
    FIELD_NAME_REF(Operation.OP_FIELD_NAME_REF, DataInfo.REF_LENGTH, OperandInfo.OFFSET),
    FIELD_NAME_SID(Operation.OP_FIELD_NAME_SID, DataInfo.SID),
    ANNOTATION_CP(Operation.OP_ANNOTATION_CP, DataInfo.CP_INDEX),
    ANNOTATION_REF(Operation.OP_ANNOTATION_REF, DataInfo.REF_LENGTH, OperandInfo.OFFSET),
    ANNOTATION_SID(Operation.OP_ANNOTATION_SID, DataInfo.SID),
    PLACEHOLDER(Operation.OP_PLACEHOLDER, DataInfo.NO_DATA),
    PLACEHOLDER_OPT(Operation.OP_PLACEHOLDER_OPT, DataInfo.BYTECODE_LENGTH),
    PLACEHOLDER_TAGLESS(Operation.OP_PLACEHOLDER_TAGLESS, DataInfo.OPCODE),
    ARGUMENT_NONE(Operation.OP_ARGUMENT_NONE, DataInfo.NO_DATA),
    IVM(Operation.OP_IVM, DataInfo.IVM),
    DIRECTIVE_SET_SYMBOLS(Operation.OP_DIRECTIVE_SET_SYMBOLS, DataInfo.NO_DATA),
    DIRECTIVE_ADD_SYMBOLS(Operation.OP_DIRECTIVE_ADD_SYMBOLS, DataInfo.NO_DATA),
    DIRECTIVE_SET_MACROS(Operation.OP_DIRECTIVE_SET_MACROS, DataInfo.NO_DATA),
    DIRECTIVE_ADD_MACROS(Operation.OP_DIRECTIVE_ADD_MACROS, DataInfo.NO_DATA),
    DIRECTIVE_USE(Operation.OP_DIRECTIVE_USE, DataInfo.NO_DATA),
    DIRECTIVE_MODULE(Operation.OP_DIRECTIVE_MODULE, DataInfo.NO_DATA),
    DIRECTIVE_ENCODING(Operation.OP_DIRECTIVE_ENCODING, DataInfo.NO_DATA),
    INVOKE(Operation.OP_INVOKE, DataInfo.MACRO_ID),
    REFILL(Operation.OP_REFILL, DataInfo.NO_DATA),
    END_TEMPLATE(Operation.OP_END_TEMPLATE, DataInfo.NO_DATA),
    END_OF_INPUT(Operation.OP_END_OF_INPUT, DataInfo.NO_DATA),
    END_CONTAINER(Operation.OP_END_CONTAINER, DataInfo.NO_DATA),
    META_OFFSET(Operation.OP_META_OFFSET, DataInfo.NO_DATA, OperandInfo.OFFSET),
    META_ROWCOL(Operation.OP_META_ROWCOL, DataInfo.NO_DATA, OperandInfo.ROW),
    META_COMMENT(Operation.OP_META_COMMENT, DataInfo.REF_LENGTH, OperandInfo.OFFSET),
    ;

    companion object {
        /**
         * 32-bit bitmask used for extracting the lower 32 bits from a long value.
         * Used in operand formatting operations to handle 64-bit values split across two 32-bit integers.
         */
        private val BITMASK_32 = 0xFFFFFFFFL
    }

    /**
     * Enumeration defining how instruction data should be formatted for display and debugging.
     *
     * Each data type has an associated formatter function that converts raw integer data
     * into a human-readable representation appropriate for that data type.
     *
     * @property formatter Function that converts an integer value to its formatted representation
     */
    @OptIn(ExperimentalStdlibApi::class)
    enum class DataInfo(val formatter: (Int) -> Any) {
        /** No data associated with this instruction */
        NO_DATA({ "" }),
        /** Constant pool index reference */
        CP_INDEX(Int::toString),
        /** Symbol ID with $ prefix for display */
        SID({ "\$$it" }),
        /** 16-bit signed integer */
        I16(Int::toShort),
        /** Single character value */
        CHAR(Int::toChar),
        /** Boolean value (1 = true, 0 = false) */
        BOOLEAN({ it == 1 }),
        /** Bytecode length with L= prefix */
        BYTECODE_LENGTH({ "L=$it" }),
        /** Reference length with L= prefix */
        REF_LENGTH({ "L=$it" }),
        /** Operation code as hexadecimal byte */
        OPCODE({ it.toByte().toHexString() }),
        /** Macro identifier */
        MACRO_ID({ it }),
        /** Ion Version Marker as hexadecimal short */
        IVM({ "${it.shr(8)}.${it.and(0xFF)}" }),
        /** Column number with col= prefix */
        COLUMN({ "col=$it" }),
    }

    /**
     * Enumeration defining operand information for instructions.
     *
     * Specifies the number of operands an instruction expects and provides
     * formatting functions for displaying operand values in human-readable form.
     *
     * @property n The number of operands this instruction type expects
     * @property formatter1Int Formatter for single-operand instructions
     * @property formatter2Int Formatter for two-operand instructions
     */
    @OptIn(ExperimentalStdlibApi::class)
    enum class OperandInfo(
        val n: Int,
        val formatter1Int: (Int) -> String = { "" },
        val formatter2Int: (Int, Int) -> String = { _, _ -> "" }
    ) {
        /** Instruction takes no operands */
        NO_OPERANDS(0),
        /** 32-bit signed integer operand */
        I32(1, { "$it" }),
        /** 64-bit signed integer operand (split across two 32-bit values) */
        I64(
            2,
            formatter1Int = { "─┐" },
            formatter2Int = { msb, lsb -> "─┴─ " + msb.toLong().shl(32).or(lsb.toLong() and BITMASK_32).toString() }
        ),
        /** 32-bit floating point operand */
        F32(1, { Float.fromBits(it).toString() }),
        /** 64-bit floating point operand (split across two 32-bit values) */
        F64(
            2,
            formatter1Int = { "─┐" },
            formatter2Int = { msb, lsb -> "─┴─ " + Double.fromBits(msb.toLong().shl(32).or(lsb.toLong() and BITMASK_32)).toString() }
        ),
        /** Input offset operand */
        OFFSET(1, { "offset=${it.toLong().and(BITMASK_32)}" }),
        /** Row number operand for source location tracking */
        ROW(1, { "row=$it" })
    }
}
