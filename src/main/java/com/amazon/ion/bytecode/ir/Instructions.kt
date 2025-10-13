// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.ir

import com.amazon.ion.bytecode.ir.Operation.OPERATION_KIND_OFFSET

/**
 * Utility object for working with packed instruction formats and instruction constants.
 *
 * This object provides functions for extracting components from packed instruction integers
 * and defines all the pre-computed instruction constants used throughout the bytecode system.
 * Instructions are packed into 32-bit integers with specific bit layouts for efficient processing.
 *
 * See `com/amazon/ion/bytecode/ir/instruction_reference.md` for more details about the instruction set.
 */
internal object Instructions {

    /**
     * Extracts the operation code from a packed instruction.
     *
     * @param instruction The packed instruction integer
     * @return The operation code portion of the instruction
     */
    @JvmStatic
    fun toOperation(instruction: Int) = instruction ushr OPERATION_OFFSET

    /**
     * Given an [OperationKind] value that represents an Ion type, returns the appropriate NULL variant operation.
     */
    @JvmStatic
    fun typedNullFromOperationKind(operationKind: Int): Int = (operationKind.shl(OPERATION_KIND_OFFSET) + Operation.NULL_VARIANT).shl(OPERATION_OFFSET)

    /**
     * Extracts the operand count bits from a packed instruction.
     *
     * @param instruction The packed instruction integer
     * @return The operand count bits indicating how many operands this instruction has
     */
    @JvmStatic
    fun getOperandCountBits(instruction: Int) = (instruction and OPERAND_COUNT_MASK) ushr OPERAND_COUNT_BITS_OFFSET

    /**
     * Extracts the data portion from a packed instruction.
     *
     * @param instruction The packed instruction integer
     * @return The data bits containing instruction-specific information
     */
    @JvmStatic
    fun getData(instruction: Int) = instruction and DATA_MASK

    /**
     * Packs a data value with an instruction to create a packed instruction.
     */
    @JvmStatic
    fun Int.packInstructionData(data: Int): Int {
        return this.and(DATA_MASK.inv()).or(data.and(DATA_MASK))
    }

    /** Bit offset for the operation code in packed instructions */
    const val OPERATION_OFFSET = 24
    /** Bit offset for operand count bits in packed instructions */
    const val OPERAND_COUNT_BITS_OFFSET = 22
    /** Bitmask for extracting operand count bits */
    const val OPERAND_COUNT_MASK = 0b00000000_11000000_00000000_00000000
    /** Bitmask for extracting data bits */
    const val DATA_MASK = 0b00000000_00111111_11111111_11111111

    /** Variant identifier used for null value instructions */
    private const val NULL_VARIANT = 7

    /** Constant indicating instruction takes zero operands */
    const val ZERO_OPERANDS = 0
    /** Constant indicating instruction takes one operand */
    const val ONE_OPERAND = 1
    /** Constant indicating instruction takes two operands */
    const val TWO_OPERANDS = 2
    /** Constant indicating instruction takes variable number of operands */
    const val N_OPERANDS = 3

    // Pre-computed instruction constants
    // Each constant combines an operation code with operand count information
    // Format: (operation << OPERATION_OFFSET) | (operand_count << OPERAND_BITS_OFFSET)

    const val I_NULL_NULL = (Operation.OP_NULL_NULL shl OPERATION_OFFSET).or(ZERO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_BOOL = (Operation.OP_BOOL shl OPERATION_OFFSET).or(ZERO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_NULL_BOOL = (Operation.OP_NULL_BOOL shl OPERATION_OFFSET).or(ZERO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_INT_I16 = (Operation.OP_INT_I16 shl OPERATION_OFFSET).or(ZERO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_INT_I32 = (Operation.OP_INT_I32 shl OPERATION_OFFSET).or(ONE_OPERAND shl OPERAND_COUNT_BITS_OFFSET)
    const val I_INT_I64 = (Operation.OP_INT_I64 shl OPERATION_OFFSET).or(TWO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_INT_CP = (Operation.OP_INT_CP shl OPERATION_OFFSET).or(ZERO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_INT_REF = (Operation.OP_INT_REF shl OPERATION_OFFSET).or(ONE_OPERAND shl OPERAND_COUNT_BITS_OFFSET)
    const val I_NULL_INT = (Operation.OP_NULL_INT shl OPERATION_OFFSET).or(ZERO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_FLOAT_F32 = (Operation.OP_FLOAT_F32 shl OPERATION_OFFSET).or(ONE_OPERAND shl OPERAND_COUNT_BITS_OFFSET)
    const val I_FLOAT_F64 = (Operation.OP_FLOAT_F64 shl OPERATION_OFFSET).or(TWO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_NULL_FLOAT = (Operation.OP_NULL_FLOAT shl OPERATION_OFFSET).or(ZERO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_DECIMAL_CP = (Operation.OP_DECIMAL_CP shl OPERATION_OFFSET).or(ZERO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_DECIMAL_REF = (Operation.OP_DECIMAL_REF shl OPERATION_OFFSET).or(ONE_OPERAND shl OPERAND_COUNT_BITS_OFFSET)
    const val I_NULL_DECIMAL = (Operation.OP_NULL_DECIMAL shl OPERATION_OFFSET).or(ZERO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_TIMESTAMP_CP = (Operation.OP_TIMESTAMP_CP shl OPERATION_OFFSET).or(ZERO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_SHORT_TIMESTAMP_REF = (Operation.OP_SHORT_TIMESTAMP_REF shl OPERATION_OFFSET).or(ONE_OPERAND shl OPERAND_COUNT_BITS_OFFSET)
    const val I_TIMESTAMP_REF = (Operation.OP_TIMESTAMP_REF shl OPERATION_OFFSET).or(ONE_OPERAND shl OPERAND_COUNT_BITS_OFFSET)
    const val I_NULL_TIMESTAMP = (Operation.OP_NULL_TIMESTAMP shl OPERATION_OFFSET).or(ZERO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_STRING_CP = (Operation.OP_STRING_CP shl OPERATION_OFFSET).or(ZERO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_STRING_REF = (Operation.OP_STRING_REF shl OPERATION_OFFSET).or(ONE_OPERAND shl OPERAND_COUNT_BITS_OFFSET)
    const val I_NULL_STRING = (Operation.OP_NULL_STRING shl OPERATION_OFFSET).or(ZERO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_SYMBOL_CP = (Operation.OP_SYMBOL_CP shl OPERATION_OFFSET).or(ZERO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_SYMBOL_REF = (Operation.OP_SYMBOL_REF shl OPERATION_OFFSET).or(ONE_OPERAND shl OPERAND_COUNT_BITS_OFFSET)
    const val I_SYMBOL_SID = (Operation.OP_SYMBOL_SID shl OPERATION_OFFSET).or(ZERO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_SYMBOL_CHAR = (Operation.OP_SYMBOL_CHAR shl OPERATION_OFFSET).or(ZERO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_NULL_SYMBOL = (Operation.OP_NULL_SYMBOL shl OPERATION_OFFSET).or(ZERO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_BLOB_CP = (Operation.OP_BLOB_CP shl OPERATION_OFFSET).or(ZERO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_BLOB_REF = (Operation.OP_BLOB_REF shl OPERATION_OFFSET).or(ONE_OPERAND shl OPERAND_COUNT_BITS_OFFSET)
    const val I_NULL_BLOB = (Operation.OP_NULL_BLOB shl OPERATION_OFFSET).or(ZERO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_CLOB_CP = (Operation.OP_CLOB_CP shl OPERATION_OFFSET).or(ZERO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_CLOB_REF = (Operation.OP_CLOB_REF shl OPERATION_OFFSET).or(ONE_OPERAND shl OPERAND_COUNT_BITS_OFFSET)
    const val I_NULL_CLOB = (Operation.OP_NULL_CLOB shl OPERATION_OFFSET).or(ZERO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_LIST_START = (Operation.OP_LIST_START shl OPERATION_OFFSET).or(N_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_NULL_LIST = (Operation.OP_NULL_LIST shl OPERATION_OFFSET).or(ZERO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_SEXP_START = (Operation.OP_SEXP_START shl OPERATION_OFFSET).or(N_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_NULL_SEXP = (Operation.OP_NULL_SEXP shl OPERATION_OFFSET).or(ZERO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_STRUCT_START = (Operation.OP_STRUCT_START shl OPERATION_OFFSET).or(N_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_NULL_STRUCT = (Operation.OP_NULL_STRUCT shl OPERATION_OFFSET).or(ZERO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_FIELD_NAME_CP = (Operation.OP_FIELD_NAME_CP shl OPERATION_OFFSET).or(ZERO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_FIELD_NAME_REF = (Operation.OP_FIELD_NAME_REF shl OPERATION_OFFSET).or(ONE_OPERAND shl OPERAND_COUNT_BITS_OFFSET)
    const val I_FIELD_NAME_SID = (Operation.OP_FIELD_NAME_SID shl OPERATION_OFFSET).or(ZERO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_ANNOTATION_CP = (Operation.OP_ANNOTATION_CP shl OPERATION_OFFSET).or(ZERO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_ANNOTATION_REF = (Operation.OP_ANNOTATION_REF shl OPERATION_OFFSET).or(ONE_OPERAND shl OPERAND_COUNT_BITS_OFFSET)
    const val I_ANNOTATION_SID = (Operation.OP_ANNOTATION_SID shl OPERATION_OFFSET).or(ZERO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_PLACEHOLDER_TAGGED = (Operation.OP_PLACEHOLDER_TAGGED shl OPERATION_OFFSET).or(N_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_PLACEHOLDER_TAGLESS = (Operation.OP_PLACEHOLDER_TAGLESS shl OPERATION_OFFSET).or(ZERO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_ARGUMENT_NONE = (Operation.OP_ARGUMENT_NONE shl OPERATION_OFFSET).or(ZERO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_IVM = (Operation.OP_IVM shl OPERATION_OFFSET).or(ZERO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_DIRECTIVE_SET_SYMBOLS = (Operation.OP_DIRECTIVE_SET_SYMBOLS shl OPERATION_OFFSET).or(ZERO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_DIRECTIVE_ADD_SYMBOLS = (Operation.OP_DIRECTIVE_ADD_SYMBOLS shl OPERATION_OFFSET).or(ZERO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_DIRECTIVE_SET_MACROS = (Operation.OP_DIRECTIVE_SET_MACROS shl OPERATION_OFFSET).or(ZERO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_DIRECTIVE_ADD_MACROS = (Operation.OP_DIRECTIVE_ADD_MACROS shl OPERATION_OFFSET).or(ZERO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_DIRECTIVE_USE = (Operation.OP_DIRECTIVE_USE shl OPERATION_OFFSET).or(ZERO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_DIRECTIVE_MODULE = (Operation.OP_DIRECTIVE_MODULE shl OPERATION_OFFSET).or(ZERO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_DIRECTIVE_ENCODING = (Operation.OP_DIRECTIVE_ENCODING shl OPERATION_OFFSET).or(ZERO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)

    const val I_INVOKE = (Operation.OP_INVOKE shl OPERATION_OFFSET).or(ZERO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)

    const val I_REFILL = (Operation.OP_REFILL shl OPERATION_OFFSET).or(ZERO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_END_TEMPLATE = (Operation.OP_END_TEMPLATE shl OPERATION_OFFSET).or(ZERO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_END_OF_INPUT = (Operation.OP_END_OF_INPUT shl OPERATION_OFFSET).or(ZERO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_END_CONTAINER = (Operation.OP_END_CONTAINER shl OPERATION_OFFSET).or(ZERO_OPERANDS shl OPERAND_COUNT_BITS_OFFSET)
    const val I_META_OFFSET = (Operation.OP_META_OFFSET shl OPERATION_OFFSET).or(ONE_OPERAND shl OPERAND_COUNT_BITS_OFFSET)
    const val I_META_ROWCOL = (Operation.OP_META_ROWCOL shl OPERATION_OFFSET).or(ONE_OPERAND shl OPERAND_COUNT_BITS_OFFSET)
    const val I_META_COMMENT = (Operation.OP_META_COMMENT shl OPERATION_OFFSET).or(ONE_OPERAND shl OPERAND_COUNT_BITS_OFFSET)
}
