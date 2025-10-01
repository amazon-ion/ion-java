// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.ir

/**
 * Defines operation codes for all Ion bytecode instructions.
 *
 * Operation codes are constructed by combining instruction kinds with variant identifiers.
 * Each operation represents a specific instruction that can be executed by the Ion bytecode
 * interpreter. Operations are organized by data type and functionality.
 *
 * See `com/amazon/ion/bytecode/ir/instruction_reference.md` for more details about the instruction set.
 */
internal object Operation {

    /**
     * Extracts the instruction kind from an operation code.
     *
     * @param operation The operation code to extract the instruction kind from
     * @return The instruction kind portion of the operation code
     */
    @JvmStatic
    fun toOperationKind(operation: Int): Int = operation ushr OPERATION_KIND_OFFSET

    /** Variant identifier used for null value operations */
    private const val NULL_VARIANT = 7
    /** Bit offset for extracting instruction kind from operation codes */
    private const val OPERATION_KIND_OFFSET = 3

    // Operation code constants
    // Each constant combines an instruction kind with a variant identifier
    // Format: (instruction_kind << OPERATION_KIND_OFFSET) + variant

    const val OP_NULL_NULL = (OperationKind.NULL shl OPERATION_KIND_OFFSET) + NULL_VARIANT

    const val OP_BOOL = OperationKind.BOOL shl OPERATION_KIND_OFFSET
    const val OP_NULL_BOOL = (OperationKind.BOOL shl OPERATION_KIND_OFFSET) + NULL_VARIANT

    const val OP_INT_I16 = OperationKind.INT shl OPERATION_KIND_OFFSET
    const val OP_INT_I32 = (OperationKind.INT shl OPERATION_KIND_OFFSET) + 1
    const val OP_INT_I64 = (OperationKind.INT shl OPERATION_KIND_OFFSET) + 2
    const val OP_INT_CP = (OperationKind.INT shl OPERATION_KIND_OFFSET) + 3
    const val OP_INT_REF = (OperationKind.INT shl OPERATION_KIND_OFFSET) + 4
    const val OP_NULL_INT = (OperationKind.INT shl OPERATION_KIND_OFFSET) + NULL_VARIANT

    const val OP_FLOAT_F32 = (OperationKind.FLOAT shl OPERATION_KIND_OFFSET)
    const val OP_FLOAT_F64 = (OperationKind.FLOAT shl OPERATION_KIND_OFFSET) + 1
    const val OP_NULL_FLOAT = (OperationKind.FLOAT shl OPERATION_KIND_OFFSET) + NULL_VARIANT

    const val OP_DECIMAL_CP = (OperationKind.DECIMAL shl OPERATION_KIND_OFFSET)
    const val OP_DECIMAL_REF = (OperationKind.DECIMAL shl OPERATION_KIND_OFFSET) + 1
    const val OP_NULL_DECIMAL = (OperationKind.DECIMAL shl OPERATION_KIND_OFFSET) + NULL_VARIANT

    const val OP_TIMESTAMP_CP = (OperationKind.TIMESTAMP shl OPERATION_KIND_OFFSET)
    const val OP_SHORT_TIMESTAMP_REF = (OperationKind.TIMESTAMP shl OPERATION_KIND_OFFSET) + 1
    const val OP_TIMESTAMP_REF = (OperationKind.TIMESTAMP shl OPERATION_KIND_OFFSET) + 2
    const val OP_NULL_TIMESTAMP = (OperationKind.TIMESTAMP shl OPERATION_KIND_OFFSET) + NULL_VARIANT

    const val OP_STRING_CP = (OperationKind.STRING shl OPERATION_KIND_OFFSET)
    const val OP_STRING_REF = (OperationKind.STRING shl OPERATION_KIND_OFFSET) + 1
    const val OP_NULL_STRING = (OperationKind.STRING shl OPERATION_KIND_OFFSET) + NULL_VARIANT

    const val OP_SYMBOL_CP = (OperationKind.SYMBOL shl OPERATION_KIND_OFFSET)
    const val OP_SYMBOL_REF = (OperationKind.SYMBOL shl OPERATION_KIND_OFFSET) + 1
    const val OP_SYMBOL_SID = (OperationKind.SYMBOL shl OPERATION_KIND_OFFSET) + 2
    const val OP_SYMBOL_CHAR = (OperationKind.SYMBOL shl OPERATION_KIND_OFFSET) + 3
    const val OP_NULL_SYMBOL = (OperationKind.SYMBOL shl OPERATION_KIND_OFFSET) + NULL_VARIANT

    const val OP_BLOB_CP = (OperationKind.BLOB shl OPERATION_KIND_OFFSET)
    const val OP_BLOB_REF = (OperationKind.BLOB shl OPERATION_KIND_OFFSET) + 1
    const val OP_NULL_BLOB = (OperationKind.BLOB shl OPERATION_KIND_OFFSET) + NULL_VARIANT

    const val OP_CLOB_CP = (OperationKind.CLOB shl OPERATION_KIND_OFFSET)
    const val OP_CLOB_REF = (OperationKind.CLOB shl OPERATION_KIND_OFFSET) + 1
    const val OP_NULL_CLOB = (OperationKind.CLOB shl OPERATION_KIND_OFFSET) + NULL_VARIANT

    const val OP_LIST_START = OperationKind.LIST shl OPERATION_KIND_OFFSET
    const val OP_NULL_LIST = (OperationKind.LIST shl OPERATION_KIND_OFFSET) + NULL_VARIANT

    const val OP_SEXP_START = OperationKind.SEXP shl OPERATION_KIND_OFFSET
    const val OP_NULL_SEXP = (OperationKind.SEXP shl OPERATION_KIND_OFFSET) + NULL_VARIANT

    const val OP_STRUCT_START = OperationKind.STRUCT shl OPERATION_KIND_OFFSET
    const val OP_NULL_STRUCT = (OperationKind.STRUCT shl OPERATION_KIND_OFFSET) + NULL_VARIANT

    const val OP_FIELD_NAME_CP = (OperationKind.FIELD_NAME shl OPERATION_KIND_OFFSET)
    const val OP_FIELD_NAME_REF = (OperationKind.FIELD_NAME shl OPERATION_KIND_OFFSET) + 1
    const val OP_FIELD_NAME_SID = (OperationKind.FIELD_NAME shl OPERATION_KIND_OFFSET) + 2

    const val OP_ANNOTATION_CP = (OperationKind.ANNOTATIONS shl OPERATION_KIND_OFFSET) + 0
    const val OP_ANNOTATION_REF = (OperationKind.ANNOTATIONS shl OPERATION_KIND_OFFSET) + 1
    const val OP_ANNOTATION_SID = (OperationKind.ANNOTATIONS shl OPERATION_KIND_OFFSET) + 2

    const val OP_PLACEHOLDER_TAGGED = (OperationKind.PLACEHOLDER shl OPERATION_KIND_OFFSET)
    const val OP_PLACEHOLDER_TAGLESS = (OperationKind.PLACEHOLDER shl OPERATION_KIND_OFFSET) + 1

    const val OP_ARGUMENT_NONE = (OperationKind.ARGUMENT shl OPERATION_KIND_OFFSET)

    const val OP_IVM = (OperationKind.IVM shl OPERATION_KIND_OFFSET)

    const val OP_DIRECTIVE_SET_SYMBOLS = (OperationKind.DIRECTIVE shl OPERATION_KIND_OFFSET)
    const val OP_DIRECTIVE_ADD_SYMBOLS = (OperationKind.DIRECTIVE shl OPERATION_KIND_OFFSET) + 1
    const val OP_DIRECTIVE_SET_MACROS = (OperationKind.DIRECTIVE shl OPERATION_KIND_OFFSET) + 2
    const val OP_DIRECTIVE_ADD_MACROS = (OperationKind.DIRECTIVE shl OPERATION_KIND_OFFSET) + 3
    const val OP_DIRECTIVE_USE = (OperationKind.DIRECTIVE shl OPERATION_KIND_OFFSET) + 4
    const val OP_DIRECTIVE_MODULE = (OperationKind.DIRECTIVE shl OPERATION_KIND_OFFSET) + 5
    const val OP_DIRECTIVE_ENCODING = (OperationKind.DIRECTIVE shl OPERATION_KIND_OFFSET) + 6

    const val OP_INVOKE = (OperationKind.INVOKE_TEMPLATE shl OPERATION_KIND_OFFSET)

    const val OP_REFILL = (OperationKind.REFILL shl OPERATION_KIND_OFFSET)

    const val OP_END_TEMPLATE = OperationKind.END shl OPERATION_KIND_OFFSET
    const val OP_END_OF_INPUT = (OperationKind.END shl OPERATION_KIND_OFFSET) + 1
    const val OP_END_CONTAINER = (OperationKind.END shl OPERATION_KIND_OFFSET) + 2

    const val OP_META_OFFSET = OperationKind.METADATA shl OPERATION_KIND_OFFSET
    const val OP_META_ROWCOL = (OperationKind.METADATA shl OPERATION_KIND_OFFSET) + 1
    const val OP_META_COMMENT = (OperationKind.METADATA shl OPERATION_KIND_OFFSET) + 2
}
