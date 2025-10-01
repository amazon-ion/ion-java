// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.ir

/**
 * Constants defining the different categories of operations for the bytecode instruction set.
 *
 * Each operation kind represents a high-level category of operations that can be
 * performed in Ion bytecode. These constants are used to classify instructions
 * and determine their general behavior and purpose.
 *
 * See `com/amazon/ion/bytecode/ir/instruction_reference.md` for more details about the instruction set.
 */
internal object OperationKind {
    /** Uninitialized or invalid instruction kind */
    const val UNSET = 0
    /** Null value instructions */
    const val NULL = 1
    /** Boolean value instructions */
    const val BOOL = 2
    /** Integer value instructions */
    const val INT = 3
    /** Floating-point value instructions */
    const val FLOAT = 4
    /** Decimal value instructions */
    const val DECIMAL = 5
    /** Timestamp value instructions */
    const val TIMESTAMP = 6
    /** String value instructions */
    const val STRING = 7
    /** Symbol value instructions */
    const val SYMBOL = 8
    /** Character LOB (CLOB) value instructions */
    const val CLOB = 9
    /** Binary LOB (BLOB) value instructions */
    const val BLOB = 10
    /** List container instructions */
    const val LIST = 11
    /** S-expression container instructions */
    const val SEXP = 12
    /** Struct container instructions */
    const val STRUCT = 13
    /** Annotation instructions */
    const val ANNOTATIONS = 14
    /** Struct field name instructions */
    const val FIELD_NAME = 15
    /** Ion Version Marker instructions */
    const val IVM = 16
    /** Symbol table and macro directive instructions */
    const val DIRECTIVE = 17
    /** Template placeholder instructions */
    const val PLACEHOLDER = 18
    /** Template argument instructions */
    const val ARGUMENT = 19
    /** Template invocation instructions */
    const val INVOKE_TEMPLATE = 20
    /** Buffer refill instructions */
    const val REFILL = 21
    /** End-of-stream and container termination instructions */
    const val END = 22
    /** Metadata and debugging instructions */
    const val METADATA = 23

    /**
     * Returns the human-readable name for the given instruction kind constant.
     *
     * @param instructionKind The instruction kind constant to get the name for
     * @return The string name of the instruction kind
     * @throws IllegalArgumentException if the instruction kind is not recognized
     */
    @JvmStatic
    fun nameOf(instructionKind: Int): String {
        return when (instructionKind) {
            UNSET -> "UNSET"
            NULL -> "NULL"
            BOOL -> "BOOL"
            INT -> "INT"
            FLOAT -> "FLOAT"
            DECIMAL -> "DECIMAL"
            TIMESTAMP -> "TIMESTAMP"
            STRING -> "STRING"
            SYMBOL -> "SYMBOL"
            CLOB -> "CLOB"
            BLOB -> "BLOB"
            LIST -> "LIST"
            SEXP -> "SEXP"
            STRUCT -> "STRUCT"
            ANNOTATIONS -> "ANNOTATIONS"
            FIELD_NAME -> "FIELD_NAME"
            IVM -> "IVM"
            DIRECTIVE -> "DIRECTIVE"
            PLACEHOLDER -> "PLACEHOLDER"
            ARGUMENT -> "ARGUMENT"
            INVOKE_TEMPLATE -> "INVOKE_TEMPLATE"
            REFILL -> "REFILL"
            END -> "END"
            METADATA -> "METADATA"
            else -> throw IllegalArgumentException("Not a valid instruction kind: $instructionKind")
        }
    }
}
