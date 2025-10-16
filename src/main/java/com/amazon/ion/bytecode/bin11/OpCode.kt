// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11

/**
 * Constants representing Ion 1.1 opcode values.
 *
 * Not all opcodes are represented here—when there are a range of opcodes for a particular thing (macro addresses or
 * length prefixed values), we don't necessarily want to write code for each of these opcodes individually.
 *
 * For dealing with tagless encodings, there are a few supplemental/replacement opcodes that have a `TE` prefix (for
 * tagless encoding) on the name of the constant. See also [TaglessScalarType].
 *
 * TODO: Consider whether there is some other location more suitable for this class.
 */
internal object OpCode {
    const val MACRO_0 = 0x00

    const val MACRO_47 = 0x47

    const val EXTENSIBLE_MACRO_ADDRESS_0 = 0x48
    const val EXTENSIBLE_MACRO_ADDRESS_7 = 0x4F

    const val SYMBOL_SID_FLEX_0 = 0x50
    const val SYMBOL_SID_FLEX_7 = 0x57

    const val ANNOTATION_SID = 0x58
    const val ANNOTATION_TEXT = 0x59

    const val RESERVED_5A = 0x5A

    const val TAGLESS_ELEMENT_LIST = 0x5B
    const val TAGLESS_ELEMENT_SEXP = 0x5C

    const val RESERVED_5D = 0x5D
    const val RESERVED_5E = 0x5E
    const val RESERVED_5F = 0x5F

    const val INT_0 = 0x60
    const val INT_8 = 0x61
    const val INT_16 = 0x62
    const val INT_24 = 0x63
    const val INT_32 = 0x64
    const val INT_40 = 0x65
    const val INT_48 = 0x66
    const val INT_56 = 0x67
    const val INT_64 = 0x68

    const val RESERVED_69 = 0x69

    const val FLOAT_0 = 0x6A
    const val FLOAT_16 = 0x6B
    const val FLOAT_32 = 0x6C
    const val FLOAT_64 = 0x6D

    const val BOOL_TRUE = 0x6E
    const val BOOL_FALSE = 0x6F

    const val DECIMAL_0 = 0x70
    const val DECIMAL_LENGTH_15 = 0x7F

    const val TIMESTAMP_YEAR_PRECISION = 0x80
    const val TIMESTAMP_MONTH_PRECISION = 0x81
    const val TIMESTAMP_DAY_PRECISION = 0x82
    const val TIMESTAMP_MINUTE_PRECISION = 0x83
    const val TIMESTAMP_SECOND_PRECISION = 0x84
    const val TIMESTAMP_MILLIS_PRECISION = 0x85
    const val TIMESTAMP_MICROS_PRECISION = 0x86
    const val TIMESTAMP_NANOS_PRECISION = 0x87
    const val TIMESTAMP_MINUTE_PRECISION_WITH_OFFSET = 0x88
    const val TIMESTAMP_SECOND_PRECISION_WITH_OFFSET = 0x89
    const val TIMESTAMP_MILLIS_PRECISION_WITH_OFFSET = 0x8A
    const val TIMESTAMP_MICROS_PRECISION_WITH_OFFSET = 0x8B
    const val TIMESTAMP_NANOS_PRECISION_WITH_OFFSET = 0x8C

    const val RESERVED_8D = 0x8D

    const val NULL_NULL = 0x8E
    const val TYPED_NULL = 0x8F

    const val STRING_LENGTH_0 = 0x90
    const val STRING_LENGTH_15 = 0x9F

    const val SYMBOL_LENGTH_0 = 0xA0
    const val SYMBOL_LENGTH_15 = 0xAF

    const val LIST_LENGTH_0 = 0xB0
    const val LIST_LENGTH_15 = 0xBF

    const val SEXP_LENGTH_0 = 0xC0
    const val SEXP_LENGTH_15 = 0xCF

    const val STRUCT_LENGTH_0 = 0xD0
    const val RESERVED_D1 = 0xD1
    const val STRUCT_LENGTH_15 = 0xDF

    const val IVM = 0xE0
    const val NO_ARGUMENT = 0xE0

    const val DIRECTIVE_SET_SYMBOLS = 0xE1
    const val DIRECTIVE_ADD_SYMBOLS = 0xE2
    const val DIRECTIVE_SET_MACROS = 0xE3
    const val DIRECTIVE_ADD_MACROS = 0xE4
    const val DIRECTIVE_USE = 0xE5
    const val DIRECTIVE_MODULE = 0xE6
    const val DIRECTIVE_IMPORT = 0xE7
    const val DIRECTIVE_ENCODING = 0xE8

    const val TAGGED_PLACEHOLDER = 0xE9
    const val TAGGED_PLACEHOLDER_WITH_DEFAULT = 0xEA
    const val TAGLESS_PLACEHOLDER = 0xEB

    const val NOP = 0xEC
    const val NOP_L = 0xED

    const val STRUCT_SWITCH_MODES = 0xEE
    const val DELIMITED_CONTAINER_END = 0xEF

    const val DELIMITED_LIST = 0xF0
    const val DELIMITED_SEXP = 0xF1
    const val DELIMITED_STRUCT_SID_MODE = 0xF2
    const val DELIMITED_STRUCT_FS_MODE = 0xF3

    const val LENGTH_PREFIXED_MACRO_INVOCATION = 0xF4
    const val VARIABLE_LENGTH_INTEGER = 0xF5
    const val VARIABLE_LENGTH_DECIMAL = 0xF6
    const val VARIABLE_LENGTH_TIMESTAMP = 0xF7
    const val VARIABLE_LENGTH_STRING = 0xF8
    const val VARIABLE_LENGTH_SYMBOL = 0xF9
    const val VARIABLE_LENGTH_LIST = 0xFA
    const val VARIABLE_LENGTH_SEXP = 0xFB
    const val VARIABLE_LENGTH_STRUCT_SID_MODE = 0xFC
    const val VARIABLE_LENGTH_STRUCT_FS_MODE = 0xFD
    const val VARIABLE_LENGTH_BLOB = 0xFE
    const val VARIABLE_LENGTH_CLOB = 0xFF

    const val TE_FLEX_INT = 0x60
    const val TE_SMALL_DECIMAL = 0x70
    const val TE_FLEX_UINT = 0xE0
    const val TE_UINT_8 = 0xE1
    const val TE_UINT_16 = 0xE2
    const val TE_UINT_32 = 0xE4
    const val TE_UINT_64 = 0xE8
    const val TE_SYMBOL_FS = 0xEE
}
