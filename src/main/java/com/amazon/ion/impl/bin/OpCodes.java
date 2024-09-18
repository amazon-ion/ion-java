// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.bin;

/**
 * Utility class holding Ion 1.1 Op Codes.
 */
public class OpCodes {
    private OpCodes() {}

    public static final byte BIASED_E_EXPRESSION_ONE_BYTE_FIXED_INT = 0x40;
    public static final byte BIASED_E_EXPRESSION_TWO_BYTE_FIXED_INT = 0x50;

    public static final byte INTEGER_ZERO_LENGTH = 0x60;
    // 0x61-0x68 are additional lengths of integers.
    // 0x69 Reserved
    public static final byte FLOAT_ZERO_LENGTH = 0x6A;
    public static final byte FLOAT_16 = 0x6B;
    public static final byte FLOAT_32 = 0x6C;
    public static final byte FLOAT_64 = 0x6D;
    public static final byte BOOLEAN_TRUE = 0x6E;
    public static final byte BOOLEAN_FALSE = 0x6F;

    public static final byte DECIMAL_ZERO_LENGTH = 0x70;

    public static final byte TIMESTAMP_YEAR_PRECISION = (byte) 0x80;
    public static final byte TIMESTAMP_MONTH_PRECISION = (byte) 0x81;
    public static final byte TIMESTAMP_DAY_PRECISION = (byte) 0x82;
    public static final byte TIMESTAMP_MINUTE_PRECISION = (byte) 0x83;
    public static final byte TIMESTAMP_SECOND_PRECISION = (byte) 0x84;
    public static final byte TIMESTAMP_MILLIS_PRECISION = (byte) 0x85;
    public static final byte TIMESTAMP_MICROS_PRECISION = (byte) 0x86;
    public static final byte TIMESTAMP_NANOS_PRECISION = (byte) 0x87;
    public static final byte TIMESTAMP_MINUTE_PRECISION_WITH_OFFSET = (byte) 0x88;
    public static final byte TIMESTAMP_SECOND_PRECISION_WITH_OFFSET = (byte) 0x89;
    public static final byte TIMESTAMP_MILLIS_PRECISION_WITH_OFFSET = (byte) 0x8A;
    public static final byte TIMESTAMP_MICROS_PRECISION_WITH_OFFSET = (byte) 0x8B;
    public static final byte TIMESTAMP_NANOS_PRECISION_WITH_OFFSET = (byte) 0x8C;
    // 0x8D-0x8F Reserved

    public static final byte STRING_ZERO_LENGTH = (byte) 0x90;
    // 0x91-0x9F are additional lengths of strings.

    public static final byte INLINE_SYMBOL_ZERO_LENGTH = (byte) 0xA0;
    // 0xA1-0xAF are additional lengths of symbols.

    public static final byte LIST_ZERO_LENGTH = (byte) 0xB0;
    // 0xB1-0xBF are additional lengths of lists.

    public static final byte SEXP_ZERO_LENGTH = (byte) 0xC0;
    // 0xC1-0xCF are additional lengths of sexps.

    public static final byte STRUCT_SID_ZERO_LENGTH = (byte) 0xD0;
    // 0xD1 Reserved
    // 0xD2-0xDF are additional lengths of structs.

    public static final byte SYMBOL_ADDRESS_1_BYTE = (byte) 0xE1;
    public static final byte SYMBOL_ADDRESS_2_BYTES = (byte) 0xE2;
    public static final byte SYMBOL_ADDRESS_MANY_BYTES = (byte) 0xE3;
    public static final byte ANNOTATIONS_1_SYMBOL_ADDRESS = (byte) 0xE4;
    public static final byte ANNOTATIONS_2_SYMBOL_ADDRESS = (byte) 0xE5;
    public static final byte ANNOTATIONS_MANY_SYMBOL_ADDRESS = (byte) 0xE6;
    public static final byte ANNOTATIONS_1_FLEX_SYM = (byte) 0xE7;
    public static final byte ANNOTATIONS_2_FLEX_SYM = (byte) 0xE8;
    public static final byte ANNOTATIONS_MANY_FLEX_SYM = (byte) 0xE9;
    public static final byte NULL_UNTYPED = (byte) 0xEA;
    public static final byte NULL_TYPED = (byte) 0xEB;
    public static final byte ONE_BYTE_NOP = (byte) 0xEC;
    public static final byte VARIABLE_LENGTH_NOP = (byte) 0xED;
    public static final byte SYSTEM_SYMBOL = (byte) 0xEE;
    public static final byte SYSTEM_MACRO_INVOCATION = (byte) 0xEF;

    public static final byte DELIMITED_END_MARKER = (byte) 0xF0;
    public static final byte DELIMITED_LIST = (byte) 0xF1;
    public static final byte DELIMITED_SEXP = (byte) 0xF2;
    public static final byte DELIMITED_STRUCT = (byte) 0xF3;
    public static final byte E_EXPRESSION_FLEX_UINT = (byte) 0xF4;
    public static final byte LENGTH_PREFIXED_MACRO_INVOCATION = (byte) 0xF5;
    public static final byte VARIABLE_LENGTH_INTEGER = (byte) 0xF6;
    public static final byte VARIABLE_LENGTH_DECIMAL = (byte) 0xF7;
    public static final byte VARIABLE_LENGTH_TIMESTAMP = (byte) 0xF8;
    public static final byte VARIABLE_LENGTH_STRING = (byte) 0xF9;
    public static final byte VARIABLE_LENGTH_INLINE_SYMBOL = (byte) 0xFA;
    public static final byte VARIABLE_LENGTH_LIST = (byte) 0xFB;
    public static final byte VARIABLE_LENGTH_SEXP = (byte) 0xFC;
    public static final byte VARIABLE_LENGTH_STRUCT_WITH_SIDS = (byte) 0xFD;
    public static final byte VARIABLE_LENGTH_BLOB = (byte) 0xFE;
    public static final byte VARIABLE_LENGTH_CLOB = (byte) 0xFF;
}
