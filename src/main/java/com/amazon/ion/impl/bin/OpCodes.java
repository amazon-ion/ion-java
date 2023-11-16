package com.amazon.ion.impl.bin;

/**
 * Utility class holding Ion 1.1 Op Codes.
 */
public class OpCodes {
    private OpCodes() {}

    public static final byte INTEGER_ZERO_LENGTH = 0x50;
    // 0x51-0x58 are additional lengths of integers.
    // 0x59 Reserved
    public static final byte FLOAT_ZERO_LENGTH = 0x5A;
    public static final byte FLOAT_16 = 0x5B;
    public static final byte FLOAT_32 = 0x5C;
    public static final byte FLOAT_64 = 0x5D;
    public static final byte BOOLEAN_TRUE = 0x5E;
    public static final byte BOOLEAN_FALSE = 0x5F;

    public static final byte DECIMAL_ZERO_LENGTH = 0x60;
    // 0x61-0x6E are additional lengths of decimals.
    public static final byte POSITIVE_ZERO_DECIMAL = 0x6F;

    public static final byte TIMESTAMP_YEAR_PRECISION = 0x70;
    public static final byte TIMESTAMP_MONTH_PRECISION = 0x71;
    public static final byte TIMESTAMP_DAY_PRECISION = 0x72;
    public static final byte TIMESTAMP_MINUTE_PRECISION = 0x73;
    public static final byte TIMESTAMP_SECOND_PRECISION = 0x74;
    public static final byte TIMESTAMP_MILLIS_PRECISION = 0x75;
    public static final byte TIMESTAMP_MICROS_PRECISION = 0x76;
    public static final byte TIMESTAMP_NANOS_PRECISION = 0x77;
    public static final byte TIMESTAMP_MINUTE_PRECISION_WITH_OFFSET = 0x78;
    public static final byte TIMESTAMP_SECOND_PRECISION_WITH_OFFSET = 0x79;
    public static final byte TIMESTAMP_MILLIS_PRECISION_WITH_OFFSET = 0x7A;
    public static final byte TIMESTAMP_MICROS_PRECISION_WITH_OFFSET = 0x7B;
    public static final byte TIMESTAMP_NANOS_PRECISION_WITH_OFFSET = 0x7C;
    // 0x7D-0x7F Reserved

    public static final byte STRING_ZERO_LENGTH = (byte) 0x80;

    public static final byte INLINE_SYMBOL_ZERO_LENGTH = (byte) 0x90;

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
    // 0xEE Reserved
    public static final byte SYSTEM_MACRO_INVOCATION = (byte) 0xEF;

    public static final byte DELIMITED_END_MARKER = (byte) 0xF0;
    public static final byte DELIMITED_LIST = (byte) 0xF1;
    public static final byte DELIMITED_SEXP = (byte) 0xF2;
    public static final byte DELIMITED_STRUCT = (byte) 0xF3;
    public static final byte LENGTH_PREFIXED_MACRO_INVOCATION = (byte) 0xF4;
    public static final byte VARIABLE_LENGTH_INTEGER = (byte) 0xF5;
    public static final byte VARIABLE_LENGTH_DECIMAL = (byte) 0xF6;
    public static final byte VARIABLE_LENGTH_TIMESTAMP = (byte) 0xF7;
    public static final byte VARIABLE_LENGTH_STRING = (byte) 0xF8;
    public static final byte VARIABLE_LENGTH_INLINE_SYMBOL = (byte) 0xF9;
    public static final byte VARIABLE_LENGTH_LIST = (byte) 0xFA;
    public static final byte VARIABLE_LENGTH_SEXP = (byte) 0xFB;
    public static final byte VARIABLE_LENGTH_STRUCT_WITH_SIDS = (byte) 0xFC;
    public static final byte VARIABLE_LENGTH_STRUCT_WITH_FLEXSYMS = (byte) 0xFD;
    public static final byte VARIABLE_LENGTH_BLOB = (byte) 0xFE;
    public static final byte VARIABLE_LENGTH_CLOB = (byte) 0xFF;
}
