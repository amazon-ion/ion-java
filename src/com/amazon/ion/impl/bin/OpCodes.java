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
    public static final byte NEGATIVE_ZERO_DECIMAL = 0x6F;

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

    public static final byte NULL_UNTYPED = (byte) 0xEA;
    public static final byte NULL_TYPED = (byte) 0xEB;

    public static final byte VARIABLE_LENGTH_INTEGER = (byte) 0xF5;
    public static final byte VARIABLE_LENGTH_TIMESTAMP = (byte) 0xF7;
}
