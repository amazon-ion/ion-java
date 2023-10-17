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


    public static final byte NULL_UNTYPED = (byte) 0xEA;
    public static final byte NULL_TYPED = (byte) 0xEB;

    public static final byte VARIABLE_LENGTH_INTEGER = (byte) 0xF5;
}
