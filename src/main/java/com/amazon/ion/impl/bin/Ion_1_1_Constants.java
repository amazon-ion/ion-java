package com.amazon.ion.impl.bin;

/**
 * Contains constants (other than OpCodes) which are generally applicable to both reading and writing binary Ion 1.1
 */
public class Ion_1_1_Constants {
    private Ion_1_1_Constants() {}

    //////// Timestamp Field Constants ////////

    // S_TIMESTAMP_* is applicable to all short-form timestamps
    static final int S_TIMESTAMP_MONTH_BIT_OFFSET = 7;
    static final int S_TIMESTAMP_DAY_BIT_OFFSET = 11;
    static final int S_TIMESTAMP_HOUR_BIT_OFFSET = 16;
    static final int S_TIMESTAMP_MINUTE_BIT_OFFSET = 21;
    // S_U_TIMESTAMP_* is applicable to all short-form timestamps with a `U` bit
    static final int S_U_TIMESTAMP_UTC_FLAG = 1 << 27;
    static final int S_U_TIMESTAMP_SECOND_BIT_OFFSET = 28;
    static final int S_U_TIMESTAMP_FRACTION_BIT_OFFSET = 34;
    // S_O_TIMESTAMP_* is applicable to all short-form timestamps with `o` (offset) bits
    static final int S_O_TIMESTAMP_OFFSET_BIT_OFFSET = 27;
    static final int S_O_TIMESTAMP_SECOND_BIT_OFFSET = 34;

    // L_TIMESTAMP_* is applicable to all long-form timestamps
    static final int L_TIMESTAMP_MONTH_BIT_OFFSET = 14;
    static final int L_TIMESTAMP_DAY_BIT_OFFSET = 18;
    static final int L_TIMESTAMP_HOUR_BIT_OFFSET = 23;
    static final int L_TIMESTAMP_MINUTE_BIT_OFFSET = 28;
    static final int L_TIMESTAMP_OFFSET_BIT_OFFSET = 34;
    static final int L_TIMESTAMP_SECOND_BIT_OFFSET = 46;
    static final int L_TIMESTAMP_UNKNOWN_OFFSET_VALUE = 0b111111111111;

    //////// Bit masks ////////

    static final long LEAST_SIGNIFICANT_7_BITS = 0b01111111L;
}
