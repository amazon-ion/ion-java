// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.bin;

import com.amazon.ion.Timestamp;
import com.amazon.ion._private.SuppressFBWarnings;

/**
 * Contains constants (other than OpCodes) which are generally applicable to both reading and writing binary Ion 1.1
 */
public class Ion_1_1_Constants {
    private Ion_1_1_Constants() {}

    static final int FIRST_2_BYTE_SYMBOL_ADDRESS = 256;
    static final int FIRST_MANY_BYTE_SYMBOL_ADDRESS = 65792;

    /**
     * FlexSym byte that indicates an opcode is coming; it's value is the FlexInt encoding of 0.
     */
    static final byte FLEX_SYM_ESCAPE_BYTE = 1;

    static final byte SID_TO_FLEX_SYM_SWITCH_MARKER = FlexInt.ZERO;

    public static final int MAX_NANOSECONDS = 999999999;
    public static final int NANOSECOND_SCALE = 9;
    public static final int MAX_MICROSECONDS = 999999;
    public static final int MICROSECOND_SCALE = 6;
    public static final int MAX_MILLISECONDS = 999;
    public static final int MILLISECOND_SCALE = 3;

    //////// Timestamp Field Constants ////////

    // S_TIMESTAMP_* is applicable to all short-form timestamps
    public static final int S_TIMESTAMP_YEAR_BIAS = 1970;
    public static final int S_TIMESTAMP_MONTH_BIT_OFFSET = 7;
    public static final int S_TIMESTAMP_DAY_BIT_OFFSET = 11;
    public static final int S_TIMESTAMP_HOUR_BIT_OFFSET = 16;
    public static final int S_TIMESTAMP_MINUTE_BIT_OFFSET = 21;
    // S_U_TIMESTAMP_* is applicable to all short-form timestamps with a `U` bit
    public static final int S_U_TIMESTAMP_NANOSECOND_LOWER_NIBBLE = 0x7;
    public static final int S_U_TIMESTAMP_MICROSECOND_LOWER_NIBBLE = 0x6;
    public static final int S_U_TIMESTAMP_MILLISECOND_LOWER_NIBBLE = 0x5;
    public static final int S_U_TIMESTAMP_UTC_FLAG = 1 << 27;
    public static final int S_U_TIMESTAMP_SECOND_BIT_OFFSET = 28;
    public static final int S_U_TIMESTAMP_FRACTION_BIT_OFFSET = 34;
    // S_O_TIMESTAMP_* is applicable to all short-form timestamps with `o` (offset) bits
    public static final int S_O_TIMESTAMP_NANOSECOND_LOWER_NIBBLE = 0xC;
    public static final int S_O_TIMESTAMP_MICROSECOND_LOWER_NIBBLE = 0xB;
    public static final int S_O_TIMESTAMP_MILLISECOND_LOWER_NIBBLE = 0xA;
    public static final int S_O_TIMESTAMP_MINUTE_LOWER_NIBBLE = 0x8;
    public static final int S_O_TIMESTAMP_OFFSET_BIT_OFFSET = 27;
    public static final int S_O_TIMESTAMP_SECOND_BIT_OFFSET = 34;
    public static final int S_O_TIMESTAMP_FRACTION_BIT_OFFSET = 40;
    public static final int S_O_TIMESTAMP_NANOSECOND_BITS_IN_EIGHTH_BYTE = 24;

    // Explicit offsets are encoded in increments of 15 minutes, from -56.
    public static final int S_O_TIMESTAMP_OFFSET_BIAS = 56;
    public static final int S_O_TIMESTAMP_OFFSET_INCREMENT = 15;

    // L_TIMESTAMP_* is applicable to all long-form timestamps
    public static final int L_TIMESTAMP_MONTH_BIT_OFFSET = 14;
    public static final int L_TIMESTAMP_DAY_BIT_OFFSET = 18;
    public static final int L_TIMESTAMP_HOUR_BIT_OFFSET = 23;
    public static final int L_TIMESTAMP_MINUTE_BIT_OFFSET = 28;
    public static final int L_TIMESTAMP_OFFSET_BIT_OFFSET = 34;
    public static final int L_TIMESTAMP_SECOND_BIT_OFFSET = 46;
    public static final int L_TIMESTAMP_UNKNOWN_OFFSET_VALUE = 0b111111111111;
    public static final int L_TIMESTAMP_SECOND_BYTE_LENGTH = 7;
    public static final int L_TIMESTAMP_MINUTE_BYTE_LENGTH = 6;
    public static final int L_TIMESTAMP_DAY_OR_MONTH_BYTE_LENGTH = 3;
    public static final int L_TIMESTAMP_YEAR_BYTE_LENGTH = 2;
    public static final int L_TIMESTAMP_OFFSET_BIAS = 1440; // 24 hours * 60 min/hour

    //////// Lookup tables ////////
    @SuppressFBWarnings("MS_MUTABLE_ARRAY")
    public static final Timestamp.Precision[] S_TIMESTAMP_PRECISION_FOR_TYPE_ID_OFFSET = new Timestamp.Precision[] {
        Timestamp.Precision.YEAR, // 0x70
        Timestamp.Precision.MONTH, // 0x71
        Timestamp.Precision.DAY, // 0x72
        Timestamp.Precision.MINUTE, // 0x73 (minute UTC)
        Timestamp.Precision.SECOND, // 0x74 (second UTC)
        Timestamp.Precision.SECOND, // 0x75 (millisecond UTC)
        Timestamp.Precision.SECOND, // 0x76 (microsecond UTC)
        Timestamp.Precision.SECOND, // 0x77 (nanosecond UTC)
        Timestamp.Precision.MINUTE, // 0x78 (minute offset)
        Timestamp.Precision.SECOND, // 0x79 (second offset)
        Timestamp.Precision.SECOND, // 0x7A (millisecond offset)
        Timestamp.Precision.SECOND, // 0x7B (microsecond offset)
        Timestamp.Precision.SECOND, // 0x7C (nanosecond offset)
    };

    @SuppressFBWarnings("MS_MUTABLE_ARRAY")
    public static final Timestamp.Precision[] L_TIMESTAMP_PRECISION_FOR_LENGTH = new Timestamp.Precision[] {
        null, // Length 0: illegal
        null, // Length 1: illegal
        Timestamp.Precision.YEAR,
        null, // Length 3: Month or Day; additional examination required.
        null, // Length 4: illegal
        null, // Length 5: illegal
        Timestamp.Precision.MINUTE,
        Timestamp.Precision.SECOND
    };

    //////// Bit masks ////////

    public static final int FOUR_BIT_MASK = 0xF;
    public static final int FIVE_BIT_MASK = 0x1F;
    public static final int SIX_BIT_MASK = 0x3F;
    public static final int SEVEN_BIT_MASK = 0x7F;
    public static final int TEN_BIT_MASK = 0x3FF;
    public static final int TWELVE_BIT_MASK = 0xFFF;
    public static final int FOURTEEN_BIT_MASK = 0x3FFF;
    public static final int TWENTY_BIT_MASK = 0xFFFFF;
    public static final int TWENTY_FOUR_BIT_MASK = 0xFFFFFF;
    public static final int THIRTY_BIT_MASK = 0x3FFFFFFF;

    public static final long L_TIMESTAMP_SECOND_MASK = (long) SIX_BIT_MASK << L_TIMESTAMP_SECOND_BIT_OFFSET;
    public static final long L_TIMESTAMP_OFFSET_MASK = (long) TWELVE_BIT_MASK << L_TIMESTAMP_OFFSET_BIT_OFFSET;
    public static final long L_TIMESTAMP_MINUTE_MASK = (long) SIX_BIT_MASK << L_TIMESTAMP_MINUTE_BIT_OFFSET;
    public static final int L_TIMESTAMP_HOUR_MASK = FIVE_BIT_MASK << L_TIMESTAMP_HOUR_BIT_OFFSET;
    public static final int L_TIMESTAMP_DAY_MASK = FIVE_BIT_MASK << L_TIMESTAMP_DAY_BIT_OFFSET;
    public static final int L_TIMESTAMP_MONTH_MASK = FOUR_BIT_MASK << L_TIMESTAMP_MONTH_BIT_OFFSET;
    public static final int L_TIMESTAMP_YEAR_MASK = FOURTEEN_BIT_MASK;

    public static final long S_O_TIMESTAMP_NANOSECOND_EIGHTH_BYTE_MASK = (long) TWENTY_FOUR_BIT_MASK << S_O_TIMESTAMP_FRACTION_BIT_OFFSET;
    public static final long S_O_TIMESTAMP_NANOSECOND_NINTH_BYTE_MASK = SIX_BIT_MASK;
    public static final long S_U_TIMESTAMP_NANOSECOND_MASK = (long) THIRTY_BIT_MASK << S_U_TIMESTAMP_FRACTION_BIT_OFFSET;
    public static final long S_O_TIMESTAMP_MICROSECOND_MASK = (long) TWENTY_BIT_MASK << S_O_TIMESTAMP_FRACTION_BIT_OFFSET;
    public static final long S_U_TIMESTAMP_MICROSECOND_MASK = (long) TWENTY_BIT_MASK << S_U_TIMESTAMP_FRACTION_BIT_OFFSET;
    public static final long S_O_TIMESTAMP_MILLISECOND_MASK = (long) TEN_BIT_MASK << S_O_TIMESTAMP_FRACTION_BIT_OFFSET;
    public static final long S_U_TIMESTAMP_MILLISECOND_MASK = (long) TEN_BIT_MASK << S_U_TIMESTAMP_FRACTION_BIT_OFFSET;
    public static final long S_O_TIMESTAMP_SECOND_MASK = (long) SIX_BIT_MASK << S_O_TIMESTAMP_SECOND_BIT_OFFSET;
    public static final long S_U_TIMESTAMP_SECOND_MASK = (long) SIX_BIT_MASK << S_U_TIMESTAMP_SECOND_BIT_OFFSET;
    public static final long S_O_TIMESTAMP_OFFSET_MASK = (long) SEVEN_BIT_MASK << S_O_TIMESTAMP_OFFSET_BIT_OFFSET;
    public static final int S_TIMESTAMP_MINUTE_MASK = SIX_BIT_MASK << S_TIMESTAMP_MINUTE_BIT_OFFSET;
    public static final int S_TIMESTAMP_HOUR_MASK = FIVE_BIT_MASK << S_TIMESTAMP_HOUR_BIT_OFFSET;
    public static final int S_TIMESTAMP_DAY_MASK = FIVE_BIT_MASK << S_TIMESTAMP_DAY_BIT_OFFSET;
    public static final int S_TIMESTAMP_MONTH_MASK = FOUR_BIT_MASK << S_TIMESTAMP_MONTH_BIT_OFFSET;
    public static final int S_TIMESTAMP_YEAR_MASK = SEVEN_BIT_MASK;
}
