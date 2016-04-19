/*
 * Copyright 2007-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package software.amazon.ion.impl;

import software.amazon.ion.IonException;

/**
 * @deprecated This is an internal API that is subject to change without notice.
 */
@Deprecated
public final class PrivateIonConstants
{
    private PrivateIonConstants() { }


    public final static int BB_TOKEN_LEN           =    1;

    // 31 bits (java limit) / 7 bits per byte = 5 bytes
    public final static int BB_VAR_INT32_LEN_MAX   =    5;

    // 31 bits (java limit) / 7 bits per byte = 5 bytes
    public final static int BB_VAR_INT64_LEN_MAX   =   10;

    public final static int BB_INT64_LEN_MAX       =    8;
    public final static int BB_VAR_LEN_MIN         =    1;
    public final static int BB_MAX_7BIT_INT        =  127;

    public final static int INT32_SIZE            = 4;

    /** maximum size of textual representation of a {@code long}. */
    public final static int MAX_LONG_TEXT_SIZE    =
        Math.max(Long.toString(Long.MAX_VALUE).length(),
                 Long.toString(Long.MIN_VALUE).length());

    // these are used for various Unicode translation where
    // we need to convert the utf-16 Java characters into
    // unicode scalar values (utf-32 more or less) and back
    public static final int high_surrogate_value = 0xD800;
    public static final int low_surrogate_value = 0xDC00;
    public static final int surrogate_mask = 0xFFFFFC00; // 0x3f << 10; or the top 6 bits is the marker the low 10 is the 1/2 character
    public static final int surrogate_value_mask = ~0xFFFFFC00; // 0x3f << 10; or the top 6 bits is the marker the low 10 is the 1/2 character
    public static final int surrogate_utf32_offset = 0x10000;
    public static final int surrogate_utf32_shift = 10;

    // these help convert from Java UTF-16 to Unicode Scalars (aka unicode code
    // points (aka characters)) which are "32" bit values (really just 21 bits)
    // the DON'T check validity of their input, they expect that to have happened
    // already.  This is a perf issue since normally this check has been done
    // to detect that these routines should be called at all - no need to do it
    // twice.
    public static final int makeUnicodeScalar(int high_surrogate, int low_surrogate) {
        int c;
        c = (high_surrogate & surrogate_value_mask) << surrogate_utf32_shift;
        c |= (low_surrogate & surrogate_value_mask);
        c += surrogate_utf32_offset;
        return c;
    }
    public static final int makeHighSurrogate(int unicodeScalar) {
        int c;
        c = unicodeScalar - surrogate_utf32_offset;
        c >>>= surrogate_utf32_shift;
        c |= high_surrogate_value;
        return c;
    }
    public static final int makeLowSurrogate(int unicodeScalar) {
        int c;
        c = unicodeScalar - surrogate_utf32_offset;
        c &= surrogate_value_mask;
        c |= low_surrogate_value;
        return c;
    }
    public static final boolean isHighSurrogate(int c) {
        boolean is;
        is = (c & surrogate_mask) == high_surrogate_value;
        return is;
    }
    public static final boolean isLowSurrogate(int c) {
        boolean is;
        is = (c & surrogate_mask) == low_surrogate_value;
        return is;
    }
    public static final boolean isSurrogate(int c) {
        boolean is;
        is = (c & (surrogate_mask | (low_surrogate_value - high_surrogate_value))) == high_surrogate_value;
        return is;
    }

    /**
     * The byte sequence indicating use of Ion 1.0 binary format.
     */
    public static final byte[] BINARY_VERSION_MARKER_1_0 = { (byte) 0xE0,
                                                             (byte) 0x01,
                                                             (byte) 0x00,
                                                             (byte) 0xEA };

    /**
     * The number of bytes in {@link #BINARY_VERSION_MARKER_1_0}
     * ({@value #BINARY_VERSION_MARKER_SIZE}).
     */
    public static final int BINARY_VERSION_MARKER_SIZE =
        BINARY_VERSION_MARKER_1_0.length;


    public static final int tidNull         =  0;
    public static final int tidBoolean      =  1;
    public static final int tidPosInt       =  2;
    public static final int tidNegInt       =  3;
    public static final int tidFloat        =  4;
    public static final int tidDecimal      =  5;
    public static final int tidTimestamp    =  6;
    public static final int tidSymbol       =  7;
    public static final int tidString       =  8;
    public static final int tidClob         =  9;
    public static final int tidBlob         = 10; // a
    public static final int tidList         = 11; // b
    public static final int tidSexp         = 12; // c
    public static final int tidStruct       = 13; // d
    public static final int tidTypedecl     = 14; // e
    public static final int tidUnused       = 15; // f

    public static final int tidDATAGRAM     = 16; // not a real type id

/* this is just here to help programmer productivity ...
    switch (((td & 0xf0) >> 4)) {
    case IonConstants.tidNull:      // 0
    case IonConstants.tidBoolean:   // 1
    case IonConstants.tidPosInt:    // 2
    case IonConstants.tidNegInt:    // 3
    case IonConstants.tidFloat:     // 4
    case IonConstants.tidDecimal:   // 5
    case IonConstants.tidTimestamp: // 6
    case IonConstants.tidSymbol:    // 7
    case IonConstants.tidString:    // 8
    case IonConstants.tidClob:      // 9
    case IonConstants.tidBlob:      // 10 A
    case IonConstants.tidList:      // 11 B
    case IonConstants.tidSexp:      // 12 C
    case IonConstants.tidStruct:    // 13 D
    case IonConstants.tidTypedecl:  // 14 E
    case IonConstants.tidUnused:    // 15 F
    default:
        throw new IonException("???");
    }
*/


    public enum HighNibble {

        //           hnvalue, lengthFollows, isContainer
        hnNull      (tidNull,       false,    false),
        hnBoolean   (tidBoolean,    false,    false),
        hnPosInt    (tidPosInt,     false,    false),
        hnNegInt    (tidNegInt,     false,    false),
        hnFloat     (tidFloat,      false,    false),
        hnDecimal   (tidDecimal,    false,    false),
        hnTimestamp (tidTimestamp,  false,    false),
        hnSymbol    (tidSymbol,     false,    false),
        hnString    (tidString,     false,    false),
        hnClob      (tidClob,       false,    false),
        hnBlob      (tidBlob,       false,    false),
        hnList      (tidList,       true,     true),
        hnSexp      (tidSexp,       true,     true),
        hnStruct    (tidStruct,     true,     true),
        hnTypedecl  (tidTypedecl,   false,    false),
        hnUnused    (tidUnused,     false,    false);

        private int     _value;
        private boolean _lengthFollows;
        private boolean _isContainer;

        HighNibble(int value, boolean lengthFollows, boolean isContainer) {
            if ((value & (~0xF)) != 0) {
                throw new IonException("illegal high nibble initialization");
            }
            _value = value;
            _lengthFollows = lengthFollows;
            _isContainer = isContainer;
        }
        static public HighNibble getHighNibble(int hn) {
            switch (hn) {
            case tidNull:       return hnNull;
            case tidBoolean:    return hnBoolean;
            case tidPosInt:     return hnPosInt;
            case tidNegInt:     return hnNegInt;
            case tidFloat:      return hnFloat;
            case tidDecimal:    return hnDecimal;
            case tidTimestamp:  return hnTimestamp;
            case tidSymbol:     return hnSymbol;
            case tidString:     return hnString;
            case tidClob:       return hnClob;
            case tidBlob:       return hnBlob;
            case tidList:       return hnList;
            case tidSexp:       return hnSexp;
            case tidStruct:     return hnStruct;
            case tidTypedecl:   return hnTypedecl;
            case tidUnused:     return hnUnused;
            }
            return null;
        }
        public int     value()               { return _value; }
        public boolean lengthAlwaysFollows() { return _lengthFollows; }
        public boolean isContainer()         { return _isContainer; }
    }

    // TODO unify these
    public static final int lnIsNull           = 0x0f;
    public static final int lnIsNullAtom       = lnIsNull;
    public static final int lnIsNullSequence   = lnIsNull;
    public static final int lnIsNullStruct     = lnIsNull;

    public static final int lnIsEmptyContainer = 0x00;
    public static final int lnIsOrderedStruct  = 0x01;
    public static final int lnIsVarLen         = 0x0e;

    public static final int lnBooleanTrue     = 0x01;
    public static final int lnBooleanFalse    = 0x00;
    public static final int lnNumericZero     = 0x00;


    /**
     * Make a type descriptor from two nibbles; all of which are represented as
     * ints.
     *
     * @param highNibble must be a positive int between 0x00 and 0x0F.
     * @param lowNibble must be a positive int between 0x00 and 0x0F.
     *
     * @return the combined nibbles, between 0x00 and 0xFF.
     */
    public static final int makeTypeDescriptor(int highNibble,
                                               int lowNibble)
    {
        assert highNibble == (highNibble & 0xF);
        assert lowNibble == (lowNibble & 0xF);

        return ((highNibble << 4) | lowNibble);
    }

    /**
     * Extract the type code (high nibble) from a type descriptor.
     *
     * @param td must be a positive int between 0x00 and 0xFF.
     *
     * @return the high nibble of the input byte, between 0x00 and 0x0F.
     */
    public static final int getTypeCode(int td)
    {
        assert td >= 0 && td <= 0xFF;

        return (td >> 4);
    }

    public static final int getLowNibble(int td)
    {
        return td & 0xf;
    }

    public static final int True =
        makeTypeDescriptor(PrivateIonConstants.tidBoolean,
                           PrivateIonConstants.lnBooleanTrue);

    public static final int False =
        makeTypeDescriptor(PrivateIonConstants.tidBoolean,
                           PrivateIonConstants.lnBooleanFalse);

    /**
     * Prefix string used in IonStructs' equality checks.
     * When a IonValue's field name's text is unknown, this String is prepended
     * to the field name's SID to coerce it to a string to be used as the key.
     * This will eliminate collisions with IonValues with numbers as their
     * field names.
     * <p>
     * For example, these two IonValues (nested in the IonStructs) will have
     * distinct keys:
     *
     * <pre>
     * {"$99":random_value},
     * {$99:random_value}
     * </pre>
     *
     * <p>
     * TODO amznlabs/ion-java#23 However, there is still a potential failure if one of the
     * IonStruct's nested value has a field name with text
     * {@code " -- UNKNOWN SYMBOL TEXT -- $123"}, and that another nested value
     * of an IonStruct has a field name with unknown text and sid 123, these
     * two values will be considered a match within IonStruct's equality checks,
     * which is wrong.
     * <p>
     * See IonAssert for another use of this idiom.
     */
    public static final String UNKNOWN_SYMBOL_TEXT_PREFIX =
        " -- UNKNOWN SYMBOL TEXT -- $";
}
