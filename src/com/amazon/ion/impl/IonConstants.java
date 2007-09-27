/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.impl;

import com.amazon.ion.IonException;

/**
 *
 */
public class IonConstants
{

    public final static int BB_TOKEN_LEN           =    1;

    // 31 bits (java limit) / 7 bits per byte = 5 bytes
    public final static int BB_VAR_INT32_LEN_MAX   =    5;

    // 31 bits (java limit) / 7 bits per byte = 5 bytes
    public final static int BB_VAR_INT64_LEN_MAX   =   10;

    public final static int BB_INT64_LEN_MAX       =    8;
    public final static int BB_VAR_LEN_MIN         =    1;
    public final static int BB_MAX_7BIT_INT        =  127;

    public final static int INT32_SIZE            = 4;

    /**
     * Only valid for Ion 1.0
     */
    public static final int MAGIC_COOKIE = 0x10140100;

    /**
     * The number of bytes in {@link #MAGIC_COOKIE} when encoded in a buffer.
     */
    public static final int MAGIC_COOKIE_SIZE = 4;


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
            if ((value & (~0xF)) != 0)
                throw new IonException("illegal high nibble initialization");
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
    public static final int lnIsNullAtom       = 0x0f;
    public static final int lnIsNullSequence   = 0x0f;
    public static final int lnIsNullStruct     = 0x0f;

    public static final int lnIsEmptyContainer = 0x00;
    public static final int lnIsOrderedStruct  = 0x01;
    public static final int lnIsVarLen         = 0x0e;

    /** @deprecated */
    public static final int lnIsDatagram      = 0x04;

    public static final int lnBooleanTrue     = 0x01;
    public static final int lnBooleanFalse    = 0x00;
    public static final int lnNumericZero     = 0x00;


    public static final byte makeTypeDescriptorByte(int highNibble,
                                                    int lowNibble)
    {
        return (byte)( ((((highNibble & 0xF) << 4) | (lowNibble & 0xF)) & 0xFF) );
    }
    public static final int getTypeDescriptor(int td)
    {
        return (td >> 4) & 0xf;
    }
    public static final int getLowNibble(int td)
    {
        return td & 0xf;
    }

    public static final byte True =
        makeTypeDescriptorByte(IonConstants.tidBoolean,
                               IonConstants.lnBooleanTrue);

    public static final byte False =
        makeTypeDescriptorByte(IonConstants.tidBoolean,
                               IonConstants.lnBooleanFalse);
}
