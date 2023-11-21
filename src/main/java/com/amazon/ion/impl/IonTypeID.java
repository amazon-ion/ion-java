// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ion.impl;

import com.amazon.ion.IonType;

import static com.amazon.ion.impl.bin.OpCodes.*;

/**
 * Holds pre-computed information about a binary Ion type ID byte.
 */
final class IonTypeID {

    private static final int NUMBER_OF_BYTES = 0x100;
    private static final int BITS_PER_NIBBLE = 4;
    private static final int LOW_NIBBLE_BITMASK = 0x0F;
    private static final int NULL_VALUE_NIBBLE = 0xF;
    private static final int VARIABLE_LENGTH_NIBBLE = 0xE;
    private static final int NEGATIVE_INT_TYPE_CODE = 0x3;
    private static final int TYPE_CODE_INVALID = 0xF;
    private static final int ANNOTATION_WRAPPER_MIN_LENGTH = 0x3;
    private static final int ANNOTATION_WRAPPER_MAX_LENGTH = 0xE;
    static final int ORDERED_STRUCT_NIBBLE = 0x1;

    // NOTE: 'annotation wrapper' is not an IonType, but it is simplest to treat it as one for the purposes of this
    // implementation in order to have a direct mapping from binary type IDs to IonType enum values. IonType.DATAGRAM
    // does not have a type ID, so we will use it to mean 'annotation wrapper' instead.
    static final IonType ION_TYPE_ANNOTATION_WRAPPER = IonType.DATAGRAM;

    // Lookup table from type ID to "binary token type", loosely represented by the IonType enum to avoid the need to
    // define a completely new enum with translations between them. "Binary token types" are a superset of IonType,
    // adding annotation wrapper and `null` (i.e., illegal).
    // See https://amzn.github.io/ion-docs/docs/binary.html#typed-value-formats
    static final IonType[] BINARY_TOKEN_TYPES_1_0 = new IonType[] {
        IonType.NULL,
        IonType.BOOL,
        IonType.INT,
        IonType.INT,
        IonType.FLOAT,
        IonType.DECIMAL,
        IonType.TIMESTAMP,
        IonType.SYMBOL,
        IonType.STRING,
        IonType.CLOB,
        IonType.BLOB,
        IonType.LIST,
        IonType.SEXP,
        IonType.STRUCT,
        ION_TYPE_ANNOTATION_WRAPPER,
        null // The 0xF type code is illegal in Ion 1.0.
    };

    private static final IonType[] BINARY_TOKEN_TYPES_1_1 = new IonType[] {
        null, // 0: macro invocation
        null, // 1: macro invocation
        null, // 2: macro invocation
        null, // 3: macro invocation
        null, // 4: macro invocation
        null, // 5: int, float, bool
        IonType.DECIMAL,
        IonType.TIMESTAMP,
        IonType.STRING,
        IonType.SYMBOL,
        IonType.LIST,
        IonType.SEXP,
        IonType.STRUCT, // symbol ID field names
        IonType.STRUCT, // FlexSym field names
        null, // E: symbol ID, annotated value, NOP, null, system macro invocation
        null  // F: variable length macro, variable length of all types, delimited start/end
    };

    // Singleton invalid type ID.
    private static final IonTypeID ALWAYS_INVALID_TYPE_ID = new IonTypeID((byte) 0xFF, 0);

    // Pre-compute all possible type ID bytes.
    static final IonTypeID[] TYPE_IDS_NO_IVM;
    static final IonTypeID[] TYPE_IDS_1_0;
    static final IonTypeID[] TYPE_IDS_1_1;
    static final IonTypeID[] NULL_TYPE_IDS_1_1;
    static {
        TYPE_IDS_NO_IVM = new IonTypeID[NUMBER_OF_BYTES];
        TYPE_IDS_1_0 = new IonTypeID[NUMBER_OF_BYTES];
        TYPE_IDS_1_1 = new IonTypeID[NUMBER_OF_BYTES];
        for (int b = 0x00; b < NUMBER_OF_BYTES; b++) {
            TYPE_IDS_NO_IVM[b] = ALWAYS_INVALID_TYPE_ID;
            TYPE_IDS_1_0[b] = new IonTypeID((byte) b, 0);
            TYPE_IDS_1_1[b] = new IonTypeID((byte) b, 1);
        }
        // In Ion 1.1, typed nulls are represented by the type ID 0xEB followed by a 1-byte UInt indicating the type.
        // Therefore, the type of the typed null cannot be precomputed in Ion 1.1. In order to avoid adding more hot
        // path branching to the reader, we create IonTypeIDs that mimic precomputed typed nulls in Ion 1.1 by reusing
        // the typed null type IDs from Ion 1.0. When the type of the typed null is determined, the reader's current
        // IonTypeID will be replaced with one of these. The index is the one-byte value that follows 0xEB.
        NULL_TYPE_IDS_1_1 = new IonTypeID[12];
        NULL_TYPE_IDS_1_1[0x0] = TYPE_IDS_1_0[0x1F]; // null.bool
        NULL_TYPE_IDS_1_1[0x1] = TYPE_IDS_1_0[0x2F]; // null.int
        NULL_TYPE_IDS_1_1[0x2] = TYPE_IDS_1_0[0x4F]; // null.float
        NULL_TYPE_IDS_1_1[0x3] = TYPE_IDS_1_0[0x5F]; // null.decimal
        NULL_TYPE_IDS_1_1[0x4] = TYPE_IDS_1_0[0x6F]; // null.timestamp
        NULL_TYPE_IDS_1_1[0x5] = TYPE_IDS_1_0[0x8F]; // null.string
        NULL_TYPE_IDS_1_1[0x6] = TYPE_IDS_1_0[0x7F]; // null.symbol
        NULL_TYPE_IDS_1_1[0x7] = TYPE_IDS_1_0[0xAF]; // null.blob
        NULL_TYPE_IDS_1_1[0x8] = TYPE_IDS_1_0[0x9F]; // null.clob
        NULL_TYPE_IDS_1_1[0x9] = TYPE_IDS_1_0[0xBF]; // null.list
        NULL_TYPE_IDS_1_1[0xA] = TYPE_IDS_1_0[0xCF]; // null.sexp
        NULL_TYPE_IDS_1_1[0xB] = TYPE_IDS_1_0[0xDF]; // null.struct
    }

    final IonType type;
    final int length;
    final boolean variableLength;
    final boolean isNull;
    final boolean isNopPad;
    final byte lowerNibble;
    final boolean isValid;
    final boolean isNegativeInt;
    final boolean isMacroInvocation;
    final int macroId;
    final boolean isDelimited;
    // For structs, denotes whether field names are VarSyms. For symbols, denotes whether the text is inline.
    // For annotation wrappers, denotes whether tokens are VarSyms.
    final boolean isInlineable;

    /**
     * Determines whether the Ion 1.0 spec allows this particular upperNibble/lowerNibble pair.
     */
    private static boolean isValid_1_0(byte upperNibble, byte lowerNibble, IonType type) {
        if (upperNibble == TYPE_CODE_INVALID) {
            // Type code F is unused in Ion 1.0.
            return false;
        }
        if (type == IonType.BOOL) {
            // Bool values can only be false (0), true (1), or null (F).
            return lowerNibble <= 1 || lowerNibble == NULL_VALUE_NIBBLE;
        }
        if (type == IonType.INT && upperNibble == NEGATIVE_INT_TYPE_CODE) {
            // There is no negative zero int.
            return lowerNibble != 0;
        }
        if (type == IonType.FLOAT) {
            // Floats are either 0e0 (0), 32-bit (4), 64-bit (8), or null (F).
            return lowerNibble == 0 || lowerNibble == 4 || lowerNibble == 8 || lowerNibble == NULL_VALUE_NIBBLE;
        }
        if (type == IonType.TIMESTAMP) {
            // There is no zero-length timestamp representation.
            return lowerNibble > 1;
        }
        if (type == ION_TYPE_ANNOTATION_WRAPPER) {
            return lowerNibble >= ANNOTATION_WRAPPER_MIN_LENGTH && lowerNibble <= ANNOTATION_WRAPPER_MAX_LENGTH;
        }
        return true;
    }

    /**
     * Determines whether the Ion 1.1 spec allows this particular upperNibble/lowerNibble pair.
     */
    private static boolean isValid_1_1(byte id) {
        return !(
            id == (byte) 0x59
            || id == (byte) 0xC1
            || id == (byte) 0xD0
            || id == (byte) 0xD1
            || id == (byte) 0xE0
            || id == (byte) 0xEE
        );
    }

    private IonTypeID(byte id, int minorVersion) {
        if (minorVersion == 0) {
            byte upperNibble = (byte) ((id >> BITS_PER_NIBBLE) & LOW_NIBBLE_BITMASK);
            this.lowerNibble = (byte) (id & LOW_NIBBLE_BITMASK);
            if (upperNibble == 0 && lowerNibble != NULL_VALUE_NIBBLE) {
                this.isNopPad = true;
                this.type = null;
            } else {
                this.isNopPad = false;
                this.type = BINARY_TOKEN_TYPES_1_0[upperNibble];
            }
            this.isValid = isValid_1_0(upperNibble, lowerNibble, type);
            this.isNull = lowerNibble == NULL_VALUE_NIBBLE;
            byte length = lowerNibble;
            if (type == IonType.NULL || type == IonType.BOOL || !isValid) {
                variableLength = false;
                length = 0;
            } else if (type == IonType.STRUCT && length == ORDERED_STRUCT_NIBBLE) {
                variableLength = true;
            } else {
                variableLength = length == VARIABLE_LENGTH_NIBBLE;
            }
            if (isNull) {
                length = 0;
            }
            this.isNegativeInt = type == IonType.INT && upperNibble == NEGATIVE_INT_TYPE_CODE;
            this.length = length;
            this.isMacroInvocation = false;
            this.macroId = -1;
            this.isDelimited = false;
            this.isInlineable = false;
        } else {
            isValid = isValid_1_1(id);
            byte upperNibble = (byte) ((id >> BITS_PER_NIBBLE) & LOW_NIBBLE_BITMASK);
            // For 0xF0 (delimited end byte) the entire byte is included. This avoids having to create a separate field
            // just to identify this byte.
            lowerNibble = (id == DELIMITED_END_MARKER) ? DELIMITED_END_MARKER : (byte) (id & LOW_NIBBLE_BITMASK);
            isNegativeInt = false; // Not applicable for Ion 1.1; sign is conveyed by the representation.
            isMacroInvocation = upperNibble <= 0x4 || id == LENGTH_PREFIXED_MACRO_INVOCATION || id == SYSTEM_MACRO_INVOCATION;
            boolean isNopPad = false;
            boolean isNull = false;
            int length = -1;
            if (isMacroInvocation) {
                if (upperNibble == 0x4) {
                    variableLength = true;
                    // This isn't the whole macro ID, but it's all the relevant bits from the type ID byte (the 4
                    // least-significant bits).
                    macroId = lowerNibble;
                } else if (upperNibble < 0x4){
                    variableLength = false;
                    macroId = id;
                } else {
                    // System or length-prefixed macro invocation.
                    variableLength = upperNibble == 0xF;
                    macroId = -1;
                }
                type = null;
                isInlineable = false;
            } else {
                macroId = -1;
                variableLength =
                       (upperNibble == 0xF && lowerNibble >= 0x4) // Variable length, all types.
                    || id == ANNOTATIONS_MANY_SYMBOL_ADDRESS
                    || id == ANNOTATIONS_MANY_FLEX_SYM
                    || id == VARIABLE_LENGTH_NOP;
                isInlineable =
                       // struct with VarSym field names.
                       (upperNibble == 0xD && lowerNibble >= 0x2)
                    || id == DELIMITED_STRUCT
                    || id == VARIABLE_LENGTH_INLINE_SYMBOL
                    || id == VARIABLE_LENGTH_STRUCT_WITH_FLEXSYMS
                    || id == ANNOTATIONS_1_FLEX_SYM
                    || id == ANNOTATIONS_2_FLEX_SYM
                    || id == ANNOTATIONS_MANY_FLEX_SYM
                       // Symbol values with inline text.
                    || upperNibble == 0x9;
                IonType typeFromUpperNibble = BINARY_TOKEN_TYPES_1_1[upperNibble];
                if (typeFromUpperNibble == null) {
                    if (!isValid) {
                        type = null;
                    } else if (upperNibble == 0x5) {
                        if (lowerNibble <= 0x8) {
                            type = IonType.INT;
                            length = lowerNibble;
                        } else if (id == BOOLEAN_TRUE || id == BOOLEAN_FALSE) {
                            type = IonType.BOOL;
                            length = 0;
                        } else {
                            type = IonType.FLOAT;
                            if (id == FLOAT_ZERO_LENGTH) {
                                length = 0; // 0e0
                            } else if (id == FLOAT_16) {
                                length = 2;
                            } else if (id == FLOAT_32) {
                                length = 4;
                            } else if (id == FLOAT_64) {
                                length = 8;
                            }
                        }
                    } else if (upperNibble == 0xE) {
                        if (id == SYMBOL_ADDRESS_1_BYTE || id == SYMBOL_ADDRESS_2_BYTES || id == SYMBOL_ADDRESS_MANY_BYTES) {
                            type = IonType.SYMBOL;
                            length = id == SYMBOL_ADDRESS_MANY_BYTES ? -1 : lowerNibble;
                        } else if (lowerNibble <= 0x9) {
                            type = ION_TYPE_ANNOTATION_WRAPPER;
                        } else if (id == NULL_UNTYPED) {
                            type = IonType.NULL;
                            isNull = true;
                            length = 0;
                        } else if (id == NULL_TYPED) {
                            // Typed null. Type byte follows.
                            type = null;
                            isNull = true;
                            length = 1;
                        } else if (id == ONE_BYTE_NOP || id == VARIABLE_LENGTH_NOP) {
                            isNopPad = true;
                            type = null;
                            length = variableLength ? -1 : 0;
                        } else { // 0xF
                            // System macro invocation.
                            type = null;
                        }
                    } else { // 0xF
                        if (id == DELIMITED_END_MARKER) {
                            type = null;
                            length = 0;
                        } else if (id == DELIMITED_STRUCT || id == VARIABLE_LENGTH_STRUCT_WITH_SIDS || id == VARIABLE_LENGTH_STRUCT_WITH_FLEXSYMS) {
                            type = IonType.STRUCT;
                        } else if (id == VARIABLE_LENGTH_INTEGER) {
                            type = IonType.INT;
                        } else if (id == VARIABLE_LENGTH_DECIMAL) {
                            type = IonType.DECIMAL;
                        } else if (id == VARIABLE_LENGTH_TIMESTAMP) {
                            type = IonType.TIMESTAMP;
                        } else if (id == VARIABLE_LENGTH_INLINE_SYMBOL) {
                            type = IonType.SYMBOL;
                        } else if (id == VARIABLE_LENGTH_STRING) {
                            type = IonType.STRING;
                        } else if (id == VARIABLE_LENGTH_BLOB) {
                            type = IonType.BLOB;
                        } else if (id == VARIABLE_LENGTH_CLOB) {
                            type = IonType.CLOB;
                        } else if (id == DELIMITED_LIST || id == VARIABLE_LENGTH_LIST) {
                            type = IonType.LIST;
                        } else if  (id == DELIMITED_SEXP || id == VARIABLE_LENGTH_SEXP) {
                            type = IonType.SEXP;
                        } else { // 0x4
                            // Variable length macro invocation
                            type = null;
                        }
                    }
                } else {
                    type = typeFromUpperNibble;
                    if (type == IonType.TIMESTAMP) {
                        // Short-form timestamps. Long-form timestamps use the upper nibble 0xF, forcing them to take
                        // the previous branch.
                        switch (id) {
                            case TIMESTAMP_YEAR_PRECISION:
                                length = 1;
                                break;
                            case TIMESTAMP_MONTH_PRECISION:
                            case TIMESTAMP_DAY_PRECISION:
                                length = 2;
                                break;
                            case TIMESTAMP_MINUTE_PRECISION:
                                length = 4;
                                break;
                            case TIMESTAMP_SECOND_PRECISION:
                            case TIMESTAMP_MINUTE_PRECISION_WITH_OFFSET:
                            case TIMESTAMP_SECOND_PRECISION_WITH_OFFSET:
                                length = 5;
                                break;
                            case TIMESTAMP_MILLIS_PRECISION:
                                length = 6;
                                break;
                            case TIMESTAMP_MICROS_PRECISION:
                            case TIMESTAMP_MILLIS_PRECISION_WITH_OFFSET:
                                length = 7;
                                break;
                            case TIMESTAMP_NANOS_PRECISION:
                            case TIMESTAMP_MICROS_PRECISION_WITH_OFFSET:
                                length = 8;
                                break;
                            case TIMESTAMP_NANOS_PRECISION_WITH_OFFSET:
                                length = 9;
                                break;
                        }
                    } else {
                        length = lowerNibble;
                    }
                }
            }
            isDelimited = id == DELIMITED_LIST || id == DELIMITED_SEXP || id == DELIMITED_STRUCT;
            this.isNopPad = isNopPad;
            this.isNull = isNull;
            this.length = length;
        }
    }

    /**
     * @return a String representation of this object (for debugging).
     */
    @Override
    public String toString() {
        return String.format("%s(%s)", type, length);
    }
}
