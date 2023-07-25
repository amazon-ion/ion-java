// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ion.impl;

import com.amazon.ion.IonType;

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

    // Lookup table from type ID to IonType. See https://amzn.github.io/ion-docs/docs/binary.html#typed-value-formats
    static final IonType[] ION_TYPES_1_0 = new IonType[] {
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

    // Singleton invalid type ID.
    private static final IonTypeID ALWAYS_INVALID_TYPE_ID = new IonTypeID((byte) 0xFF, 0);

    // Pre-compute all possible type ID bytes.
    static final IonTypeID[] TYPE_IDS_NO_IVM;
    static final IonTypeID[] TYPE_IDS_1_0;
    static {
        TYPE_IDS_NO_IVM = new IonTypeID[NUMBER_OF_BYTES];
        TYPE_IDS_1_0 = new IonTypeID[NUMBER_OF_BYTES];
        for (int b = 0x00; b < NUMBER_OF_BYTES; b++) {
            TYPE_IDS_NO_IVM[b] = ALWAYS_INVALID_TYPE_ID;
            TYPE_IDS_1_0[b] = new IonTypeID((byte) b, 0);
        }
    }

    final IonType type;
    final int length;
    final boolean variableLength;
    final boolean isNull;
    final boolean isNopPad;
    final byte lowerNibble;
    final boolean isValid;
    final boolean isNegativeInt;
    final boolean isTemplateInvocation; // Unused in Ion 1.0
    final int templateId; // Unused in Ion 1.0
    final boolean isDelimited; // Unused in Ion 1.0
    // For structs, denotes whether field names are VarSyms. For symbols, denotes whether the text is inline.
    // For annotation wrappers, denotes whether tokens are VarSyms.
    final boolean isInlineable; // Unused in Ion 1.0

    /**
     * Determines whether the Ion spec allows this particular upperNibble/lowerNibble pair.
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

    private IonTypeID(byte id, int minorVersion) {
        if (minorVersion == 0) {
            byte upperNibble = (byte) ((id >> BITS_PER_NIBBLE) & LOW_NIBBLE_BITMASK);
            this.lowerNibble = (byte) (id & LOW_NIBBLE_BITMASK);
            if (upperNibble == 0 && lowerNibble != NULL_VALUE_NIBBLE) {
                this.isNopPad = true;
                this.type = null;
            } else {
                this.isNopPad = false;
                this.type = ION_TYPES_1_0[upperNibble];
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
            this.isTemplateInvocation = false;
            this.templateId = -1;
            this.isDelimited = false;
            this.isInlineable = false;
        } else {
            throw new IllegalStateException("Only Ion 1.0 is currently supported.");
        }
    }
}
