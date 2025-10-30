// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.bin;

/**
 * Represents a slice of bytes that need to be overwritten by a variable length, unsigned integer that is too large
 * to fit into the specified slice.
 */
public class PatchPoint {
    /**
     * position of the data being patched out.
     */
    public long oldPosition;
    /**
     * length of the data being patched out.
     */
    public int oldLength;
    /**
     * size of the container data or annotations.
     */
    public long length;

    public PatchPoint() {
        oldPosition = -1;
        oldLength = -1;
        length = -1;
    }

    @Override
    public String toString() {
        return "(PP old::(" + oldPosition + " " + oldLength + ") patch::(" + length + ")";
    }

    public PatchPoint initialize(final long oldPosition, final int oldLength, final long length) {
        this.oldPosition = oldPosition;
        this.oldLength = oldLength;
        this.length = length;
        return this;
    }

    public PatchPoint clear() {
        return initialize(-1, -1, -1);
    }
}
