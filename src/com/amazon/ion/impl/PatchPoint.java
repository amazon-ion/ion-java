package com.amazon.ion.impl;

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
    public void initialize(final long oldPosition, final int oldLength, final long patchLength) {
        this.oldPosition = oldPosition;
        this.oldLength = oldLength;
        this.length = patchLength;
    }
}
