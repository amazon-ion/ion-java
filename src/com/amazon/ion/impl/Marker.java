// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ion.impl;

/**
 * Holds the start and end indices of a slice of the buffer.
 */
class Marker {

    /**
     * The type ID that governs the slice.
     */
    IonTypeID typeId = null;

    /**
     * Index of the first byte in the slice.
     */
    long startIndex;

    /**
     * Index of the first byte after the end of the slice.
     */
    long endIndex;

    /**
     * @param startIndex index of the first byte in the slice.
     * @param length     the number of bytes in the slice.
     */
    Marker(final int startIndex, final int length) {
        this.startIndex = startIndex;
        this.endIndex = startIndex + length;
    }
}
