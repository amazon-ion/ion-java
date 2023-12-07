// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ion.impl;

/**
 * A list of {@link Marker} values that grows as necessary, and serves as a pool to avoid excessive allocation.
 */
public class MarkerList {

    private Marker[] data;
    private int numberOfValues;
    private int provisionalIndex;

    /**
     * Constructs a new MarkerList with the specified capacity.
     * @param initialCapacity   The number of Markers that can be stored in this MarkerList before it will need to be
     *                          reallocated.
     */
    public MarkerList(final int initialCapacity) {
        data = new Marker[initialCapacity];
        for (int i = 0; i < initialCapacity; i++) {
            data[i] = new Marker(-1, -1);
        }
        numberOfValues = 0;
        provisionalIndex = 0;
    }

    /**
     * Accessor.
     * @return  The number of Markers currently stored in the list.
     */
    public int size() {
        return numberOfValues;
    }

    /**
     * @return {@code true} if there are Markers stored in the list.
     */
    public boolean isEmpty() {
        return numberOfValues == 0;
    }

    /**
     * Empties the list.
     * Note that this method does not shrink the size of the backing data store.
     */
    public void clear() {
        numberOfValues = 0;
        provisionalIndex = 0;
    }

    /**
     * Returns the {@code index}th Marker in the list.
     * @param index     The list index of the desired Marker.
     * @return          The Marker at index {@code index} in the list.
     * @throws IndexOutOfBoundsException    if the index is negative or greater than the number of Markers stored in the
     *                                      list.
     */
    public Marker get(int index) {
        if (index < 0 || index >= numberOfValues) {
            throw new IndexOutOfBoundsException(
                "Invalid index " + index + " requested from IntList with " + numberOfValues + " values."
            );
        }
        return data[index];
    }

    /**
     * @return The Marker that, if committed, will become the next element in the list. Grows the list if necessary.
     */
    public Marker provisionalElement() {
        if (provisionalIndex == data.length) {
            grow();
        }
        Marker provisional = data[provisionalIndex];
        provisional.startIndex = -1;
        provisional.endIndex = -1;
        provisional.typeId = null;
        provisionalIndex += 1;
        return provisional;
    }

    /**
     * Commits a provisional element, increasing the size of the list by one. It is the caller's responsibility to
     * ensure that a provisional element exists.
     */
    public void commit() {
        numberOfValues += 1;
    }

    /**
     * Reallocates the backing array to accommodate storing more ints.
     */
    private void grow() {
        Marker[] newData = new Marker[data.length * 2];
        System.arraycopy(data, 0, newData, 0, data.length);
        for (int i = data.length; i < newData.length; i++) {
            newData[i] = new Marker(-1, -1);
        }
        data = newData;
    }
}
