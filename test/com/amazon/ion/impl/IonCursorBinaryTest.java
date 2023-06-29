// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ion.impl;

import com.amazon.ion.IonBufferConfiguration;
import com.amazon.ion.IonCursor;
import com.amazon.ion.IvmNotificationConsumer;
import org.junit.Before;
import org.junit.Test;

import static com.amazon.ion.BitUtils.bytes;
import static com.amazon.ion.IonCursor.Event.END_CONTAINER;
import static com.amazon.ion.IonCursor.Event.NEEDS_DATA;
import static com.amazon.ion.IonCursor.Event.NEEDS_INSTRUCTION;
import static com.amazon.ion.IonCursor.Event.VALUE_READY;
import static com.amazon.ion.IonCursor.Event.START_CONTAINER;
import static com.amazon.ion.IonCursor.Event.START_SCALAR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class IonCursorBinaryTest {

    private static final IonBufferConfiguration STANDARD_BUFFER_CONFIGURATION = IonBufferConfiguration.Builder.standard().build();

    private static Marker loadScalar(IonCursorBinary cursor) {
        IonCursor.Event event = cursor.fillValue();
        assertEquals(VALUE_READY, event);
        Marker marker = cursor.getValueMarker();
        assertNotNull(marker);
        return marker;
    }

    private IonCursorBinary cursor = null;
    private int numberOfIvmsEncountered = 0;

    private final IvmNotificationConsumer countingIvmConsumer = (majorVersion, minorVersion) -> numberOfIvmsEncountered++;

    @Before
    public void setup() {
        cursor = null;
        numberOfIvmsEncountered = 0;
    }

    private void initializeCursor(int... data) {
        cursor = new IonCursorBinary(STANDARD_BUFFER_CONFIGURATION, bytes(data), 0, data.length);
        cursor.registerIvmNotificationConsumer(countingIvmConsumer);
    }

    @Test
    public void basicContainer() {
        initializeCursor(
            0xE0, 0x01, 0x00, 0xEA,
            0xD3, // Struct length 3
            0x84, // Field SID 4
            0x21, 0x01 // Int length 1, starting at byte index 7
        );
        assertEquals(START_CONTAINER, cursor.nextValue());
        assertEquals(NEEDS_INSTRUCTION, cursor.stepIntoContainer());
        assertEquals(START_SCALAR, cursor.nextValue());
        Marker marker = loadScalar(cursor);
        assertEquals(7, marker.startIndex);
        assertEquals(8, marker.endIndex);
        assertEquals(END_CONTAINER, cursor.nextValue());
        assertEquals(NEEDS_INSTRUCTION, cursor.stepOutOfContainer());
        assertEquals(NEEDS_DATA, cursor.nextValue());
    }

    @Test
    public void basicStrings() {
        initializeCursor(
            0xE0, 0x01, 0x00, 0xEA,
            0x83, 'f', 'o', 'o', // String length 3, starting at byte index 5
            0x83, 'b', 'a', 'r' // String length 3, starting at byte index 9
        );
        assertEquals(START_SCALAR, cursor.nextValue());
        Marker marker = loadScalar(cursor);
        assertEquals(5, marker.startIndex);
        assertEquals(8, marker.endIndex);
        assertEquals(START_SCALAR, cursor.nextValue());
        marker = loadScalar(cursor);
        assertEquals(9, marker.startIndex);
        assertEquals(12, marker.endIndex);
        assertEquals(NEEDS_DATA, cursor.nextValue());
    }

    @Test
    public void basicNoFill() {
        initializeCursor(
            0xE0, 0x01, 0x00, 0xEA,
            0xD3, // Struct length 3
            0x84, // Field SID 4
            0x21, 0x01 // Int length 1, starting at byte index 7
        );
        assertEquals(START_CONTAINER, cursor.nextValue());
        assertEquals(NEEDS_INSTRUCTION, cursor.stepIntoContainer());
        assertEquals(START_SCALAR, cursor.nextValue());
        assertEquals(END_CONTAINER, cursor.nextValue());
        assertEquals(NEEDS_INSTRUCTION, cursor.stepOutOfContainer());
        assertEquals(NEEDS_DATA, cursor.nextValue());
    }

    @Test
    public void basicStepOutEarly() {
        initializeCursor(
            0xE0, 0x01, 0x00, 0xEA,
            0xD3, // Struct length 3
            0x84, // Field SID 4
            0x21, 0x01 // Int length 1, starting at byte index 7
        );
        assertEquals(START_CONTAINER, cursor.nextValue());
        assertEquals(NEEDS_INSTRUCTION, cursor.stepIntoContainer());
        assertEquals(START_SCALAR, cursor.nextValue());
        assertEquals(NEEDS_INSTRUCTION, cursor.stepOutOfContainer());
        assertEquals(NEEDS_DATA, cursor.nextValue());
    }

    @Test
    public void basicTopLevelSkip() {
        initializeCursor(
            0xE0, 0x01, 0x00, 0xEA,
            0xD3, // Struct length 3
            0x84, // Field SID 4
            0x21, 0x01 // Int length 1, starting at byte index 7
        );
        assertEquals(START_CONTAINER, cursor.nextValue());
        assertEquals(NEEDS_DATA, cursor.nextValue());
    }

    @Test
    public void basicTopLevelSkipThenConsume() {
        initializeCursor(
            0xE0, 0x01, 0x00, 0xEA,
            0xD3, // Struct length 3
            0x84, // Field SID 4
            0x21, 0x01, // Int length 1, starting at byte index 7
            0x21, 0x03 // Int length 1, starting at byte index 9
        );
        assertEquals(START_CONTAINER, cursor.nextValue());
        assertEquals(START_SCALAR, cursor.nextValue());
        Marker marker = loadScalar(cursor);
        assertEquals(9, marker.startIndex);
        assertEquals(10, marker.endIndex);
        assertEquals(NEEDS_DATA, cursor.nextValue());
    }

    @Test
    public void nestedContainers() {
        initializeCursor(
            0xE0, 0x01, 0x00, 0xEA,
            0xD6, // Struct length 6
            0x83, // Field SID 3
            0xB1, // List length 1
            0x40, // Float length 0
            0x84, // Field SID 4
            0x21, 0x01 // Int length 1, starting at byte index 10
        );
        assertEquals(START_CONTAINER, cursor.nextValue());
        assertEquals(NEEDS_INSTRUCTION, cursor.stepIntoContainer());
        assertEquals(START_CONTAINER, cursor.nextValue());
        assertEquals(NEEDS_INSTRUCTION, cursor.stepIntoContainer());
        assertEquals(START_SCALAR, cursor.nextValue());
        assertEquals(NEEDS_INSTRUCTION, cursor.stepOutOfContainer());
        assertEquals(START_SCALAR, cursor.nextValue());
        Marker marker = loadScalar(cursor);
        assertEquals(10, marker.startIndex);
        assertEquals(11, marker.endIndex);
        assertEquals(NEEDS_INSTRUCTION, cursor.stepOutOfContainer());
        assertEquals(NEEDS_DATA, cursor.nextValue());
    }

    @Test
    public void fillContainerAtDepth0() {
        initializeCursor(
            0xE0, 0x01, 0x00, 0xEA,
            0xD6, // Struct length 6, contents start at index 5
            0x83, // Field SID 3
            0xB1, // List length 1
            0x40, // Float length 0
            0x84, // Field SID 4
            0x21, 0x01 // Int length 1, starting at byte index 10
        );
        assertEquals(START_CONTAINER, cursor.nextValue());
        assertEquals(VALUE_READY, cursor.fillValue());
        Marker marker = cursor.getValueMarker();
        assertEquals(5, marker.startIndex);
        assertEquals(11, marker.endIndex);
        assertEquals(NEEDS_INSTRUCTION, cursor.stepIntoContainer());
        assertEquals(START_CONTAINER, cursor.nextValue());
        assertEquals(NEEDS_INSTRUCTION, cursor.stepIntoContainer());
        assertEquals(START_SCALAR, cursor.nextValue());
        assertEquals(NEEDS_INSTRUCTION, cursor.stepOutOfContainer());
        assertEquals(START_SCALAR, cursor.nextValue());
        marker = loadScalar(cursor);
        assertEquals(10, marker.startIndex);
        assertEquals(11, marker.endIndex);
        assertEquals(NEEDS_INSTRUCTION, cursor.stepOutOfContainer());
        assertEquals(NEEDS_DATA, cursor.nextValue());
    }

    @Test
    public void fillContainerAtDepth1() {
        initializeCursor(
            0xE0, 0x01, 0x00, 0xEA,
            0xD6, // Struct length 6
            0x83, // Field SID 3
            0xB1, // List length 1, contents start at index 7
            0x40, // Float length 0
            0x84, // Field SID 4
            0x21, 0x01 // Int length 1, starting at byte index 10
        );
        assertEquals(START_CONTAINER, cursor.nextValue());
        assertEquals(NEEDS_INSTRUCTION, cursor.stepIntoContainer());
        assertEquals(START_CONTAINER, cursor.nextValue());
        assertEquals(VALUE_READY, cursor.fillValue());
        Marker marker = cursor.getValueMarker();
        assertEquals(7, marker.startIndex);
        assertEquals(8, marker.endIndex);
        assertEquals(NEEDS_INSTRUCTION, cursor.stepIntoContainer());
        assertEquals(START_SCALAR, cursor.nextValue());
        assertEquals(END_CONTAINER, cursor.nextValue());
        assertEquals(NEEDS_INSTRUCTION, cursor.stepOutOfContainer());
    }

    @Test
    public void fillContainerThenSkip() {
        initializeCursor(
            0xE0, 0x01, 0x00, 0xEA,
            0xD6, // Struct length 6
            0x83, // Field SID 3
            0xB1, // List length 1, contents start at index 7
            0x40, // Float length 0
            0x84, // Field SID 4
            0x21, 0x01, // Int length 1, starting at byte index 10
            0xD2, // Struct length 2
            0x84, // Field SID 4
            0x20 // Int length 0, at byte index 14
        );
        assertEquals(START_CONTAINER, cursor.nextValue());
        assertEquals(VALUE_READY, cursor.fillValue());
        assertEquals(START_CONTAINER, cursor.nextValue());
        assertEquals(NEEDS_INSTRUCTION, cursor.stepIntoContainer());
        assertEquals(START_SCALAR, cursor.nextValue());
        assertEquals(VALUE_READY, cursor.fillValue());
        Marker marker = cursor.getValueMarker();
        assertEquals(14, marker.startIndex);
        assertEquals(14, marker.endIndex);
        assertEquals(END_CONTAINER, cursor.nextValue());
        assertEquals(NEEDS_INSTRUCTION, cursor.stepOutOfContainer());
        assertEquals(NEEDS_DATA, cursor.nextValue());
    }
}
