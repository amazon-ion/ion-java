// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ion.impl;

import com.amazon.ion.IonBufferConfiguration;
import com.amazon.ion.IonCursor;
import com.amazon.ion.IvmNotificationConsumer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayInputStream;

import static com.amazon.ion.BitUtils.bytes;
import static com.amazon.ion.IonCursor.Event.END_CONTAINER;
import static com.amazon.ion.IonCursor.Event.NEEDS_DATA;
import static com.amazon.ion.IonCursor.Event.NEEDS_INSTRUCTION;
import static com.amazon.ion.IonCursor.Event.VALUE_READY;
import static com.amazon.ion.IonCursor.Event.START_CONTAINER;
import static com.amazon.ion.IonCursor.Event.START_SCALAR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(Parameterized.class)
public class IonCursorBinaryTest {

    private static final IonBufferConfiguration STANDARD_BUFFER_CONFIGURATION = IonBufferConfiguration.Builder.standard().build();

    private static Marker loadScalar(IonCursorBinary cursor) {
        IonCursor.Event event = cursor.fillValue();
        assertEquals(VALUE_READY, event);
        Marker marker = cursor.getValueMarker();
        assertNotNull(marker);
        return marker;
    }

    @Parameterized.Parameters(name = "constructWithBytes={0}")
    public static Object[] parameters() {
        return new Object[]{true, false};
    }

    @Parameterized.Parameter
    public boolean constructFromBytes;

    private IonCursorBinary cursor = null;
    private int numberOfIvmsEncountered = 0;

    private final IvmNotificationConsumer countingIvmConsumer = (majorVersion, minorVersion) -> numberOfIvmsEncountered++;

    @Before
    public void setup() {
        cursor = null;
        numberOfIvmsEncountered = 0;
    }

    private void initializeCursor(int... data) {
        if (constructFromBytes) {
            cursor = new IonCursorBinary(STANDARD_BUFFER_CONFIGURATION, bytes(data), 0, data.length);
        } else {
            cursor = new IonCursorBinary(
                STANDARD_BUFFER_CONFIGURATION,
                new ByteArrayInputStream(bytes(data)),
                null,
                0,
                0
            );
        }
        cursor.registerIvmNotificationConsumer(countingIvmConsumer);
        cursor.registerOversizedValueHandler(STANDARD_BUFFER_CONFIGURATION.getOversizedValueHandler());
    }

    private void nextExpect(IonCursor.Event expected) {
        assertEquals(expected, cursor.nextValue());
    }

    private void fillExpect(IonCursor.Event expected) {
        assertEquals(expected, cursor.fillValue());
    }

    private void stepIn() {
        assertEquals(NEEDS_INSTRUCTION, cursor.stepIntoContainer());
    }

    private void stepOut() {
        assertEquals(NEEDS_INSTRUCTION, cursor.stepOutOfContainer());
    }

    private void expectMarker(int expectedStart, int expectedEnd) {
        Marker marker = loadScalar(cursor);
        assertEquals(expectedStart, marker.startIndex);
        assertEquals(expectedEnd, marker.endIndex);
    }

    @Test
    public void basicContainer() {
        initializeCursor(
            0xE0, 0x01, 0x00, 0xEA,
            0xD3, // Struct length 3
            0x84, // Field SID 4
            0x21, 0x01 // Int length 1, starting at byte index 7
        );
        nextExpect(START_CONTAINER);
        stepIn();
        nextExpect(START_SCALAR);
        expectMarker(7, 8);
        nextExpect(END_CONTAINER);
        stepOut();
        nextExpect(NEEDS_DATA);
    }

    @Test
    public void basicStrings() {
        initializeCursor(
            0xE0, 0x01, 0x00, 0xEA,
            0x83, 'f', 'o', 'o', // String length 3, starting at byte index 5
            0x83, 'b', 'a', 'r' // String length 3, starting at byte index 9
        );
        nextExpect(START_SCALAR);
        expectMarker(5, 8);
        nextExpect(START_SCALAR);
        expectMarker(9, 12);
        nextExpect(NEEDS_DATA);
    }

    @Test
    public void basicNoFill() {
        initializeCursor(
            0xE0, 0x01, 0x00, 0xEA,
            0xD3, // Struct length 3
            0x84, // Field SID 4
            0x21, 0x01 // Int length 1, starting at byte index 7
        );
        nextExpect(START_CONTAINER);
        stepIn();
        nextExpect(START_SCALAR);
        nextExpect(END_CONTAINER);
        stepOut();
        nextExpect(NEEDS_DATA);
    }

    @Test
    public void basicStepOutEarly() {
        initializeCursor(
            0xE0, 0x01, 0x00, 0xEA,
            0xD3, // Struct length 3
            0x84, // Field SID 4
            0x21, 0x01 // Int length 1, starting at byte index 7
        );
        nextExpect(START_CONTAINER);
        stepIn();
        nextExpect(START_SCALAR);
        stepOut();
        nextExpect(NEEDS_DATA);
    }

    @Test
    public void basicTopLevelSkip() {
        initializeCursor(
            0xE0, 0x01, 0x00, 0xEA,
            0xD3, // Struct length 3
            0x84, // Field SID 4
            0x21, 0x01 // Int length 1, starting at byte index 7
        );
        nextExpect(START_CONTAINER);
        nextExpect(NEEDS_DATA);
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
        nextExpect(START_CONTAINER);
        nextExpect(START_SCALAR);
        expectMarker(9, 10);
        nextExpect(NEEDS_DATA);
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
        nextExpect(START_CONTAINER);
        stepIn();
        nextExpect(START_CONTAINER);
        stepIn();
        nextExpect(START_SCALAR);
        stepOut();
        nextExpect(START_SCALAR);
        expectMarker(10, 11);
        stepOut();
        nextExpect(NEEDS_DATA);
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
        nextExpect(START_CONTAINER);
        fillExpect(VALUE_READY);
        expectMarker(5, 11);
        stepIn();
        nextExpect(START_CONTAINER);
        stepIn();
        nextExpect(START_SCALAR);
        stepOut();
        nextExpect(START_SCALAR);
        expectMarker(10, 11);
        stepOut();
        nextExpect(NEEDS_DATA);
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
        nextExpect(START_CONTAINER);
        stepIn();
        nextExpect(START_CONTAINER);
        fillExpect(VALUE_READY);
        expectMarker(7, 8);
        stepIn();
        nextExpect(START_SCALAR);
        nextExpect(END_CONTAINER);
        stepOut();
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
        nextExpect(START_CONTAINER);
        fillExpect(VALUE_READY);
        nextExpect(START_CONTAINER);
        stepIn();
        nextExpect(START_SCALAR);
        fillExpect(VALUE_READY);
        expectMarker(14, 14);
        nextExpect(END_CONTAINER);
        stepOut();
        nextExpect(NEEDS_DATA);
    }
}
