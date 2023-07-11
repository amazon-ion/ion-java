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
import static com.amazon.ion.IonCursor.Event.START_CONTAINER;
import static com.amazon.ion.IonCursor.Event.START_SCALAR;
import static com.amazon.ion.IonCursor.Event.VALUE_READY;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class IonReaderContinuableCoreBinaryTest {
    private static final IonBufferConfiguration STANDARD_BUFFER_CONFIGURATION = IonBufferConfiguration.Builder.standard().build();

    @Parameterized.Parameters(name = "constructWithBytes={0}")
    public static Object[] parameters() {
        return new Object[]{true, false};
    }

    @Parameterized.Parameter
    public boolean constructFromBytes;

    IonReaderContinuableCoreBinary reader = null;
    int numberOfIvmsEncountered = 0;

    private final IvmNotificationConsumer countingIvmConsumer = (majorVersion, minorVersion) -> numberOfIvmsEncountered++;

    @Before
    public void setup() {
        reader = null;
        numberOfIvmsEncountered = 0;
    }

    private void initializeReader(int... data) {
        if (constructFromBytes) {
            reader = new IonReaderContinuableCoreBinary(STANDARD_BUFFER_CONFIGURATION, bytes(data), 0, data.length);
        } else {
            reader = new IonReaderContinuableCoreBinary(
                STANDARD_BUFFER_CONFIGURATION,
                new ByteArrayInputStream(bytes(data)),
                null,
                0,
                0
            );
        }
        reader.registerIvmNotificationConsumer(countingIvmConsumer);
        reader.registerOversizedValueHandler(
            STANDARD_BUFFER_CONFIGURATION.getOversizedValueHandler()
        );
    }

    private void nextExpect(IonCursor.Event expected) {
        assertEquals(expected, reader.nextValue());
    }

    private void fillExpect(IonCursor.Event expected) {
        assertEquals(expected, reader.fillValue());
    }

    private void stepIn() {
        assertEquals(NEEDS_INSTRUCTION, reader.stepIntoContainer());
    }

    private void stepOut() {
        assertEquals(NEEDS_INSTRUCTION, reader.stepOutOfContainer());
    }

    private void expectField(int sid) {
        assertEquals(sid, reader.getFieldId());
    }

    private void expectInt(int value) {
        assertEquals(value, reader.intValue());
    }

    private void expectString(String value) {
        assertEquals(value, reader.stringValue());
    }

    @Test
    public void basicContainer() {
        initializeReader(
            0xE0, 0x01, 0x00, 0xEA,
            0xD3, // Struct length 3
            0x84, // Field SID 4
            0x21, 0x01 // Int 1
        );
        nextExpect(START_CONTAINER);
        stepIn();
        nextExpect(START_SCALAR);
        expectField(4);
        fillExpect(VALUE_READY);
        expectInt(1);
        nextExpect(END_CONTAINER);
        stepOut();
        nextExpect(NEEDS_DATA);
    }

    @Test
    public void basicStrings() {
        initializeReader(
            0xE0, 0x01, 0x00, 0xEA,
            0x83, 'f', 'o', 'o', // String length 3
            0x83, 'b', 'a', 'r' // String length 3
        );
        nextExpect(START_SCALAR);
        fillExpect(VALUE_READY);
        expectString("foo");
        nextExpect(START_SCALAR);
        fillExpect(VALUE_READY);
        expectString("bar");
        nextExpect(NEEDS_DATA);
    }

    @Test
    public void basicNoFill() {
        initializeReader(
            0xE0, 0x01, 0x00, 0xEA,
            0xD3, // Struct length 3
            0x84, // Field SID 4
            0x21, 0x01 // Int 1
        );
        nextExpect(START_CONTAINER);
        stepIn();
        nextExpect(START_SCALAR);
        expectField(4);
        nextExpect(END_CONTAINER);
        stepOut();
        nextExpect(NEEDS_DATA);
    }

    @Test
    public void basicStepOutEarly() {
        initializeReader(
            0xE0, 0x01, 0x00, 0xEA,
            0xD3, // Struct length 3
            0x84, // Field SID 4
            0x21, 0x01 // Int 1
        );
        nextExpect(START_CONTAINER);
        stepIn();
        nextExpect(START_SCALAR);
        stepOut();
        expectField(-1);
        nextExpect(NEEDS_DATA);
    }

    @Test
    public void basicTopLevelSkip() {
        initializeReader(
            0xE0, 0x01, 0x00, 0xEA,
            0xD3, // Struct length 3
            0x84, // Field SID 4
            0x21, 0x01 // Int 1
        );
        nextExpect(START_CONTAINER);
        nextExpect(NEEDS_DATA);
    }

    @Test
    public void basicTopLevelSkipThenConsume() {
        initializeReader(
            0xE0, 0x01, 0x00, 0xEA,
            0xD3, // Struct length 3
            0x84, // Field SID 4
            0x21, 0x01, // Int 1
            0x21, 0x03 // Int 3
        );
        nextExpect(START_CONTAINER);
        nextExpect(START_SCALAR);
        fillExpect(VALUE_READY);
        expectInt(3);
        nextExpect(NEEDS_DATA);
    }

    @Test
    public void nestedContainers() {
        initializeReader(
            0xE0, 0x01, 0x00, 0xEA,
            0xD6, // Struct length 6
            0x83, // Field SID 3
            0xB1, // List length 1
            0x40, // Float length 0
            0x84, // Field SID 4
            0x21, 0x01 // Int 1
        );
        nextExpect(START_CONTAINER);
        stepIn();
        nextExpect(START_CONTAINER);
        stepIn();
        nextExpect(START_SCALAR);
        stepOut();
        nextExpect(START_SCALAR);
        fillExpect(VALUE_READY);
        expectInt(1);
        stepOut();
        nextExpect(NEEDS_DATA);
    }

    @Test
    public void fillContainerAtDepth0() {
        initializeReader(
            0xE0, 0x01, 0x00, 0xEA,
            0xD6, // Struct length 6
            0x83, // Field SID 3
            0xB1, // List length 1
            0x40, // Float 0
            0x84, // Field SID 4
            0x21, 0x01 // Int 1
        );
        nextExpect(START_CONTAINER);
        fillExpect(VALUE_READY);
        stepIn();
        nextExpect(START_CONTAINER);
        stepIn();
        nextExpect(START_SCALAR);
        stepOut();
        nextExpect(START_SCALAR);
        fillExpect(VALUE_READY);
        expectInt(1);
        stepOut();
        nextExpect(NEEDS_DATA);
    }

    @Test
    public void fillContainerAtDepth1() {
        initializeReader(
            0xE0, 0x01, 0x00, 0xEA,
            0xD6, // Struct length 6
            0x83, // Field SID 3
            0xB1, // List length 1
            0x40, // Float 0
            0x84, // Field SID 4
            0x21, 0x01 // Int 1
        );
        nextExpect(START_CONTAINER);
        stepIn();
        nextExpect(START_CONTAINER);
        fillExpect(VALUE_READY);
        stepIn();
        nextExpect(START_SCALAR);
        nextExpect(END_CONTAINER);
        stepOut();
    }

    @Test
    public void fillContainerThenSkip() {
        initializeReader(
            0xE0, 0x01, 0x00, 0xEA,
            0xD6, // Struct length 6
            0x83, // Field SID 3
            0xB1, // List length 1
            0x40, // Float 0
            0x84, // Field SID 4
            0x21, 0x01, // Int 1
            0xD2, // Struct length 2
            0x84, // Field SID 4
            0x20 // Int 0
        );
        nextExpect(START_CONTAINER);
        fillExpect(VALUE_READY);
        nextExpect(START_CONTAINER);
        stepIn();
        nextExpect(START_SCALAR);
        fillExpect(VALUE_READY);
        expectInt(0);
        nextExpect(END_CONTAINER);
        stepOut();
        nextExpect(NEEDS_DATA);
    }
}
