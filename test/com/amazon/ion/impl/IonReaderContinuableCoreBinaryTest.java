// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ion.impl;

import com.amazon.ion.IonBufferConfiguration;
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

    @Test
    public void basicContainer() {
        initializeReader(
            0xE0, 0x01, 0x00, 0xEA,
            0xD3, // Struct length 3
            0x84, // Field SID 4
            0x21, 0x01 // Int 1
        );
        assertEquals(START_CONTAINER, reader.nextValue());
        assertEquals(NEEDS_INSTRUCTION, reader.stepIntoContainer());
        assertEquals(START_SCALAR, reader.nextValue());
        assertEquals(4, reader.getFieldId());
        assertEquals(VALUE_READY, reader.fillValue());
        assertEquals(1, reader.intValue());
        assertEquals(END_CONTAINER, reader.nextValue());
        assertEquals(NEEDS_INSTRUCTION, reader.stepOutOfContainer());
        assertEquals(NEEDS_DATA, reader.nextValue());
    }

    @Test
    public void basicStrings() {
        initializeReader(
            0xE0, 0x01, 0x00, 0xEA,
            0x83, 'f', 'o', 'o', // String length 3
            0x83, 'b', 'a', 'r' // String length 3
        );
        assertEquals(START_SCALAR, reader.nextValue());
        assertEquals(VALUE_READY, reader.fillValue());
        assertEquals("foo", reader.stringValue());
        assertEquals(START_SCALAR, reader.nextValue());
        assertEquals(VALUE_READY, reader.fillValue());
        assertEquals("bar", reader.stringValue());
        assertEquals(NEEDS_DATA, reader.nextValue());
    }

    @Test
    public void basicNoFill() {
        initializeReader(
            0xE0, 0x01, 0x00, 0xEA,
            0xD3, // Struct length 3
            0x84, // Field SID 4
            0x21, 0x01 // Int 1
        );
        assertEquals(START_CONTAINER, reader.nextValue());
        assertEquals(NEEDS_INSTRUCTION, reader.stepIntoContainer());
        assertEquals(START_SCALAR, reader.nextValue());
        assertEquals(4, reader.getFieldId());
        assertEquals(END_CONTAINER, reader.nextValue());
        assertEquals(NEEDS_INSTRUCTION, reader.stepOutOfContainer());
        assertEquals(NEEDS_DATA, reader.nextValue());
    }

    @Test
    public void basicStepOutEarly() {
        initializeReader(
            0xE0, 0x01, 0x00, 0xEA,
            0xD3, // Struct length 3
            0x84, // Field SID 4
            0x21, 0x01 // Int 1
        );
        assertEquals(START_CONTAINER, reader.nextValue());
        assertEquals(NEEDS_INSTRUCTION, reader.stepIntoContainer());
        assertEquals(START_SCALAR, reader.nextValue());
        assertEquals(NEEDS_INSTRUCTION, reader.stepOutOfContainer());
        assertEquals(-1, reader.getFieldId());
        assertEquals(NEEDS_DATA, reader.nextValue());
    }

    @Test
    public void basicTopLevelSkip() {
        initializeReader(
            0xE0, 0x01, 0x00, 0xEA,
            0xD3, // Struct length 3
            0x84, // Field SID 4
            0x21, 0x01 // Int 1
        );
        assertEquals(START_CONTAINER, reader.nextValue());
        assertEquals(NEEDS_DATA, reader.nextValue());
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
        assertEquals(START_CONTAINER, reader.nextValue());
        assertEquals(START_SCALAR, reader.nextValue());
        assertEquals(VALUE_READY, reader.fillValue());
        assertEquals(3, reader.intValue());
        assertEquals(NEEDS_DATA, reader.nextValue());
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
        assertEquals(START_CONTAINER, reader.nextValue());
        assertEquals(NEEDS_INSTRUCTION, reader.stepIntoContainer());
        assertEquals(START_CONTAINER, reader.nextValue());
        assertEquals(NEEDS_INSTRUCTION, reader.stepIntoContainer());
        assertEquals(START_SCALAR, reader.nextValue());
        assertEquals(NEEDS_INSTRUCTION, reader.stepOutOfContainer());
        assertEquals(START_SCALAR, reader.nextValue());
        assertEquals(VALUE_READY, reader.fillValue());
        assertEquals(1, reader.intValue());
        assertEquals(NEEDS_INSTRUCTION, reader.stepOutOfContainer());
        assertEquals(NEEDS_DATA, reader.nextValue());
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
        assertEquals(START_CONTAINER, reader.nextValue());
        assertEquals(VALUE_READY, reader.fillValue());
        assertEquals(NEEDS_INSTRUCTION, reader.stepIntoContainer());
        assertEquals(START_CONTAINER, reader.nextValue());
        assertEquals(NEEDS_INSTRUCTION, reader.stepIntoContainer());
        assertEquals(START_SCALAR, reader.nextValue());
        assertEquals(NEEDS_INSTRUCTION, reader.stepOutOfContainer());
        assertEquals(START_SCALAR, reader.nextValue());
        assertEquals(VALUE_READY, reader.fillValue());
        assertEquals(1, reader.intValue());
        assertEquals(NEEDS_INSTRUCTION, reader.stepOutOfContainer());
        assertEquals(NEEDS_DATA, reader.nextValue());
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
        assertEquals(START_CONTAINER, reader.nextValue());
        assertEquals(NEEDS_INSTRUCTION, reader.stepIntoContainer());
        assertEquals(START_CONTAINER, reader.nextValue());
        assertEquals(VALUE_READY, reader.fillValue());
        assertEquals(NEEDS_INSTRUCTION, reader.stepIntoContainer());
        assertEquals(START_SCALAR, reader.nextValue());
        assertEquals(END_CONTAINER, reader.nextValue());
        assertEquals(NEEDS_INSTRUCTION, reader.stepOutOfContainer());
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
        assertEquals(START_CONTAINER, reader.nextValue());
        assertEquals(VALUE_READY, reader.fillValue());
        assertEquals(START_CONTAINER, reader.nextValue());
        assertEquals(NEEDS_INSTRUCTION, reader.stepIntoContainer());
        assertEquals(START_SCALAR, reader.nextValue());
        assertEquals(VALUE_READY, reader.fillValue());
        assertEquals(0, reader.intValue());
        assertEquals(END_CONTAINER, reader.nextValue());
        assertEquals(NEEDS_INSTRUCTION, reader.stepOutOfContainer());
        assertEquals(NEEDS_DATA, reader.nextValue());
    }
}
