// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ion.impl;

import com.amazon.ion.IonBufferConfiguration;
import com.amazon.ion.IvmNotificationConsumer;
import com.amazon.ion.system.IonReaderBuilder;
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
import static org.junit.Assert.assertNull;

@RunWith(Parameterized.class)
public class IonReaderContinuableApplicationBinaryTest {

    private static final IonReaderBuilder STANDARD_READER_BUILDER = IonReaderBuilder.standard()
        .withBufferConfiguration(IonBufferConfiguration.Builder.standard().build());

    @Parameterized.Parameters(name = "constructWithBytes={0}")
    public static Object[] parameters() {
        return new Object[]{true, false};
    }

    @Parameterized.Parameter
    public boolean constructFromBytes;

    IonReaderContinuableApplicationBinary reader = null;
    int numberOfIvmsEncountered = 0;

    private final IvmNotificationConsumer countingIvmConsumer = (majorVersion, minorVersion) -> numberOfIvmsEncountered++;

    @Before
    public void setup() {
        reader = null;
        numberOfIvmsEncountered = 0;
    }

    private void initializeReader(int... data) {
        if (constructFromBytes) {
            reader = new IonReaderContinuableApplicationBinary(STANDARD_READER_BUILDER, bytes(data), 0, data.length);
        } else {
            reader = new IonReaderContinuableApplicationBinary(
                STANDARD_READER_BUILDER,
                new ByteArrayInputStream(bytes(data)),
                null,
                0,
                0
            );
        }
        reader.registerIvmNotificationConsumer(countingIvmConsumer);
        reader.registerOversizedValueHandler(
            STANDARD_READER_BUILDER.getBufferConfiguration().getOversizedValueHandler()
        );
    }

    @Test
    public void basicContainer() {
        initializeReader(
            0xE0, 0x01, 0x00, 0xEA,
            0xD3, // Struct length 3
            0x84, // Field SID 4 ("name")
            0x21, 0x01 // Int 1
        );
        assertEquals(START_CONTAINER, reader.nextValue());
        assertEquals(NEEDS_INSTRUCTION, reader.stepIntoContainer());
        assertEquals(START_SCALAR, reader.nextValue());
        assertEquals("name", reader.getFieldName());
        assertEquals(VALUE_READY, reader.fillValue());
        assertEquals(1, reader.intValue());
        assertEquals(END_CONTAINER, reader.nextValue());
        assertEquals(NEEDS_INSTRUCTION, reader.stepOutOfContainer());
        assertEquals(NEEDS_DATA, reader.nextValue());
    }

    @Test
    public void basicSystemSymbols() {
        initializeReader(
            0xE0, 0x01, 0x00, 0xEA,
            0x71, 0x04, // Symbol value SID 4 ("name")
            0x71, 0x05 // Symbol value SID 5 ("version")
        );
        assertEquals(START_SCALAR, reader.nextValue());
        assertEquals(VALUE_READY, reader.fillValue());
        assertEquals("name", reader.stringValue());
        assertEquals(START_SCALAR, reader.nextValue());
        assertEquals(VALUE_READY, reader.fillValue());
        assertEquals("version", reader.stringValue());
        assertEquals(NEEDS_DATA, reader.nextValue());
    }

    @Test
    public void basicLocalSymbols() {
        initializeReader(
            0xE0, 0x01, 0x00, 0xEA,
            0xE9, // Annotation wrapper length 9
            0x81, // 1 byte of annotations
            0x83, // Annotation SID 3 ($ion_symbol_table)
            0xD6, // Struct length 6 (local symbol table)
            0x87, // Field SID 7 ("symbols")
            0xB4, // List length 4
            0x81, 'A', // String "A"
            0x81, 'B', // String "B"
            0x71, 0x0A, // Symbol value SID 10 ("A")
            0x71, 0x0B // Symbol value SID 11 ("B")
        );
        assertEquals(START_SCALAR, reader.nextValue());
        assertEquals(VALUE_READY, reader.fillValue());
        assertEquals("A", reader.stringValue());
        assertEquals(START_SCALAR, reader.nextValue());
        assertEquals(VALUE_READY, reader.fillValue());
        assertEquals("B", reader.stringValue());
        assertEquals(NEEDS_DATA, reader.nextValue());
    }

    @Test
    public void basicNoFill() {
        initializeReader(
            0xE0, 0x01, 0x00, 0xEA,
            0xD3, // Struct length 3
            0x84, // Field SID 4 ("name")
            0x21, 0x01 // Int 1
        );
        assertEquals(START_CONTAINER, reader.nextValue());
        assertEquals(NEEDS_INSTRUCTION, reader.stepIntoContainer());
        assertEquals(START_SCALAR, reader.nextValue());
        assertEquals("name", reader.getFieldName());
        assertEquals(END_CONTAINER, reader.nextValue());
        assertEquals(NEEDS_INSTRUCTION, reader.stepOutOfContainer());
        assertEquals(NEEDS_DATA, reader.nextValue());
    }

    @Test
    public void basicStepOutEarly() {
        initializeReader(
            0xE0, 0x01, 0x00, 0xEA,
            0xD3, // Struct length 3
            0x84, // Field SID 4 ("name")
            0x21, 0x01 // Int 1
        );
        assertEquals(START_CONTAINER, reader.nextValue());
        assertEquals(NEEDS_INSTRUCTION, reader.stepIntoContainer());
        assertEquals(START_SCALAR, reader.nextValue());
        assertEquals(NEEDS_INSTRUCTION, reader.stepOutOfContainer());
        assertNull(reader.getFieldName());
        assertEquals(NEEDS_DATA, reader.nextValue());
    }


}
