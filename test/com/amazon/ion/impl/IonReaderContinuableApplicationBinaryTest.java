// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ion.impl;

import com.amazon.ion.IonBufferConfiguration;
import com.amazon.ion.IonCursor;
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

    private void expectField(String name) {
        assertEquals(name, reader.getFieldName());
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
            0x84, // Field SID 4 ("name")
            0x21, 0x01 // Int 1
        );
        nextExpect(START_CONTAINER);
        stepIn();
        nextExpect(START_SCALAR);
        expectField("name");
        fillExpect(VALUE_READY);
        expectInt(1);
        nextExpect(END_CONTAINER);
        stepOut();
        nextExpect(NEEDS_DATA);
    }

    @Test
    public void basicSystemSymbols() {
        initializeReader(
            0xE0, 0x01, 0x00, 0xEA,
            0x71, 0x04, // Symbol value SID 4 ("name")
            0x71, 0x05 // Symbol value SID 5 ("version")
        );
        nextExpect(START_SCALAR);
        fillExpect(VALUE_READY);
        expectString("name");
        nextExpect(START_SCALAR);
        fillExpect(VALUE_READY);
        expectString("version");
        nextExpect(NEEDS_DATA);
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
        nextExpect(START_SCALAR);
        fillExpect(VALUE_READY);
        expectString("A");
        nextExpect(START_SCALAR);
        fillExpect(VALUE_READY);
        expectString("B");
        nextExpect(NEEDS_DATA);
    }

    @Test
    public void basicNoFill() {
        initializeReader(
            0xE0, 0x01, 0x00, 0xEA,
            0xD3, // Struct length 3
            0x84, // Field SID 4 ("name")
            0x21, 0x01 // Int 1
        );
        nextExpect(START_CONTAINER);
        stepIn();
        nextExpect(START_SCALAR);
        expectField("name");
        nextExpect(END_CONTAINER);
        stepOut();
        nextExpect(NEEDS_DATA);
    }

    @Test
    public void basicStepOutEarly() {
        initializeReader(
            0xE0, 0x01, 0x00, 0xEA,
            0xD3, // Struct length 3
            0x84, // Field SID 4 ("name")
            0x21, 0x01 // Int 1
        );
        nextExpect(START_CONTAINER);
        stepIn();
        nextExpect(START_SCALAR);
        stepOut();
        assertNull(reader.getFieldName());
        nextExpect(NEEDS_DATA);
    }


}
