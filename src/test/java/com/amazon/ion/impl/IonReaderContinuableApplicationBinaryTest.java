// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl;

import com.amazon.ion.system.IonReaderBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;

import static com.amazon.ion.BitUtils.bytes;
import static com.amazon.ion.impl.IonCursorTestUtilities.STANDARD_BUFFER_CONFIGURATION;
import static com.amazon.ion.impl.IonCursorTestUtilities.Expectation;
import static com.amazon.ion.impl.IonCursorTestUtilities.ExpectationProvider;
import static com.amazon.ion.impl.IonCursorTestUtilities.assertSequence;
import static com.amazon.ion.impl.IonCursorTestUtilities.container;
import static com.amazon.ion.impl.IonCursorTestUtilities.endContainer;
import static com.amazon.ion.impl.IonCursorTestUtilities.endStream;
import static com.amazon.ion.impl.IonCursorTestUtilities.fillIntValue;
import static com.amazon.ion.impl.IonCursorTestUtilities.scalar;
import static com.amazon.ion.impl.IonCursorTestUtilities.fillSymbolValue;

public class IonReaderContinuableApplicationBinaryTest {

    private static final IonReaderBuilder STANDARD_READER_BUILDER = IonReaderBuilder.standard()
        .withBufferConfiguration(STANDARD_BUFFER_CONFIGURATION);

    private IonReaderContinuableApplicationBinary initializeReader(boolean constructFromBytes, int... data) {
        IonReaderContinuableApplicationBinary reader;
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
        reader.registerOversizedValueHandler(
            STANDARD_READER_BUILDER.getBufferConfiguration().getOversizedValueHandler()
        );
        return reader;
    }

    /**
     * Provides an Expectation that verifies that the value on which the cursor is currently positioned has the given
     * field name.
     */
    private static Expectation<IonReaderContinuableApplicationBinary> fieldName(String expectedFieldName) {
        return new Expectation<>(
            String.format("fieldName(%s)", expectedFieldName),
            reader -> Assertions.assertEquals(expectedFieldName, reader.getFieldName())
        );
    }

    /**
     * Provides Expectations that verify that advancing the cursor positions it on a scalar value with the given field
     * name, without filling the scalar.
     */
    private static ExpectationProvider<IonReaderContinuableApplicationBinary> scalarFieldName(String expectedFieldName) {
        return IonCursorTestUtilities.scalar(fieldName(expectedFieldName));
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void basicContainer(boolean constructFromBytes) {
        IonReaderContinuableApplicationBinary reader = initializeReader(
            constructFromBytes,
            0xE0, 0x01, 0x00, 0xEA,
            0xD3, // Struct length 3
            0x84, // Field SID 4 ("name")
            0x21, 0x01 // Int 1
        );
        assertSequence(
            reader,
            container(
                scalarFieldName("name"), fillIntValue(1),
                endContainer()
            ),
            endStream()
        );
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void basicSystemSymbols(boolean constructFromBytes) {
        IonReaderContinuableApplicationBinary reader = initializeReader(
            constructFromBytes,
            0xE0, 0x01, 0x00, 0xEA,
            0x71, 0x04, // Symbol value SID 4 ("name")
            0x71, 0x05 // Symbol value SID 5 ("version")
        );
        assertSequence(
            reader,
            scalar(), fillSymbolValue("name"),
            scalar(), fillSymbolValue("version")
        );
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void basicSystemSymbols_1_1(boolean constructFromBytes) {
        IonReaderContinuableApplicationBinary reader = initializeReader(
            constructFromBytes,
            0xE0, 0x01, 0x01, 0xEA,
            0xEE, 0x04, // Symbol value SID 4 ("name")
            0xEE, 0x05 // Symbol value SID 5 ("version")
        );
        assertSequence(
            reader,
            scalar(), fillSymbolValue("name"),
            scalar(), fillSymbolValue("version")
        );
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void basicLocalSymbols(boolean constructFromBytes) {
        IonReaderContinuableApplicationBinary reader = initializeReader(
            constructFromBytes,
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
        assertSequence(
            reader,
            scalar(), fillSymbolValue("A"),
            scalar(), fillSymbolValue("B")
        );
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void basicInlineSymbols(boolean constructFromBytes) {
        IonReaderContinuableApplicationBinary reader = initializeReader(
            constructFromBytes,
            0xE0, 0x01, 0x01, 0xEA,
            0xA0, // Empty inline symbol
            0xA3, 0x61, 0x62, 0x63 // Inline symbol 'abc'
        );
        assertSequence(
            reader,
            scalar(), fillSymbolValue(""),
            scalar(), fillSymbolValue("abc")
        );
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void basicNoFill(boolean constructFromBytes) {
        IonReaderContinuableApplicationBinary reader = initializeReader(
            constructFromBytes,
            0xE0, 0x01, 0x00, 0xEA,
            0xD3, // Struct length 3
            0x84, // Field SID 4 ("name")
            0x21, 0x01 // Int 1
        );
        assertSequence(
            reader,
            container(
                scalarFieldName("name"), // Do not fill or consume the associated value
                endContainer()
            ),
            endStream()
        );
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void basicStepOutEarly(boolean constructFromBytes) {
        IonReaderContinuableApplicationBinary reader = initializeReader(
            constructFromBytes,
            0xE0, 0x01, 0x00, 0xEA,
            0xD3, // Struct length 3
            0x84, // Field SID 4 ("name")
            0x21, 0x01 // Int 1
        );
        assertSequence(
            reader,
            container(
                scalar()
            ),
            endStream()
        );
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void systemSymbolsEncodedUsingUserIdsAndInlineText_1_1(boolean constructFromBytes) {
        IonReaderContinuableApplicationBinary reader = initializeReader(
            constructFromBytes,
            0xE0, 0x01, 0x01, 0xEA,
            0xE7, 0xF9, '$', 'i', 'o', 'n', // $ion::
            0xCD, // s-expression, length 13
            0xEE, 0x0F, // 'module' (encoded as system symbol ID 15)
            0xA1, '_', // Inline symbol '_'
            0xC8, // s-expression, length 8
            0xEE, 0x07, // 'symbols' (encoded as system symbol ID 7)
            0xB5, 0x94, '$', 'i', 'o', 'n', // ["$ion"]
            0xE4, 0x03, // $1::, where $1 is a local SID that points to the text "$ion"
            0xCA, // s-expression, length 10
            0xEE, 0x0F, // 'module' (encoded as system symbol ID 15)
            0xA1, '_', // Inline symbol '_'
            0xC5, // s-expression, length 5
            0xEE, 0x07, // 'symbols' (encoded as system symbol ID 7)
            0xB2, 0x91, 'a', // ["a"]
            0xE1, 0x01 // $1, which now points to "a"
        );
        assertSequence(
            reader,
            scalar(), fillSymbolValue("a"),
            endStream()
        );
    }
}
