// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ion.impl;

import com.amazon.ion.IonType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;

import static com.amazon.ion.BitUtils.bytes;
import static com.amazon.ion.impl.IonCursorTestUtilities.STANDARD_BUFFER_CONFIGURATION;
import static com.amazon.ion.impl.IonCursorTestUtilities.Expectation;
import static com.amazon.ion.impl.IonCursorTestUtilities.ExpectationProvider;
import static com.amazon.ion.impl.IonCursorTestUtilities.assertSequence;
import static com.amazon.ion.impl.IonCursorTestUtilities.container;
import static com.amazon.ion.impl.IonCursorTestUtilities.container;
import static com.amazon.ion.impl.IonCursorTestUtilities.endContainer;
import static com.amazon.ion.impl.IonCursorTestUtilities.endStream;
import static com.amazon.ion.impl.IonCursorTestUtilities.fillContainer;
import static com.amazon.ion.impl.IonCursorTestUtilities.fillIntValue;
import static com.amazon.ion.impl.IonCursorTestUtilities.scalar;
import static com.amazon.ion.impl.IonCursorTestUtilities.scalar;
import static com.amazon.ion.impl.IonCursorTestUtilities.startContainer;
import static com.amazon.ion.impl.IonCursorTestUtilities.fillStringValue;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class IonReaderContinuableCoreBinaryTest {

    private IonReaderContinuableCoreBinary initializeReader(boolean constructFromBytes, int... data) {
        IonReaderContinuableCoreBinary reader;
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
        reader.registerOversizedValueHandler(
            STANDARD_BUFFER_CONFIGURATION.getOversizedValueHandler()
        );
        return reader;
    }

    /**
     * Provides an Expectation that verifies that the value on which the cursor is currently positioned has the given
     * field name SID.
     */
    private static Expectation<IonReaderContinuableCoreBinary> fieldSid(int expectedFieldSid) {
        return new Expectation<>(
            String.format("fieldSid(%d)", expectedFieldSid),
            reader -> assertEquals(expectedFieldSid, reader.getFieldId())
        );
    }

    /**
     * Provides Expectations that verify that advancing the cursor positions it on a scalar value with the given field
     * SID, without filling the scalar.
     */
    private static ExpectationProvider<IonReaderContinuableCoreBinary> scalarFieldSid(int expectedFieldSid) {
        return IonCursorTestUtilities.scalar(fieldSid(expectedFieldSid));
    }


    /**
     * Provides Expectations that verify that advancing the cursor positions it on a container value with the given
     * field sid, and that the container's child values match the given expectations, without filling the container
     * up-front.
     */
    @SafeVarargs
    private static ExpectationProvider<IonReaderContinuableCoreBinary> containerFieldSid(int expectedFieldSid, ExpectationProvider<IonReaderContinuableCoreBinary>... expectations) {
        return IonCursorTestUtilities.container(fieldSid(expectedFieldSid), expectations);
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void basicContainer(boolean constructFromBytes) {
        IonReaderContinuableCoreBinary reader = initializeReader(
            constructFromBytes,
            0xE0, 0x01, 0x00, 0xEA,
            0xD3, // Struct length 3
            0x84, // Field SID 4
            0x21, 0x01 // Int 1
        );
        assertSequence(
            reader,
            container(
                scalarFieldSid(4), fillIntValue(1),
                endContainer()
            ),
            endStream()
        );
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void basicStrings(boolean constructFromBytes) {
        IonReaderContinuableCoreBinary reader = initializeReader(
            constructFromBytes,
            0xE0, 0x01, 0x00, 0xEA,
            0x83, 'f', 'o', 'o', // String length 3
            0x83, 'b', 'a', 'r' // String length 3
        );
        assertSequence(
            reader,
            scalar(), fillStringValue("foo"),
            scalar(), fillStringValue("bar"),
            endStream()
        );
    }


    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void basicNoFill(boolean constructFromBytes) {
        IonReaderContinuableCoreBinary reader = initializeReader(
            constructFromBytes,
            0xE0, 0x01, 0x00, 0xEA,
            0xD3, // Struct length 3
            0x84, // Field SID 4
            0x21, 0x01 // Int 1
        );
        assertSequence(
            reader,
            container(
                scalarFieldSid(4), // Do not fill or consume the associated value
                endContainer()
            ),
            endStream()
        );
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void basicStepOutEarly(boolean constructFromBytes) {
        IonReaderContinuableCoreBinary reader = initializeReader(
            constructFromBytes,
            0xE0, 0x01, 0x00, 0xEA,
            0xD3, // Struct length 3
            0x84, // Field SID 4
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
    public void basicTopLevelSkip(boolean constructFromBytes) {
        IonReaderContinuableCoreBinary reader = initializeReader(
            constructFromBytes,
            0xE0, 0x01, 0x00, 0xEA,
            0xD3, // Struct length 3
            0x84, // Field SID 4
            0x21, 0x01 // Int 1
        );
        assertSequence(
            reader,
            startContainer(),
            endStream()
        );
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void basicTopLevelSkipThenConsume(boolean constructFromBytes) {
        IonReaderContinuableCoreBinary reader = initializeReader(
            constructFromBytes,
            0xE0, 0x01, 0x00, 0xEA,
            0xD3, // Struct length 3
            0x84, // Field SID 4
            0x21, 0x01, // Int 1
            0x21, 0x03 // Int 3
        );
        assertSequence(
            reader,
            startContainer(),
            scalar(), fillIntValue(3),
            endStream()
        );
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void nestedContainers(boolean constructFromBytes) {
        IonReaderContinuableCoreBinary reader = initializeReader(
            constructFromBytes,
            0xE0, 0x01, 0x00, 0xEA,
            0xD6, // Struct length 6
            0x83, // Field SID 3
            0xB1, // List length 1
            0x40, // Float length 0
            0x84, // Field SID 4
            0x21, 0x01 // Int 1
        );
        assertSequence(
            reader,
            container(
                containerFieldSid(3,
                    scalar()
                ),
                scalar(), fillIntValue(1)
            ),
            endStream()
        );
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void fillContainerAtDepth0(boolean constructFromBytes) {
        IonReaderContinuableCoreBinary reader = initializeReader(
            constructFromBytes,
            0xE0, 0x01, 0x00, 0xEA,
            0xD6, // Struct length 6
            0x83, // Field SID 3
            0xB1, // List length 1
            0x40, // Float 0
            0x84, // Field SID 4
            0x21, 0x01 // Int 1
        );
        assertSequence(
            reader,
            fillContainer(IonType.STRUCT,
                containerFieldSid(3,
                    scalar()
                ),
                scalar(), fillIntValue(1)
            ),
            endStream()
        );
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void fillContainerAtDepth1(boolean constructFromBytes) {
        IonReaderContinuableCoreBinary reader = initializeReader(
            constructFromBytes,
            0xE0, 0x01, 0x00, 0xEA,
            0xD6, // Struct length 6
            0x83, // Field SID 3
            0xB1, // List length 1
            0x40, // Float 0
            0x84, // Field SID 4
            0x21, 0x01 // Int 1
        );
        assertSequence(
            reader,
            container(
                fillContainer(IonType.LIST,
                    scalar(),
                    endContainer()
                )
            )
        );
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void fillContainerThenSkip(boolean constructFromBytes) {
        IonReaderContinuableCoreBinary reader = initializeReader(
            constructFromBytes,
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
        assertSequence(
            reader,
            fillContainer(IonType.STRUCT),
            container(
                scalar(), fillIntValue(0),
                endContainer()
            ),
            endStream()
        );
    }
}
