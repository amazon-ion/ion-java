// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl;

import com.amazon.ion.IonBufferConfiguration;
import com.amazon.ion.IonCursor;
import com.amazon.ion.IonException;
import com.amazon.ion.IonType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.amazon.ion.BitUtils.bytes;
import static com.amazon.ion.IonCursor.Event.NEEDS_DATA;
import static com.amazon.ion.IonCursor.Event.NEEDS_INSTRUCTION;
import static com.amazon.ion.IonCursor.Event.VALUE_READY;
import static com.amazon.ion.IonCursor.Event.START_CONTAINER;
import static com.amazon.ion.IonCursor.Event.START_SCALAR;
import static com.amazon.ion.TestUtils.withIvm;
import static com.amazon.ion.impl.IonCursorTestUtilities.STANDARD_BUFFER_CONFIGURATION;
import static com.amazon.ion.impl.IonCursorTestUtilities.Expectation;
import static com.amazon.ion.impl.IonCursorTestUtilities.ExpectationProvider;
import static com.amazon.ion.impl.IonCursorTestUtilities.STEP_IN;
import static com.amazon.ion.impl.IonCursorTestUtilities.STEP_OUT;
import static com.amazon.ion.impl.IonCursorTestUtilities.assertSequence;
import static com.amazon.ion.impl.IonCursorTestUtilities.container;
import static com.amazon.ion.impl.IonCursorTestUtilities.endContainer;
import static com.amazon.ion.impl.IonCursorTestUtilities.endStream;
import static com.amazon.ion.impl.IonCursorTestUtilities.scalar;
import static com.amazon.ion.impl.IonCursorTestUtilities.startContainer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IonCursorBinaryTest {

    private static IonCursorBinary initializeCursor(IonBufferConfiguration configuration, boolean constructFromBytes, byte[] data) {
        IonCursorBinary cursor;
        if (constructFromBytes) {
            cursor = new IonCursorBinary(configuration, data, 0, data.length);
        } else {
            cursor = new IonCursorBinary(
                configuration,
                new ByteArrayInputStream(data),
                null,
                0,
                0
            );
        }
        cursor.registerOversizedValueHandler(configuration.getOversizedValueHandler());
        return cursor;
    }

    private static IonCursorBinary initializeCursor(boolean constructFromBytes, int... data) {
        return initializeCursor(STANDARD_BUFFER_CONFIGURATION, constructFromBytes, bytes(data));
    }

    public enum InputType {

        /**
         * The cursor will be constructed from a fixed byte array.
         */
        FIXED_BYTES {
            @Override
            IonCursorBinary initializeCursor(byte[] data) {
                return IonCursorBinaryTest.initializeCursor(STANDARD_BUFFER_CONFIGURATION, true, data);
            }
        },

        /**
         * The cursor will be constructed from an InputStream with all bytes available up front.
         */
        FIXED_STREAM {
            @Override
            IonCursorBinary initializeCursor(byte[] data) {
                return IonCursorBinaryTest.initializeCursor(STANDARD_BUFFER_CONFIGURATION, false, data);
            }
        },

        /**
         * The cursor will be constructed from an InputStream that is fed bytes one by one, expecting NEEDS_DATA
         * after each byte except the final one.
         */
        INCREMENTAL {
            @Override
            IonCursorBinary initializeCursor(byte[] data) {
                ResizingPipedInputStream pipe = new ResizingPipedInputStream(data.length);
                IonCursorBinary cursor = new IonCursorBinary(STANDARD_BUFFER_CONFIGURATION, pipe, null, 0, 0);
                for (byte b : data) {
                    assertEquals(NEEDS_DATA, cursor.nextValue());
                    pipe.receive(b);
                }
                return cursor;
            }
        };

        abstract IonCursorBinary initializeCursor(byte[] data);
    }

    /**
     * Provides Expectations that verify that advancing the cursor to the next value results in the given event, and
     * filling that value results in a Marker with the given start and end indices.
     */
    private static ExpectationProvider<IonCursorBinary> fill(IonCursor.Event expectedEvent, int expectedStart, int expectedEnd) {
        return consumer -> consumer.accept(new Expectation<>(
            String.format("fill(%s, %d, %d)", expectedEvent, expectedStart, expectedEnd),
            cursor -> {
                assertEquals(expectedEvent, cursor.nextValue());
                assertEquals(VALUE_READY, cursor.fillValue());
                Marker marker = cursor.getValueMarker();
                assertEquals(expectedStart, marker.startIndex);
                assertEquals(expectedEnd, marker.endIndex);
            }
        ));
    }

    /**
     * Provides Expectations that verify that advancing the cursor to the next value results in the given event, and
     * attempting to fill that value results in NEEDS_INSTRUCTION, indicating that the value could not be filled due
     * to being oversize.
     */
    private static ExpectationProvider<IonCursorBinary> fillIsOversize(IonCursor.Event expectedEvent, Supplier<Integer> oversizeCounter) {
        return consumer -> consumer.accept(new Expectation<>(
            String.format("fillOversized(%s)", expectedEvent),
            cursor -> {
                assertEquals(expectedEvent, cursor.nextValue());
                assertEquals(NEEDS_INSTRUCTION, cursor.fillValue());
                assertEquals(1, oversizeCounter.get());
            }
        ));
    }

    /**
     * Provides an Expectation that verifies that the value on which the cursor is currently positioned has the given
     * type.
     */
    static <T extends IonCursorBinary> ExpectationProvider<T> type(IonType expectedType) {
        return consumer -> consumer.accept(new Expectation<>(
            String.format("type(%s)", expectedType),
            cursor -> assertEquals(expectedType, cursor.getValueMarker().typeId.type))
        );
    }

    /**
     * Provides Expectations that verify that advancing the cursor positions it on a scalar, and filling that scalar
     * results in a Marker with the given start and end indices.
     */
    private static ExpectationProvider<IonCursorBinary> fillScalar(int expectedStart, int expectedEnd) {
        return fill(START_SCALAR, expectedStart, expectedEnd);
    }

    /**
     * Provides Expectations that verify 1) that advancing the cursor positions it on a container, 2) filling that
     * container results in a Marker with the given start and end indices, and 3) the container's child values match
     * the given expectations.
     */
    @SafeVarargs
    @SuppressWarnings("unchecked")
    private static ExpectationProvider<IonCursorBinary> fillContainer(int expectedStart, int expectedEnd, ExpectationProvider<IonCursorBinary>... expectations) {
        return consumer -> {
            fill(START_CONTAINER, expectedStart, expectedEnd).accept(consumer);
            consumer.accept((Expectation<IonCursorBinary>) STEP_IN);
            for (Consumer<Consumer<Expectation<IonCursorBinary>>> expectation : expectations) {
                expectation.accept(consumer);
            }
            consumer.accept((Expectation<IonCursorBinary>) STEP_OUT);
        };
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void basicContainer(boolean constructFromBytes) {
        IonCursorBinary cursor = initializeCursor(
            constructFromBytes,
            0xE0, 0x01, 0x00, 0xEA,
            0xD3, // Struct length 3
            0x84, // Field SID 4
            0x21, 0x01 // Int length 1, starting at byte index 7
        );
        assertSequence(
            cursor,
            container(
                fillScalar(7, 8)
            ),
            endStream()
        );
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void basicStrings(boolean constructFromBytes) {
        IonCursorBinary cursor = initializeCursor(
            constructFromBytes,
            0xE0, 0x01, 0x00, 0xEA,
            0x83, 'f', 'o', 'o', // String length 3, starting at byte index 5
            0x83, 'b', 'a', 'r' // String length 3, starting at byte index 9
        );
        assertSequence(
            cursor,
            fillScalar(5, 8),
            fillScalar(9, 12),
            endStream()
        );
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void basicNoFill(boolean constructFromBytes) {
        IonCursorBinary cursor = initializeCursor(
            constructFromBytes,
            0xE0, 0x01, 0x00, 0xEA,
            0xD3, // Struct length 3
            0x84, // Field SID 4
            0x21, 0x01 // Int length 1, starting at byte index 7
        );
        assertSequence(
            cursor,
            container(
                scalar(),
                endContainer()
            ),
            endStream()
        );
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void basicStepOutEarly(boolean constructFromBytes) {
        IonCursorBinary cursor = initializeCursor(
            constructFromBytes,
            0xE0, 0x01, 0x00, 0xEA,
            0xD3, // Struct length 3
            0x84, // Field SID 4
            0x21, 0x01 // Int length 1, starting at byte index 7
        );
        assertSequence(
            cursor,
            container(
                scalar()
            ),
            endStream()
        );
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void basicTopLevelSkip(boolean constructFromBytes) {
        IonCursorBinary cursor = initializeCursor(
            constructFromBytes,
            0xE0, 0x01, 0x00, 0xEA,
            0xD3, // Struct length 3
            0x84, // Field SID 4
            0x21, 0x01 // Int length 1, starting at byte index 7
        );
        assertSequence(
            cursor,
            startContainer(),
            endStream()
        );
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void basicTopLevelSkipThenConsume(boolean constructFromBytes) {
        IonCursorBinary cursor = initializeCursor(
            constructFromBytes,
            0xE0, 0x01, 0x00, 0xEA,
            0xD3, // Struct length 3
            0x84, // Field SID 4
            0x21, 0x01, // Int length 1, starting at byte index 7
            0x21, 0x03 // Int length 1, starting at byte index 9
        );
        assertSequence(
            cursor,
            startContainer(),
            fillScalar(9, 10),
            endStream()
        );
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void nestedContainers(boolean constructFromBytes) {
        IonCursorBinary cursor = initializeCursor(
            constructFromBytes,
            0xE0, 0x01, 0x00, 0xEA,
            0xD6, // Struct length 6
            0x83, // Field SID 3
            0xB1, // List length 1
            0x40, // Float length 0
            0x84, // Field SID 4
            0x21, 0x01 // Int length 1, starting at byte index 10
        );
        assertSequence(
            cursor,
            container(
                container(
                    scalar()
                ),
                fillScalar(10, 11)
            ),
            endStream()
        );
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void fillContainerAtDepth0(boolean constructFromBytes) {
        IonCursorBinary cursor = initializeCursor(
            constructFromBytes,
            0xE0, 0x01, 0x00, 0xEA,
            0xD6, // Struct length 6, contents start at index 5
            0x83, // Field SID 3
            0xB1, // List length 1
            0x40, // Float length 0
            0x84, // Field SID 4
            0x21, 0x01 // Int length 1, starting at byte index 10
        );
        assertSequence(
            cursor,
            fillContainer(5, 11,
                container(
                    scalar()
                ),
                fillScalar(10, 11)
            ),
            endStream()
        );
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void fillDelimitedContainerAtDepth0(boolean constructFromBytes) {
        IonCursorBinary cursor = initializeCursor(
            constructFromBytes,
            0xE0, 0x01, 0x01, 0xEA,
            0xF3, // Delimited struct
            0x07, // Field SID 3
            0xF1, // Delimited list, contents start at index 7
            0x6A, // Float length 0
            0xF0, // End delimited list
            0x09, // Field SID 4
            0x61, 0x01, // Int length 1, starting at byte index 11
            0x01, 0xF0 // End delimited struct
        );
        assertSequence(
            cursor,
            // When reading from a fixed-size input source, the cursor does not need peek ahead to find the end of
            // the delimited container during fill, so it remains -1 in that case. Otherwise, fill looks ahead to
            // find the end index and stores in the index so that it does not need to be repetitively calculated.
            fillContainer(5, constructFromBytes ? -1 : 14,
                container(
                    scalar()
                ),
                fillScalar(11, 12)
            ),
            endStream()
        );
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void fillContainerAtDepth1(boolean constructFromBytes) {
        IonCursorBinary cursor = initializeCursor(
            constructFromBytes,
            0xE0, 0x01, 0x00, 0xEA,
            0xD6, // Struct length 6
            0x83, // Field SID 3
            0xB1, // List length 1, contents start at index 7
            0x40, // Float length 0
            0x84, // Field SID 4
            0x21, 0x01 // Int length 1, starting at byte index 10
        );
        assertSequence(
            cursor,
            container(
                fillContainer(7, 8,
                    scalar(),
                    endContainer()
                )
            )
        );
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void fillDelimitedContainerAtDepth1(boolean constructFromBytes) {
        IonCursorBinary cursor = initializeCursor(
            constructFromBytes,
            0xE0, 0x01, 0x01, 0xEA,
            0xF3, // Delimited struct
            0x07, // Field SID 3
            0xF1, // Delimited list, contents start at index 7
            0x6A, // Float length 0
            0xF0, // End delimited list
            0x09, // Field SID 4
            0x61, 0x01, // Int length 1, starting at byte index 11
            0x01, 0xF0 // End delimited struct
        );
        assertSequence(
            cursor,
            container(
                // When reading from a fixed-size input source, the cursor does not need peek ahead to find the end of
                // the delimited container during fill, so it remains -1 in that case. Otherwise, fill looks ahead to
                // find the end index and stores in the index so that it does not need to be repetitively calculated.
                fillContainer(7, constructFromBytes ? -1 : 9,
                    scalar(),
                    endContainer()
                )
            )
        );
    }

    @Test
    public void skipOversizeDelimitedContainerAtDepth1() {
        AtomicInteger oversizeValueCounter = new AtomicInteger(0);
        AtomicInteger oversizeSymbolTableCounter = new AtomicInteger(0);
        AtomicInteger byteCounter = new AtomicInteger(0);
        byte[] data = bytes(
            0xE0, 0x01, 0x01, 0xEA,
            0xF3, // Delimited struct
            0x07, // Field SID 3
            0xF1, // Delimited list, contents start at index 7
            0x6A, 0x6A, 0x6A, 0x6A, 0x6A, 0x6A, // Six floats 0e0
            0xF0, // End delimited list
            0x09, // Field SID 4
            0x61, 0x01, // Int length 1, starting at byte index 16
            0x01, 0xF0 // End delimited struct
        );
        IonCursorBinary cursor = initializeCursor(
            IonBufferConfiguration.Builder.standard()
                .withInitialBufferSize(5)
                .withMaximumBufferSize(5)
                .onData(byteCounter::addAndGet)
                .onOversizedValue(oversizeValueCounter::incrementAndGet)
                .onOversizedSymbolTable(oversizeSymbolTableCounter::incrementAndGet)
                .build(),
            false,
            data
        );
        assertSequence(
            cursor,
            container(
                // The oversize delimited list is skipped.
                fillIsOversize(START_CONTAINER, oversizeValueCounter::get),
                scalar(), type(IonType.INT),
                endContainer()
            ),
            endStream()
        );
        cursor.close();
        assertEquals(1, oversizeValueCounter.get());
        assertEquals(0, oversizeSymbolTableCounter.get());
        assertEquals(data.length, byteCounter.get());
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void fillContainerThenSkip(boolean constructFromBytes) {
        IonCursorBinary cursor = initializeCursor(
            constructFromBytes,
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
        assertSequence(
            cursor,
            fill(START_CONTAINER, 5, 11),
            container(
                fillScalar(14, 14)
            ),
            endStream()
        );
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void fillDelimitedContainerThenSkip(boolean constructFromBytes) {
        IonCursorBinary cursor = initializeCursor(
            constructFromBytes,
            0xE0, 0x01, 0x01, 0xEA,
            0xF3, // Delimited struct
            0x07, // Field SID 3
            0xF1, // Delimited list, contents start at index 7
            0x6A, // Float length 0
            0xF0, // End delimited list
            0x09, // Field SID 4
            0x61, 0x01, // Int length 1, starting at byte index 11
            0x01, 0xF0, // End delimited struct
            0xF3, // Delimited struct
            0x09, // Field SID 4
            0x60, // Int length 0, at byte index 17
            0x01, 0xF0 // End delimited struct
        );
        assertSequence(
            cursor,
            // When reading from a fixed-size input source, the cursor does not need peek ahead to find the end of
            // the delimited container during fill, so it remains -1 in that case. Otherwise, fill looks ahead to
            // find the end index and stores in the index so that it does not need to be repetitively calculated.
            fill(START_CONTAINER, 5, constructFromBytes ? -1 : 14),
            container(
                fillScalar(17, 17)
            ),
            endStream()
        );
    }

    @Test
    public void expectMalformedListHeaderToFailCleanly() {
        // The following test is expected to fail because the VarUInt length would extend beyond the end of the buffer.
        // Since a byte array is provided as the input source, no additional input can be provided, so this must be
        // treated as an error.
        IonCursorBinary cursor = initializeCursor(
            true,
            0xE0, 0x01, 0x00, 0xEA,
            0xBE, // List with VarUInt length
            0x01  // Non-terminal VarUInt byte
        );
        assertThrows(IonException.class, cursor::nextValue);
        cursor.close();
    }

    @Test
    public void expectMissingListLengthToFailCleanly() {
        // The following test is expected to fail because the VarUInt length is missing.
        // Since a byte array is provided as the input source, no additional input can be provided, so this must be
        // treated as an error.
        IonCursorBinary cursor = initializeCursor(
            true,
            0xE0, 0x01, 0x00, 0xEA,
            0xBE // List with VarUInt length, no length follows
        );
        assertThrows(IonException.class, cursor::nextValue);
        cursor.close();
    }

    @Test
    public void expectMalformedStructFieldNameToFailCleanly() {
        IonCursorBinary cursor = initializeCursor(
            true,
            0xE0, 0x01, 0x00, 0xEA,
            0xD2, // Struct length 2
            0x01, // Non-terminal VarUInt byte
            0x01, // Non-terminal VarUInt byte
            0x01  // Non-terminal VarUInt byte
        );
        cursor.nextValue();
        cursor.stepIntoContainer();
        assertThrows(IonException.class, cursor::nextValue);
        cursor.close();
    }

    @Test
    public void expectMissingAnnotationWrapperLengthToFailCleanly() {
        IonCursorBinary cursor = initializeCursor(
            true,
            0xE0, 0x01, 0x00, 0xEA,
            0xEE // Annotation wrapper with VarUInt length, no length follows
        );
        assertThrows(IonException.class, cursor::nextValue);
        cursor.close();
    }

    @Test
    public void expectMalformedAnnotationWrapperHeaderToFailCleanly() {
        IonCursorBinary cursor = initializeCursor(
            true,
            0xE0, 0x01, 0x00, 0xEA,
            0xEE, // Annotation wrapper with VarUInt length
            0x01  // Non-terminal VarUInt byte
        );
        assertThrows(IonException.class, cursor::nextValue);
        cursor.close();
    }

    @Test
    public void expectMalformedAnnotationWrapperAnnotationLengthToFailCleanly() {
        IonCursorBinary cursor = initializeCursor(
            true,
            0xE0, 0x01, 0x00, 0xEA,
            0xEE, // Annotation wrapper with VarUInt length
            0x83, // VarUInt length 3
            0x01, // Non-terminal VarUInt byte
            0x01, // Non-terminal VarUInt byte
            0x01  // Non-terminal VarUInt byte
        );
        assertThrows(IonException.class, cursor::nextValue);
        cursor.close();
    }

    @Test
    public void expectMissingAnnotationWrapperAnnotationLengthToFailCleanly() {
        IonCursorBinary cursor = initializeCursor(
            true,
            0xE0, 0x01, 0x00, 0xEA,
            0xEE, // Annotation wrapper with VarUInt length
            0x83  // VarUInt length 3, no annotation length follows
        );
        assertThrows(IonException.class, cursor::nextValue);
        cursor.close();
    }

    @Test
    public void expectValueLargerThanMaxArraySizeToFailCleanly() {
        int[] data = new int[]{
                0xE0, 0x01, 0x00, 0xEA,      // Ion 1.0 IVM
                0x2E,                        // Int with VarUInt length, 6 bytes total
                0x07, 0x7f, 0x7f, 0x7f, 0xf9 // VarUInt length (Integer.MAX_LENGTH - 6)
        };                                   // Because that's clearly a reasonable thing to find
        ByteArrayInputStream in = new ByteArrayInputStream(bytes(data));

        // We need a custom initial buffer size so that the cursor doesn't know there are fewer bytes remaining
        // than the value header promises
        IonBufferConfiguration config = IonBufferConfiguration.Builder.standard()
                .withInitialBufferSize(8)
                .build();

        IonCursorBinary cursor = new IonCursorBinary(config, in, null, 0, 0);

        cursor.nextValue(); // Position cursor on unreal oversized value

        // Try to get all the value bytes, and fail because arrays can't be that large
        IonException ie = assertThrows(IonException.class, cursor::fillValue);
        assertThat(ie.getMessage(),
                containsString("An oversized value was found even though no maximum size was configured."));

        cursor.close();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void expectUseAfterCloseToHaveNoEffect(boolean constructFromBytes) {
        // Using the cursor after close should not fail with an obscure unchecked
        // exception like NullPointerException, ArrayIndexOutOfBoundsException, etc.
        IonCursorBinary cursor = initializeCursor(
            constructFromBytes,
            0xE0, 0x01, 0x00, 0xEA,
            0x20
        );
        cursor.close();
        assertEquals(IonCursor.Event.NEEDS_DATA, cursor.nextValue());
        assertNull(cursor.getValueMarker().typeId);
        assertEquals(IonCursor.Event.NEEDS_DATA, cursor.nextValue());
        assertNull(cursor.getValueMarker().typeId);
        cursor.close();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void expectIncompleteIvmToFailCleanly(boolean constructFromBytes) {
        IonCursorBinary cursor = initializeCursor(
            constructFromBytes,
            0xE0, 0x01, 0x00, 0xEA, // Complete IVM
            0x20, // Int 0
            0xE0, 0x01 // Incomplete IVM
        );
        assertEquals(START_SCALAR, cursor.nextValue());
        if (constructFromBytes) {
            // This is a fixed stream, so no more bytes will become available. An error must be raised when the
            // incomplete IVM is encountered.
            assertThrows(IonException.class, cursor::nextValue);
        } else {
            // This is a growing stream, so the cursor waits for more bytes.
            assertEquals(NEEDS_DATA, cursor.nextValue());
        }
        cursor.close();
    }

    /**
     * Asserts that the given data contains macro invocation that matches the given attributes.
     * @param input the data (without IVM) to test.
     * @param inputType the type of input to provide to the cursor.
     * @param expectedStartIndex the expected start index of the invocation's body.
     * @param expectedEndIndex the expected end index of the invocation's body, or -1 if the end index cannot be
     *                         computed from the encoding alone.
     * @param expectedId the ID of the macro being invoked.
     * @param isSystemInvocation whether the invocation is of a system macro.
     */
    private static void testMacroInvocation(
        byte[] input,
        InputType inputType,
        int expectedStartIndex,
        int expectedEndIndex,
        int expectedId,
        boolean isSystemInvocation
    ) throws Exception {
        try (IonCursorBinary cursor = inputType.initializeCursor(withIvm(1, input))) {
            assertEquals(NEEDS_INSTRUCTION, cursor.nextValue());
            Marker invocationMarker = cursor.getValueMarker();
            assertTrue(invocationMarker.typeId.isMacroInvocation);
            assertEquals(expectedStartIndex, invocationMarker.startIndex);
            assertEquals(expectedEndIndex, invocationMarker.endIndex);
            assertEquals(expectedId, cursor.getMacroInvocationId());
            assertEquals(isSystemInvocation, cursor.isSystemInvocation());
        }
    }

    @ParameterizedTest(name = "inputType={0}")
    @EnumSource(InputType.class)
    public void macroInvocationWithIdInOpcode(InputType inputType) throws Exception {
        // Opcode 0x13 -> macro ID 0x13
        testMacroInvocation(bytes(0x13), inputType, 5, -1, 0x13, false);
    }

    @ParameterizedTest(name = "inputType={0}")
    @EnumSource(InputType.class)
    public void macroInvocationWithOneByteFixedUIntId(InputType inputType) throws Exception {
        // Opcode 0x43; 1-byte FixedUInt 0x09 follows
        testMacroInvocation(bytes(0x43, 0x09), inputType, 6, -1, 841, false);
    }

    @ParameterizedTest(name = "inputType={0}")
    @EnumSource(InputType.class)
    public void macroInvocationWithTwoByteFixedUIntId(InputType inputType) throws Exception {
        // Opcode 0x52; 2-byte FixedUInt 0x06, 0x1E follows
        testMacroInvocation(bytes(0x52, 0x06, 0x1E), inputType, 7, -1, 142918, false);
    }

    @ParameterizedTest(name = "inputType={0}")
    @EnumSource(InputType.class)
    public void macroInvocationWithFlexUIntId(InputType inputType) throws Exception {
        // Opcode 0xEE; 3-byte FlexUInt 0xFC, 0xFF, 0xFF follows
        testMacroInvocation(bytes(0xEE, 0xFC, 0xFF, 0xFF), inputType, 8, -1, 2097151, false);
    }

    @ParameterizedTest(name = "inputType={0}")
    @EnumSource(InputType.class)
    public void macroInvocationLengthPrefixed(InputType inputType) throws Exception {
        // Opcode 0xF5; FlexUInt length 1 followed by FlexUInt ID 2
        testMacroInvocation(bytes(0xF5, 0x03, 0x05), inputType, 7, 7, 2, false);
    }

    @ParameterizedTest(name = "inputType={0}")
    @EnumSource(InputType.class)
    public void systemMacroInvocation(InputType inputType) throws Exception {
        // Opcode 0xEF; 1-byte FixedInt follows. Positive 4 indicates system macro ID 4.
        testMacroInvocation(bytes(0xEF, 0x04), inputType, 6, -1, 4, true);
    }

    @ParameterizedTest(name = "inputType={0}")
    @EnumSource(InputType.class)
    public void systemSymbolValue(InputType inputType) throws Exception {
        // Opcode 0xEF; 1-byte FixedInt follows. 0xFE (-2) indicates system symbol ID 2.
        byte[] data = withIvm(1, bytes(0xEF, 0xFE));
        try (IonCursorBinary cursor = inputType.initializeCursor(data)) {
            assertEquals(START_SCALAR, cursor.nextValue());
            assertTrue(cursor.isSystemInvocation());
            Marker invocationMarker = cursor.getValueMarker();
            assertFalse(invocationMarker.typeId.isMacroInvocation);
            assertEquals(6, invocationMarker.startIndex);
            assertEquals(6, invocationMarker.endIndex);
            // Note: a higher-level reader will use the sign to direct the lookup to the system symbol table instead of
            // the system macro table.
            assertEquals(-2, cursor.getMacroInvocationId());
        }
    }
}
