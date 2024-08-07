// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl;

import com.amazon.ion.IonCursor;
import com.amazon.ion.IonException;
import com.amazon.ion.IonType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;

import static com.amazon.ion.BitUtils.bytes;
import static com.amazon.ion.IonCursor.Event.START_SCALAR;
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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

    @Test
    public void expectMalformedDecimalScaleToFailCleanly() {
        IonReaderContinuableCoreBinary reader = initializeReader(
            true,
            0xE0, 0x01, 0x00, 0xEA,
            0x51, // Decimal length 1
            0x01  // Non-terminal VarInt byte
        );
        reader.nextValue();
        assertThrows(IonException.class, reader::decimalValue);
        assertThrows(IonException.class, reader::bigDecimalValue);
        reader.close();
    }

    @Test
    public void expectMissingDecimalScaleToFailCleanly() {
        IonReaderContinuableCoreBinary reader = initializeReader(
            true,
            0xE0, 0x01, 0x00, 0xEA,
            0x51 // Decimal length 1, missing scale
        );
        reader.nextValue();
        assertThrows(IonException.class, reader::decimalValue);
        assertThrows(IonException.class, reader::bigDecimalValue);
        reader.close();
    }

    @Test
    public void expectMalformedDecimalHeaderToFailCleanly() {
        IonReaderContinuableCoreBinary reader = initializeReader(
            true,
            0xE0, 0x01, 0x00, 0xEA,
            0x53, // Decimal length 3, only 2 bytes follow
            0xC1, // VarInt -1
            0x01  // Non-terminal VarInt byte
        );
        reader.nextValue();
        assertThrows(IonException.class, reader::decimalValue);
        assertThrows(IonException.class, reader::bigDecimalValue);
        reader.close();
    }

    @Test
    public void expectMalformedIntHeaderToFailCleanly() {
        IonReaderContinuableCoreBinary reader = initializeReader(
            true,
            0xE0, 0x01, 0x00, 0xEA,
            0x21 // Int length 1
        );
        reader.nextValue();
        assertThrows(IonException.class, reader::intValue);
        assertThrows(IonException.class, reader::bigIntegerValue);
        assertThrows(IonException.class, reader::getIntegerSize);
        reader.close();
    }

    @Test
    public void expectMalformedTimestampOffsetToFailCleanly() {
        IonReaderContinuableCoreBinary reader = initializeReader(
            true,
            0xE0, 0x01, 0x00, 0xEA,
            0x63, // Timestamp length 3
            0x01, // Non-terminal VarInt byte
            0x01, // Non-terminal VarInt byte
            0x01 // Non-terminal VarInt byte
        );
        reader.nextValue();
        assertThrows(IonException.class, reader::timestampValue);
        reader.close();
    }

    @Test
    public void expectMalformedTimestampYearToFailCleanly() {
        IonReaderContinuableCoreBinary reader = initializeReader(
            true,
            0xE0, 0x01, 0x00, 0xEA,
            0x63, // Timestamp length 3
            0xC0, // VarInt -0 (unknown offset)
            0x01, // Non-terminal VarUInt byte
            0x01 // Non-terminal VarUInt byte
        );
        reader.nextValue();
        assertThrows(IonException.class, reader::timestampValue);
        reader.close();
    }

    @Test
    public void expectMissingTimestampBodyToFailCleanly() {
        IonReaderContinuableCoreBinary reader = initializeReader(
            true,
            0xE0, 0x01, 0x00, 0xEA,
            0x63 // Timestamp length 3, missing body
        );
        reader.nextValue();
        assertThrows(IonException.class, reader::timestampValue);
        reader.close();
    }

    private static void assertReaderThrowsForAllPrimitives(IonReaderContinuableCoreBinary reader) {
        // Note: ideally these would throw IonException instead of IllegalStateException, but there is long-standing
        // precedent in IonJava for throwing IllegalStateException when these methods are used improperly. We maintain
        // that convention for consistency.
        assertThrows(IllegalStateException.class, reader::booleanValue);
        assertThrows(IllegalStateException.class, reader::intValue);
        assertThrows(IllegalStateException.class, reader::longValue);
        assertThrows(IllegalStateException.class, reader::doubleValue);
    }

    @Test
    public void expectAttemptToParseNullNullAsJavaPrimitiveToFailCleanly() {
        IonReaderContinuableCoreBinary reader = initializeReader(
            true,
            0xE0, 0x01, 0x00, 0xEA,
            0x0F // null.null
        );
        reader.nextValue();
        assertEquals(IonType.NULL, reader.getType());
        assertReaderThrowsForAllPrimitives(reader);
        reader.close();
    }

    @Test
    public void expectAttemptToParseNullBoolAsJavaPrimitiveToFailCleanly() {
        IonReaderContinuableCoreBinary reader = initializeReader(
            true,
            0xE0, 0x01, 0x00, 0xEA,
            0x1F // null.bool
        );
        reader.nextValue();
        assertEquals(IonType.BOOL, reader.getType());
        assertReaderThrowsForAllPrimitives(reader);
        reader.close();
    }

    @Test
    public void expectAttemptToParseNullIntAsJavaPrimitiveToFailCleanly() {
        IonReaderContinuableCoreBinary reader = initializeReader(
            true,
            0xE0, 0x01, 0x00, 0xEA,
            0x2F, // null.int
            0x3F // null.int
        );
        reader.nextValue();
        assertEquals(IonType.INT, reader.getType());
        assertReaderThrowsForAllPrimitives(reader);
        assertNull(reader.bigIntegerValue());
        assertNull(reader.decimalValue());
        assertNull(reader.bigDecimalValue());
        reader.nextValue();
        assertEquals(IonType.INT, reader.getType());
        assertReaderThrowsForAllPrimitives(reader);
        assertNull(reader.bigIntegerValue());
        assertNull(reader.decimalValue());
        assertNull(reader.bigDecimalValue());
        reader.close();
    }

    @Test
    public void expectAttemptToParseNullFloatAsJavaPrimitiveToFailCleanly() {
        IonReaderContinuableCoreBinary reader = initializeReader(
            true,
            0xE0, 0x01, 0x00, 0xEA,
            0x4F // null.float
        );
        reader.nextValue();
        assertEquals(IonType.FLOAT, reader.getType());
        assertReaderThrowsForAllPrimitives(reader);
        assertNull(reader.bigIntegerValue());
        reader.close();
    }

    @Test
    public void expectAttemptToParseNullDecimalAsJavaPrimitiveToFailCleanly() {
        IonReaderContinuableCoreBinary reader = initializeReader(
            true,
            0xE0, 0x01, 0x00, 0xEA,
            0x5F  // null.decimal
        );
        reader.nextValue();
        assertEquals(IonType.DECIMAL, reader.getType());
        assertNull(reader.decimalValue());
        assertNull(reader.bigDecimalValue());
        assertReaderThrowsForAllPrimitives(reader);
        reader.close();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void expectLobWithOverflowingEndIndexToFailCleanly(boolean constructFromBytes) {
        // This test exercises the case where a value's length itself does not overflow a java long, but its end index
        // (calculated by adding the reader's current index in the buffer to the value's length) does.
        IonReaderContinuableCoreBinary reader = initializeReader(
            constructFromBytes,
            0xE0, 0x01, 0x00, 0xEA, // IVM
            0xAE, // blob with length VarUInt to follow
            0x7F, 0x7F, 0x7F, 0x7F, 0x7F, 0x7F, 0x7F, 0x7F, 0xFE, // 9-byte VarUInt with value just below Long.MAX_VALUE
            0xAF // The first byte of the blob
        );
        assertThrows(IonException.class, reader::nextValue);
        reader.close();
    }

    @Test
    public void expectIncompleteAnnotationWrapperWithOverflowingEndIndexToFailCleanly() {
        // This test exercises the case where an annotation wrapper's length itself does not overflow a java long,
        // but its end index (calculated by adding the reader's current index in the buffer to the value's length) does.
        // Note: this is only tested for fixed input sources because non-fixed sources will wait for more input
        // to determine if the annotation wrapper is well-formed. The next test tests a similar failure case for
        // both types of inputs.
        IonReaderContinuableCoreBinary reader = initializeReader(
            true,
            0xE0, 0x01, 0x00, 0xEA,
            0xEE,
            0x7F, 0x7F, 0x7F, 0x7F, 0x7F, 0x7F, 0x7F, 0x7F, 0xFE // 9-byte VarUInt with value just below Long.MAX_VALUE
        );
        assertThrows(IonException.class, reader::nextValue);
        reader.close();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void expectCompleteAnnotationWrapperWithOverflowingEndIndexToFailCleanly(boolean constructFromBytes) {
        // This test exercises the case where an annotation wrapper's length itself does not overflow a java long,
        // but its end index (calculated by adding the reader's current index in the buffer to the value's length) does.
        // Unlike the previous test, this test contains the remaining bytes of the annotation wrapper, allowing the
        // cursor to advance to a failure condition even when the input is not fixed.
        IonReaderContinuableCoreBinary reader = initializeReader(
            constructFromBytes,
            0xE0, 0x01, 0x00, 0xEA,
            0xEE,
            0x7F, 0x7F, 0x7F, 0x7F, 0x7F, 0x7F, 0x7F, 0x7F, 0xFE, // 9-byte VarUInt with value just below Long.MAX_VALUE
            0x81, 0x83
        );
        assertThrows(IonException.class, reader::nextValue);
        reader.close();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void expectLobWithOverflowingLengthToFailCleanly(boolean constructFromBytes) {
        // This test exercises the case where a value's length overflows a java long.
        IonReaderContinuableCoreBinary reader = initializeReader(
            constructFromBytes,
            0xE0, 0x01, 0x00, 0xEA, // IVM
            0x9E, // clob with length VarUInt
            0x7F, 0x7F, 0x7F, 0x7F, 0x7F, 0x7F, 0x7F, 0x7F, 0x7F, 0xFF, // 10-byte VarUInt with value that exceeds Long.MAX_VALUE
            0x00 // The first byte of the clob
        );
        assertThrows(IonException.class, reader::nextValue);
        reader.close();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void expectAnnotationWrapperWithOverflowingLengthToFailCleanly(boolean constructFromBytes) {
        // This test exercises the case where an annotation wrapper's length overflows a java long.
        IonReaderContinuableCoreBinary reader = initializeReader(
            constructFromBytes,
            0xE0, 0x01, 0x00, 0xEA,
            0xEE,
            0x7F, 0x7F, 0x7F, 0x7F, 0x7F, 0x7F, 0x7F, 0x7F, 0x7F, 0xFF // 10-byte VarUInt with value that exceeds Long.MAX_VALUE
        );
        assertThrows(IonException.class, reader::nextValue);
        reader.close();
    }

    @Test
    public void expectIncompleteContainerToFailCleanlyAfterFieldSid() {
        IonReaderContinuableCoreBinary reader = initializeReader(
            true,
            0xE0, 0x01, 0x00, 0xEA, // IVM
            0xDC, // Struct, length 12
            0x9A  // Field SID 26
            // The struct ends unexpectedly
        );
        assertEquals(IonCursor.Event.START_CONTAINER, reader.nextValue());
        assertEquals(IonType.STRUCT, reader.getType());
        reader.stepIntoContainer();
        // This is an unexpected EOF, so the reader should fail cleanly.
        assertThrows(IonException.class, reader::nextValue);
        reader.close();
    }

    @Test
    public void expectIncompleteContainerToFailCleanlyAfterTwoByteFieldSid() {
        IonReaderContinuableCoreBinary reader = initializeReader(
            true,
            0xE0, 0x01, 0x00, 0xEA, // IVM
            0xDC, // Struct, length 12
            0x00, // First byte of overpadded 2-byte field SID
            0x9A  // Field SID 26
            // The struct ends unexpectedly
        );
        assertEquals(IonCursor.Event.START_CONTAINER, reader.nextValue());
        assertEquals(IonType.STRUCT, reader.getType());
        reader.stepIntoContainer();
        // This is an unexpected EOF, so the reader should fail cleanly.
        assertThrows(IonException.class, reader::nextValue);
        reader.close();
    }

    @Test
    public void expectIncompleteContainerToFailCleanlyAfterAnnotationHeader() {
        IonReaderContinuableCoreBinary reader = initializeReader(
            true,
            0xE0, 0x01, 0x00, 0xEA, // IVM
            0xDC, // Struct, length 12
            0x9A, // Field SID 26
            0xE4, // Annotation wrapper length 4
            0x00, 0x81, // VarUInt length 1 (overpadded by 1 byte)
            0x00, 0x84  // VarUInt SID 4 (overpadded by 1 byte)
            // The value ends unexpectedly
        );
        assertEquals(IonCursor.Event.START_CONTAINER, reader.nextValue());
        assertEquals(IonType.STRUCT, reader.getType());
        reader.stepIntoContainer();
        // This is an unexpected EOF, so the reader should fail cleanly.
        assertThrows(IonException.class, reader::nextValue);
        reader.close();
    }

    @Test
    public void expectIncompleteAnnotationHeaderToFailCleanly() {
        IonReaderContinuableCoreBinary reader = initializeReader(
            true,
            0xE0, 0x01, 0x00, 0xEA, // IVM
            0xE4, // Annotation wrapper length 5
            0x81, // VarUInt length 1
            0x84  // VarUInt SID 4
            // The value ends unexpectedly
        );
        // This is an unexpected EOF, so the reader should fail cleanly.
        assertThrows(IonException.class, reader::nextValue);
        reader.close();
    }

    @Test
    public void timestampLengthZeroAtStreamEndFailsCleanly() {
        try (
            IonReaderContinuableCoreBinary reader = initializeReader(
                // Note: a refillable reader would await more bytes before throwing. See the next test.
                true,
                0xE0, 0x01, 0x00, 0xEA, // IVM
                0x6E, // Timestamp value, variable length.
                0x80  // VarUInt 0 at stream end. This is an error because there is no length 0 timestamp.
            )
        ) {
            assertEquals(START_SCALAR, reader.nextValue());
            assertThrows(IonException.class, reader::timestampValue);
        }
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void timestampLengthZeroFailsCleanly(boolean constructFromBytes) {
        try (
            IonReaderContinuableCoreBinary reader = initializeReader(
                constructFromBytes,
                0xE0, 0x01, 0x00, 0xEA, // IVM
                0x6E, // Timestamp value, variable length.
                0x80, // VarUInt 0 at stream end. This is an error because there is no length 0 timestamp.
                0x20   // Value byte to pad the input. A refillable reader expects at least this many bytes to compose a valid timestamp.
            )
        ) {
            assertEquals(START_SCALAR, reader.nextValue());
            assertThrows(IonException.class, reader::timestampValue);
        }
    }
}
