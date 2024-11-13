// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl;

import com.amazon.ion.IntegerSize;
import com.amazon.ion.IonCursor;
import com.amazon.ion.IonException;
import com.amazon.ion.IonType;
import com.amazon.ion.TestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.math.BigInteger;

import static com.amazon.ion.BitUtils.bytes;
import static com.amazon.ion.IonCursor.Event.START_SCALAR;
import static com.amazon.ion.IonCursor.Event.VALUE_READY;
import static com.amazon.ion.TestUtils.withIvm;
import static com.amazon.ion.impl.IonCursorTestUtilities.annotations;
import static com.amazon.ion.impl.IonCursorTestUtilities.fieldName;
import static com.amazon.ion.impl.TaglessEncoding.FLEX_INT;
import static com.amazon.ion.impl.TaglessEncoding.FLEX_UINT;
import static com.amazon.ion.impl.TaglessEncoding.INT16;
import static com.amazon.ion.impl.TaglessEncoding.INT32;
import static com.amazon.ion.impl.TaglessEncoding.INT64;
import static com.amazon.ion.impl.TaglessEncoding.UINT32;
import static com.amazon.ion.impl.TaglessEncoding.UINT64;
import static com.amazon.ion.impl.TaglessEncoding.UINT8;
import static com.amazon.ion.impl.IonCursorBinaryTest.nextMacroInvocation;
import static com.amazon.ion.impl.IonCursorTestUtilities.STANDARD_BUFFER_CONFIGURATION;
import static com.amazon.ion.impl.IonCursorTestUtilities.Expectation;
import static com.amazon.ion.impl.IonCursorTestUtilities.ExpectationProvider;
import static com.amazon.ion.impl.IonCursorTestUtilities.assertSequence;
import static com.amazon.ion.impl.IonCursorTestUtilities.container;
import static com.amazon.ion.impl.IonCursorTestUtilities.doubleValue;
import static com.amazon.ion.impl.IonCursorTestUtilities.endContainer;
import static com.amazon.ion.impl.IonCursorTestUtilities.endStream;
import static com.amazon.ion.impl.IonCursorTestUtilities.fillContainer;
import static com.amazon.ion.impl.IonCursorTestUtilities.fillIntValue;
import static com.amazon.ion.impl.IonCursorTestUtilities.fillStringValue;
import static com.amazon.ion.impl.IonCursorTestUtilities.fillSymbolValue;
import static com.amazon.ion.impl.IonCursorTestUtilities.scalar;
import static com.amazon.ion.impl.IonCursorTestUtilities.startContainer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IonReaderContinuableCoreBinaryTest {

    private IonReaderContinuableCoreBinary initializeReader(boolean constructFromBytes, int... data) {
        return initializeReader(constructFromBytes, bytes(data));
    }

    private IonReaderContinuableCoreBinary initializeReader(boolean constructFromBytes, byte[] data) {
        IonReaderContinuableCoreBinary reader;
        if (constructFromBytes) {
            reader = new IonReaderContinuableCoreBinary(STANDARD_BUFFER_CONFIGURATION, data, 0, data.length);
        } else {
            reader = new IonReaderContinuableCoreBinary(
                STANDARD_BUFFER_CONFIGURATION,
                new ByteArrayInputStream(data),
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
    @CsvSource({
        "0,                   E1 00 60",
        "1,                   E1 01 60",
        "255,                 E1 FF 60",
        "256,                 E2 00 00 60",
        "257,                 E2 01 00 60",
        "512,                 E2 00 01 60",
        "513,                 E2 01 01 60",
        "65535,               E2 FF FE 60",
        "65791,               E2 FF FF 60",
        "65792,               E3 01 60",
        "65793,               E3 03 60",
        "65919,               E3 FF 60",
        "65920,               E3 02 02 60",
        "2147483647         , E3 F0 DF DF FF 0F 60",
    })
    public void sidSymbols_1_1(int sid, String bytes) {
        sidSymbols_1_1_helper(sid, bytes, true);
        sidSymbols_1_1_helper(sid, bytes, false);
    }
    void sidSymbols_1_1_helper(int sid, String bytes, boolean constructFromBytes) {
        IonReaderContinuableCoreBinary reader = initializeReader(
            constructFromBytes,
            TestUtils.hexStringToByteArray("E0 01 01 EA " + bytes)
        );
        assertSequence(
            reader,
            scalar(), fillSymbolValue(sid),
            scalar(), fillIntValue(0),
            endStream()
        );
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @EnumSource(SystemSymbols_1_1.class)
    public void systemSymbols_1_1(SystemSymbols_1_1 systemSymbol) {
        systemSymbols_1_1_helper(systemSymbol, true);
        systemSymbols_1_1_helper(systemSymbol, false);
    }
    void systemSymbols_1_1_helper(SystemSymbols_1_1 systemSymbol, boolean constructFromBytes) {
        String systemSidBytes = Integer.toHexString(systemSymbol.getId());
        IonReaderContinuableCoreBinary reader = initializeReader(
            constructFromBytes,
            TestUtils.hexStringToByteArray("E0 01 01 EA EE " + systemSidBytes + " 60")
        );
        assertSequence(
            reader,
            scalar(),
            symbolValue(systemSymbol.getText()),
            scalar(),
            fillIntValue(0),
            endStream()
        );
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @EnumSource(SystemSymbols_1_1.class)
    public void systemSymbols_1_1_fieldNames(SystemSymbols_1_1 systemSymbol) {
        systemSymbols_1_1_fieldNamesHelper(systemSymbol, true);
        systemSymbols_1_1_fieldNamesHelper(systemSymbol, false);
    }
    void systemSymbols_1_1_fieldNamesHelper(SystemSymbols_1_1 systemSymbol, boolean constructFromBytes) {
        String systemSidBytes = Integer.toHexString(0x60 + systemSymbol.getId());
        IonReaderContinuableCoreBinary reader = initializeReader(
            constructFromBytes,
            TestUtils.hexStringToByteArray("E0 01 01 EA F3 01 " + systemSidBytes + " 60 01 F0")
        );
        assertSequence(
            reader,
            fillContainer(IonType.STRUCT,
                scalar(),
                fieldName(systemSymbol.getText()),
                fillIntValue(0)
            ),
            endStream()
        );
    }

    @ParameterizedTest(name = "symbol={0}")
    @EnumSource(SystemSymbols_1_1.class)
    public void systemSymbols_1_1_annotations(SystemSymbols_1_1 systemSymbol) {
        systemSymbols_1_1_annotationsHelper(systemSymbol, true);
        systemSymbols_1_1_annotationsHelper(systemSymbol, false);
    }
    void systemSymbols_1_1_annotationsHelper(SystemSymbols_1_1 systemSymbol, boolean constructFromBytes) {
        String systemSidBytes = Integer.toHexString(0x60 + systemSymbol.getId());
        IonReaderContinuableCoreBinary reader = initializeReader(
            constructFromBytes,
            TestUtils.hexStringToByteArray("E0 01 01 EA E7 01 " + systemSidBytes + " 60")
        );
        assertSequence(
            reader,
            annotations(systemSymbol.getText()),
            fillIntValue(0),
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

    /**
     * Provides Expectations that advance the reader to the next tagless value, fill the value, and verify that it has
     * the given attributes.
     */
    private static ExpectationProvider<IonReaderContinuableCoreBinary> fillNextTaglessValue(TaglessEncoding taglessEncoding, IonType expectedType) {
        return consumer -> consumer.accept(new Expectation<>(
            String.format("fill tagless %s", taglessEncoding.name()),
            reader -> {
                assertEquals(START_SCALAR, reader.nextTaglessValue(taglessEncoding));
                assertEquals(VALUE_READY, reader.fillValue());
                assertEquals(expectedType, reader.getType());
            }
        ));
    }

    /**
     * Provides Expectations that advance the reader to the next tagless value, fill the value, and verify that it is
     * an integer that fits in a Java int with the expected value.
     */
    private static ExpectationProvider<IonReaderContinuableCoreBinary> nextTaglessIntValue(TaglessEncoding taglessEncoding, int expectedValue) {
        return consumer -> consumer.accept(new Expectation<>(
            String.format("fill tagless int from %s", taglessEncoding.name()),
            reader -> {
                assertEquals(START_SCALAR, reader.nextTaglessValue(taglessEncoding));
                assertEquals(VALUE_READY, reader.fillValue());
                assertEquals(IonType.INT, reader.getType());
                assertEquals(IntegerSize.INT, reader.getIntegerSize());
                assertEquals(expectedValue, reader.intValue());
            }
        ));
    }

    /**
     * Provides Expectations that advance the reader to the next tagless value, fill the value, and verify that it is
     * an integer that fits in a Java long with the expected value.
     */
    private static ExpectationProvider<IonReaderContinuableCoreBinary> nextTaglessLongValue(TaglessEncoding taglessEncoding, long expectedValue) {
        return consumer -> consumer.accept(new Expectation<>(
            String.format("fill tagless long from %s", taglessEncoding.name()),
            reader -> {
                assertEquals(START_SCALAR, reader.nextTaglessValue(taglessEncoding));
                assertEquals(VALUE_READY, reader.fillValue());
                assertEquals(IonType.INT, reader.getType());
                assertEquals(IntegerSize.LONG, reader.getIntegerSize());
                assertEquals(expectedValue, reader.longValue());
            }
        ));
    }

    /**
     * Provides Expectations that advance the reader to the next tagless value, fill the value, and verify that it is
     * an integer that fits in a BigInteger with the expected value.
     */
    private static ExpectationProvider<IonReaderContinuableCoreBinary> nextTaglessBigIntegerValue(TaglessEncoding taglessEncoding, BigInteger expectedValue) {
        return consumer -> consumer.accept(new Expectation<>(
            String.format("fill tagless BigInteger from %s", taglessEncoding.name()),
            reader -> {
                assertEquals(START_SCALAR, reader.nextTaglessValue(taglessEncoding));
                assertEquals(VALUE_READY, reader.fillValue());
                assertEquals(IonType.INT, reader.getType());
                assertEquals(IntegerSize.BIG_INTEGER, reader.getIntegerSize());
                assertEquals(expectedValue, reader.bigIntegerValue());
            }
        ));
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void taglessInts(boolean constructFromBytes) throws Exception {
        byte[] data = withIvm(1, bytes(
            0x00, // User macro ID 0
            0xFF, // Interpreted as uint8
            0xFF, 0xFF, // Interpreted as int16
            0xFF, 0xFF, 0xFF, 0xFF, // Interpreted as uint32
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // Interpreted as int64
            0xFC, 0xFF, 0xFF, // Interpreted as flex_uint
            0xFC, 0xFF, 0xFF // Interpreted as flex_int
        ));
        try (IonReaderContinuableCoreBinary reader = initializeReader(constructFromBytes, data)) {
            assertSequence(
                reader,
                nextMacroInvocation(0),
                nextTaglessIntValue(UINT8, 0xFF),
                nextTaglessIntValue(INT16, -1),
                nextTaglessLongValue(UINT32, 0xFFFFFFFFL),
                nextTaglessLongValue(INT64, -1),
                nextTaglessIntValue(FLEX_UINT, 0x1FFFFF),
                nextTaglessIntValue(FLEX_INT, -1),
                endStream()
            );
        }
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void taglessFixedIntBoundaries(boolean constructFromBytes) throws Exception {
        byte[] data = withIvm(1, bytes(
            0x00, // User macro ID 0
            0xFF, 0xFF, 0xFF, 0x7F, // Interpreted as uint32 -- this is Integer.MAX_VALUE
            0xFF, 0xFF, 0xFF, 0x7F, // Interpreted as int32 -- this is Integer.MAX_VALUE
            0x00, 0x00, 0x00, 0x80, // Interpreted as uint32 -- this won't fit in a Java int, which is signed
            0x00, 0x00, 0x00, 0x80, // Interpreted as int32 -- this is Integer.MIN_VALUE
            0xFF, 0xFF, 0xFF, 0xFF, // Interpreted as uint32 -- this won't fit in a Java int
            0xFF, 0xFF, 0xFF, 0xFF  // Interpreted as int32 -- this is -1
         ));
        try (IonReaderContinuableCoreBinary reader = initializeReader(constructFromBytes, data)) {
            assertSequence(
                reader,
                nextMacroInvocation(0),
                nextTaglessIntValue(UINT32, Integer.MAX_VALUE),
                nextTaglessIntValue(INT32, Integer.MAX_VALUE),
                nextTaglessLongValue(UINT32, 0x80000000L),
                nextTaglessIntValue(INT32, Integer.MIN_VALUE),
                nextTaglessLongValue(UINT32, 0xFFFFFFFFL),
                nextTaglessIntValue(INT32, -1),
                endStream()
            );
        }
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void taglessFixedLongBoundaries(boolean constructFromBytes) throws Exception {
        byte[] data = withIvm(1, bytes(
            0x00, // User macro ID 0
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F, // Interpreted as uint64 -- this is Long.MAX_VALUE
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F, // Interpreted as int64 -- this is Long.MAX_VALUE
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80, // Interpreted as uint64 -- this won't fit in a Java long
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80, // Interpreted as int64 -- this is Long.MIN_VALUE
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // Interpreted as uint64 -- this won't fit in a Java long
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF  // Interpreted as int64 -- this is -1
        ));
        try (IonReaderContinuableCoreBinary reader = initializeReader(constructFromBytes, data)) {
            assertSequence(
                reader,
                nextMacroInvocation(0),
                nextTaglessLongValue(UINT64, Long.MAX_VALUE),
                nextTaglessLongValue(INT64, Long.MAX_VALUE),
                nextTaglessBigIntegerValue(UINT64, BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE)),
                nextTaglessLongValue(INT64, Long.MIN_VALUE),
                nextTaglessBigIntegerValue(UINT64, BigInteger.valueOf(Long.MAX_VALUE).shiftLeft(1).add(BigInteger.ONE)),
                nextTaglessLongValue(INT64, -1),
                endStream()
            );
        }
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void taglessFlexIntBoundaries(boolean constructFromBytes) throws Exception {
        byte[] data = withIvm(1, bytes(
            0x00, // User macro ID 0
            0xF0, 0xFF, 0xFF, 0xFF, 0x0F, // 31 set bits. As flex_uint this is Integer.MAX_VALUE
            0xF0, 0xFF, 0xFF, 0xFF, 0x0F, // 31 set bits. As flex_int this is Integer.MAX_VALUE
            0x10, 0x00, 0x00, 0x00, 0x10, // Bit 31 set. As a flex_uint this is Integer.MAX_VALUE + 1
            0x10, 0x00, 0x00, 0x00, 0x10, // Bit 31 set (sign not extended). As flex_int this is Integer.MAX_VALUE + 1
            0xF0, 0xFF, 0xFF, 0xFF, 0x1F, // 32 set bits. As flex_uint this is (Integer.MAX_VALUE << 1) + 1
            0xF0, 0xFF, 0xFF, 0xFF, 0x1F, // 32 set bits. As flex_int this is (Integer.MAX_VALUE << 1) + 1
            0x10, 0x00, 0x00, 0x00, 0xF0, // Bits 31+ set. As flex_uint this won't fit in an int
            0x10, 0x00, 0x00, 0x00, 0xF0, // Bits 31+ set (sign extended). As flex_int this is Integer.MIN_VALUE
            0xF0, 0xFF, 0xFF, 0xFF, 0xEF, // All bits except bit 31 set. As flex_uint this won't fit in an int
            0xF0, 0xFF, 0xFF, 0xFF, 0xEF, // All bits except bit 31 set (sign extended). As flex_int this is Integer.MIN_VALUE - 1
            0xF0, 0xFF, 0xFF, 0xFF, 0xFF, // All bits set. As flex_uint this won't fit in a Java int
            0xF0, 0xFF, 0xFF, 0xFF, 0xFF  // All bits set. As flex_int this is -1
        ));
        try (IonReaderContinuableCoreBinary reader = initializeReader(constructFromBytes, data)) {
            assertSequence(
                reader,
                nextMacroInvocation(0),
                nextTaglessIntValue(FLEX_UINT, Integer.MAX_VALUE),
                nextTaglessIntValue(FLEX_INT, Integer.MAX_VALUE),
                nextTaglessLongValue(FLEX_UINT, Integer.MAX_VALUE + 1L),
                nextTaglessLongValue(FLEX_INT, Integer.MAX_VALUE + 1L),
                nextTaglessLongValue(FLEX_UINT, 0xFFFFFFFFL),
                nextTaglessLongValue(FLEX_INT, 0xFFFFFFFFL),
                nextTaglessLongValue(FLEX_UINT, 0x780000000L), // 0xF000... >> 5 == 0x780...
                nextTaglessIntValue(FLEX_INT, Integer.MIN_VALUE),
                nextTaglessLongValue(FLEX_UINT, 0x77FFFFFFFL), // 0xEFFF... >> 5 == 0x77F...
                nextTaglessLongValue(FLEX_INT, Integer.MIN_VALUE - 1L),
                nextTaglessLongValue(FLEX_UINT, 0x7FFFFFFFFL), // 0xFFFF... >> 5 == 0x7FF...
                nextTaglessIntValue(FLEX_INT, -1),
                endStream()
            );
        }
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void taglessFlexLongBoundaries(boolean constructFromBytes) throws Exception {
        byte[] data = withIvm(1, bytes(
            0x00, // User macro ID 0
            0x00, 0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01, // 63 set bits. As flex_uint this is Long.MAX_VALUE
            0x00, 0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01, // 63 set bits. As flex_int this is Long.MAX_VALUE
            0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // Bit 63 set. As a flex_uint this is Long.MAX_VALUE + 1
            0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // Bit 63 set (sign not extended). As flex_int this is Long.MAX_VALUE + 1
            0x00, 0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x03, // 64 set bits. As flex_uint this is (Long.MAX_VALUE << 1) + 1
            0x00, 0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x03, // 64 set bits. As flex_int this is (Long.MAX_VALUE << 1) + 1
            0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFE, // Bits 63+ set. As flex_uint this won't fit in a long
            0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFE, // Bits 63+ set (sign extended). As flex_int this is Long.MIN_VALUE
            0x00, 0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFD, // All bits except bit 63 set. As flex_uint this won't fit in a long
            0x00, 0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFD, // All bits except bit 63 set (sign extended). As flex_int this is Long.MIN_VALUE - 1
            0x00, 0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // All bits set. As flex_uint this won't fit in a Java long
            0x00, 0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF // All bits set. As flex_int this is -1
        ));
        try (IonReaderContinuableCoreBinary reader = initializeReader(constructFromBytes, data)) {
            assertSequence(
                reader,
                nextMacroInvocation(0),
                nextTaglessLongValue(FLEX_UINT, Long.MAX_VALUE),
                nextTaglessLongValue(FLEX_INT, Long.MAX_VALUE),
                nextTaglessBigIntegerValue(FLEX_UINT, BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE)),
                nextTaglessBigIntegerValue(FLEX_INT, BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE)),
                nextTaglessBigIntegerValue(FLEX_UINT, BigInteger.valueOf(Long.MAX_VALUE).shiftLeft(1).add(BigInteger.ONE)),
                nextTaglessBigIntegerValue(FLEX_INT, BigInteger.valueOf(Long.MAX_VALUE).shiftLeft(1).add(BigInteger.ONE)),
                nextTaglessBigIntegerValue(FLEX_UINT, BigInteger.valueOf(0x3F80000000000000L).shiftLeft(8)), // 0xFE00... >>> 2 == 0x3F80...
                nextTaglessLongValue(FLEX_INT, Long.MIN_VALUE),
                nextTaglessBigIntegerValue(FLEX_UINT, BigInteger.valueOf(0x3F7FFFFFFFFFFFFFL).shiftLeft(8).or(BigInteger.valueOf(0xFF))), // 0xFDFF... >>> 2 == 0x3F7F...
                nextTaglessBigIntegerValue(FLEX_INT, BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE)),
                nextTaglessBigIntegerValue(FLEX_UINT, BigInteger.valueOf(0x3FFFFFFFFFFFFFFFL).shiftLeft(8).or(BigInteger.valueOf(0xFF))), // 0xFF... >>> 2 == 0x3F...
                nextTaglessLongValue(FLEX_INT, -1),
                endStream()
            );
        }
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void taglessFloats(boolean constructFromBytes) throws Exception {
        byte[] data = withIvm(1, bytes(
            0x00, // User macro ID 0
            0x00, 0x3C, // Interpreted as float16 (1.0)
            0x00, 0x00, 0x80, 0x3F, // Interpreted as float32 (1.0)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F // Interpreted as float64 (1.0)
        ));
        try (IonReaderContinuableCoreBinary reader = initializeReader(constructFromBytes, data)) {
            assertSequence(
                reader,
                nextMacroInvocation(0),
                fillNextTaglessValue(TaglessEncoding.FLOAT16, IonType.FLOAT),
                doubleValue(1.0),
                fillNextTaglessValue(TaglessEncoding.FLOAT32, IonType.FLOAT),
                doubleValue(1.0),
                fillNextTaglessValue(TaglessEncoding.FLOAT64, IonType.FLOAT),
                doubleValue(1.0),
                endStream()
            );
        }
    }

    static ExpectationProvider<IonReaderContinuableCoreBinary> symbolValue(String expectedText) {
        return consumer -> consumer.accept(new Expectation<>(
            String.format("symbol(%s)", expectedText),
            reader -> {
                assertTrue(reader.hasSymbolText());
                assertEquals(expectedText, reader.getSymbolText());
            }
        ));
    }

    static ExpectationProvider<IonReaderContinuableCoreBinary> symbolValue(int expectedSid) {
        return consumer -> consumer.accept(new Expectation<>(
            String.format("symbol(%d)", expectedSid),
            reader -> {
                assertFalse(reader.hasSymbolText());
                assertEquals(expectedSid, reader.symbolValueId());
            }
        ));
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void taglessCompactSymbols(boolean constructFromBytes) throws Exception {
        byte[] data = withIvm(1, bytes(
            0x00, // User macro ID 0
            0xF9, 0x6E, 0x61, 0x6D, 0x65, // interpreted as compact symbol (FlexSym with inline text "name")
            0x09, // interpreted as compact symbol (FlexSym with SID 4)
            0x01, 0x75 // interpreted as compact symbol (special FlexSym)
        ));
        try (IonReaderContinuableCoreBinary reader = initializeReader(constructFromBytes, data)) {
            assertSequence(
                reader,
                nextMacroInvocation(0),
                fillNextTaglessValue(TaglessEncoding.FLEX_SYM, IonType.SYMBOL),
                symbolValue("name"),
                fillNextTaglessValue(TaglessEncoding.FLEX_SYM, IonType.SYMBOL),
                symbolValue(4),
                fillNextTaglessValue(TaglessEncoding.FLEX_SYM, IonType.SYMBOL),
                symbolValue(""),
                endStream()
            );
        }
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void addSymbolsSystemMacro(boolean constructFromBytes) throws Exception {
        byte[] data = withIvm(1, bytes(
            0xEF, 0x0C, // system macro add_symbols
            0x02, // AEB: 0b------aa; a=10, expression group
            0x01, // FlexInt 0, a delimited expression group
            0x93, 0x61, 0x62, 0x63, // 3-byte string, utf-8 "abc"
            0xF0, // delimited end...  of expression group
            0xE1, // SID single byte
            0x42  // SID $66
        ));
        try (IonReaderContinuableCoreBinary reader = initializeReader(constructFromBytes, data)) {
            assertEquals(START_SCALAR, reader.nextValue());
            assertEquals(66, reader.symbolValueId());
        }
    }
}
