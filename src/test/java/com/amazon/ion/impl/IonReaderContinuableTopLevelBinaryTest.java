// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl;

import com.amazon.ion.BufferConfiguration;
import com.amazon.ion.Decimal;
import com.amazon.ion.IntegerSize;
import com.amazon.ion.IonBufferConfiguration;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonException;
import com.amazon.ion.IonLoader;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonString;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.OversizedValueException;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.SystemSymbols;
import com.amazon.ion.TestUtils;
import com.amazon.ion.Timestamp;
import com.amazon.ion.UnknownSymbolException;
import com.amazon.ion.impl.bin._Private_IonManagedBinaryWriterBuilder;
import com.amazon.ion.impl.bin._Private_IonManagedWriter;
import com.amazon.ion.impl.bin._Private_IonRawWriter;
import com.amazon.ion.system.IonBinaryWriterBuilder;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazon.ion.system.SimpleCatalog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.converter.ConvertWith;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.zip.GZIPInputStream;

import static com.amazon.ion.BitUtils.bytes;
import static com.amazon.ion.TestUtils.StringToTimestamp;
import static com.amazon.ion.TestUtils.bitStringToByteArray;
import static com.amazon.ion.TestUtils.gzippedBytes;
import static com.amazon.ion.TestUtils.hexStringToByteArray;
import static com.amazon.ion.impl.IonCursorTestUtilities.Expectation;
import static com.amazon.ion.impl.IonCursorTestUtilities.ExpectationProvider;
import static com.amazon.ion.impl.IonCursorTestUtilities.type;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class IonReaderContinuableTopLevelBinaryTest {

    private static final IonSystem SYSTEM = IonSystemBuilder.standard().build();

    // Builds the incremental reader. May be overwritten by individual tests.
    private IonReaderBuilder readerBuilder;
    // Builds binary writers for constructing test data. May be overwritten by individual tests.
    private IonBinaryWriterBuilder writerBuilder;
    // Counts the number of bytes reported by the reader.
    private AtomicLong byteCounter;
    // Counts the number of oversized values or symbol tables reported by the reader.
    private AtomicInteger oversizedCounter;
    // The total number of bytes in the input, to be compared against the total bytes consumed by the reader.
    private long totalBytesInStream;
    // The reader under test.
    private IonReader reader;

    /**
     * Unified handler interface to reduce boilerplate when defining test handlers.
     */
    private interface UnifiedTestHandler extends
        BufferConfiguration.OversizedValueHandler,
        IonBufferConfiguration.OversizedSymbolTableHandler,
        BufferConfiguration.DataHandler {
        // Empty.
    }

    /**
     * A handler that counts consumed bytes using `byteCounter` and throws if any oversized value is encountered.
     */
    private final UnifiedTestHandler byteCountingHandler = new UnifiedTestHandler() {
        @Override
        public void onOversizedSymbolTable() {
            fail("Oversized symbol table not expected.");
        }

        @Override
        public void onOversizedValue() {
            fail("Oversized value not expected.");
        }

        @Override
        public void onData(int numberOfBytes) {
            byteCounter.addAndGet(numberOfBytes);
        }
    };

    /**
     * A handler that counts consumed bytes using `byteCounter`, counts oversized user values using `oversizedCounter`,
     * and throws if any oversized symbol tables are encountered.
     */
    private final UnifiedTestHandler byteAndOversizedValueCountingHandler = new UnifiedTestHandler() {
        @Override
        public void onOversizedSymbolTable() {
            throw new IllegalStateException("Oversized symbol table not expected.");
        }

        @Override
        public void onOversizedValue() {
            oversizedCounter.incrementAndGet();
        }

        @Override
        public void onData(int numberOfBytes) {
            byteCounter.addAndGet(numberOfBytes);
        }
    };

    /**
     * A handler that counts consumed bytes using `byteCounter`, counts oversized symbol tables using
     * `oversizedCounter`, and throws if any oversized user values are encountered.
     */
    private final UnifiedTestHandler byteAndOversizedSymbolTableCountingHandler = new UnifiedTestHandler() {
        @Override
        public void onOversizedSymbolTable() {
            oversizedCounter.incrementAndGet();
        }

        @Override
        public void onOversizedValue() {
            throw new IllegalStateException("Oversized value not expected.");
        }

        @Override
        public void onData(int numberOfBytes) {
            byteCounter.addAndGet(numberOfBytes);
        }
    };

    // Standard byte-counting buffer configuration. Fails if any value is oversized.
    private final IonBufferConfiguration standardBufferConfiguration = IonBufferConfiguration.Builder.standard()
        .onOversizedSymbolTable(byteCountingHandler)
        .onOversizedValue(byteCountingHandler)
        .onData(byteCountingHandler)
        .build();
    
    @BeforeEach
    public void setup() {
        byteCounter = new AtomicLong();
        oversizedCounter = new AtomicInteger();
        readerBuilder = IonReaderBuilder.standard()
            .withIncrementalReadingEnabled(true)
            .withBufferConfiguration(standardBufferConfiguration);
        writerBuilder = IonBinaryWriterBuilder.standard();
        reader = null;
    }

    private void assertBytesConsumed() {
        assertEquals(totalBytesInStream, byteCounter.get());
    }

    /**
     * Writes binary Ion streams with a user-level writer.
     */
    private interface WriterFunction {
        void write(IonWriter writer) throws IOException;
    }

    /**
     * Writes binary Ion streams with a raw writer. Also allows bytes to be written directly to the stream.
     */
    private interface RawWriterFunction {
        void write(_Private_IonRawWriter writer, ByteArrayOutputStream out) throws IOException;
    }

    /**
     * Writes a raw binary stream using the provided raw writer function.
     * @param writerFunction the write function.
     * @param writeIvm true if an Ion 1.0 IVM should be written at the start of the stream; otherwise, false.
     * @return the binary Ion bytes.
     * @throws Exception if thrown during writing.
     */
    private byte[] writeRaw(RawWriterFunction writerFunction, boolean writeIvm) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        _Private_IonRawWriter writer = writerBuilder.build(out)
            .asFacet(_Private_IonManagedWriter.class)
            .getRawWriter();
        if (writeIvm) {
            out.write(_Private_IonConstants.BINARY_VERSION_MARKER_1_0);
        }
        writerFunction.write(writer, out);
        writer.close();
        return out.toByteArray();
    }

    /**
     * Converts the given text Ion to the equivalent binary Ion.
     * @param ion text Ion data.
     * @return the equivalent binary Ion data.
     */
    private static byte[] toBinary(String ion) {
        return TestUtils.ensureBinary(SYSTEM, ion.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Creates an incremental reader over the given binary Ion, constructing a reader either from byte array or
     * from InputStream depending on the value of the parameter 'constructFromBytes'.
     * @param builder the reader builder.
     * @param constructFromBytes whether to construct the reader from bytes or an InputStream.
     * @param bytes the binary Ion data.
     * @return a new reader.
     */
    private IonReader readerFor(IonReaderBuilder builder, boolean constructFromBytes, byte[] bytes) {
        totalBytesInStream = bytes.length;
        if (constructFromBytes) {
            return new IonReaderContinuableTopLevelBinary(builder, bytes, 0, bytes.length);
        }
        return new IonReaderContinuableTopLevelBinary(builder, new ByteArrayInputStream(bytes), null, 0, 0);
    }

    /**
     * Creates an incremental reader over the binary equivalent of the given text Ion.
     * @param ion text Ion data.
     * @param constructFromBytes whether to construct the reader from bytes or an InputStream.
     * @return a new reader.
     */
    private IonReader readerFor(String ion, boolean constructFromBytes) {
        byte[] binary = toBinary(ion);
        totalBytesInStream = binary.length;
        return readerFor(readerBuilder, constructFromBytes, binary);
    }

    /**
     * Creates an incremental reader over the binary Ion data created by invoking the given RawWriterFunction.
     * @param writerFunction the function used to generate the data.
     * @param constructFromBytes whether to construct the reader from bytes or an InputStream.
     * @return a new reader.
     * @throws Exception if an exception is raised while writing the Ion data.
     */
    private IonReader readerFor(RawWriterFunction writerFunction, boolean constructFromBytes) throws Exception {
        byte[] binary = writeRaw(writerFunction, true);
        totalBytesInStream = binary.length;
        return readerFor(readerBuilder, constructFromBytes, binary);
    }

    /**
     * Creates an incremental reader over the binary Ion data created by invoking the given WriterFunction.
     * @param writerFunction the function used to generate the data.
     * @param constructFromBytes whether to construct the reader from bytes or an InputStream.
     * @return a new reader.
     * @throws Exception if an exception is raised while writing the Ion data.
     */
    private IonReader readerFor(WriterFunction writerFunction, boolean constructFromBytes) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonWriter writer = writerBuilder.build(out);
        writerFunction.write(writer);
        writer.close();
        byte[] binary = out.toByteArray();
        totalBytesInStream = binary.length;
        return readerFor(readerBuilder, constructFromBytes, binary);
    }

    /**
     * Creates an incremental reader over the given bytes, prepended with the IVM.
     * @param constructFromBytes whether to construct the reader from bytes or an InputStream.
     * @param ion binary Ion bytes without an IVM.
     * @return a new reader.
     */
    private IonReader readerFor(boolean constructFromBytes, int... ion) throws Exception {
        byte[] binary = new TestUtils.BinaryIonAppender().append(ion).toByteArray();
        totalBytesInStream = binary.length;
        return readerFor(readerBuilder, constructFromBytes, binary);
    }

    /**
     * Creates an incremental reader over the given binary Ion. This should only be used in cases where tests exercise
     * behavior that does not exist when constructing a reader over a fixed buffer via byte array. In all other cases,
     * use one of the other `readerFor` variants, which construct readers according to the 'constructFromBytes'
     * parameter.
     * @param input the binary Ion data.
     * @return a new reader.
     */
    private IonReader readerFor(InputStream input) {
        return new IonReaderContinuableTopLevelBinary(readerBuilder, input, null, 0, 0);
    }

    static ExpectationProvider<IonReaderContinuableTopLevelBinary> next(IonType expectedType) {
        return consumer -> consumer.accept(new Expectation<>(
            String.format("next(%s)", expectedType),
            reader -> {
                assertEquals(expectedType, reader.next());
                assertEquals(expectedType, reader.getType());
            })
        );
    }

    static ExpectationProvider<IonReaderContinuableTopLevelBinary> next(String expectedFieldName, IonType expectedType) {
        return consumer -> consumer.accept(new Expectation<>(
            String.format("next(%s: %s)", expectedFieldName, expectedType),
            reader -> {
                assertEquals(expectedType, reader.next());
                assertEquals(expectedType, reader.getType());
                assertEquals(expectedFieldName, reader.getFieldName());
            })
        );
    }

    static final Expectation<IonReaderContinuableTopLevelBinary> STEP_IN_EXPECTATION = new Expectation<>("step_in", IonReader::stepIn);
    static final Expectation<IonReaderContinuableTopLevelBinary> STEP_OUT_EXPECTATION = new Expectation<>("step_out", IonReader::stepOut);

    static final ExpectationProvider<IonReaderContinuableTopLevelBinary> STEP_IN = consumer -> consumer.accept(STEP_IN_EXPECTATION);
    static final ExpectationProvider<IonReaderContinuableTopLevelBinary> STEP_OUT = consumer -> consumer.accept(STEP_OUT_EXPECTATION);

    @SafeVarargs
    static ExpectationProvider<IonReaderContinuableTopLevelBinary> container(IonType expectedType, ExpectationProvider<IonReaderContinuableTopLevelBinary>... expectations) {
        return consumer -> {
            next(expectedType).accept(consumer);
            STEP_IN.accept(consumer);
            for (Consumer<Consumer<Expectation<IonReaderContinuableTopLevelBinary>>> expectation : expectations) {
                expectation.accept(consumer);
            }
            STEP_OUT.accept(consumer);
        };
    }

    static ExpectationProvider<IonReaderContinuableTopLevelBinary> nullValue(IonType expectedType) {
        return consumer -> consumer.accept(new Expectation<>(
            String.format("null(%s)", expectedType),
            reader -> {
                assertTrue(reader.isNullValue());
                assertEquals(expectedType, reader.getType());
            }
        ));
    }

    static ExpectationProvider<IonReaderContinuableTopLevelBinary> booleanValue(boolean expectedValue) {
        return consumer -> consumer.accept(new Expectation<>(
            String.format("boolean(%s)", expectedValue),
            reader -> assertEquals(expectedValue, reader.booleanValue())
        ));
    }

    static ExpectationProvider<IonReaderContinuableTopLevelBinary> intValue(int expectedValue) {
        return consumer -> consumer.accept(new Expectation<>(
            String.format("int(%d)", expectedValue),
            reader -> {
                assertEquals(IntegerSize.INT, reader.getIntegerSize());
                assertEquals(expectedValue, reader.intValue());
            }
        ));
    }

    static ExpectationProvider<IonReaderContinuableTopLevelBinary> longValue(long expectedValue) {
        return consumer -> consumer.accept(new Expectation<>(
            String.format("long(%d)", expectedValue),
            reader -> {
                assertTrue(reader.getIntegerSize().ordinal() <= IntegerSize.LONG.ordinal());
                assertEquals(expectedValue, reader.longValue());
            }
        ));
    }

    static ExpectationProvider<IonReaderContinuableTopLevelBinary> bigIntegerValue(BigInteger expectedValue) {
        return consumer -> consumer.accept(new Expectation<>(
            String.format("bigInteger(%s)", expectedValue),
            reader -> assertEquals(expectedValue, reader.bigIntegerValue())
        ));
    }

    static ExpectationProvider<IonReaderContinuableTopLevelBinary> doubleValue(double expectedValue) {
        return consumer -> consumer.accept(new Expectation<>(
            String.format("double(%f)", expectedValue),
            reader -> assertEquals(expectedValue, reader.doubleValue(), 1e-9)
        ));
    }

    static ExpectationProvider<IonReaderContinuableTopLevelBinary> decimalValue(BigDecimal expectedValue) {
        return consumer -> consumer.accept(new Expectation<>(
            String.format("decimal(%s)", expectedValue),
            reader -> assertEquals(expectedValue, reader.decimalValue())
        ));
    }

    static ExpectationProvider<IonReaderContinuableTopLevelBinary> bigDecimalValue(BigDecimal expectedValue) {
        return consumer -> consumer.accept(new Expectation<>(
            String.format("bigDecimal(%s)", expectedValue),
            reader -> assertEquals(expectedValue, reader.bigDecimalValue())
        ));
    }

    static ExpectationProvider<IonReaderContinuableTopLevelBinary> timestampValue(Timestamp expectedValue) {
        return consumer -> consumer.accept(new Expectation<>(
            String.format("timestamp(%s)", expectedValue),
            reader -> assertEquals(expectedValue, reader.timestampValue())
        ));
    }

    static ExpectationProvider<IonReaderContinuableTopLevelBinary> stringValue(String expectedValue) {
        return consumer -> consumer.accept(new Expectation<>(
            String.format("string(%s)", expectedValue),
            reader -> assertEquals(expectedValue, reader.stringValue())
        ));
    }

    static ExpectationProvider<IonReaderContinuableTopLevelBinary> symbolValue(String expectedValue) {
        return consumer -> consumer.accept(new Expectation<>(
            String.format("symbol(%s)", expectedValue),
            reader -> assertSymbolEquals(expectedValue, reader.symbolValue())
        ));
    }

    static ExpectationProvider<IonReaderContinuableTopLevelBinary> symbolValue(int expectedSid) {
        return consumer -> consumer.accept(new Expectation<>(
            String.format("symbol(%d)", expectedSid),
            reader -> {
                SymbolToken symbolValue = reader.symbolValue();
                assertNull(symbolValue.getText());
                assertEquals(expectedSid, symbolValue.getSid());
            }
        ));
    }

    static ExpectationProvider<IonReaderContinuableTopLevelBinary> newBytesValue(byte[] expectedValue) {
        return consumer -> consumer.accept(new Expectation<>(
            String.format("bytes(%s)", Arrays.toString(expectedValue)),
            reader -> assertArrayEquals(expectedValue, reader.newBytes())
        ));
    }

    static ExpectationProvider<IonReaderContinuableTopLevelBinary> getBytesValue(byte[] destination, int offset, int requestedLength, int expectedLength) {
        return consumer -> consumer.accept(new Expectation<>(
            String.format("getBytes(%d, %d)", offset, expectedLength),
            reader -> assertEquals(expectedLength, reader.getBytes(destination, offset, requestedLength))
        ));
    }

    static ExpectationProvider<IonReaderContinuableTopLevelBinary> annotations(String... annotations) {
        return consumer -> consumer.accept(new Expectation<>(
            String.format("annotations(%s)", Arrays.toString(annotations)),
            reader -> assertEquals(Arrays.asList(annotations), Arrays.asList(reader.getTypeAnnotations()))
        ));
    }

    private static void assertSymbolEquals(
        String expectedText,
        SymbolToken actual
    ) {
        assertEquals(expectedText, actual.getText());
    }

    static ExpectationProvider<IonReaderContinuableTopLevelBinary> annotationSymbols(String... annotations) {
        return consumer -> consumer.accept(new Expectation<>(
            String.format("annotations(%s)", Arrays.toString(annotations)),
            reader -> {
                SymbolToken[] actualAnnotations = reader.getTypeAnnotationSymbols();
                assertEquals(annotations.length, actualAnnotations.length);
                for (int i = 0; i < annotations.length; i++) {
                    assertSymbolEquals(annotations[i], actualAnnotations[i]);
                }
            }
        ));
    }

    static ExpectationProvider<IonReaderContinuableTopLevelBinary> fieldName(String fieldName) {
        return consumer -> consumer.accept(new Expectation<>(
            String.format("field(%s)", fieldName),
            reader -> assertEquals(fieldName, reader.getFieldName())
        ));
    }

    static ExpectationProvider<IonReaderContinuableTopLevelBinary> fieldNameSymbol(String fieldName) {
        return consumer -> consumer.accept(new Expectation<>(
            String.format("field(%s)", fieldName),
            reader -> assertSymbolEquals(fieldName, reader.getFieldNameSymbol())
        ));
    }

    @SafeVarargs
    private final void assertSequence(ExpectationProvider<IonReaderContinuableTopLevelBinary>... providers) {
        IonCursorTestUtilities.assertSequence((IonReaderContinuableTopLevelBinary) reader, providers);
    }

    private void nextExpect(IonType type) {
        assertEquals(type, reader.next());
    }

    private void stepIn() {
        reader.stepIn();
    }

    private void stepOut() {
        reader.stepOut();
    }

    private void closeAndCount() throws IOException {
        reader.close();
        assertBytesConsumed();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void skipContainers(boolean constructFromBytes) throws Exception {
        reader = readerFor(
            "[123] 456 {abc: foo::bar::123, def: baz::456} [123] 789 [foo::bar::123, baz::456] [123]",
            constructFromBytes
        );
        assertSequence(
            next(IonType.LIST),
            next(IonType.INT),
            next(IonType.STRUCT),
            container(IonType.LIST,
                type(null),
                next(IonType.INT), intValue(123)
            ),
            type(null),
            next(IonType.INT), intValue(789),
            next(IonType.LIST),
            next(IonType.LIST),
            next(null)
        );
        closeAndCount();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void skipContainerAfterSteppingIn(boolean constructFromBytes) throws Exception {
        reader = readerFor("{abc: foo::bar::123, def: baz::456} 789", constructFromBytes);
        assertSequence(
            container(IonType.STRUCT,
                type(null),
                next(IonType.INT), annotations("foo", "bar"), intValue(123), type(IonType.INT)
                // Step out before completing the value
            ),
            type(null),
            next(IonType.INT), intValue(789),
            next(null)
        );
        closeAndCount();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void skipValueInContainer(boolean constructFromBytes) throws Exception {
        reader = readerFor("{foo: \"bar\", abc: 123, baz: a}", constructFromBytes);
        assertSequence(
            container(IonType.STRUCT,
                next(IonType.STRING),
                next("abc", IonType.INT),
                next("baz", IonType.SYMBOL),
                next(null)
            ),
            next(null)
        );
        closeAndCount();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void symbolsAsStrings(boolean constructFromBytes) throws Exception {
        reader = readerFor("{foo: uvw::abc, bar: qrs::xyz::def}", constructFromBytes);
        assertSequence(
            container(IonType.STRUCT,
                next("foo", IonType.SYMBOL), annotations("uvw"), stringValue("abc"),
                next("bar", IonType.SYMBOL), annotations("qrs", "xyz"), stringValue("def")
            ),
            next(null)
        );
        closeAndCount();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void lstAppend(boolean constructFromBytes) throws Exception {
        writerBuilder = writerBuilder.withLocalSymbolTableAppendEnabled();
        reader = readerFor(
            writer -> {
                writer.stepIn(IonType.STRUCT);
                writer.setFieldName("foo");
                writer.addTypeAnnotation("uvw");
                writer.writeSymbol("abc");
                writer.setFieldName("bar");
                writer.setTypeAnnotations("qrs", "xyz");
                writer.writeSymbol("def");
                writer.stepOut();
                writer.flush();
                writer.writeSymbol("orange");
            },
            constructFromBytes
        );
        assertSequence(
            container(IonType.STRUCT,
                next("foo", IonType.SYMBOL), annotations("uvw"), stringValue("abc"),
                next("bar", IonType.SYMBOL), annotations("qrs", "xyz"), stringValue("def")
            )
        );
        SymbolTable preAppend = reader.getSymbolTable();
        assertSequence(
            next(IonType.SYMBOL)
        );
        SymbolTable postAppend = reader.getSymbolTable();
        assertSequence(
            stringValue("orange"),
            next(null)
        );
        assertNull(preAppend.find("orange"));
        assertNotNull(postAppend.find("orange"));
        closeAndCount();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void lstNonAppend(boolean constructFromBytes) throws Exception {
        writerBuilder = writerBuilder.withLocalSymbolTableAppendDisabled();
        reader = readerFor(
            writer -> {
                writer.stepIn(IonType.STRUCT);
                writer.setFieldName("foo");
                writer.addTypeAnnotation("uvw");
                writer.writeSymbol("abc");
                writer.setFieldName("bar");
                writer.setTypeAnnotations("qrs", "xyz");
                writer.writeSymbol("def");
                writer.stepOut();
                writer.setTypeAnnotations("$ion_symbol_table");
                writer.stepIn(IonType.STRUCT);
                writer.setFieldName("symbols");
                writer.stepIn(IonType.LIST);
                writer.writeString("orange");
                writer.stepOut();
                writer.stepOut();
                writer.writeSymbol("orange");
            },
            constructFromBytes
        );
        assertSequence(
            container(IonType.STRUCT,
                next("foo", IonType.SYMBOL), annotations("uvw"), stringValue("abc"),
                next("bar", IonType.SYMBOL), annotations("qrs", "xyz"), stringValue("def")
            ),
            next(IonType.SYMBOL), stringValue("orange"),
            next(null)
        );
        closeAndCount();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void ivmBetweenValues(boolean constructFromBytes) throws Exception {
        writerBuilder = writerBuilder.withLocalSymbolTableAppendDisabled();
        reader = readerFor(
            writer -> {
                writer.stepIn(IonType.STRUCT);
                writer.setFieldName("foo");
                writer.addTypeAnnotation("uvw");
                writer.writeSymbol("abc");
                writer.setFieldName("bar");
                writer.setTypeAnnotations("qrs", "xyz");
                writer.writeSymbol("def");
                writer.stepOut();
                writer.finish();
                writer.writeSymbol("orange");
            },
            constructFromBytes
        );
        assertSequence(
            container(IonType.STRUCT,
                next("foo", IonType.SYMBOL), annotations("uvw"), stringValue("abc"),
                next("bar", IonType.SYMBOL), annotations("qrs", "xyz"), stringValue("def")
            ),
            next(IonType.SYMBOL), stringValue("orange"),
            next(null)
        );
        closeAndCount();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void ivmOnly(boolean constructFromBytes) throws Exception {
        reader = readerFor(constructFromBytes);
        assertSequence(next(null));
        closeAndCount();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void twoIvmsOnly(boolean constructFromBytes) throws Exception {
        reader = readerFor(constructFromBytes, 0xE0, 0x01, 0x00, 0xEA);
        assertSequence(next(null));
        closeAndCount();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void multipleSymbolTablesBetweenValues(boolean constructFromBytes) throws Exception {
        reader = readerFor(
            writer -> {
                writer.setTypeAnnotations("$ion_symbol_table");
                writer.stepIn(IonType.STRUCT);
                writer.setFieldName("symbols");
                writer.stepIn(IonType.LIST);
                writer.writeString("abc");
                writer.stepOut();
                writer.stepOut();
                writer.setTypeAnnotations("$ion_symbol_table");
                writer.stepIn(IonType.STRUCT);
                writer.setFieldName("symbols");
                writer.stepIn(IonType.LIST);
                writer.writeString("def");
                writer.stepOut();
                writer.setFieldName("imports");
                writer.writeSymbol("$ion_symbol_table");
                writer.stepOut();
                writer.writeSymbol("abc");
                writer.writeSymbol("def");
                writer.setTypeAnnotations("$ion_symbol_table");
                writer.stepIn(IonType.STRUCT);
                writer.setFieldName("symbols");
                writer.stepIn(IonType.LIST);
                writer.writeString("orange");
                writer.stepOut();
                writer.stepOut();
                writer.setTypeAnnotations("$ion_symbol_table");
                writer.stepIn(IonType.STRUCT);
                writer.setFieldName("symbols");
                writer.stepIn(IonType.LIST);
                writer.writeString("purple");
                writer.stepOut();
                writer.stepOut();
                writer.writeSymbol("purple");
            },
            constructFromBytes
        );

        assertSequence(
            next(IonType.SYMBOL), stringValue("abc"),
            next(IonType.SYMBOL), stringValue("def"),
            next(IonType.SYMBOL), stringValue("purple"),
            next(null)
        );
        closeAndCount();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void multipleIvmsBetweenValues(boolean constructFromBytes) throws Exception  {
        reader = readerFor(
            (writer, out) -> {
                writer.setTypeAnnotationSymbols(3);
                writer.stepIn(IonType.STRUCT);
                writer.setFieldNameSymbol(7);
                writer.stepIn(IonType.LIST);
                writer.writeString("abc");
                writer.stepOut();
                writer.stepOut();
                writer.finish();
                out.write(_Private_IonConstants.BINARY_VERSION_MARKER_1_0);
                writer.setTypeAnnotationSymbols(3);
                writer.stepIn(IonType.STRUCT);
                writer.setFieldNameSymbol(7);
                writer.stepIn(IonType.LIST);
                writer.writeString("def");
                writer.stepOut();
                writer.stepOut();
                writer.writeSymbolToken(10);
                writer.finish();
                out.write(_Private_IonConstants.BINARY_VERSION_MARKER_1_0);
                out.write(_Private_IonConstants.BINARY_VERSION_MARKER_1_0);
                writer.writeSymbolToken(4);
            },
            constructFromBytes
        );

        assertSequence(
            next(IonType.SYMBOL), stringValue("def"),
            next(IonType.SYMBOL), stringValue("name"),
            next(null)
        );
        closeAndCount();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void invalidVersion(boolean constructFromBytes) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(bytes(0xE0, 0x01, 0x74, 0xEA, 0x20));
        reader = readerFor(readerBuilder, constructFromBytes, out.toByteArray());
        assertThrows(IonException.class, () -> {
            reader.next();
            reader.close();
        });
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void invalidVersionMarker(boolean constructFromBytes) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(bytes(0xE0, 0x01, 0x00, 0xEB, 0x20));
        reader = readerFor(readerBuilder, constructFromBytes, out.toByteArray());
        assertThrows(IonException.class, () -> {
            reader.next();
            reader.close();
        });
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void unknownSymbolInFieldName(boolean constructFromBytes) throws Exception {
        reader = readerFor(constructFromBytes, 0xD3, 0x8A, 0x21, 0x01);
        assertSequence(next(IonType.STRUCT), STEP_IN, next(IonType.INT));
        assertThrows(UnknownSymbolException.class, reader::getFieldNameSymbol);
        assertThrows(UnknownSymbolException.class, reader::getFieldName);
        reader.close();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void unknownSymbolInAnnotation(boolean constructFromBytes) throws Exception {
        reader = readerFor(constructFromBytes, 0xE4, 0x81, 0x8A, 0x21, 0x01);
        assertSequence(next(IonType.INT));
        assertThrows(UnknownSymbolException.class, reader::getTypeAnnotationSymbols);
        assertThrows(UnknownSymbolException.class, reader::getTypeAnnotations);
        reader.close();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void unknownSymbolInValue(boolean constructFromBytes) throws Exception {
        reader = readerFor(constructFromBytes, 0x71, 0x0A);
        assertSequence(next(IonType.SYMBOL));
        assertThrows(UnknownSymbolException.class, reader::symbolValue);
        assertThrows(UnknownSymbolException.class, reader::stringValue);
        reader.close();
    }

    /**
     * Feeds all bytes from the given array into the pipe one-by-one, asserting before each byte that the reader
     * is not positioned on a value.
     * @param bytes the bytes to be fed.
     * @param pipe the pipe into which the bytes are fed, and from which the reader consumes bytes.
     * @param reader an incremental reader.
     */
    private void feedBytesOneByOne(byte[] bytes, ResizingPipedInputStream pipe, IonReader reader) {
        for (byte b : bytes) {
            nextExpect(null);
            pipe.receive(b);
        }
    }

    @Test
    public void incrementalValue() throws Exception {
        ResizingPipedInputStream pipe = new ResizingPipedInputStream(128);
        reader = readerFor(pipe);
        byte[] bytes = toBinary("\"StringValueLong\"");
        totalBytesInStream = bytes.length;
        feedBytesOneByOne(bytes, pipe, reader);
        assertSequence(
            next(IonType.STRING), stringValue("StringValueLong"),
            next(null)
        );
        closeAndCount();
    }

    @Test
    public void incrementalMultipleValues() throws Exception {
        ResizingPipedInputStream pipe = new ResizingPipedInputStream(128);
        reader = readerFor(pipe);
        byte[] bytes = toBinary("value_type::\"StringValueLong\"");
        totalBytesInStream = bytes.length;
        feedBytesOneByOne(bytes, pipe, reader);
        assertSequence(
            next(IonType.STRING), annotations("value_type"), stringValue("StringValueLong"),
            next(null)
        );
        assertBytesConsumed();
        bytes = toBinary("{foobar: \"StringValueLong\"}");
        totalBytesInStream += bytes.length;
        feedBytesOneByOne(bytes, pipe, reader);
        assertSequence(
            container(IonType.STRUCT,
                next("foobar", IonType.STRING), stringValue("StringValueLong"),
                next(null)
            ),
            next(null)
        );
        closeAndCount();
    }

    @Test
    public void incrementalMultipleValuesLoadFromReader() throws Exception {
        ResizingPipedInputStream pipe = new ResizingPipedInputStream(128);
        reader = readerFor(pipe);
        final IonLoader loader = SYSTEM.getLoader();
        byte[] bytes = toBinary("value_type::\"StringValueLong\"");
        totalBytesInStream = bytes.length;
        for (byte b : bytes) {
            IonDatagram empty = loader.load(reader);
            assertTrue(empty.isEmpty());
            pipe.receive(b);
        }
        IonDatagram firstValue = loader.load(reader);
        assertEquals(1, firstValue.size());
        IonString string = (IonString) firstValue.get(0);
        assertEquals("StringValueLong", string.stringValue());
        assertEquals(Collections.singletonList("value_type"), Arrays.asList(string.getTypeAnnotations()));
        assertBytesConsumed();
        bytes = toBinary("{foobar: \"StringValueLong\"}");
        totalBytesInStream += bytes.length;
        for (byte b : bytes) {
            IonDatagram empty = loader.load(reader);
            assertTrue(empty.isEmpty());
            pipe.receive(b);
        }
        IonDatagram secondValue = loader.load(reader);
        assertEquals(1, secondValue.size());
        IonStruct struct = (IonStruct) secondValue.get(0);
        string = (IonString) struct.get("foobar");
        assertEquals("StringValueLong", string.stringValue());
        IonDatagram empty = loader.load(reader);
        assertTrue(empty.isEmpty());
        closeAndCount();
    }

    @Test
    public void incrementalMultipleValuesLoadFromInputStreamFails() throws Exception {
        final ResizingPipedInputStream pipe = new ResizingPipedInputStream(1);
        final IonLoader loader = IonSystemBuilder.standard()
            .withReaderBuilder(readerBuilder)
            .build()
            .getLoader();
        IonDatagram empty = loader.load(pipe);
        assertTrue(empty.isEmpty());
        pipe.receive(_Private_IonConstants.BINARY_VERSION_MARKER_1_0[0]);
        // Because reader does not persist across load invocations, the loader must throw an exception if the reader
        // had an incomplete value buffered.
        assertThrows(IonException.class, () -> loader.load(pipe));
    }

    private void incrementalMultipleValuesIterate(Iterator<IonValue> iterator, ResizingPipedInputStream pipe) {
        byte[] bytes = toBinary("value_type::\"StringValueLong\"");
        totalBytesInStream = bytes.length;
        for (byte b : bytes) {
            assertFalse(iterator.hasNext());
            pipe.receive(b);
        }
        assertTrue(iterator.hasNext());
        IonString string = (IonString) iterator.next();
        assertEquals("StringValueLong", string.stringValue());
        assertEquals(Collections.singletonList("value_type"), Arrays.asList(string.getTypeAnnotations()));
        assertFalse(iterator.hasNext());
        assertBytesConsumed();
        bytes = toBinary("{foobar: \"StringValueLong\"}");
        totalBytesInStream += bytes.length;
        for (byte b : bytes) {
            assertFalse(iterator.hasNext());
            pipe.receive(b);
        }
        assertTrue(iterator.hasNext());
        IonStruct struct = (IonStruct) iterator.next();
        string = (IonString) struct.get("foobar");
        assertEquals("StringValueLong", string.stringValue());
        assertFalse(iterator.hasNext());
        assertBytesConsumed();
    }

    @Test
    public void incrementalMultipleValuesIterateFromReader() throws Exception {
        ResizingPipedInputStream pipe = new ResizingPipedInputStream(128);
        IonReader reader = readerBuilder.build(pipe);
        Iterator<IonValue> iterator = SYSTEM.iterate(reader);
        incrementalMultipleValuesIterate(iterator, pipe);
        reader.close();
    }

    @Test
    public void incrementalMultipleValuesIterateFromInputStream() {
        ResizingPipedInputStream pipe = new ResizingPipedInputStream(128);
        IonSystem system = IonSystemBuilder.standard().withReaderBuilder(readerBuilder).build();
        Iterator<IonValue> iterator = system.iterate(pipe);
        incrementalMultipleValuesIterate(iterator, pipe);
    }


    @Test
    public void incrementalReadInitiallyEmptyStreamThatTurnsOutToBeText() {
        // Note: if incremental text read support is added, this test will start failing, which is expected. For now,
        // we ensure that this fails quickly.
        ResizingPipedInputStream pipe = new ResizingPipedInputStream(1);
        reader = readerBuilder.build(pipe);
        assertSequence(next(null));
        // Valid text Ion. Also hex 0x20, which is binary int 0. However, it is not preceded by the IVM, so it must be
        // interpreted as text. The binary reader must fail.
        pipe.receive(' ');
        assertThrows(IonException.class, () -> reader.next());
    }

    @Test
    public void incrementalSymbolTables() throws Exception {
        ResizingPipedInputStream pipe = new ResizingPipedInputStream(128);
        byte[] firstValue = writeRaw(
            (writer, out) -> {
                writer.setTypeAnnotationSymbols(3);
                writer.stepIn(IonType.STRUCT);
                writer.setFieldNameSymbol(7);
                writer.stepIn(IonType.LIST);
                writer.writeString("abcdefghijklmnopqrstuvwxyz");
                writer.writeString("def");
                writer.stepOut();
                writer.stepOut();
                writer.stepIn(IonType.STRUCT);
                writer.setTypeAnnotationSymbols(11);
                writer.setFieldNameSymbol(10);
                writer.writeString("foo");
                writer.stepOut();
                writer.close();
            },
            true
        );
        byte[] secondValue = writeRaw(
            (writer, out) -> {
                writer.setTypeAnnotationSymbols(3);
                writer.stepIn(IonType.STRUCT);
                writer.setFieldNameSymbol(6);
                writer.writeSymbolToken(3);
                writer.setFieldNameSymbol(7);
                writer.stepIn(IonType.LIST);
                writer.writeString("foo");
                writer.writeString("bar");
                writer.stepOut();
                writer.stepOut();
                writer.stepIn(IonType.STRUCT);
                writer.setFieldNameSymbol(10);
                writer.setTypeAnnotationSymbols(12, 13);
                writer.writeString("fairlyLongString");
                writer.stepOut();
                writer.close();
            },
            false
        );

        reader = readerFor(pipe);
        feedBytesOneByOne(firstValue, pipe, reader);
        totalBytesInStream = firstValue.length;
        assertSequence(
            container(IonType.STRUCT,
                next("abcdefghijklmnopqrstuvwxyz", IonType.STRING), annotations("def"), stringValue("foo"),
                next(null)
            )
        );
        assertBytesConsumed();
        feedBytesOneByOne(secondValue, pipe, reader);
        totalBytesInStream += secondValue.length;
        assertSequence(
            container(IonType.STRUCT,
                next("abcdefghijklmnopqrstuvwxyz", IonType.STRING), annotations("foo", "bar"), stringValue("fairlyLongString")    ,
                next(null)
            ),
            next(null)
        );
        closeAndCount();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void lobsNewBytes(boolean constructFromBytes) throws Exception {
        final byte[] blobBytes = "abcdef".getBytes(StandardCharsets.UTF_8);
        final byte[] clobBytes = "ghijklmnopqrstuv".getBytes(StandardCharsets.UTF_8);
        reader = readerFor(
            writer -> {
                writer.writeBlob(blobBytes);
                writer.writeClob(clobBytes);
                writer.setTypeAnnotations("foo");
                writer.writeBlob(blobBytes);
                writer.stepIn(IonType.STRUCT);
                writer.setFieldName("bar");
                writer.writeClob(clobBytes);
                writer.stepOut();
            },
            constructFromBytes
        );
        assertSequence(
            next(IonType.BLOB), newBytesValue(blobBytes),
            next(IonType.CLOB), newBytesValue(clobBytes),
            next(IonType.BLOB), annotations("foo"), newBytesValue(blobBytes),
            container(IonType.STRUCT,
                next("bar", IonType.CLOB), newBytesValue(clobBytes),
                next(null)
            ),
            next(null)
        );
        closeAndCount();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void lobsGetBytes(boolean constructFromBytes) throws Exception {
        final byte[] blobBytes = "abcdef".getBytes(StandardCharsets.UTF_8);
        final byte[] clobBytes = "ghijklmnopqrstuv".getBytes(StandardCharsets.UTF_8);
        reader = readerFor(
            writer -> {
                writer.writeBlob(blobBytes);
                writer.writeClob(clobBytes);
                writer.setTypeAnnotations("foo");
                writer.writeBlob(blobBytes);
                writer.stepIn(IonType.STRUCT);
                writer.setFieldName("bar");
                writer.writeClob(clobBytes);
                writer.stepOut();
            },
            constructFromBytes
        );

        byte[] fullBlob = new byte[blobBytes.length];
        byte[] partialClob = new byte[clobBytes.length];
        assertSequence(
            next(IonType.BLOB), getBytesValue(fullBlob, 0, fullBlob.length, fullBlob.length)
        );
        assertArrayEquals(blobBytes, fullBlob);
        assertSequence(
            next(IonType.CLOB), getBytesValue(partialClob, 0, 3, 3), getBytesValue(partialClob, 3, clobBytes.length - 3, clobBytes.length - 3)
        );
        assertArrayEquals(clobBytes, partialClob);
        Arrays.fill(fullBlob, (byte) 0);
        Arrays.fill(partialClob, (byte) 0);
        assertSequence(
            next(IonType.BLOB), annotations("foo"), getBytesValue(fullBlob, 0, 100000, fullBlob.length),
            container(IonType.STRUCT,
                next(IonType.CLOB), getBytesValue(partialClob, 0, 5, 5), getBytesValue(partialClob, 5, 100000, clobBytes.length - 5),
                next(null)
            ),
            next(null)
        );
        assertArrayEquals(clobBytes, partialClob);
        closeAndCount();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void nopPad(boolean constructFromBytes) throws Exception {
        reader = readerFor(
            constructFromBytes,
            // One byte no-op pad.
            0x00,
            // Two byte no-op pad.
            0x01, 0xFF,
            // Int 0.
            0x20,
            // Three byte no-op pad.
            0x02, 0x99, 0x42,
            // 16-byte no-op pad.
            0x0E, 0x8E,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // Int 1.
            0x21, 0x01,
            // Struct with no-op pad at the start.
            0xD9,
            // Field SID 0.
            0x80,
            // Five byte no-op pad.
            0x04, 0x00, 0x00, 0x00, 0x00,
            // Field SID 4 ("name").
            0x84,
            // Int -1.
            0x31, 0x01,
            // Struct (empty) with no-op pad at the end.
            0xD8,
            // Field SID 0.
            0x80,
            // Seven byte no-op pad.
            0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // List (empty) with long no-op pad.
            0xBE,
            // Length 16.
            0x90,
            // 16-byte no-op pad.
            0x0E, 0x8E,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
        );
        assertSequence(
            next(IonType.INT), intValue(0),
            next(IonType.INT), intValue(1),
            container(IonType.STRUCT,
                next(IonType.INT), intValue(-1),
                next(null)
            ),
            container(IonType.STRUCT,
                next(null)
            ),
            container(IonType.LIST,
                next(null)
            ),
            next(null)
        );
        closeAndCount();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void symbolTableWithImportsThenSymbols(boolean constructFromBytes) throws Exception {
        SimpleCatalog catalog = new SimpleCatalog();
        catalog.putTable(SYSTEM.newSharedSymbolTable("foo", 1, Arrays.asList("abc", "def").iterator()));
        readerBuilder = readerBuilder.withCatalog(catalog);
        writerBuilder = writerBuilder.withCatalog(catalog);

        reader = readerFor(
            writer -> {
                writer.setTypeAnnotations("$ion_symbol_table");
                writer.stepIn(IonType.STRUCT);
                writer.setFieldName("imports");
                writer.stepIn(IonType.LIST);
                writer.stepIn(IonType.STRUCT);
                writer.setFieldName("name");
                writer.writeString("foo");
                writer.setFieldName("version");
                writer.writeInt(1);
                writer.setFieldName("max_id");
                writer.writeInt(2);
                writer.stepOut();
                writer.stepOut();
                writer.setFieldName("symbols");
                writer.stepIn(IonType.LIST);
                writer.writeString("ghi");
                writer.stepOut();
                writer.stepOut();
                writer.writeSymbol("abc");
                writer.writeSymbol("def");
                writer.writeSymbol("ghi");
            },
            constructFromBytes
        );
        assertSequence(
            next(IonType.SYMBOL), stringValue("abc"),
            next(IonType.SYMBOL), stringValue("def"),
            next(IonType.SYMBOL), stringValue("ghi"),
            next(null)
        );
        closeAndCount();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void symbolTableWithSymbolsThenImports(boolean constructFromBytes) throws Exception {
        SimpleCatalog catalog = new SimpleCatalog();
        catalog.putTable(SYSTEM.newSharedSymbolTable("foo", 1, Arrays.asList("abc", "def").iterator()));
        readerBuilder = readerBuilder.withCatalog(catalog);

        reader = readerFor(
            (writer, out) -> {
                SymbolTable systemTable = SharedSymbolTable.getSystemSymbolTable(1);
                writer.addTypeAnnotationSymbol(systemTable.findSymbol("$ion_symbol_table"));
                writer.stepIn(IonType.STRUCT);
                writer.setFieldNameSymbol(systemTable.findSymbol("symbols"));
                writer.stepIn(IonType.LIST);
                writer.writeString("ghi");
                writer.stepOut();
                writer.setFieldNameSymbol(systemTable.findSymbol("imports"));
                writer.stepIn(IonType.LIST);
                writer.stepIn(IonType.STRUCT);
                writer.setFieldNameSymbol(systemTable.findSymbol("name"));
                writer.writeString("foo");
                writer.setFieldNameSymbol(systemTable.findSymbol("version"));
                writer.writeInt(1);
                writer.setFieldNameSymbol(systemTable.findSymbol("max_id"));
                writer.writeInt(2);
                writer.stepOut();
                writer.stepOut();
                writer.stepOut();
                writer.writeSymbolToken(10);
                writer.writeSymbolToken(11);
                writer.writeSymbolToken(12);
            },
            constructFromBytes
        );
        assertSequence(
            next(IonType.SYMBOL), stringValue("abc"),
            next(IonType.SYMBOL), stringValue("def"),
            next(IonType.SYMBOL), stringValue("ghi"),
            next(null)
        );
        closeAndCount();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void symbolTableWithManySymbolsThenImports(boolean constructFromBytes) throws Exception {
        SimpleCatalog catalog = new SimpleCatalog();
        catalog.putTable(SYSTEM.newSharedSymbolTable("foo", 1, Arrays.asList("abc", "def").iterator()));
        readerBuilder = readerBuilder.withCatalog(catalog);

        reader = readerFor(
            (writer, out) -> {
                SymbolTable systemTable = SharedSymbolTable.getSystemSymbolTable(1);
                writer.addTypeAnnotationSymbol(systemTable.findSymbol("$ion_symbol_table"));
                writer.stepIn(IonType.STRUCT);
                writer.setFieldNameSymbol(systemTable.findSymbol("symbols"));
                writer.stepIn(IonType.LIST);
                writer.writeString("ghi");
                writer.writeString("jkl");
                writer.writeString("mno");
                writer.writeString("pqr");
                writer.stepOut();
                writer.setFieldNameSymbol(systemTable.findSymbol("imports"));
                writer.stepIn(IonType.LIST);
                writer.stepIn(IonType.STRUCT);
                writer.setFieldNameSymbol(systemTable.findSymbol("name"));
                writer.writeString("foo");
                writer.setFieldNameSymbol(systemTable.findSymbol("version"));
                writer.writeInt(1);
                writer.setFieldNameSymbol(systemTable.findSymbol("max_id"));
                writer.writeInt(2);
                writer.stepOut();
                writer.stepOut();
                writer.stepOut();
                writer.writeSymbolToken(10);
                writer.writeSymbolToken(11);
                writer.writeSymbolToken(12);
                writer.writeSymbolToken(13);
                writer.writeSymbolToken(14);
                writer.writeSymbolToken(15);
            },
            constructFromBytes
        );
        assertSequence(
            next(IonType.SYMBOL), stringValue("abc"),
            next(IonType.SYMBOL), stringValue("def"),
            next(IonType.SYMBOL), stringValue("ghi"),
            next(IonType.SYMBOL), stringValue("jkl"),
            next(IonType.SYMBOL), stringValue("mno"),
            next(IonType.SYMBOL), stringValue("pqr"),
            next(null)
        );
        closeAndCount();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void multipleSymbolTablesWithSymbolsThenImports(boolean constructFromBytes) throws Exception {
        SimpleCatalog catalog = new SimpleCatalog();
        catalog.putTable(SYSTEM.newSharedSymbolTable("foo", 1, Arrays.asList("abc", "def").iterator()));
        catalog.putTable(SYSTEM.newSharedSymbolTable("bar", 1, Collections.singletonList("baz").iterator()));
        readerBuilder = readerBuilder.withCatalog(catalog);

        reader = readerFor(
            (writer, out) -> {
                SymbolTable systemTable = SharedSymbolTable.getSystemSymbolTable(1);
                writer.addTypeAnnotationSymbol(systemTable.findSymbol("$ion_symbol_table"));
                writer.stepIn(IonType.STRUCT);
                writer.setFieldNameSymbol(systemTable.findSymbol("symbols"));
                writer.stepIn(IonType.LIST);
                writer.writeString("ghi");
                writer.stepOut();
                writer.setFieldNameSymbol(systemTable.findSymbol("imports"));
                writer.stepIn(IonType.LIST);
                writer.stepIn(IonType.STRUCT);
                writer.setFieldNameSymbol(systemTable.findSymbol("name"));
                writer.writeString("foo");
                writer.setFieldNameSymbol(systemTable.findSymbol("version"));
                writer.writeInt(1);
                writer.setFieldNameSymbol(systemTable.findSymbol("max_id"));
                writer.writeInt(2);
                writer.stepOut();
                writer.stepOut();
                writer.stepOut();
                writer.writeSymbolToken(10);
                writer.writeSymbolToken(11);
                writer.writeSymbolToken(12);
                writer.addTypeAnnotationSymbol(systemTable.findSymbol("$ion_symbol_table"));
                writer.stepIn(IonType.STRUCT);
                writer.setFieldNameSymbol(systemTable.findSymbol("symbols"));
                writer.stepIn(IonType.LIST);
                writer.writeString("xyz");
                writer.writeString("uvw");
                writer.writeString("rst");
                writer.stepOut();
                writer.setFieldNameSymbol(systemTable.findSymbol("imports"));
                writer.stepIn(IonType.LIST);
                writer.stepIn(IonType.STRUCT);
                writer.setFieldNameSymbol(systemTable.findSymbol("name"));
                writer.writeString("bar");
                writer.setFieldNameSymbol(systemTable.findSymbol("version"));
                writer.writeInt(1);
                writer.setFieldNameSymbol(systemTable.findSymbol("max_id"));
                writer.writeInt(1);
                writer.stepOut();
                writer.stepOut();
                writer.stepOut();
                writer.writeSymbolToken(10);
                writer.writeSymbolToken(11);
                writer.writeSymbolToken(12);
                writer.writeSymbolToken(13);
            },
            constructFromBytes
        );
        assertSequence(
            next(IonType.SYMBOL), stringValue("abc")
        );
        SymbolTable[] imports = reader.getSymbolTable().getImportedTables();
        assertEquals(1, imports.length);
        assertEquals("foo", imports[0].getName());
        assertSequence(
            next(IonType.SYMBOL), stringValue("def"),
            next(IonType.SYMBOL), stringValue("ghi"),
            next(IonType.SYMBOL), stringValue("baz"),
            next(IonType.SYMBOL)
        );
        imports = reader.getSymbolTable().getImportedTables();
        assertEquals(1, imports.length);
        assertEquals("bar", imports[0].getName());
        assertSequence(
            stringValue("xyz"),
            next(IonType.SYMBOL), stringValue("uvw"),
            next(IonType.SYMBOL), stringValue("rst"),
            next(null)
        );
        closeAndCount();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void ivmResetsImports(boolean constructFromBytes) throws Exception {
        SimpleCatalog catalog = new SimpleCatalog();
        catalog.putTable(SYSTEM.newSharedSymbolTable("foo", 1, Arrays.asList("abc", "def").iterator()));
        readerBuilder = readerBuilder.withCatalog(catalog);

        reader = readerFor(
            (writer, out) -> {
                SymbolTable systemTable = SharedSymbolTable.getSystemSymbolTable(1);
                writer.addTypeAnnotationSymbol(systemTable.findSymbol("$ion_symbol_table"));
                writer.stepIn(IonType.STRUCT);
                writer.setFieldNameSymbol(systemTable.findSymbol("symbols"));
                writer.stepIn(IonType.LIST);
                writer.writeString("ghi");
                writer.stepOut();
                writer.setFieldNameSymbol(systemTable.findSymbol("imports"));
                writer.stepIn(IonType.LIST);
                writer.stepIn(IonType.STRUCT);
                writer.setFieldNameSymbol(systemTable.findSymbol("name"));
                writer.writeString("foo");
                writer.setFieldNameSymbol(systemTable.findSymbol("version"));
                writer.writeInt(1);
                writer.setFieldNameSymbol(systemTable.findSymbol("max_id"));
                writer.writeInt(2);
                writer.stepOut();
                writer.stepOut();
                writer.stepOut();
                writer.writeSymbolToken(10);
                writer.writeSymbolToken(11);
                writer.writeSymbolToken(12);
                writer.close();
                out.write(_Private_IonConstants.BINARY_VERSION_MARKER_1_0);
                out.write(0x20);
            },
            constructFromBytes
        );
        assertSequence(
            next(IonType.SYMBOL), stringValue("abc")
        );
        SymbolTable[] imports = reader.getSymbolTable().getImportedTables();
        assertEquals(1, imports.length);
        assertEquals("foo", imports[0].getName());
        assertSequence(
            next(IonType.SYMBOL), stringValue("def"),
            next(IonType.SYMBOL), stringValue("ghi"),
            next(IonType.INT)
        );
        assertTrue(reader.getSymbolTable().isSystemTable());
        assertSequence(
            intValue(0),
            next(null)
        );
        closeAndCount();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void symbolsAsTokens(boolean constructFromBytes) throws Exception {
        reader = readerFor("{foo: uvw::abc, bar: qrs::xyz::def}", constructFromBytes);
        assertSequence(
            container(IonType.STRUCT,
                next(IonType.SYMBOL), fieldNameSymbol("foo"), annotationSymbols("uvw"), symbolValue("abc"),
                next(IonType.SYMBOL), fieldNameSymbol("bar"), annotationSymbols("qrs", "xyz"), symbolValue("def")
            ),
            next(null)
        );
        closeAndCount();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void intNegativeZeroFails(boolean constructFromBytes) throws Exception {
        reader = readerFor(constructFromBytes, 0x31, 0x00);
        reader.next();
        assertThrows(IonException.class, () -> reader.longValue());
        reader.close();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void bigIntNegativeZeroFails(boolean constructFromBytes) throws Exception {
        reader = readerFor(constructFromBytes, 0x31, 0x00);
        reader.next();
        assertThrows(IonException.class, () -> reader.bigIntegerValue());
        reader.close();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void listWithLengthTooShortFails(boolean constructFromBytes) throws Exception {
        reader = readerFor(constructFromBytes, 0xB1, 0x21, 0x01);
        nextExpect(IonType.LIST);
        stepIn();
        assertThrows(IonException.class, () -> reader.next());
        reader.close();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void listWithContainerValueLengthTooShortFails(boolean constructFromBytes) throws Exception {
        reader = readerFor(constructFromBytes, 0xB2, 0xB2, 0x21, 0x01);
        nextExpect(IonType.LIST);
        stepIn();
        assertThrows(IonException.class, () -> reader.next());
        reader.close();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void listWithVariableLengthTooShortFails(boolean constructFromBytes) throws Exception {
        reader = readerFor(constructFromBytes, 0xBE, 0x81, 0x21, 0x01);
        nextExpect(IonType.LIST);
        stepIn();
        assertThrows(IonException.class, () -> reader.next());
        reader.close();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void noOpPadTooShort1(boolean constructFromBytes) throws Exception {
        reader = readerFor(constructFromBytes, 0x37, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01);
        nextExpect(IonType.INT);
        assertThrows(IonException.class, () -> {
            reader.next();
            reader.close();
        });
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void noOpPadTooShort2(boolean constructFromBytes) throws Exception {
        reader = readerFor(
            constructFromBytes,
            0x0e, 0x90, 0x00, 0xde, 0xad, 0xbe, 0xef, 0xca, 0xfe, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
        );
        assertThrows(IonException.class, () -> {
            reader.next();
            reader.close();
        });
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void nopPadOneByte(boolean constructFromBytes) throws Exception {
        reader = readerFor(constructFromBytes, 0);
        assertSequence(next(null));
        closeAndCount();
        reader.close();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void localSidOutOfRangeStringValue(boolean constructFromBytes) throws Exception {
        reader = readerFor(constructFromBytes, 0x71, 0x0A); // SID 10
        nextExpect(IonType.SYMBOL);
        assertThrows(IonException.class, () -> reader.stringValue());
        reader.close();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void localSidOutOfRangeSymbolValue(boolean constructFromBytes) throws Exception {
        reader = readerFor(constructFromBytes, 0x71, 0x0A); // SID 10
        nextExpect(IonType.SYMBOL);
        assertThrows(IonException.class, () -> reader.symbolValue());
        reader.close();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void localSidOutOfRangeFieldName(boolean constructFromBytes) throws Exception {
        reader = readerFor(
            constructFromBytes,
            0xD2, // Struct, length 2
            0x8A, // SID 10
            0x20 // int 0
        );
        nextExpect(IonType.STRUCT);
        stepIn();
        nextExpect(IonType.INT);
        assertThrows(IonException.class, () -> reader.getFieldName());
        reader.close();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void localSidOutOfRangeFieldNameSymbol(boolean constructFromBytes) throws Exception {
        reader = readerFor(
            constructFromBytes,
            0xD2, // Struct, length 2
            0x8A, // SID 10
            0x20 // int 0
        );
        nextExpect(IonType.STRUCT);
        stepIn();
        nextExpect(IonType.INT);
        assertThrows(IonException.class, () -> reader.getFieldNameSymbol());
        reader.close();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void localSidOutOfRangeAnnotation(boolean constructFromBytes) throws Exception {
        reader = readerFor(
            constructFromBytes,
            0xE3, // Annotation wrapper, length 3
            0x81, // annotation SID length 1
            0x8A, // SID 10
            0x20 // int 0
        );
        nextExpect(IonType.INT);
        assertThrows(IonException.class, () -> reader.getTypeAnnotations());
        reader.close();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void localSidOutOfRangeAnnotationSymbol(boolean constructFromBytes) throws Exception {
        reader = readerFor(
            constructFromBytes,
            0xE3, // Annotation wrapper, length 3
            0x81, // annotation SID length 1
            0x8A, // SID 10
            0x20 // int 0
        );
        nextExpect(IonType.INT);
        assertThrows(IonException.class, () -> reader.getTypeAnnotationSymbols());
        reader.close();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void localSidOutOfRangeIterateAnnotations(boolean constructFromBytes) throws Exception {
        reader = readerFor(
            constructFromBytes,
            0xE3, // Annotation wrapper, length 3
            0x81, // annotation SID length 1
            0x8A, // SID 10
            0x20 // int 0
        );
        nextExpect(IonType.INT);
        Iterator<String> annotationIterator = reader.iterateTypeAnnotations();
        assertThrows(IonException.class, annotationIterator::next);
        reader.close();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void stepInOnScalarFails(boolean constructFromBytes) throws Exception {
        reader = readerFor(constructFromBytes, 0x20);
        nextExpect(IonType.INT);
        assertThrows(IllegalStateException.class, reader::stepIn);
        reader.close();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void stepInBeforeNextFails(boolean constructFromBytes) throws Exception {
        reader = readerFor(constructFromBytes, 0xD2, 0x84, 0xD0);
        reader.next();
        stepIn();
        assertThrows(IllegalStateException.class, reader::stepIn);
        reader.close();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void stepOutAtDepthZeroFails(boolean constructFromBytes) throws Exception {
        reader = readerFor(constructFromBytes, 0x20);
        reader.next();
        assertThrows(IllegalStateException.class, reader::stepOut);
        reader.close();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void byteSizeNotOnLobFails(boolean constructFromBytes) throws Exception {
        reader = readerFor(constructFromBytes, 0x20);
        reader.next();
        assertThrows(IonException.class, () -> reader.byteSize());
        reader.close();
    }

    /**
     * Verifies that the reader can read Ion int values of all sizes into the Java type T.
     */
    @SafeVarargs
    private final <T> void readIntsIntoOtherType(
        boolean constructFromBytes,
        Function<T, ExpectationProvider<IonReaderContinuableTopLevelBinary>> assertionFunction,
        T... expectedValues
    ) throws Exception {
        reader = readerFor(
        "0 " +
            "1 " +
            "-1 " +
            "2147483647 " +
            "2147483648 " +
            "-2147483648 " +
            "-2147483649 " +
            "9223372036854775807 " +
            "9223372036854775808 " +
            "-9223372036854775808 " +
            "-9223372036854775809",
            constructFromBytes
        );
        List<ExpectationProvider<IonReaderContinuableTopLevelBinary>> assertions = new ArrayList<>();
        for (T expectedValue : expectedValues) {
            assertions.add(next(IonType.INT));
            assertions.add(assertionFunction.apply(expectedValue));
        }
        assertions.add(next(null));
        assertSequence(assertions.toArray(new ExpectationProvider[0]));
        closeAndCount();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void doubleValueOnInt(boolean constructFromBytes) throws Exception {
        readIntsIntoOtherType(
            constructFromBytes,
            IonReaderContinuableTopLevelBinaryTest::doubleValue,
            0.0,
            1.0,
            -1.0,
            (double) Integer.MAX_VALUE,
            (double) (((long) Integer.MAX_VALUE) + 1),
            (double) Integer.MIN_VALUE,
            (double) (((long) Integer.MIN_VALUE) - 1),
            (double) Long.MAX_VALUE,
            ((double) Long.MAX_VALUE) + 1,
            (double) Long.MIN_VALUE,
            ((double) Long.MIN_VALUE) - 1
        );
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void decimalValueOnInt(boolean constructFromBytes) throws Exception {
        readIntsIntoOtherType(
            constructFromBytes,
            IonReaderContinuableTopLevelBinaryTest::decimalValue,
            Decimal.ZERO,
            Decimal.ONE,
            Decimal.valueOf(-1),
            Decimal.valueOf(Integer.MAX_VALUE),
            Decimal.valueOf(((long) Integer.MAX_VALUE) + 1),
            Decimal.valueOf(Integer.MIN_VALUE),
            Decimal.valueOf(((long) Integer.MIN_VALUE) - 1),
            Decimal.valueOf(Long.MAX_VALUE),
            Decimal.valueOf(Long.MAX_VALUE).add(Decimal.ONE),
            Decimal.valueOf(Long.MIN_VALUE),
            Decimal.valueOf(Long.MIN_VALUE).subtract(Decimal.ONE)
        );
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void bigDecimalValueOnInt(boolean constructFromBytes) throws Exception {
        readIntsIntoOtherType(
            constructFromBytes,
            IonReaderContinuableTopLevelBinaryTest::bigDecimalValue,
            BigDecimal.ZERO,
            BigDecimal.ONE,
            BigDecimal.valueOf(-1),
            BigDecimal.valueOf(Integer.MAX_VALUE),
            BigDecimal.valueOf(((long) Integer.MAX_VALUE) + 1),
            BigDecimal.valueOf(Integer.MIN_VALUE),
            BigDecimal.valueOf(((long) Integer.MIN_VALUE) - 1),
            BigDecimal.valueOf(Long.MAX_VALUE),
            BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE),
            BigDecimal.valueOf(Long.MIN_VALUE),
            BigDecimal.valueOf(Long.MIN_VALUE).subtract(BigDecimal.ONE)
        );
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void floatWithInvalidLengthFails(boolean constructFromBytes) throws Exception {
        reader = readerFor(constructFromBytes, 0x43, 0x01, 0x02, 0x03);
        assertThrows(IonException.class, () -> {
            reader.next();
            reader.close();
        });
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void invalidTypeIdFFailsAtTopLevel(boolean constructFromBytes) throws Exception {
        reader = readerFor(constructFromBytes, 0xF0);
        assertThrows(IonException.class, () -> {
            reader.next();
            reader.close();
        });
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void invalidTypeIdFFailsBelowTopLevel(boolean constructFromBytes) throws Exception {
        reader = readerFor(constructFromBytes, 0xB1, 0xF0);
        reader.next();
        stepIn();
        assertThrows(IonException.class, () -> reader.next());
        reader.close();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void reallyLargeString(boolean constructFromBytes) throws Exception {
        StringBuilder sb = new StringBuilder();
        // 8192 is a arbitrarily large; it requires a couple bytes of length, and it doesn't fit in the preallocated
        // string decoding buffer of size 4096.
        for (int i = 0; i < 8192; i++) {
            sb.append('a');
        }
        String string = sb.toString();
        reader = readerFor("\"" + string + "\"", constructFromBytes);
        assertSequence(
            next(IonType.STRING), stringValue(string),
            next(null)
        );
        closeAndCount();
    }

    @ParameterizedTest
    @CsvSource({
        "true, true",
        "true, false",
        "false, true",
        "false, false"
    })
    public void skipReallyLargeStringInContainer(boolean constructFromBytes, boolean incremental) throws Exception {
        StringBuilder sb = new StringBuilder();
        // 70000 is greater than twice the default buffer size of 32768. This ensures that the string, when skipped,
        // will not be fully contained in the buffer.
        for (int i = 0; i < 70000; i++) {
            sb.append('a');
        }
        String string = sb.toString();
        readerBuilder = readerBuilder.withIncrementalReadingEnabled(incremental);
        reader = readerFor("{foo: \"" + string + "\"}", constructFromBytes);
        assertSequence(
            container(IonType.STRUCT,
                next(IonType.STRING)
                // This is an early-step out that causes the string value to be skipped.
            ),
            next(null)
        );
        closeAndCount();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void nopPadInAnnotationWrapperFails(boolean constructFromBytes) throws Exception {
        reader = readerFor(
            constructFromBytes,
            0xB5, // list
            0xE4, // annotation wrapper
            0x81, // 1 byte of annotations
            0x84, // annotation: "name"
            0x01, // 2-byte no-op pad
            0x00
        );
        reader.next();
        stepIn();
        assertThrows(IonException.class, () -> reader.next());
        reader.close();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void nestedAnnotationWrapperFails(boolean constructFromBytes) throws Exception {
        reader = readerFor(
            constructFromBytes,
            0xB5, // list
            0xE4, // annotation wrapper
            0x81, // 1 byte of annotations
            0x84, // annotation: "name"
            0xE3, // annotation wrapper
            0x81, // 1 byte of annotations
            0x84, // annotation: "name"
            0x20  // int 0
        );
        reader.next();
        stepIn();
        assertThrows(IonException.class, () -> reader.next());
        reader.close();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void annotationWrapperLengthMismatchFails(boolean constructFromBytes) throws Exception {
        reader = readerFor(
            constructFromBytes,
            0xB5, // list
            0xE4, // annotation wrapper
            0x81, // 1 byte of annotations
            0x84, // annotation: "name"
            0x20, // int 0
            0x20  // next value
        );
        reader.next();
        stepIn();
        assertThrows(IonException.class, () -> reader.next());
        reader.close();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void annotationWrapperVariableLengthMismatchFails(boolean constructFromBytes) throws Exception {
        reader = readerFor(
            constructFromBytes,
            0xBE, // list
            0x90, // Length 16
            0xEE, // annotation wrapper
            0x8E, // Length 14
            0x81, // 1 byte of annotations
            0x84, // annotation: "name"
            0x89, // String length 9 (should be 11)
            '1', '2', '3', '4', '5', '6', '7', '8', // String value
            0x21, 0x01, // next value
            0x20 // Another value
        );
        reader.next();
        stepIn();
        assertThrows(IonException.class, () -> reader.next());
        reader.close();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void multipleSymbolTableImportsFieldsFails(boolean constructFromBytes) throws Exception {
        reader = readerFor(
            (writer, out) -> {
                SymbolTable systemTable = SharedSymbolTable.getSystemSymbolTable(1);
                writer.addTypeAnnotationSymbol(systemTable.findSymbol("$ion_symbol_table"));
                writer.stepIn(IonType.STRUCT);
                writer.setFieldNameSymbol(systemTable.findSymbol("imports"));
                writer.stepIn(IonType.LIST);
                writer.stepIn(IonType.STRUCT);
                writer.setFieldNameSymbol(systemTable.findSymbol("name"));
                writer.writeString("bar");
                writer.setFieldNameSymbol(systemTable.findSymbol("version"));
                writer.writeInt(1);
                writer.setFieldNameSymbol(systemTable.findSymbol("max_id"));
                writer.writeInt(1);
                writer.stepOut();
                writer.stepOut();
                writer.setFieldNameSymbol(systemTable.findSymbol("symbols"));
                writer.stepIn(IonType.LIST);
                writer.writeString("ghi");
                writer.stepOut();
                writer.setFieldNameSymbol(systemTable.findSymbol("imports"));
                writer.stepIn(IonType.LIST);
                writer.stepIn(IonType.STRUCT);
                writer.setFieldNameSymbol(systemTable.findSymbol("name"));
                writer.writeString("foo");
                writer.setFieldNameSymbol(systemTable.findSymbol("version"));
                writer.writeInt(1);
                writer.setFieldNameSymbol(systemTable.findSymbol("max_id"));
                writer.writeInt(2);
                writer.stepOut();
                writer.stepOut();
                writer.stepOut();
                writer.writeSymbolToken(10);
            },
            constructFromBytes
        );

        assertThrows(IonException.class, () -> reader.next());
        reader.close();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void multipleSymbolTableSymbolsFieldsFails(boolean constructFromBytes) throws Exception {
        reader = readerFor(
            (writer, out) -> {
                SymbolTable systemTable = SharedSymbolTable.getSystemSymbolTable(1);
                writer.addTypeAnnotationSymbol(systemTable.findSymbol("$ion_symbol_table"));
                writer.stepIn(IonType.STRUCT);
                writer.setFieldNameSymbol(systemTable.findSymbol("imports"));
                writer.stepIn(IonType.LIST);
                writer.stepIn(IonType.STRUCT);
                writer.setFieldNameSymbol(systemTable.findSymbol("name"));
                writer.writeString("bar");
                writer.setFieldNameSymbol(systemTable.findSymbol("version"));
                writer.writeInt(1);
                writer.setFieldNameSymbol(systemTable.findSymbol("max_id"));
                writer.writeInt(1);
                writer.stepOut();
                writer.stepOut();
                writer.setFieldNameSymbol(systemTable.findSymbol("symbols"));
                writer.stepIn(IonType.LIST);
                writer.writeString("ghi");
                writer.stepOut();
                writer.setFieldNameSymbol(systemTable.findSymbol("symbols"));
                writer.stepIn(IonType.LIST);
                writer.writeString("abc");
                writer.stepOut();
                writer.stepOut();
                writer.writeSymbolToken(10);
            },
            constructFromBytes
        );
        
        assertThrows(IonException.class, () -> reader.next());
        reader.close();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void nonStringInSymbolsListCreatesNullSlot(boolean constructFromBytes) throws Exception {
        reader = readerFor((
            writer, out) -> {
                SymbolTable systemTable = SharedSymbolTable.getSystemSymbolTable(1);
                writer.addTypeAnnotationSymbol(systemTable.findSymbol("$ion_symbol_table"));
                writer.stepIn(IonType.STRUCT);
                writer.setFieldNameSymbol(systemTable.findSymbol("symbols"));
                writer.stepIn(IonType.LIST);
                writer.writeString(null);
                writer.writeString("abc");
                writer.writeInt(123);
                writer.stepOut();
                writer.stepOut();
                writer.writeSymbolToken(10);
                writer.writeSymbolToken(11);
                writer.writeSymbolToken(12);
            },
            constructFromBytes
        );
        assertSequence(
            next(IonType.SYMBOL), symbolValue(0),
            next(IonType.SYMBOL), symbolValue("abc"),
            next(IonType.SYMBOL), symbolValue(0),
            next(null)
        );
        closeAndCount();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void symbolTableWithMultipleImportsCorrectlyAssignsImportLocations(boolean constructFromBytes) throws Exception {
        SimpleCatalog catalog = new SimpleCatalog();
        catalog.putTable(SYSTEM.newSharedSymbolTable("foo", 1, Arrays.asList("abc", "def").iterator()));
        catalog.putTable(SYSTEM.newSharedSymbolTable("bar", 1, Arrays.asList("123", "456").iterator()));
        readerBuilder = readerBuilder.withCatalog(catalog);

        reader = readerFor(
            (writer, out) -> {
                SymbolTable systemTable = SharedSymbolTable.getSystemSymbolTable(1);
                writer.addTypeAnnotationSymbol(systemTable.findSymbol("$ion_symbol_table"));
                writer.stepIn(IonType.STRUCT);
                writer.setFieldNameSymbol(systemTable.findSymbol("imports"));
                writer.stepIn(IonType.LIST);
                writer.stepIn(IonType.STRUCT);
                writer.setFieldNameSymbol(systemTable.findSymbol("name"));
                writer.writeString("foo");
                writer.setFieldNameSymbol(systemTable.findSymbol("version"));
                writer.writeInt(1);
                writer.setFieldNameSymbol(systemTable.findSymbol("max_id"));
                writer.writeInt(4); // The matching shared symbol table in the catalog only declares two symbols.
                writer.stepOut();
                writer.stepIn(IonType.STRUCT);
                writer.setFieldNameSymbol(systemTable.findSymbol("name"));
                writer.writeString("bar");
                writer.setFieldNameSymbol(systemTable.findSymbol("version"));
                writer.writeInt(1);
                writer.setFieldNameSymbol(systemTable.findSymbol("max_id"));
                // The matching shared symbol table in the catalog declares two symbols, but only one is used.
                writer.writeInt(1);
                writer.stepOut();
                writer.stepIn(IonType.STRUCT);
                writer.setFieldNameSymbol(systemTable.findSymbol("name"));
                writer.writeString("baz"); // There is no match in the catalog; all symbols have unknown text.
                writer.setFieldNameSymbol(systemTable.findSymbol("version"));
                writer.writeInt(1);
                writer.setFieldNameSymbol(systemTable.findSymbol("max_id"));
                writer.writeInt(2);
                writer.stepOut();
                writer.stepOut();
                writer.stepOut();
                writer.writeSymbolToken(10); // abc
                writer.writeSymbolToken(11); // def
                writer.writeSymbolToken(12); // unknown text, import SID 3 (from foo)
                writer.writeSymbolToken(13); // unknown text, import SID 4 (from foo)
                writer.writeSymbolToken(14); // 123
                writer.writeSymbolToken(15); // unknown text, import SID 1 (from baz)
                writer.writeSymbolToken(16); // unknown text, import SID 2 (from baz)
            },
            constructFromBytes
        );

        assertSequence(
            next(IonType.SYMBOL), stringValue("abc"),
            next(IonType.SYMBOL), stringValue("def"),
            next(IonType.SYMBOL), symbolValue(null),
            next(IonType.SYMBOL), symbolValue(null),
            next(IonType.SYMBOL), stringValue("123"),
            next(IonType.SYMBOL), symbolValue(15),
            next(IonType.SYMBOL), symbolValue(16),
            next(null)
        );
        closeAndCount();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void symbolTableSnapshotImplementsBasicMethods(boolean constructFromBytes) throws Exception {
        reader = readerFor("'abc'", constructFromBytes);
        reader.next();
        SymbolTable symbolTable = reader.getSymbolTable();
        assertNull(symbolTable.getName());
        assertEquals(0, symbolTable.getVersion());
        assertTrue(symbolTable.isLocalTable());
        assertTrue(symbolTable.isReadOnly());
        assertFalse(symbolTable.isSharedTable());
        assertFalse(symbolTable.isSystemTable());
        assertFalse(symbolTable.isSubstitute());
        symbolTable.makeReadOnly();
        assertEquals(10, symbolTable.getMaxId());
        assertEquals("abc", symbolTable.findKnownSymbol(10));
        assertNull(symbolTable.findKnownSymbol(symbolTable.getMaxId() + 1));
        assertThrows(IllegalArgumentException.class, () -> symbolTable.findKnownSymbol(-1));
        reader.close();
    }

    /**
     * Sets `readerBuilder`'s initial and maximum size bounds, and specifies the handler to use.
     * @param initialSize the initial buffer size.
     * @param maximumSize the maximum size to which the buffer may grow.
     * @param handler the unified handler for byte counting and oversized value handling.
     */
    private void setBufferBounds(int initialSize, int maximumSize, UnifiedTestHandler handler) {
        readerBuilder = readerBuilder.withBufferConfiguration(
            IonBufferConfiguration.Builder.standard()
                .withInitialBufferSize(initialSize)
                .withMaximumBufferSize(maximumSize)
                .onOversizedValue(handler)
                .onOversizedSymbolTable(handler)
                .onData(handler)
                .build()
        );
    }

    /**
     * Creates a bounded incremental reader over the given binary Ion, constructing a reader either from byte array or
     * from InputStream depending on the value of the parameter 'constructFromBytes'.
     * @param constructFromBytes whether to construct the reader from bytes or InputStream.
     * @param initialBufferSize the initial buffer size.
     * @param maximumBufferSize the maximum size to which the buffer may grow.
     * @param handler the unified handler for byte counting and oversized value handling.
     */
    private IonReader boundedReaderFor(boolean constructFromBytes, byte[] bytes, int initialBufferSize, int maximumBufferSize, UnifiedTestHandler handler) {
        byteCounter.set(0);
        setBufferBounds(initialBufferSize, maximumBufferSize, handler);
        return readerFor(readerBuilder, constructFromBytes, bytes);
    }

    /**
     * Creates a bounded incremental reader over the given binary Ion. This should only be used in cases where tests
     * exercise behavior that does not exist when constructing a reader over a fixed buffer via byte array. In all other
     * cases, use one of the other `readerFor` variants, which construct readers according to the 'constructFromBytes'
     * parameter.
     * @param initialBufferSize the initial buffer size.
     * @param maximumBufferSize the maximum size to which the buffer may grow.
     * @param handler the unified handler for byte counting and oversized value handling.
     */
    private IonReader boundedReaderFor(InputStream stream, int initialBufferSize, int maximumBufferSize, UnifiedTestHandler handler) {
        byteCounter.set(0);
        setBufferBounds(initialBufferSize, maximumBufferSize, handler);
        return readerFor(stream);
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void singleValueExceedsInitialBufferSize(boolean constructFromBytes) throws Exception {
        reader = boundedReaderFor(constructFromBytes, 
            toBinary("\"abcdefghijklmnopqrstuvwxyz\""),
            8,
            Integer.MAX_VALUE,
            byteCountingHandler
        );
        assertSequence(
            next(IonType.STRING), stringValue("abcdefghijklmnopqrstuvwxyz"),
            next(null)
        );
        closeAndCount();
    }

    @Test
    public void maximumBufferSizeTooSmallFails() {
        IonBufferConfiguration.Builder builder = IonBufferConfiguration.Builder.standard();
        builder
            .withMaximumBufferSize(builder.getMinimumMaximumBufferSize() - 1)
            .withInitialBufferSize(builder.getMinimumMaximumBufferSize() - 1)
            .onOversizedValue(builder.getNoOpOversizedValueHandler())
            .onOversizedSymbolTable(builder.getNoOpOversizedSymbolTableHandler())
            .onData(builder.getNoOpDataHandler());
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    public void maximumBufferSizeWithoutHandlerFails() {
        IonBufferConfiguration.Builder builder = IonBufferConfiguration.Builder.standard();
        builder
            .withMaximumBufferSize(9)
            .withInitialBufferSize(9);
        assertThrows(IllegalArgumentException.class, builder::build);
    }
    
    private void expectOversized(int numberOfValues) {
        assertEquals(numberOfValues, oversizedCounter.get());
    }

    @Test
    public void oversizeValueDetectedDuringScalarFill() throws Exception {
        byte[] bytes = toBinary(
            "\"abcdefghijklmnopqrstuvwxyz\" " + // Requires 32 bytes (4 IVM, 1 TID, 1 length, 26 chars)
            "\"abc\" " +
            "\"abcdefghijklmnopqrstuvwxyz\" " +
            "\"def\""
        );

        reader = boundedReaderFor(new ByteArrayInputStream(bytes), 8, 16, byteAndOversizedValueCountingHandler);
        assertSequence(
            next(IonType.STRING), stringValue("abc")
        );
        expectOversized(1);
        assertSequence(
            next(IonType.STRING), stringValue("def"),
            next(null)
        );
        reader.close();
        expectOversized(2);
        totalBytesInStream = bytes.length;
        assertBytesConsumed();
    }

    @Test
    public void oversizeValueDetectedDuringScalarFillIncremental() throws Exception {
        byte[] bytes = toBinary(
            "\"abcdefghijklmnopqrstuvwxyz\" " + // Requires 32 bytes (4 IVM, 1 TID, 1 length, 26 chars)
            "\"abc\" " +
            "\"abcdefghijklmnopqrstuvwxyz\" " +
            "\"def\""
        );

        ResizingPipedInputStream pipe = new ResizingPipedInputStream(bytes.length);
        reader = boundedReaderFor(pipe, 8, 16, byteAndOversizedValueCountingHandler);
        int valueCounter = 0;
        for (byte b : bytes) {
            pipe.receive(b);
            IonType type = reader.next();
            if (type != null) {
                valueCounter++;
                assertTrue(valueCounter < 3);
                if (valueCounter == 1) {
                    assertSequence(type(IonType.STRING), stringValue("abc"));
                    expectOversized(1);
                } else {
                    assertEquals(2, valueCounter);
                    assertSequence(type(IonType.STRING), stringValue("def"));
                    expectOversized(2);
                }
            }
        }
        assertEquals(2, valueCounter);
        assertSequence(next(null));
        reader.close();
        expectOversized(2);
        totalBytesInStream = bytes.length;
        assertBytesConsumed();
    }

    @Test
    public void oversizeValueDetectedDuringFillOfOnlyScalarInStream() throws Exception {
        byte[] bytes = toBinary("\"abcdefghijklmnopqrstuvwxyz\""); // Requires a 2-byte header.
        reader = boundedReaderFor(new ByteArrayInputStream(bytes), 5, 5, byteAndOversizedValueCountingHandler);

        // The maximum buffer size is 5, which will be exceeded when attempting to fill the value with length 26.
        assertSequence(next(null));
        reader.close();
        expectOversized(1);
        totalBytesInStream = bytes.length;
        assertBytesConsumed();
    }

    @Test
    public void oversizeValueDetectedDuringFillOfOnlyScalarInStreamIncremental() throws Exception {
        byte[] bytes = toBinary("\"abcdefghijklmnopqrstuvwxyz\""); // Requires a 2-byte header.

        ResizingPipedInputStream pipe = new ResizingPipedInputStream(bytes.length);
        reader = boundedReaderFor(pipe, 5, 5, byteAndOversizedValueCountingHandler);
        feedBytesOneByOne(bytes, pipe, reader);
        // The maximum buffer size is 5, which will be exceeded when attempting to fill the value with length 26.
        assertSequence(next(null));
        reader.close();
        expectOversized(1);
        totalBytesInStream = bytes.length;
        assertBytesConsumed();
    }

    @Test
    public void oversizeValueDetectedDuringReadOfTypeIdOfSymbolTableAnnotatedValue() throws Exception {
        // This value is not a symbol table, but follows most of the code path that symbol tables follow. Ensure that
        // `onOversizedValue` (NOT `onOversizedSymbolTable`) is called, and that the stream continues to be read.
        byte[] bytes = bytes(
            0xE0, 0x01, 0x00, 0xEA, // IVM
            0xE6, 0x83, // Annotation wrapper with 3 bytes of annotations
            0x00, 0x00, 0x83, // A single (overpadded) SID 3 ($ion_symbol_table)
            0x21, 0x7B, // int 123
            0x81, 'a' // String "a"
        );
        reader = boundedReaderFor(new ByteArrayInputStream(bytes), 5, 5, byteAndOversizedValueCountingHandler);

        // The maximum buffer size is 5, which will be exceeded after the annotation wrapper type ID
        // (1 byte), the annotations length (1 byte), and the annotation SID 3 (3 bytes). The next byte is the wrapped
        // value type ID byte.
        assertSequence(
            next(IonType.STRING), stringValue("a")
        );
        expectOversized(1);
        assertSequence(next(null));
        reader.close();
        totalBytesInStream = bytes.length;
        assertBytesConsumed();
    }

    @Test
    public void oversizeValueDetectedDuringReadOfTypeIdOfSymbolTableAnnotatedValueIncremental() throws Exception {
        // This value is not a symbol table, but follows most of the code path that symbol tables follow. If all bytes
        // were available up-front it would be possible to determine that this value is not a symbol table and call
        // `onOversizedValue` (see the test above). However, since the value is determined to be oversized in the
        // annotation wrapper at the top level, it must yield back to the user before determining whether the value
        // is actually a symbol table. Therefore, it must call `onOversizedSymbolTable` conservatively, as the value
        // *might* end up being a symbol table.
        byte[] bytes = bytes(
            0xE0, 0x01, 0x00, 0xEA, // IVM
            0xE6, 0x83, // Annotation wrapper with 3 bytes of annotations
            0x00, 0x00, 0x83, // A single (overpadded) SID 3 ($ion_symbol_table)
            0x21, 0x7B, // int 123
            0x81, 'a' // String "a"
        );

        ResizingPipedInputStream pipe = new ResizingPipedInputStream(bytes.length);
        reader = boundedReaderFor(pipe, 5, 5, byteAndOversizedSymbolTableCountingHandler);

        // The maximum buffer size is 5, which will be exceeded after the annotation wrapper type ID
        // (1 byte), the annotations length (1 byte), and the annotation SID 3 (3 bytes). The next byte is the wrapped
        // value type ID byte.
        feedBytesOneByOne(bytes, pipe, reader);
        expectOversized(1);
        assertSequence(
            next(null),
            next(null)
        );
        reader.close();
    }

    @Test
    public void valueAfterLargeSymbolTableNotOversized() throws Exception {
        // The first value is not oversized even though its size plus the size of the preceding symbol table
        // exceeds the maximum buffer size.
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonWriter writer = writerBuilder.build(out);
        writer.writeString("12345678");
        writer.writeSymbol("abcdefghijklmnopqrstuvwxyz"); // Requires 32 bytes (4 IVM, 1 TID, 1 length, 26 chars)
        writer.close();
        // The system values require ~40 bytes (4 IVM, 5 symtab struct header, 1 'symbols' sid, 2 list header, 2 + 26
        // for symbol 10.
        // The string "12345678" requires 9 bytes, bringing the total to ~49, above the max of 48.

        reader = boundedReaderFor(new ByteArrayInputStream(out.toByteArray()), 8, 48, byteAndOversizedValueCountingHandler);
        assertSequence(
            next(IonType.STRING)
        );
        expectOversized(0);
        assertSequence(
            stringValue("12345678"),
            next(IonType.SYMBOL)
        );
        expectOversized(0);
        assertSequence(
            stringValue("abcdefghijklmnopqrstuvwxyz"),
            next(null)
        );
        reader.close();
        expectOversized(0);
        totalBytesInStream = out.size();
        assertBytesConsumed();
    }

    @Test
    public void valueAfterLargeSymbolTableNotOversizedIncremental() throws Exception {
        // The first value is not oversized even though its size plus the size of the preceding symbol table
        // exceeds the maximum buffer size.
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonWriter writer = writerBuilder.build(out);
        writer.writeString("12345678");
        writer.writeSymbol("abcdefghijklmnopqrstuvwxyz"); // Requires 32 bytes (4 IVM, 1 TID, 1 length, 26 chars)
        writer.close();
        // The system values require ~40 bytes (4 IVM, 5 symtab struct header, 1 'symbols' sid, 2 list header, 2 + 26
        // for symbol 10.
        // The string "12345678" requires 9 bytes, bringing the total to ~49, above the max of 48.

        ResizingPipedInputStream pipe = new ResizingPipedInputStream(out.size());
        reader = boundedReaderFor(pipe, 8, 48, byteAndOversizedValueCountingHandler);
        byte[] bytes = out.toByteArray();
        int valueCounter = 0;
        for (byte b : bytes) {
            pipe.receive(b);
            IonType type = reader.next();
            if (type != null) {
                valueCounter++;
                if (valueCounter == 1) {
                    assertSequence(type(IonType.STRING), stringValue("12345678"));
                    expectOversized(0);
                } else if (valueCounter == 2) {
                    assertSequence(type(IonType.SYMBOL), stringValue("abcdefghijklmnopqrstuvwxyz"));
                    expectOversized(0);
                }
            }
        }
        assertEquals(2, valueCounter);
        assertSequence(next(null));
        reader.close();
        expectOversized(0);
        totalBytesInStream = out.size();
        assertBytesConsumed();
    }

    /**
     * Calls next() on the given reader and returns the result.
     * @param reader an Ion reader.
     * @param pipe the stream from which the reader pulls data. If null, all data is available up front.
     * @param source the source of data to be fed into the pipe. Only used if pipe is not null.
     * @return the result of the first non-null call to reader.next(), or null if the source data is exhausted before
     *  reader.next() returns non-null.
     */
    private static IonType ionReaderNext(IonReader reader, ResizingPipedInputStream pipe, ByteArrayInputStream source) {
        if (pipe == null) {
            return reader.next();
        }
        while (source.available() > 0) {
            pipe.receive(source.read());
            if (reader.next() != null) {
                return reader.getType();
            }
        }
        return reader.next();
    }

    /**
     * Verifies that oversized value handling works properly when the second value is oversized.
     * @param withSymbolTable true if the first value should be preceded by a symbol table.
     * @param withThirdValue true if the second (oversized) value should be followed by a third value that fits.
     * @param byteByByte true if bytes should be fed to the reader one at a time.
     * @throws Exception if thrown unexpectedly.
     */
    private void oversizedSecondValue(
        boolean withSymbolTable,
        boolean withThirdValue,
        boolean byteByByte
    ) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonWriter writer = writerBuilder.build(out);
        String firstValue = "12345678";
        if (withSymbolTable) {
            writer.writeSymbol(firstValue);
        } else {
            writer.writeString(firstValue);
        }
        writer.writeString("abcdefghijklmnopqrstuvwxyz");
        String thirdValue = "abc";
        if (withThirdValue) {
            writer.writeString(thirdValue);
        }
        writer.close();
        oversizedCounter.set(0);

        // Greater than the first value (and symbol table, if any) and third value, less than the second value.
        int maximumBufferSize = 25;
        ResizingPipedInputStream pipe = null;
        ByteArrayInputStream source = new ByteArrayInputStream(out.toByteArray());
        InputStream in;
        if (byteByByte) {
            pipe = new ResizingPipedInputStream(out.size());
            in = pipe;
        } else {
            in = source;
        }
        reader = boundedReaderFor(in, maximumBufferSize, maximumBufferSize, byteAndOversizedValueCountingHandler);
        assertEquals(withSymbolTable ? IonType.SYMBOL : IonType.STRING, ionReaderNext(reader, pipe, source));
        expectOversized(0);
        assertEquals(firstValue, reader.stringValue());
        if (withThirdValue) {
            assertEquals(IonType.STRING, ionReaderNext(reader, pipe, source));
            assertEquals(thirdValue, reader.stringValue());
        }
        assertNull(ionReaderNext(reader, pipe, source));
        reader.close();
        expectOversized(1);
        totalBytesInStream = out.size();
        assertBytesConsumed();
    }

    @Test
    public void oversizedSecondValueWithoutSymbolTable() throws Exception {
        oversizedSecondValue(false, false, false);
        oversizedSecondValue(false, true, false);
    }

    @Test
    public void oversizedSecondValueWithoutSymbolTableIncremental() throws Exception {
        oversizedSecondValue(false, false, true);
        oversizedSecondValue(false, true, true);
    }

    @Test
    public void oversizedSecondValueWithSymbolTable() throws Exception {
        oversizedSecondValue(true, false, false);
        oversizedSecondValue(true, true, false);
    }

    @Test
    public void oversizedSecondValueWithSymbolTableIncremental() throws Exception {
        oversizedSecondValue(true, false, true);
        oversizedSecondValue(true, true, true);
    }

    private void oversizeSymbolTableDetectedInHeader(int maximumBufferSize) throws Exception {
        byte[] bytes = bytes(
            0xE0, 0x01, 0x00, 0xEA, // IVM
            0xEE, 0x00, 0x00, 0x00, 0x00, 0x86, // Annotation wrapper length 5 (overpadded)
            0x81, // Annotation wrapper with 3 bytes of annotations
            0x83, // SID 3 ($ion_symbol_table)
            0xDE, 0x82, 0x84, 0x20, // Struct with overpadded length
            0x81, 'a' // String "a"
        );
        oversizedCounter.set(0);
        reader = boundedReaderFor(new ByteArrayInputStream(bytes), maximumBufferSize, maximumBufferSize, byteAndOversizedSymbolTableCountingHandler);
        assertSequence(next(null));
        expectOversized(1);
        reader.close();
    }

    @Test
    public void oversizeSymbolTableDetectedInHeader() throws Exception {
        // The symbol table is determined to be oversized when reading the length of the annotation wrapper.
        oversizeSymbolTableDetectedInHeader(5);
        // The symbol table is determined to be oversized when reading the annotations length.
        oversizeSymbolTableDetectedInHeader(6);
        // The symbol table is determined to be oversized when reading SID 3.
        oversizeSymbolTableDetectedInHeader(7);
        // The symbol table is determined to be oversized when reading the type ID of the wrapped struct.
        oversizeSymbolTableDetectedInHeader(8);
        // The symbol table is determined to be oversized when reading the length of the wrapped struct.
        oversizeSymbolTableDetectedInHeader(9);
    }

    private void oversizeSymbolTableDetectedInHeaderIncremental(int maximumBufferSize) throws Exception {
        byte[] bytes = bytes(
            0xE0, 0x01, 0x00, 0xEA, // IVM
            0xEE, 0x00, 0x00, 0x00, 0x00, 0x86, // Annotation wrapper length 5 (overpadded)
            0x81, // Annotation wrapper with 3 bytes of annotations
            0x83, // SID 3 ($ion_symbol_table)
            0xDE, 0x82, 0x84, 0x20, // Struct with overpadded length
            0x81, 'a' // String "a"
        );
        oversizedCounter.set(0);
        ResizingPipedInputStream pipe = new ResizingPipedInputStream(bytes.length);
        reader = boundedReaderFor(pipe, maximumBufferSize, maximumBufferSize, byteAndOversizedSymbolTableCountingHandler);
        feedBytesOneByOne(bytes, pipe, reader);
        assertSequence(next(null));
        expectOversized(1);
        reader.close();
    }

    @Test
    public void oversizeSymbolTableDetectedInHeaderIncremental() throws Exception {
        // The symbol table is determined to be oversized when reading the length of the annotation wrapper.
        oversizeSymbolTableDetectedInHeaderIncremental(5);
        // The symbol table is determined to be oversized when reading the annotations length.
        oversizeSymbolTableDetectedInHeaderIncremental(6);
        // The symbol table is determined to be oversized when reading SID 3.
        oversizeSymbolTableDetectedInHeaderIncremental(7);
        // The symbol table is determined to be oversized when reading the type ID of the wrapped struct.
        oversizeSymbolTableDetectedInHeaderIncremental(8);
        // The symbol table is determined to be oversized when reading the length of the wrapped struct.
        oversizeSymbolTableDetectedInHeaderIncremental(9);
    }

    @Test
    public void oversizeSymbolTableDetectedInTheMiddle() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonWriter writer = writerBuilder.build(out);
        writer.writeString("12345678");
        writer.finish();
        writer.writeSymbol("abcdefghijklmnopqrstuvwxyz"); // Requires 32 bytes (4 IVM, 1 TID, 1 length, 26 chars)
        writer.close();
        // The system values require ~40 bytes (4 IVM, 5 symtab struct header, 1 'symbols' sid, 2 list header, 2 + 26
        // for symbol 10.
        reader = boundedReaderFor(new ByteArrayInputStream(out.toByteArray()), 8, 25, byteAndOversizedSymbolTableCountingHandler);
        assertSequence(
            next(IonType.STRING), stringValue("12345678"),
            next(null)
        );
        expectOversized(1);
        reader.close();
    }

    @Test
    public void oversizeSymbolTableDetectedInTheMiddleIncremental() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonWriter writer = writerBuilder.build(out);
        writer.writeString("12345678");
        writer.finish();
        writer.writeSymbol("abcdefghijklmnopqrstuvwxyz"); // Requires 32 bytes (4 IVM, 1 TID, 1 length, 26 chars)
        writer.close();
        // The system values require ~40 bytes (4 IVM, 5 symtab struct header, 1 'symbols' sid, 2 list header, 2 + 26
        // for symbol 10.

        ResizingPipedInputStream pipe = new ResizingPipedInputStream(out.size());
        byte[] bytes = out.toByteArray();
        reader = boundedReaderFor(pipe, 8, 25, byteAndOversizedSymbolTableCountingHandler);
        boolean foundValue = false;
        for (byte b : bytes) {
            IonType type = reader.next();
            if (type != null) {
                assertFalse(foundValue);
                assertSequence(type(IonType.STRING), stringValue("12345678"));
                foundValue = true;
            }
            pipe.receive(b);
        }
        assertTrue(foundValue);
        assertSequence(next(null));
        expectOversized(1);
        reader.close();
    }

    @Test
    public void skipOversizeScalarBelowTopLevelNonIncremental() throws Exception {
        byte[] bytes = toBinary("[\"abcdefg\", 123]");
        readerBuilder = readerBuilder.withIncrementalReadingEnabled(false);
        reader = boundedReaderFor(new ByteArrayInputStream(bytes), 5, 5, byteAndOversizedValueCountingHandler);
        assertSequence(next(IonType.LIST), STEP_IN);
        expectOversized(0);
        // This value is oversized, but since it is not filled, `onOversizedValue` does not need to be called.
        assertSequence(next(IonType.STRING));
        expectOversized(0);
        assertSequence(next(IonType.INT));
        expectOversized(0);
        assertSequence(
                intValue(123),
                next(null),
            STEP_OUT,
            next(null)
        );
        reader.close();
        expectOversized(0);
        totalBytesInStream = bytes.length;
        assertBytesConsumed();
    }

    @Test
    public void fillOversizeScalarBelowTopLevelNonIncremental() throws Exception {
        byte[] bytes = toBinary("[\"abcdefg\", 123]");
        readerBuilder = readerBuilder.withIncrementalReadingEnabled(false);
        reader = boundedReaderFor(new ByteArrayInputStream(bytes), 5, 5, byteAndOversizedValueCountingHandler);
        assertSequence(next(IonType.LIST), STEP_IN);
        expectOversized(0);
        assertSequence(next(IonType.STRING));
        // This value is oversized. Since the user attempts to consume it, `onOversizedValue` is called. An
        // OversizedValueException is called because the user attempted to force parsing of an oversized scalar
        // via an IonReader method that has no other way of conveying the failure.
        try {
            assertNull(reader.stringValue());
            fail("Expected oversized value.");
        } catch (OversizedValueException e) {
            // Continue
        }
        expectOversized(1);
        assertSequence(next(IonType.INT));
        expectOversized(1);
        assertSequence(
                intValue(123),
                next(null),
            STEP_OUT,
            next(null)
        );
        reader.close();
        expectOversized(1);
        totalBytesInStream = bytes.length;
        assertBytesConsumed();
    }

    @Test
    public void oversizedValueBelowTopLevelDetectedInHeaderNonIncremental() throws Exception {
        byte[] bytes = bytes(
            0xE0, 0x01, 0x00, 0xEA, // IVM
            0xC7, // s-exp length 7
            0xBE, 0x00, 0x00, 0x00, 0x00, 0x81, // List length 1 (overpadded)
            0x20, // int 0
            0x81, 'a' // String "a"
        );
        readerBuilder = readerBuilder.withIncrementalReadingEnabled(false);
        reader = boundedReaderFor(new ByteArrayInputStream(bytes), 5, 5, byteAndOversizedValueCountingHandler);
        assertSequence(next(IonType.SEXP), STEP_IN);
        expectOversized(0);
        assertSequence(next(null));
        expectOversized(1);
        assertSequence(
            STEP_OUT,
            next(IonType.STRING), stringValue("a"),
            next(null)
        );
        reader.close();
        expectOversized(1);
        totalBytesInStream = bytes.length;
        assertBytesConsumed();
    }

    @Test
    public void oversizedAnnotatedValueBelowTopLevelDetectedInHeaderNonIncremental() throws Exception {
        byte[] bytes = bytes(
            0xE0, 0x01, 0x00, 0xEA, // IVM
            0xC8, // s-exp length 8
            0xE7, // Annotation wrapper length 7
            0x84, // 4 byte annotation SID sequence
            0x00, 0x00, 0x00, 0x84, // Annotation SID 4 (overpadded)
            0xB1, // List length 1
            0x20, // int 0
            0x81, 'a' // String "a"
        );
        readerBuilder = readerBuilder.withIncrementalReadingEnabled(false);
        reader = boundedReaderFor(new ByteArrayInputStream(bytes), 5, 5, byteAndOversizedValueCountingHandler);
        assertSequence(next(IonType.SEXP), STEP_IN);
        expectOversized(0);
        assertSequence(next(null));
        expectOversized(1);
        assertSequence(
            STEP_OUT,
            next(IonType.STRING), stringValue("a"),
            next(null)
        );
        reader.close();
        expectOversized(1);
        totalBytesInStream = bytes.length;
        assertBytesConsumed();
    }

    @Test
    public void deeplyNestedValueNotOversizedDueToContainerHeaderLengthsNonIncremental() throws Exception {
        // The string value requires 5 bytes, the maximum buffer size. The value should not be considered oversized
        // even though it is contained within containers whose headers cannot fit in the buffer.
        byte[] bytes = bytes(
            0xE0, 0x01, 0x00, 0xEA, // IVM
            0x82, '1', '2', // String "12"
            0xC7, // s-exp length 7
            0xC6, // s-exp length 6
            0xC5, // s-exp length 5
            0x84, '1', '2', '3', '4', // String "1234"
            0x20 // int 0
        );
        readerBuilder = readerBuilder.withIncrementalReadingEnabled(false);
        reader = boundedReaderFor(new ByteArrayInputStream(bytes), 5, 5, byteAndOversizedValueCountingHandler);
        assertSequence(
            next(IonType.STRING), stringValue("12"),
            container(IonType.SEXP,
                container(IonType.SEXP,
                    container(IonType.SEXP,
                        next(IonType.STRING), stringValue("1234"),
                        next(null)
                    )
                )
            ),
            next(IonType.INT), intValue(0),
            next(null)
        );
        reader.close();
        expectOversized(0);
        totalBytesInStream = bytes.length;
        assertBytesConsumed();
    }

    private static void writeFirstStruct(IonWriter writer) throws IOException {
        //{
        //    foo: bar,
        //    abc: [123, 456]
        //}
        writer.stepIn(IonType.STRUCT);
        writer.setFieldName("foo");
        writer.writeSymbol("bar");
        writer.setFieldName("abc");
        writer.stepIn(IonType.LIST);
        writer.writeInt(123);
        writer.writeInt(456);
        writer.stepOut();
        writer.stepOut();
    }

    private void assertFirstStruct() {
        assertSequence(
            container(IonType.STRUCT,
                next("foo", IonType.SYMBOL), stringValue("bar"),
                next(IonType.LIST), fieldName("abc"), STEP_IN,
                    next(IonType.INT), intValue(123),
                    next(IonType.INT), intValue(456),
                STEP_OUT
            )
        );
    }

    private static void writeSecondStruct(IonWriter writer) throws IOException {
        //{
        //    foo: baz,
        //    abc: [42.0, 43e0]
        //}
        writer.stepIn(IonType.STRUCT);
        writer.setFieldName("foo");
        writer.writeSymbol("baz");
        writer.setFieldName("abc");
        writer.stepIn(IonType.LIST);
        writer.writeDecimal(new BigDecimal("42.0"));
        writer.writeFloat(43.);
        writer.stepOut();
        writer.stepOut();
    }

    private void assertSecondStruct() {
        assertSequence(
            container(IonType.STRUCT,
                next("foo", IonType.SYMBOL), stringValue("baz"),
                next(IonType.LIST), fieldName("abc"), STEP_IN,
                    next(IonType.DECIMAL), decimalValue(new BigDecimal("42.0")),
                    next(IonType.FLOAT), doubleValue(43.),
                STEP_OUT
            ),
            next(null)
        );
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void flushBetweenStructs(boolean constructFromBytes) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonWriter writer = _Private_IonManagedBinaryWriterBuilder
            .create(_Private_IonManagedBinaryWriterBuilder.AllocatorMode.BASIC)
                .withLocalSymbolTableAppendEnabled()
                .newWriter(out);
        writeFirstStruct(writer);
        writer.flush();
        writeSecondStruct(writer);
        writer.close();

        reader = boundedReaderFor(constructFromBytes, out.toByteArray(), 64, 64, byteCountingHandler);
        assertFirstStruct();
        assertSecondStruct();
        closeAndCount();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void structsWithFloat32AndPreallocatedLength(boolean constructFromBytes) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonWriter writer = _Private_IonManagedBinaryWriterBuilder
            .create(_Private_IonManagedBinaryWriterBuilder.AllocatorMode.BASIC)
                .withPaddedLengthPreallocation(2)
                .withFloatBinary32Enabled()
                .newWriter(out);
        writeFirstStruct(writer);
        writeSecondStruct(writer);
        writer.close();

        reader = boundedReaderFor(constructFromBytes, out.toByteArray(), 64, 64, byteCountingHandler);
        assertFirstStruct();
        assertSecondStruct();
        assertBytesConsumed();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void nopPadThatFillsBufferFollowedByValueNotOversized(boolean constructFromBytes) throws Exception {
        TestUtils.BinaryIonAppender out = new TestUtils.BinaryIonAppender().append(
            0x03, 0x00, 0x00, 0x00, // 4 byte NOP pad.
            0x20 // Int 0.
        );
        // The IVM is 4 bytes and the NOP pad is 4 bytes. The first value is the 9th byte and should not be considered
        // oversize because the NOP pad can be discarded.
        reader = boundedReaderFor(constructFromBytes, out.toByteArray(), 8, 8, byteCountingHandler);
        assertSequence(
            next(IonType.INT), intValue(0),
            next(null)
        );
        closeAndCount();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void nopPadFollowedByValueThatOverflowsBufferNotOversized(boolean constructFromBytes) throws Exception {
        TestUtils.BinaryIonAppender out = new TestUtils.BinaryIonAppender().append(
            0x03, 0x00, 0x00, 0x00, // 4 byte NOP pad.
            0x21, 0x01 // Int 1.
        );
        // The IVM is 4 bytes and the NOP pad is 4 bytes. The first byte of the value is the 9th byte and fits in the
        // buffer. Even though there is a 10th byte, the value should not be considered oversize because the NOP pad
        // can be discarded.
        reader = boundedReaderFor(constructFromBytes, out.toByteArray(), 9, 9, byteCountingHandler);
        assertSequence(
            next(IonType.INT), intValue(1),
            next(null)
        );
        closeAndCount();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void symbolTableFollowedByNopPadFollowedByValueThatOverflowsBufferNotOversized(boolean constructFromBytes) throws Exception {
        TestUtils.BinaryIonAppender out = new TestUtils.BinaryIonAppender().append(
            // Symbol table with the symbol 'hello'.
            0xEB, 0x81, 0x83, 0xD8, 0x87, 0xB6, 0x85, 'h', 'e', 'l', 'l', 'o',
            0x00, // 1-byte NOP pad.
            0x71, 0x0A // SID 10 (hello).
        );
        // The IVM is 4 bytes, the symbol table is 12 bytes, and the symbol value is 2 bytes (total 18). The 1-byte NOP
        // pad needs to be reclaimed to make space for the value. Once that is done, the value will fit.
        reader = boundedReaderFor(constructFromBytes, out.toByteArray(), 18, 18, byteCountingHandler);
        assertSequence(
            next(IonType.SYMBOL), stringValue("hello"),
            next(null)
        );
        closeAndCount();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void multipleNopPadsFollowedByValueThatOverflowsBufferNotOversized(boolean constructFromBytes) throws Exception {
        TestUtils.BinaryIonAppender out = new TestUtils.BinaryIonAppender().append(
            // One byte no-op pad.
            0x00,
            // Two byte no-op pad.
            0x01, 0xFF,
            // Three byte no-op pad.
            0x02, 0x99, 0x42,
            // Int 1
            0x21, 0x01,
            // IVM 1.0
            0xE0, 0x01, 0x00, 0xEA,
            // The following no-op pads exceed the maximum buffer size, but should not cause an error to be raised.
            // 16-byte no-op pad.
            0x0E, 0x8E,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // Three byte no-op pad.
            0x02, 0x99, 0x42,
            // Int 2.
            0x21, 0x02,
            // 16-byte no-op pad.
            0x0E, 0x8E,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
        );

        reader = boundedReaderFor(constructFromBytes, out.toByteArray(), 11, 11, byteCountingHandler);
        assertSequence(
            next(IonType.INT), intValue(1),
            next(IonType.INT), intValue(2),
            next(null)
        );
        closeAndCount();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void nopPadsInterspersedWithSystemValuesDoNotCauseOversizedErrors(boolean constructFromBytes) throws Exception {
        TestUtils.BinaryIonAppender out = new TestUtils.BinaryIonAppender().append(
            // One byte no-op pad.
            0x00,
            // Two byte no-op pad.
            0x01, 0xFF,
            // Three byte no-op pad.
            0x02, 0x99, 0x42,
            // IVM 1.0
            0xE0, 0x01, 0x00, 0xEA,
            // 15-byte no-op pad.
            0x0E, 0x8D,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // Symbol table with the symbol 'hello'.
            0xEB, 0x81, 0x83, 0xD8, 0x87, 0xB6, 0x85, 'h', 'e', 'l', 'l', 'o',
            // Three byte no-op pad.
            0x02, 0x99, 0x42,
            // Symbol 10 (hello)
            0x71, 0x0A
        );

        // Set the maximum size at 2 IVMs (8 bytes) + the symbol table (12 bytes) + the value (2 bytes).
        reader = boundedReaderFor(constructFromBytes, out.toByteArray(), 22, 22, byteCountingHandler);
        assertSequence(
            next(IonType.SYMBOL), stringValue("hello"),
            next(null)
        );
        closeAndCount();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void nopPadsInterspersedWithSystemValuesDoNotCauseOversizedErrors2(boolean constructFromBytes) throws Exception {
        TestUtils.BinaryIonAppender out = new TestUtils.BinaryIonAppender().append(
            // One byte no-op pad.
            0x00,
            // Two byte no-op pad.
            0x01, 0xFF,
            // Three byte no-op pad.
            0x02, 0x99, 0x42,
            // IVM 1.0
            0xE0, 0x01, 0x00, 0xEA,
            // 16-byte no-op pad.
            0x0E, 0x8E,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // Symbol table with the symbol 'hello'.
            0xEB, 0x81, 0x83, 0xD8, 0x87, 0xB6, 0x85, 'h', 'e', 'l', 'l', 'o',
            // Three byte no-op pad.
            0x02, 0x99, 0x42,
            // Symbol 10 (hello)
            0x71, 0x0A
        );

        // Set the maximum size at 2 IVMs (8 bytes) + the symbol table (12 bytes) + the value (2 bytes).
        reader = boundedReaderFor(constructFromBytes, out.toByteArray(), 22, 22, byteCountingHandler);
        assertSequence(
            next(IonType.SYMBOL), stringValue("hello"),
            next(null)
        );
        closeAndCount();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void nopPadsInterspersedWithSystemValuesDoNotCauseOversizedErrors3(boolean constructFromBytes) throws Exception {
        TestUtils.BinaryIonAppender out = new TestUtils.BinaryIonAppender().append(
            // One byte no-op pad.
            0x00,
            // Two byte no-op pad.
            0x01, 0xFF,
            // Three byte no-op pad.
            0x02, 0x99, 0x42,
            // IVM 1.0
            0xE0, 0x01, 0x00, 0xEA,
            // 14-byte no-op pad.
            0x0E, 0x8C,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // Symbol table with the symbol 'hello'.
            0xEB, 0x81, 0x83, 0xD8, 0x87, 0xB6, 0x85, 'h', 'e', 'l', 'l', 'o',
            // Three byte no-op pad.
            0x02, 0x99, 0x42,
            // Symbol 10 (hello)
            0x71, 0x0A
        );

        // Set the maximum size at 2 IVMs (8 bytes) + the symbol table (12 bytes) + the value (2 bytes).
        reader = boundedReaderFor(constructFromBytes, out.toByteArray(), 22, 22, byteCountingHandler);
        assertSequence(
            next(IonType.SYMBOL), stringValue("hello"),
            next(null)
        );
        closeAndCount();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void nopPadSurroundingSymbolTableThatFitsInBuffer(boolean constructFromBytes) throws Exception {
        TestUtils.BinaryIonAppender out = new TestUtils.BinaryIonAppender().append(
            // 14-byte no-op pad.
            0x0E, 0x8C,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // Symbol table with the symbol 'hello'.
            0xEB, 0x81, 0x83, 0xD8, 0x87, 0xB6, 0x85, 'h', 'e', 'l', 'l', 'o',
            // 14-byte no-op pad.
            0x0E, 0x8C,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // String abcdefg
            0x87, 'a', 'b', 'c', 'd', 'e', 'f', 'g',
            // Symbol 10 (hello)
            0x71, 0x0A
        );

        // Set the maximum size at IVM (4 bytes) + 14-byte NOP pad + the symbol table (12 bytes) + 2 value bytes.
        reader = boundedReaderFor(constructFromBytes, out.toByteArray(), 32, 32, byteCountingHandler);
        assertSequence(
            next(IonType.STRING), stringValue("abcdefg"),
            next(IonType.SYMBOL), stringValue("hello"),
            next(null)
        );
        closeAndCount();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void nopPadInStructNonIncremental(boolean constructFromBytes) throws Exception {
        TestUtils.BinaryIonAppender out = new TestUtils.BinaryIonAppender().append(
            0xD6, // Struct length 6
            0x80, // Field name SID 0
            0x04, 0x00, 0x00, 0x00, 0x00 // 5-byte NOP pad.
        );
        readerBuilder = readerBuilder.withIncrementalReadingEnabled(false);
        reader = boundedReaderFor(constructFromBytes, out.toByteArray(), 5, Integer.MAX_VALUE, byteCountingHandler);
        assertSequence(
            container(IonType.STRUCT,
                next(null)
            ),
            next(null)
        );
        closeAndCount();
    }

    /**
     * Compares an iterator to the given list.
     * @param expected the list containing the elements to compare to.
     * @param actual the iterator to be compared.
     */
    private static void compareIterator(List<String> expected, Iterator<String> actual) {
        int numberOfElements = 0;
        while (actual.hasNext()) {
            String actualValue = actual.next();
            if (numberOfElements >= expected.size()) {
                assertTrue(actual.hasNext());
                break;
            }
            String expectedValue = expected.get(numberOfElements++);
            assertEquals(expectedValue, actualValue);
        }
        assertEquals(expected.size(), numberOfElements);
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void annotationIteratorReuse(boolean constructFromBytes) throws Exception {
        reader = readerFor("foo::bar::123 baz::456", constructFromBytes);

        assertSequence(next(IonType.INT));
        Iterator<String> firstValueAnnotationIterator = reader.iterateTypeAnnotations();
        assertSequence(
            intValue(123),
            next(IonType.INT)
        );
        compareIterator(Arrays.asList("foo", "bar"), firstValueAnnotationIterator);
        Iterator<String> secondValueAnnotationIterator = reader.iterateTypeAnnotations();
        assertSequence(
            intValue(456),
            next(null)
        );
        compareIterator(Collections.singletonList("baz"), secondValueAnnotationIterator);
        closeAndCount();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void failsOnMalformedSymbolTable(boolean constructFromBytes) throws Exception {
        byte[] data = bytes(
            0xE0, 0x01, 0x00, 0xEA, // Binary IVM
            0xE6, // 6-byte annotation wrapper
            0x81, // 1 byte of annotation SIDs
            0x83, // SID 3 ($ion_symbol_table)
            0xD3, // 3-byte struct
            0x84, // Field name SID 4 (name)
            0xE7, // 7-byte annotation wrapper (error: there should only be two bytes remaining).
            0x81, // Junk byte to fill the 6 bytes of the annotation wrapper and 3 bytes of the struct.
            0x20  // Next top-level value (int 0).
        );
        reader = boundedReaderFor(constructFromBytes, data, 1024, 1024, byteCountingHandler);
        assertThrows(IonException.class, () -> reader.next());
        reader.close();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void multiByteSymbolTokens(boolean constructFromBytes) throws Exception {
        reader = readerFor(
            (writer, out) -> {
                writer.addTypeAnnotationSymbol(SystemSymbols.ION_SYMBOL_TABLE_SID);
                writer.stepIn(IonType.STRUCT);
                writer.setFieldNameSymbol(SystemSymbols.SYMBOLS_SID);
                writer.stepIn(IonType.LIST);
                for (int i = SystemSymbols.ION_1_0_MAX_ID ; i < 332; i++) {
                    writer.writeNull(IonType.STRING);
                }
                writer.writeString("a");
                writer.writeString("b");
                writer.writeString("c");
                writer.stepOut();
                writer.stepOut();
                writer.stepIn(IonType.STRUCT);
                writer.addTypeAnnotationSymbol(333);
                writer.setFieldNameSymbol(334);
                writer.writeSymbolToken(335);
                writer.stepOut();
            },
            constructFromBytes
        );
        assertSequence(
            next(IonType.STRUCT), STEP_IN,
                next(IonType.SYMBOL), annotationSymbols("a")
        );
        Iterator<String> annotationsIterator = reader.iterateTypeAnnotations();
        assertTrue(annotationsIterator.hasNext());
        assertEquals("a", annotationsIterator.next());
        assertFalse(annotationsIterator.hasNext());
        assertSequence(
                fieldNameSymbol("b"), symbolValue("c"),
            STEP_OUT,
            next(null)
        );
        closeAndCount();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void symbolTableWithOpenContentImportsListField(boolean constructFromBytes) throws Exception {
        reader = readerFor(
            (writer, out) -> {
                writer.addTypeAnnotationSymbol(SystemSymbols.ION_SYMBOL_TABLE_SID);
                writer.stepIn(IonType.STRUCT);
                writer.setFieldNameSymbol(SystemSymbols.NAME_SID);
                writer.writeInt(123);
                writer.setFieldNameSymbol(SystemSymbols.SYMBOLS_SID);
                writer.stepIn(IonType.LIST);
                writer.writeString("a");
                writer.stepOut();
                writer.setFieldNameSymbol(SystemSymbols.IMPORTS_SID);
                writer.writeString("foo");
                writer.stepOut();
                writer.writeSymbolToken(SystemSymbols.ION_1_0_MAX_ID + 1);
            },
            constructFromBytes
        );
        assertSequence(
            next(IonType.SYMBOL), stringValue("a"),
            next(null)
        );
        closeAndCount();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void symbolTableWithOpenContentImportsSymbolField(boolean constructFromBytes) throws Exception {
        reader = readerFor(
            (writer, out) -> {
                writer.addTypeAnnotationSymbol(SystemSymbols.ION_SYMBOL_TABLE_SID);
                writer.stepIn(IonType.STRUCT);
                writer.setFieldNameSymbol(SystemSymbols.SYMBOLS_SID);
                writer.stepIn(IonType.LIST);
                writer.writeString("a");
                writer.stepOut();
                writer.setFieldNameSymbol(SystemSymbols.IMPORTS_SID);
                writer.writeSymbolToken(SystemSymbols.NAME_SID);
                writer.stepOut();
                writer.writeSymbolToken(SystemSymbols.ION_1_0_MAX_ID + 1);
            },
            constructFromBytes
        );
        assertSequence(
            next(IonType.SYMBOL), stringValue("a"),
            next(null)
        );
        closeAndCount();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void symbolTableWithOpenContentSymbolField(boolean constructFromBytes) throws Exception {
        reader = readerFor(
            (writer, out) -> {
                writer.addTypeAnnotationSymbol(SystemSymbols.ION_SYMBOL_TABLE_SID);
                writer.stepIn(IonType.STRUCT);
                writer.setFieldNameSymbol(SystemSymbols.SYMBOLS_SID);
                writer.writeString("foo");
                writer.setFieldNameSymbol(SystemSymbols.IMPORTS_SID);
                writer.writeSymbolToken(SystemSymbols.NAME_SID);
                writer.stepOut();
                writer.writeSymbolToken(SystemSymbols.VERSION_SID);
            },
            constructFromBytes
        );
        assertSequence(
            next(IonType.SYMBOL), stringValue(SystemSymbols.VERSION),
            next(null)
        );
        closeAndCount();
    }

    @Test
    public void stepOutWithoutEnoughDataNonIncrementalFails() throws Exception {
        byte[] bytes = bytes(
            0xE0, 0x01, 0x00, 0xEA,
            0xB6, // List length 6
            0x21, 0x01, // Int 1
            0x21, 0x02 // Int 2
        );
        readerBuilder = readerBuilder.withIncrementalReadingEnabled(false);
        reader = readerFor(new ByteArrayInputStream(bytes));
        assertSequence(
            container(IonType.LIST,
                next(IonType.INT), intValue(1)
                // Early step out. Not enough bytes to complete the value. Throw if the reader attempts
                // to advance the cursor.
            )
        );
        assertThrows(IonException.class, () -> reader.next());
        reader.close();
    }

    @Test
    public void skipWithoutEnoughDataNonIncrementalFails() throws Exception {
        byte[] bytes = bytes(
            0xE0, 0x01, 0x00, 0xEA,
            0x86, '1', '2', '3', '4' // String length 6; only 4 value bytes
        );
        readerBuilder = readerBuilder.withIncrementalReadingEnabled(false);
        reader = readerFor(new ByteArrayInputStream(bytes));
        assertSequence(next(IonType.STRING));
        assertThrows(IonException.class, () -> reader.next());
        reader.close();
    }

    /**
     * An InputStream that returns less than the number of bytes requested from bulk reads.
     */
    private static class ThrottlingInputStream extends InputStream {

        private final byte[] data;
        private final boolean throwFromReadOnEof;
        private int offset = 0;

        /**
         * @param data the data for the InputStream to provide.
         * @param throwFromReadOnEof true if the stream should throw {@link java.io.EOFException} when read() is called
         *                           at EOF. If false, simply returns -1.
         */
        protected ThrottlingInputStream(byte[] data, boolean throwFromReadOnEof) {
            this.data = data;
            this.throwFromReadOnEof = throwFromReadOnEof;
        }

        @Override
        public int read() {
            return data[offset++] & 0xFF;
        }

        private int calculateNumberOfBytesToReturn(int numberOfBytesRequested) {
            int available = data.length - offset;
            int numberOfBytesToReturn;
            if (available > 1 && numberOfBytesRequested > 1) {
                // Return fewer bytes than requested and fewer than are available, avoiding EOF.
                numberOfBytesToReturn = Math.min(available - 1, numberOfBytesRequested - 1);
            } else if (available <= 0) {
                return -1; // EOF
            } else {
                // Only 1 byte is available, so return it as long as at least 1 byte was requested.
                numberOfBytesToReturn = Math.min(numberOfBytesRequested, available);
            }
            return numberOfBytesToReturn;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (off + len > b.length) {
                throw new IndexOutOfBoundsException();
            }
            int numberOfBytesToReturn = calculateNumberOfBytesToReturn(len);
            if (numberOfBytesToReturn < 0) {
                if (throwFromReadOnEof) {
                    throw new EOFException();
                }
                return -1;
            }
            System.arraycopy(data, offset, b, off, numberOfBytesToReturn);
            offset += numberOfBytesToReturn;
            return numberOfBytesToReturn;
        }

        @Override
        public long skip(long len) {
            int numberOfBytesToSkip = calculateNumberOfBytesToReturn((int) len);
            if (numberOfBytesToSkip < 0) {
                // The contract for InputStream.skip() does not use -1 to indicate EOF. It always returns the actual
                // number of bytes skipped.
                return 0;
            }
            offset += numberOfBytesToSkip;
            return numberOfBytesToSkip;
        }
    }

    @ParameterizedTest(name = "incrementalReadingEnabled={0},throwOnEof={1}")
    @CsvSource({
        "true, true",
        "true, false",
        "false, true",
        "false, false"
    })
    public void shouldNotFailWhenAnInputStreamProvidesFewerBytesThanRequestedWithoutReachingEof(boolean incrementalReadingEnabled, boolean throwOnEof) throws Exception {
        readerBuilder = readerBuilder.withIncrementalReadingEnabled(incrementalReadingEnabled)
            .withBufferConfiguration(IonBufferConfiguration.Builder.standard().withInitialBufferSize(8).build());
        reader = readerFor(new ThrottlingInputStream(bytes(0xE0, 0x01, 0x00, 0xEA, 0x89, 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i'), throwOnEof));
        assertSequence(
            next(IonType.STRING), stringValue("abcdefghi"),
            next(null)
        );
        reader.close();
    }

    @ParameterizedTest(name = "throwOnEof={0}")
    @ValueSource(booleans = {true, false})
    public void shouldNotFailWhenAnInputStreamProvidesFewerBytesThanRequestedWithoutReachingEofAndTheIncrementalReaderSkipsTheValue(boolean throwOnEof) throws Exception {
        readerBuilder = readerBuilder.withIncrementalReadingEnabled(true);
        reader = boundedReaderFor(new ThrottlingInputStream(bytes(0xE0, 0x01, 0x00, 0xEA, 0x89, 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 0x20), throwOnEof), 8, 8, byteAndOversizedValueCountingHandler);
        assertSequence(
            // The string value is oversized, and is automatically skipped to comply with the incremental reader's
            // guarantee that next() results in a fully-buffered top-level value.
            next(IonType.INT), intValue(0),
            next(null)
        );
        reader.close();
        assertEquals(1, oversizedCounter.get());
    }

    @ParameterizedTest(name = "throwOnEof={0}")
    @ValueSource(booleans = {true, false})
    public void shouldNotFailWhenAnInputStreamProvidesFewerBytesThanRequestedWithoutReachingEofAndTheNonIcrementalReaderSkipsTheValue(boolean throwOnEof) throws Exception {
        readerBuilder = readerBuilder.withIncrementalReadingEnabled(false);
        reader = boundedReaderFor(new ThrottlingInputStream(bytes(0xE0, 0x01, 0x00, 0xEA, 0x89, 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 0x20), throwOnEof), 8, 8, byteAndOversizedValueCountingHandler);
        assertSequence(
            // The string value is oversized, but since the reader is non-incremental, an error is only surfaced if the
            // user tries to materialize it.
            next(IonType.STRING),
            next(IonType.INT), intValue(0),
            next(null)
        );
        reader.close();
        assertEquals(0, oversizedCounter.get());
    }

    @ParameterizedTest(name = "incrementalReadingEnabled={0},throwOnEof={1}")
    @CsvSource({
        "true, true",
        "true, false",
        "false, true",
        "false, false"
    })
    public void shouldNotFailWhenAnInputStreamProvidesFewerBytesThanRequestedWithoutReachingEofWithAnUnboundedBufferAndTheReaderSkipsTheValue(boolean incrementalReadingEnabled, boolean throwOnEof) throws Exception {
        readerBuilder = readerBuilder.withIncrementalReadingEnabled(incrementalReadingEnabled);
        // The reader's buffer size is unbounded, but the string value is larger than the initial buffer size. Skipping
        // the value therefore causes some of its bytes to be skipped without ever being buffered, by calling
        // InputStream.skip().
        reader = boundedReaderFor(new ThrottlingInputStream(bytes(0xE0, 0x01, 0x00, 0xEA, 0x89, 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 0x20), throwOnEof), 6, Integer.MAX_VALUE, byteAndOversizedValueCountingHandler);
        assertSequence(
            next(IonType.STRING), // skip the string value, causing unbuffered bytes to be skipped
            next(IonType.INT), intValue(0),
            next(null)
        );
        reader.close();
        assertEquals(0, oversizedCounter.get());
    }

    @ParameterizedTest(name = "incrementalReadingEnabled={0},throwOnEof={1}")
    @CsvSource({
        "true, true",
        "true, false",
        "false, true",
        "false, false"
    })
    public void shouldNotFailWhenAnUnbufferedNestedContainerIsSkipped(boolean incrementalReadingEnabled, boolean throwOnEof) throws Exception {
        readerBuilder = readerBuilder.withIncrementalReadingEnabled(incrementalReadingEnabled);
        // The reader's buffer size is unbounded, but the nested container value is larger than the initial buffer size.
        // Skipping the nested container therefore causes some of its bytes to be skipped without ever being buffered,
        // by calling InputStream.skip().
        reader = boundedReaderFor(
            new ThrottlingInputStream(
                bytes(
                    0xE0, 0x01, 0x00, 0xEA, // IVM
                    0xB9, // List, length 9
                    0xB7, // List, length 7
                    0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, // 7 int 0 values
                    0x20, // int 0 value
                    0x20 // Int 0 value
                ),
                throwOnEof
            ),
            5,
            Integer.MAX_VALUE,
            byteAndOversizedValueCountingHandler
        );
        assertSequence(
            container(IonType.LIST,
                next(IonType.LIST),
                next(IonType.INT)
            ),
            next(IonType.INT), intValue(0),
            next(null)
        );
        reader.close();
    }

    @ParameterizedTest(name = "incrementalReadingEnabled={0},throwOnEof={1}")
    @CsvSource({
        "true, true",
        "true, false",
        "false, true",
        "false, false"
    })
    public void shouldNotFailWhenAnUnbufferedNestedContainerIsSteppedOutEarly(boolean incrementalReadingEnabled, boolean throwOnEof) throws Exception {
        readerBuilder = readerBuilder.withIncrementalReadingEnabled(incrementalReadingEnabled);
        // The reader's buffer size is unbounded, but the nested container value is larger than the initial buffer size.
        // Stepping out of the nested container early therefore causes some of its bytes to be skipped without ever
        // being buffered, by calling InputStream.skip().
        reader = boundedReaderFor(
            new ThrottlingInputStream(
                bytes(
                    0xE0, 0x01, 0x00, 0xEA, // IVM
                    0xB9, // List, length 9
                        0xB7, // List, length 7
                            0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, // 7 int 0 values
                        0x20, // int 0 value
                    0x20 // Int 0 value
                ),
                throwOnEof
            ),
            5,
            Integer.MAX_VALUE,
            byteAndOversizedValueCountingHandler
        );
        assertSequence(
            container(IonType.LIST,
                next(IonType.LIST), STEP_IN, STEP_OUT
            ),
            next(IonType.INT), intValue(0),
            next(null)
        );
        reader.close();
    }

    @Test
    public void nestedNullContainerIsParsedSuccessfully() throws Exception {
        readerBuilder = readerBuilder.withIncrementalReadingEnabled(false);
        ResizingPipedInputStream pipe = new ResizingPipedInputStream(128);
        reader = readerFor(pipe);
        pipe.receive(bytes(
            0xE0, 0x01, 0x00, 0xEA, // IVM
            0xB5, // List, length 5
                0xB3, // List, length 3
                    0xBF, 0xCF // Null list, null s-exp
        ));
        assertSequence(
            next(IonType.LIST), STEP_IN,
            next(IonType.LIST), STEP_IN,
            next(IonType.LIST),
            next(IonType.SEXP)
        );
        pipe.receive(bytes(
                    0xDF, // Null struct
                0x20 // Int 0
        ));
        assertSequence(
            next(IonType.STRUCT),
            STEP_OUT,
            next(IonType.INT), intValue(0),
            STEP_OUT,
            next(null)
        );
        reader.close();
    }

    @Test
    public void nestedEmptyContainerIsParsedSuccessfully() throws Exception {
        readerBuilder = readerBuilder.withIncrementalReadingEnabled(false);
        ResizingPipedInputStream pipe = new ResizingPipedInputStream(128);
        reader = readerFor(pipe);
        pipe.receive(bytes(
            0xE0, 0x01, 0x00, 0xEA, // IVM
            0xB5, // List, length 5
                0xB3, // List, length 3
                    0xB0, 0xC0 // Empty list, empty s-exp
        ));
        assertSequence(
            next(IonType.LIST), STEP_IN,
            next(IonType.LIST), STEP_IN,
            next(IonType.LIST),
            next(IonType.SEXP)
        );
        pipe.receive(bytes(
                    0xD0, // empty struct
                0x20 // Int 0
        ));
        assertSequence(
            next(IonType.STRUCT),
            STEP_OUT,
            next(IonType.INT), intValue(0),
            STEP_OUT,
            next(null)
        );
        reader.close();
    }

    @Test
    public void nestedContainerIsSkippedSuccessfully() throws Exception {
        readerBuilder = readerBuilder.withIncrementalReadingEnabled(false);
        ResizingPipedInputStream pipe = new ResizingPipedInputStream(128);
        reader = readerFor(pipe);
        pipe.receive(bytes(
            0xE0, 0x01, 0x00, 0xEA, // IVM
            0xB5, // List, length 5
                0xB3, // List, length 3
                    0xB2,  // List, length 2
                        0xC0  // Empty s-exp
        ));
        assertSequence(
            next(IonType.LIST), STEP_IN,
            next(IonType.LIST), STEP_IN,
            next(IonType.LIST)
        );
        pipe.receive(bytes(
                        0xD0, // Empty struct
                0x20 // Int 0
        ));
        assertSequence(
            next(null), // Skip the list
            STEP_OUT,
            next(IonType.INT), intValue(0),
            STEP_OUT,
            next(null)
        );
        reader.close();
    }

    @Test
    public void nestedContainerIsSteppedOutEarlySuccessfully() throws Exception {
        readerBuilder = readerBuilder.withIncrementalReadingEnabled(false);
        ResizingPipedInputStream pipe = new ResizingPipedInputStream(128);
        reader = readerFor(pipe);
        pipe.receive(bytes(
            0xE0, 0x01, 0x00, 0xEA, // IVM
            0xB5, // List, length 5
                0xB3, // List, length 3
                    0xB2,  // List, length 2
                        0xC0  // Empty s-exp
        ));
        assertSequence(
            next(IonType.LIST), STEP_IN,
            next(IonType.LIST), STEP_IN,
            next(IonType.LIST), STEP_IN
        );
        pipe.receive(bytes(
                        0xD0, // Empty struct
            0x20 // Int 0
        ));
        assertSequence(
            STEP_OUT, // Step out early
            STEP_OUT, // Step out early
            next(IonType.INT), intValue(0),
            STEP_OUT,
            next(null)
        );
        reader.close();
    }

    @Test
    public void shouldNotFailWhenGZIPBoundaryIsEncounteredInStringValue() throws Exception {
        ResizingPipedInputStream pipe = new ResizingPipedInputStream(128);
        // The following lines create a GZIP payload boundary (trailer/header) in the middle of an Ion string value.
        pipe.receive(gzippedBytes(0xE0, 0x01, 0x00, 0xEA, 0x89, 'a', 'b'));
        pipe.receive(gzippedBytes('c', 'd', 'e', 'f', 'g', 'h', 'i'));
        reader = readerFor(new GZIPInputStream(pipe));
        assertSequence(
            next(IonType.STRING), stringValue("abcdefghi"),
            next(null)
        );
    }

    @Test
    public void concatenatedAfterGZIPHeader() throws Exception {
        // Tests that a stream that initially contains only a GZIP header can be read successfully if more data
        // is later made available.
        final int gzipHeaderLength = 10; // Length of the GZIP header, as defined by the GZIP spec.
        byte[] gzIVM = gzippedBytes(0xE0, 0x01, 0x00, 0xEA);  // IVM
        ResizingPipedInputStream pipe = new ResizingPipedInputStream(128);
        // First, feed just the GZIP header bytes.
        pipe.receive(gzIVM, 0, gzipHeaderLength); // Just the GZIP header
        // We must build the reader after the input stream has some content
        reader = readerFor(new GZIPInputStream(pipe));
        //  On next(), the GZIPInputStream will throw EOFException, which is handled by the reader.
        assertSequence(next(null));
        // Finish feeding the gzipped IVM payload
        pipe.receive(gzIVM, gzipHeaderLength, gzIVM.length - gzipHeaderLength);
        // Now feed the bytes for an Ion value, spanning the value across two GZIP payloads.
        pipe.receive(gzippedBytes(0x2E)); // Positive int with length subfield
        pipe.receive(gzippedBytes(0x81, 0x01)); // Length 1, value 1
        assertSequence(
            next(IonType.INT), intValue(1),
            next(null)
        );
    }

    @Test
    public void concatenatedAfterMissingGZIPTrailer() throws Exception {
        // Tests that a stream that initially ends without a GZIP trailer can be read successfully if more data
        // eventually ending with a GZIP trailer is made available.
        final int gzipTrailerLength = 8; // Length of the GZIP trailer, as defined by the GZIP spec.
        // Limiting the size of the buffer allows the test to exercise a single-byte read from the underlying input,
        // which only occurs after a value is determined to be oversized, but still needs to be partially processed
        // to determine whether it might be a symbol table.
        final int maxBufferSize = 6;
        ResizingPipedInputStream pipe = new ResizingPipedInputStream(128);
        // IVM followed by an annotation wrapper that declares length 7, then three overpadded VarUInt bytes that
        // indicate two bytes of annotations follow, followed by two overpadded VarUInt bytes that indicate that
        // the value's only annotation has SID 4 ('name').
        byte[] gzChunk = gzippedBytes(0xE0, 0x01, 0x00, 0xEA, 0xE7, 0x00, 0x00, 0x82, 0x00, 0x84);
        // First, feed the bytes (representing an incomplete Ion value) to the reader, but without a GZIP trailer.
        pipe.receive(gzChunk, 0, gzChunk.length - gzipTrailerLength);
        // We must build the reader after the input stream has some content
        reader = boundedReaderFor(new GZIPInputStream(pipe), maxBufferSize, maxBufferSize, byteAndOversizedValueCountingHandler);
        // During this next(), the reader will read and discard
        // the IVM, then hold all the rest of these bytes in its buffer, which is then at its maximum size. At that
        // point, the reader will attempt to read another byte (expecting a type ID byte), and determine that the
        // value is oversized because the buffer is already at its maximum size. Since the byte cannot be buffered,
        // the reader consumes it directly from the underlying InputStream using a single-byte read, which causes
        // GZIPInputStream to throw EOFException because the GZIP trailer is missing.
        assertSequence(next(null));
        // Now, finish the previous GZIP payload by appending the trailer, then append a concatenated GZIP payload
        // that contains the rest of the Ion value.
        pipe.receive(gzChunk, gzChunk.length - gzipTrailerLength, gzipTrailerLength);
        // Positive integer length 1, with value 1 (the rest of the annotated value), then integer 0.
        pipe.receive(gzippedBytes(0x21, 0x01, 0x20));
        // On this next, the reader will finish skipping the oversized value by calling skip() on the underlying
        // GZIPInputStream, which is able to complete the request successfully now that the GZIP trailer is available.
        // The following value (integer 0) is then read successfully.
        assertSequence(
            next(IonType.INT), intValue(0),
            next(null)
        );
        assertEquals(1, oversizedCounter.get());
    }

    @Test
    public void shouldNotFailWhenProvidedWithAnEmptyByteArrayInputStream() throws Exception {
        reader = IonReaderBuilder.standard().build(new ByteArrayInputStream(new byte[]{}));
        assertSequence(next(null));
        reader.close();
        // The following ByteArrayInputStream is weird, but not disallowed. Its available() method will return -1.
        reader = IonReaderBuilder.standard().build(new ByteArrayInputStream(new byte[]{}, 1, 1));
        assertSequence(next(null));
        reader.close();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void incompleteContainerNonContinuable(boolean constructFromBytes) throws Exception {
        readerBuilder.withIncrementalReadingEnabled(false);
        reader = readerFor(constructFromBytes, 0xB6, 0x20);
        // Because the reader is non-continuable, it can safely convey the incomplete value's type without making any
        // guarantees about the completeness of the value. This allows users of the non-continuable reader to partially
        // read incomplete values. If the user attempts to consume or skip past the value, the reader will raise an
        // unexpected EOF exception.
        assertSequence(next(IonType.LIST));
        // Note: the legacy binary reader implementation did not throw in this case; that behavior remains.
        reader.close();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void incompleteContainerContinuable(boolean constructFromBytes) throws Exception {
        readerBuilder.withIncrementalReadingEnabled(true);
        reader = readerFor(constructFromBytes, 0xB6, 0x20);
        // Unlike the non-continuable case, the continuable reader cannot return LIST in this case because the
        // value is not yet complete. If it returned LIST, it would indicate to the user that it is safe to consume
        // or skip the value with the guarantee that no unexpected EOF exception can occur.
        assertSequence(next(null));
        // When closed, the reader must throw an exception for unexpected EOF to disambiguate clean stream end from
        // incomplete value.
        assertThrows(IonException.class, () -> reader.close());
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void skipIncompleteContainerNonContinuable(boolean constructFromBytes) throws Exception {
        readerBuilder.withIncrementalReadingEnabled(false);
        reader = readerFor(constructFromBytes, 0xB6, 0x20);
        // Because the reader is non-continuable, it can safely convey the incomplete value's type without making any
        // guarantees about the completeness of the value. This allows users of the non-continuable reader to partially
        // read incomplete values.
        assertSequence(next(IonType.LIST));
        // Attempting to skip past the value, results in an unexpected EOF exception.
        assertThrows(IonException.class, () -> reader.next());
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void skipIncompleteContainerContinuable(boolean constructFromBytes) throws Exception {
        readerBuilder.withIncrementalReadingEnabled(true);
        reader = readerFor(constructFromBytes, 0xB6, 0x20);
        assertSequence(
            // Unlike the non-continuable case, the continuable reader cannot return LIST in this case because the
            // value is not yet complete. If it returned LIST, it would indicate to the user that it is safe to consume
            // or skip the value with the guarantee that no unexpected EOF exception can occur.
            next(null),
            // Attempting to skip past the incomplete value should still not raise an exception in continuable mode.
            next(null)
        );
        // When closed, the reader must throw an exception for unexpected EOF to disambiguate clean stream end from
        // incomplete value.
        assertThrows(IonException.class, () -> reader.close());
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void completeValueInIncompleteContainerNonContinuable(boolean constructFromBytes) throws Exception {
        readerBuilder.withIncrementalReadingEnabled(false);
        reader = readerFor(constructFromBytes, 0xB6, 0x20);
        // Because the reader is non-continuable, it can safely convey the incomplete value's type without making any
        // guarantees about the completeness of the value. This allows users of the non-continuable reader to partially
        // read incomplete values. If the user attempts to consume or skip past the value, the reader will raise an
        // unexpected EOF exception.
        assertSequence(
            next(IonType.LIST), STEP_IN,
                // The incomplete container can be partially read until the bytes run out.
                next(IonType.INT), intValue(0),
                next(null)
        );
        // Note: the legacy binary reader implementation did not throw in this case; that behavior remains.
        reader.close();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void incompleteValueInIncompleteContainerNonContinuable(boolean constructFromBytes) throws Exception {
        readerBuilder.withIncrementalReadingEnabled(false);
        reader = readerFor(constructFromBytes, 0xB6, 0x21);
        // Because the reader is non-continuable, it can safely convey the incomplete value's type without making any
        // guarantees about the completeness of the value. This allows users of the non-continuable reader to partially
        // read incomplete values. If the user attempts to consume or skip past the value, the reader will raise an
        // unexpected EOF exception.
        assertSequence(
            next(IonType.LIST), STEP_IN,
            next(IonType.INT)
        );
        // Attempting to skip past the incomplete value results in an error.
        assertThrows(IonException.class, () -> reader.next());
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void earlyStepOutNonIncremental(boolean constructFromBytes) throws Exception {
        readerBuilder = readerBuilder.withIncrementalReadingEnabled(false);
        reader = readerFor(constructFromBytes, 0xB4, 0x20); // List length 4 followed by a single byte
        assertSequence(
            next(IonType.LIST), STEP_IN,
            // Early step-out. The reader could choose to fail here, but it is expensive to check for EOF on every stepOut.
            STEP_OUT
        );
        // However, the reader *must* fail if the user requests the next value, because the stream is incomplete.
        assertThrows(IonException.class, () -> reader.next()); // Unexpected EOF
    }

    /**
     * Verifies that the reader throws IonException when the provided code is executed over the given input.
     * @param constructFromBytes whether to provide bytes (true) or an InputStream (false) to the reader.
     * @param codeExpectedToThrow the code expected to throw IonException.
     * @param input the input data.
     */
    private void expectIonException(
        boolean constructFromBytes,
        Consumer<IonReader> codeExpectedToThrow,
        int... input
    ) throws Exception {
        readerBuilder = readerBuilder.withIncrementalReadingEnabled(false).withBufferConfiguration(IonBufferConfiguration.DEFAULT);
        reader = readerFor(constructFromBytes, input);
        assertThrows(IonException.class, () -> codeExpectedToThrow.accept(reader));
        reader.close();
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void scalarValueLargerThan2GB(boolean constructFromBytes) throws Exception {
        expectIonException(
            constructFromBytes,
            reader -> {
                reader.next();
                reader.longValue();
            },
            0x2E, 0x7F, 0x7F, 0x7F, 0x7F, 0x7F, 0xFF // Int with length larger than 2GB
        );
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void scalarValueLargerThan2GBWithinSymbolTable(boolean constructFromBytes) throws Exception {
        expectIonException(
            constructFromBytes,
            IonReader::next,
            0xEB, 0x81, 0x83, // Annotation wrapper length 11 with one annotation: 3 ($ion_symbol_table)
            0xD8, // Struct length 8
            0x87, 0x2E, 0x7F, 0x7F, 0x7F, 0x7F, 0x7F, 0xFF // Field name 7 (symbols), int with length larger than 2GB
        );
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void possibleSymbolTableStructDeclaresLengthLargerThan2GB(boolean constructFromBytes) throws Exception {
        expectIonException(
            constructFromBytes,
            IonReader::next,
            0xEB, 0x81, 0x83, // Annotation wrapper length 11 with one annotation: 3 ($ion_symbol_table)
            0xDE, 0x7F, 0x7F, 0x7F, 0x7F, 0x7F, 0xFF, // Struct with length larger than 2GB
            0x87, 0x20 // Field name 7 (symbols), int 0
        );
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void possibleSymbolTableAnnotationDeclaresLengthLargerThan2GB(boolean constructFromBytes) throws Exception {
        expectIonException(
            constructFromBytes,
            IonReader::next,
            0xEE, 0x7F, 0x7F, 0x7F, 0x7F, 0x7F, 0xFF, // Annotation wrapper with length larger than 2GB
            0x81, 0x83, // One annotation: 3 ($ion_symbol_table)
            0xDE, 0x7F, 0x7F, 0x7F, 0x7F, 0x7F, 0x7C, // Struct with length larger than 2GB
            0x87, 0x20 // Field name 7 (symbols), int 0
        );
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void annotationSequenceLengthThatOverflowsBufferThrowsIonException(boolean constructFromBytes) throws Exception {
        // This emulates a bad byte (0x52) replacing the annotation sequence length VarUInt in a symbol table annotation
        // wrapper. This results in a declared annotation sequence length much larger than the total length of the
        // stream. This should be detected and raised as an IonException.
        expectIonException(
            constructFromBytes,
            IonReader::next,
            0xEE, 0x9A, 0x52, 0x83, 0xDE, 0x96, 0x87
        );
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void incompleteAnnotationAfterStructFailsCleanly(boolean constructFromBytes) throws Exception {
        expectIonException(
            constructFromBytes,
            reader -> {
                try {
                    assertEquals(IonType.STRUCT, reader.next());
                } catch (Exception e) {
                    fail();
                }
                // This should fail with IonException due to unexpected EOF.
                reader.next();
            },
            0xDF, // null.struct
            0xE6, 0x81 // Incomplete annotation wrapper declaring length 6
        );
    }

    /**
     * Verifies that corrupting each byte in the input data results in IonException, or nothing.
     * @param constructFromBytes whether to provide bytes (true) or an InputStream (false) to the reader.
     * @param incrementalReadingEnabled whether to enable incremental reading.
     * @param emptyBufferConfiguration whether to set the buffer configuration to null (true), or use the standard test configuration (false).
     */
    private void corruptEachByteThrowsIonException(
        boolean constructFromBytes,
        boolean incrementalReadingEnabled,
        boolean emptyBufferConfiguration
    ) {
        IonReaderBuilder builder = readerBuilder.copy();
        builder = builder.withIncrementalReadingEnabled(incrementalReadingEnabled);
        if (emptyBufferConfiguration) {
            builder = builder.withBufferConfiguration(IonBufferConfiguration.DEFAULT);
        }
        // The following data begins with a symbol table, contains nested containers with length subfields, and
        // contains a string, an integer, and blobs.
        byte[] original = toBinary(
            "{\n" +
            "  C:\"CRDR\",\n" +
            "  V:3,\n" +
            "  DR:{\n" +
            "    P:\"dummyPartitionKey\",\n" +
            "    D:{{ dGVzdERhdGE= }},\n" +
            "    S:{{ AeJA }},\n" +
            "    DC:{{ brzdAsJNBCzI3S63zno7Uw== }},\n" +
            "    HC:{{ f5C7STBM1f2S4SFkJWgbIizTYBE= }}\n" +
            "  }\n" +
            "}"
        );
        byte[] corrupted = new byte[original.length];
        for (int i = 0; i < corrupted.length; i++) {
            // Fill with the original data
            System.arraycopy(original, 0, corrupted, 0, original.length);
            // Corrupt the byte at index i
            corrupted[i] = 0x52;
            // Ensure the corruption results in IonException or nothing.
            reader = readerFor(builder, constructFromBytes, corrupted);
            try {
                TestUtils.deepRead(reader);
                // Successful parsing is possible because the input contains blobs whose content is not inspected for corruption.
                reader.close();
            } catch (IonException e) {
                // This is expected to be thrown when corruption is detected.
            } catch (Exception f) {
                fail("Expected IonException to be thrown, but was: ", f);
            }
        }
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void corruptEachByteThrowsIonException(boolean constructFromBytes) {
        corruptEachByteThrowsIonException(constructFromBytes, true, true);
        corruptEachByteThrowsIonException(constructFromBytes, true, false);
        corruptEachByteThrowsIonException(constructFromBytes, false, true);
        corruptEachByteThrowsIonException(constructFromBytes, false, false);
    }

    @ParameterizedTest(name = "constructFromBytes={0}")
    @ValueSource(booleans = {true, false})
    public void expectUseAfterCloseToHaveNoEffect(boolean constructFromBytes) throws Exception {
        // Using the cursor after close should not fail with an obscure unchecked
        // exception like NullPointerException, ArrayIndexOutOfBoundsException, etc.
        reader = readerFor(
            constructFromBytes,
            0xE0, 0x01, 0x00, 0xEA,
            0x20
        );
        reader.close();
        assertNull(reader.next());
        assertNull(reader.getType());
        assertNull(reader.next());
        assertNull(reader.getType());
        reader.close();
    }

    /**
     * Creates an IonReader over the given data, which will be prepended with a binary Ion 1.1 IVM.
     * @param data the data to read.
     * @param constructFromBytes whether to construct the reader from bytes or an InputStream.
     * @return a new reader.
     */
    private IonReader readerForIon11(byte[] data, boolean constructFromBytes) throws Exception {
        byte[] inputBytes = new TestUtils.BinaryIonAppender(1).append(data).toByteArray();
        reader = readerFor(readerBuilder, constructFromBytes, inputBytes);
        byteCounter.set(0);
        return reader;
    }

    /**
     * Checks that the reader reads a null value of the expected type from the given input bytes.
     */
    private void assertNullCorrectlyParsed(boolean constructFromBytes, IonType expectedType, String inputBytes) throws Exception {
        reader = readerForIon11(hexStringToByteArray(inputBytes), constructFromBytes);
        assertSequence(
            next(expectedType), nullValue(expectedType),
            next(null)
        );
        closeAndCount();
    }

    @ParameterizedTest
    @CsvSource({
        "     NULL, EA",
        "     BOOL, EB 00",
        "      INT, EB 01",
        "    FLOAT, EB 02",
        "  DECIMAL, EB 03",
        "TIMESTAMP, EB 04",
        "   STRING, EB 05",
        "   SYMBOL, EB 06",
        "     BLOB, EB 07",
        "     CLOB, EB 08",
        "     LIST, EB 09",
        "     SEXP, EB 0A",
        "   STRUCT, EB 0B",
    })
    public void readNullValue(IonType expectedType, String inputBytes) throws Exception {
        assertNullCorrectlyParsed(true, expectedType, inputBytes);
        assertNullCorrectlyParsed(false, expectedType, inputBytes);
    }

    /**
     * Checks that the reader reads the expected boolean from the given input bits.
     */
    private void assertBooleanCorrectlyParsed(boolean constructFromBytes, boolean expectedValue, String inputBytes) throws Exception {
        reader = readerForIon11(hexStringToByteArray(inputBytes), constructFromBytes);
        assertSequence(
            next(IonType.BOOL), booleanValue(expectedValue),
            next(null)
        );
        closeAndCount();
    }

    @ParameterizedTest
    @CsvSource({
        "true, 5E",
        "false, 5F",
    })
    public void readBooleanValue(Boolean expectedValue, String inputBytes) throws Exception {
        assertBooleanCorrectlyParsed(true, expectedValue, inputBytes);
        assertBooleanCorrectlyParsed(false, expectedValue, inputBytes);
    }

    /**
     * Checks that the reader reads the expected int from the given input bits.
     */
    private void assertIntCorrectlyParsed(boolean constructFromBytes, int expectedValue, String inputBytes) throws Exception {
        reader = readerForIon11(hexStringToByteArray(inputBytes), constructFromBytes);
        assertSequence(
            next(IonType.INT), intValue(expectedValue),
            next(null)
        );
        closeAndCount();
    }

    @ParameterizedTest
    @CsvSource({
        "                             0, 50",
        "                             1, 51 01",
        "                            17, 51 11",
        "                           127, 51 7F",
        "                           128, 52 80 00",
        "                          5555, 52 B3 15",
        "                         32767, 52 FF 7F",
        "                         32768, 53 00 80 00",
        "                        292037, 53 C5 74 04",
        "                     321672342, 54 96 54 2C 13",
        "                    2147483647, 54 FF FF FF 7F", // Integer.MAX_VALUE
        "                            -1, 51 FF",
        "                            -2, 51 FE",
        "                           -14, 51 F2",
        "                          -128, 51 80",
        "                          -129, 52 7F FF",
        "                          -944, 52 50 FC",
        "                        -32768, 52 00 80",
        "                        -32769, 53 FF 7F FF",
        "                      -8388608, 53 00 00 80",
        "                      -8388609, 54 FF FF 7F FF",
        "                   -2147483648, 54 00 00 00 80", // Integer.MIN_VALUE
    })
    public void readIntValue(int expectedValue, String inputBytes) throws Exception {
        assertIntCorrectlyParsed(true, expectedValue, inputBytes);
        assertIntCorrectlyParsed(false, expectedValue, inputBytes);
    }

    /**
     * Checks that the reader reads the expected long from the given input bits.
     */
    private void assertLongCorrectlyParsed(boolean constructFromBytes, long expectedValue, String inputBytes) throws Exception {
        reader = readerForIon11(hexStringToByteArray(inputBytes), constructFromBytes);
        assertSequence(
            next(IonType.INT), longValue(expectedValue),
            next(null)
        );
        closeAndCount();
    }

    @ParameterizedTest
    @CsvSource({
        "                             0, 50",
        "                             1, 51 01",
        "                            17, 51 11",
        "                           127, 51 7F",
        "                           127, 52 7F 00",
        "                           127, 54 7F 00 00 00",
        "                           127, 58 7F 00 00 00 00 00 00 00",
        "                           128, 52 80 00",
        "                          5555, 52 B3 15",
        "                         32767, 52 FF 7F",
        "                         32768, 53 00 80 00",
        "                        292037, 53 C5 74 04",
        "                     321672342, 54 96 54 2C 13",
        "                    2147483647, 54 FF FF FF 7F", // Integer.MAX_VALUE
        "                   64121672342, 55 96 12 F3 ED 0E",
        "                 1274120283167, 56 1F A4 7C A7 28 01",
        "               851274120283167, 57 1F C4 8B B3 3A 06 03",
        "             72624976668147840, 58 80 40 20 10 08 04 02 01",
        "           9223372036854775807, 58 FF FF FF FF FF FF FF 7F", // Long.MAX_VALUE
        "                            -1, 51 FF",
        "                            -2, 51 FE",
        "                           -14, 51 F2",
        "                          -128, 51 80",
        "                          -129, 52 7F FF",
        "                          -944, 52 50 FC",
        "                        -32768, 52 00 80",
        "                        -32769, 53 FF 7F FF",
        "                      -8388608, 53 00 00 80",
        "                      -8388609, 54 FF FF 7F FF",
        "                   -2147483648, 54 00 00 00 80", // Integer.MIN_VALUE
        "            -72624976668147841, 58 7F BF DF EF F7 FB FD FE",
        "          -9223372036854775808, 58 00 00 00 00 00 00 00 80", // Long.MIN_VALUE
    })
    public void readLongValue(long expectedValue, String inputBytes) throws Exception {
        assertLongCorrectlyParsed(true, expectedValue, inputBytes);
        assertLongCorrectlyParsed(false, expectedValue, inputBytes);
    }

    @ParameterizedTest
    @CsvSource({
        "                             0, F5 01",
        "                             1, F5 03 01",
        "                            17, F5 03 11",
        "                           127, F5 03 7F",
        "                           128, F5 05 80 00",
        "                    2147483647, F5 09 FF FF FF 7F", // Integer.MAX_VALUE
        "             72624976668147840, F5 11 80 40 20 10 08 04 02 01",
        "           9223372036854775807, F5 11 FF FF FF FF FF FF FF 7F", // Long.MAX_VALUE
        "                            -1, F5 03 FF",
        "                            -2, F5 03 FE",
        "                           -14, F5 03 F2",
        "                          -128, F5 03 80",
        "                          -129, F5 05 7F FF",
        "                   -2147483648, F5 09 00 00 00 80", // Integer.MIN_VALUE
        "            -72624976668147841, F5 11 7F BF DF EF F7 FB FD FE",
        "          -9223372036854775808, F5 11 00 00 00 00 00 00 00 80", // Long.MIN_VALUE
    })
    public void readLongValueFromVariableLengthEncoding(long expectedValue, String inputBytes) throws Exception {
        assertLongCorrectlyParsed(true, expectedValue, inputBytes);
        assertLongCorrectlyParsed(false, expectedValue, inputBytes);
    }

    /**
     * Checks that the reader reads the expected BigInteger from the given input bits.
     */
    private void assertBigIntegerCorrectlyParsed(boolean constructFromBytes, BigInteger expectedValue, String inputBytes) throws Exception {
        reader = readerForIon11(hexStringToByteArray(inputBytes), constructFromBytes);
        assertSequence(
            next(IonType.INT), bigIntegerValue(expectedValue),
            next(null)
        );
        closeAndCount();
    }

    @ParameterizedTest
    @CsvSource({
        "                             0, 50",
        "                             1, 51 01",
        "                            17, 51 11",
        "                           127, 51 7F",
        "                           127, 52 7F 00",
        "                           127, 54 7F 00 00 00",
        "                           127, 58 7F 00 00 00 00 00 00 00",
        "                           128, 52 80 00",
        "                          5555, 52 B3 15",
        "                         32767, 52 FF 7F",
        "                         32768, 53 00 80 00",
        "                        292037, 53 C5 74 04",
        "                     321672342, 54 96 54 2C 13",
        "                    2147483647, 54 FF FF FF 7F", // Integer.MAX_VALUE
        "                   64121672342, 55 96 12 F3 ED 0E",
        "                 1274120283167, 56 1F A4 7C A7 28 01",
        "               851274120283167, 57 1F C4 8B B3 3A 06 03",
        "             72624976668147840, 58 80 40 20 10 08 04 02 01",
        "           9223372036854775807, 58 FF FF FF FF FF FF FF 7F", // Long.MAX_VALUE
        "           9223372036854775808, F5 13 00 00 00 00 00 00 00 80 00",
        "999999999999999999999999999999, F5 1B FF FF FF 3F EA ED 74 46 D0 9C 2C 9F 0C",
        "                            -1, 51 FF",
        "                            -2, 51 FE",
        "                           -14, 51 F2",
        "                          -128, 51 80",
        "                          -129, 52 7F FF",
        "                          -944, 52 50 FC",
        "                        -32768, 52 00 80",
        "                        -32769, 53 FF 7F FF",
        "                      -8388608, 53 00 00 80",
        "                      -8388609, 54 FF FF 7F FF",
        "                   -2147483648, 54 00 00 00 80", // Integer.MIN_VALUE
        "            -72624976668147841, 58 7F BF DF EF F7 FB FD FE",
        "          -9223372036854775808, 58 00 00 00 00 00 00 00 80", // Long.MIN_VALUE
        "          -9223372036854775809, F5 13 FF FF FF FF FF FF FF 7F FF",
        "-99999999999999999999999999999, F5 1B 01 00 00 60 35 E8 8D 92 51 F0 E1 BC FE",
    })
    public void readBigIntegerValue(BigInteger expectedValue, String inputBytes) throws Exception {
        assertBigIntegerCorrectlyParsed(true, expectedValue, inputBytes);
        assertBigIntegerCorrectlyParsed(false, expectedValue, inputBytes);
    }

    /**
     * Checks that the reader reads the expected double from the given input bits.
     */
    private void assertDoubleCorrectlyParsed(boolean constructFromBytes, double expectedValue, String inputBytes) throws Exception {
        reader = readerForIon11(hexStringToByteArray(inputBytes), constructFromBytes);
        assertSequence(
            next(IonType.FLOAT), doubleValue(expectedValue),
            next(null)
        );
        closeAndCount();
    }

    @ParameterizedTest
    @CsvSource({
        "                      0.0, 5A",
        "                      0.0, 5B 00 00",
        "                      0.0, 5C 00 00 00 00",
        "                      0.0, 5D 00 00 00 00 00 00 00 00",
        "                     -0.0, 5B 80 00",
        "                     -0.0, 5C 80 00 00 00",
        "                     -0.0, 5D 80 00 00 00 00 00 00 00",
        "                      1.0, 5B 3C 00",
        "                      1.0, 5C 3F 80 00 00",
        "                      1.0, 5D 3F F0 00 00 00 00 00 00",
        "                      1.5, 5C 3F C0 00 00",
        "         0.00006103515625, 5B 04 00", // Smallest positive normal half-precision float
        "           0.333251953125, 5B 35 55", // Nearest half-precision representation of one third
        "        3.141592653589793, 5D 40 09 21 FB 54 44 2D 18",
        "            4.00537109375, 5C 40 80 2C 00",
        "            4.11111111111, 5D 40 10 71 C7 1C 71 C2 39",
        "                    65504, 5B 7B FF", // Largest normal half-precision float
        "             423542.09375, 5C 48 CE CE C3",
        "         8236423542.09375, 5D 41 FE AE DD 97 61 80 00",
        " 1.79769313486231570e+308, 5D 7F EF FF FF FF FF FF FF", // Double.MAX_VALUE
        "                     -1.0, 5C BF 80 00 00",
        "                     -1.5, 5C BF C0 00 00",
        "                       -2, 5B C0 00",
        "       -3.141592653589793, 5D C0 09 21 FB 54 44 2D 18",
        "           -4.00537109375, 5C C0 80 2C 00",
        "           -4.11111111111, 5D C0 10 71 C7 1C 71 C2 39",
        "                   -65504, 5B FB FF", // Smallest normal half-precision float
        "            -423542.09375, 5C C8 CE CE C3",
        "        -8236423542.09375, 5D C1 FE AE DD 97 61 80 00",
        "-1.79769313486231570e+308, 5D FF EF FF FF FF FF FF FF", // Double.MIN_VALUE
        "                      NaN, 5B 7C 01",
        "                 Infinity, 5B 7C 00",
        "                -Infinity, 5B FC 00",
        "                      NaN, 5C 7F C0 00 00",
        "                 Infinity, 5C 7F 80 00 00",
        "                -Infinity, 5C FF 80 00 00",
        "                      NaN, 5D 7F F0 00 00 00 00 00 01",
        "                 Infinity, 5D 7F F0 00 00 00 00 00 00",
        "                -Infinity, 5D FF F0 00 00 00 00 00 00",
    })
    public void readDoubleValue(double expectedValue, String inputBytes) throws Exception {
        assertDoubleCorrectlyParsed(true, expectedValue, inputBytes);
        assertDoubleCorrectlyParsed(false, expectedValue, inputBytes);
    }

    /**
     * Checks that the reader reads the expected Decimal or BigDecimal from the given input bits.
     */
    private void assertDecimalCorrectlyParsed(
        boolean constructFromBytes,
        BigDecimal expectedValue,
        String inputBytes,
        Function<BigDecimal, ExpectationProvider<IonReaderContinuableTopLevelBinary>> expectationProviderFunction
    ) throws Exception {
        reader = readerForIon11(hexStringToByteArray(inputBytes), constructFromBytes);
        assertSequence(
            next(IonType.DECIMAL), expectationProviderFunction.apply(expectedValue),
            next(null)
        );
        closeAndCount();
    }

    @ParameterizedTest
    @CsvSource({
        "                                 0., 60",
        "                                0e1, 61 03",
        "                               0e63, 61 7F",
        "                               0e64, 62 02 01",
        "                               0e99, 62 8E 01",
        "                                0.0, 61 FF",
        "                               0.00, 61 FD",
        "                              0.000, 61 FB",
        "                              0e-64, 61 81",
        "                              0e-99, 62 76 FE",
        "                                -0., 62 01 00",
        "                               -0e1, 62 03 00",
        "                               -0e3, 62 07 00",
        "                              -0e63, 62 7F 00",
        "                             -0e199, 63 1E 03 00",
        "                              -0e-1, 62 FF 00",
        "                              -0e-2, 62 FD 00",
        "                              -0e-3, 62 FB 00",
        "                             -0e-63, 62 83 00",
        "                             -0e-64, 62 81 00",
        "                             -0e-65, 63 FE FE 00",
        "                            -0e-199, 63 E6 FC 00",
        "                               0.01, 62 FD 01",
        "                                0.1, 62 FF 01",
        "                                  1, 62 01 01",
        "                                1e1, 62 03 01",
        "                                1e2, 62 05 01",
        "                               1e63, 62 7F 01",
        "                               1e64, 63 02 01 01",
        "                            1e65536, 64 04 00 08 01",
        "                                  2, 62 01 02",
        "                                  7, 62 01 07",
        "                                 14, 62 01 0E",
        "                                 14, 63 02 00 0E", // overpadded exponent
        "                                 14, 64 01 0E 00 00", // Overpadded coefficient
        "                                 14, 65 02 00 0E 00 00", // Overpadded coefficient and exponent
        "                                1.0, 62 FF 0A",
        "                               1.00, 62 FD 64",
        "                               1.27, 62 FD 7F",
        "                               1.28, 63 FD 80 00",
        "                              3.142, 63 FB 46 0C",
        "                            3.14159, 64 F7 2F CB 04",
        "                          3.1415927, 65 F3 77 5E DF 01",
        "                        3.141592653, 66 EF 4D E6 40 BB 00",
        "                     3.141592653590, 67 E9 16 9F 83 75 DB 02",
        "                3.14159265358979323, 69 DF FB A0 9E F6 2F 1E 5C 04",
        "           3.1415926535897932384626, 6B D5 72 49 64 CC AF EF 8F 0F A7 06",
        "      3.141592653589793238462643383, 6D CB B7 3C 92 86 40 9F 1B 01 1F AA 26 0A",
        " 3.14159265358979323846264338327950, 6F C1 8E 29 E5 E3 56 D5 DF C5 10 8F 55 3F 7D 0F",
        "3.141592653589793238462643383279503, F6 21 BF 8F 9F F3 E6 64 55 BE BA A7 96 57 79 E4 9A 00",
    })
    public void readDecimalValue(@ConvertWith(TestUtils.StringToDecimal.class) Decimal expectedValue, String inputBytes) throws Exception {
        assertDecimalCorrectlyParsed(true, expectedValue, inputBytes, IonReaderContinuableTopLevelBinaryTest::decimalValue);
        assertDecimalCorrectlyParsed(false, expectedValue, inputBytes, IonReaderContinuableTopLevelBinaryTest::decimalValue);
        assertDecimalCorrectlyParsed(true, expectedValue, inputBytes, IonReaderContinuableTopLevelBinaryTest::bigDecimalValue);
        assertDecimalCorrectlyParsed(false, expectedValue, inputBytes, IonReaderContinuableTopLevelBinaryTest::bigDecimalValue);
    }

    @ParameterizedTest
    @CsvSource({
        "                                 0., F6 01",
        "                               0e99, F6 05 8E 01",
        "                                0.0, F6 03 FF",
        "                               0.00, F6 03 FD",
        "                              0e-99, F6 05 76 FE",
        "                                -0., F6 05 01 00",
        "                             -0e199, F6 07 1E 03 00",
        "                              -0e-1, F6 05 FF 00",
        "                             -0e-65, F6 07 FE FE 00",
        "                               0.01, F6 05 FD 01",
        "                                  1, F6 05 01 01",
        "                            1e65536, F6 09 04 00 08 01",
        "                                1.0, F6 05 FF 0A",
        "                               1.28, F6 07 FD 80 00",
        "                     3.141592653590, F6 0F E9 16 9F 83 75 DB 02",
        "                3.14159265358979323, F6 13 DF FB A0 9E F6 2F 1E 5C 04",
        "           3.1415926535897932384626, F6 17 D5 72 49 64 CC AF EF 8F 0F A7 06",
        "      3.141592653589793238462643383, F6 1B CB B7 3C 92 86 40 9F 1B 01 1F AA 26 0A",
        " 3.14159265358979323846264338327950, F6 1F C1 8E 29 E5 E3 56 D5 DF C5 10 8F 55 3F 7D 0F",
    })
    public void readDecimalValueFromVariableLengthEncoding(@ConvertWith(TestUtils.StringToDecimal.class) Decimal expectedValue, String inputBytes) throws Exception {
        assertDecimalCorrectlyParsed(true, expectedValue, inputBytes, IonReaderContinuableTopLevelBinaryTest::decimalValue);
        assertDecimalCorrectlyParsed(false, expectedValue, inputBytes, IonReaderContinuableTopLevelBinaryTest::decimalValue);
        assertDecimalCorrectlyParsed(true, expectedValue, inputBytes, IonReaderContinuableTopLevelBinaryTest::bigDecimalValue);
        assertDecimalCorrectlyParsed(false, expectedValue, inputBytes, IonReaderContinuableTopLevelBinaryTest::bigDecimalValue);
    }

    /**
     * Checks that the reader reads the expected timestamp value from the given input bits.
     */
    private void assertIonTimestampCorrectlyParsed(boolean constructFromBytes, Timestamp expected, String inputBits) throws Exception {
        reader = readerForIon11(bitStringToByteArray(inputBits), constructFromBytes);
        assertSequence(
            next(IonType.TIMESTAMP), timestampValue(expected),
            next(null)
        );
        closeAndCount();
    }

    @ParameterizedTest
    @CsvSource({
        //                               OpCode   MYYYYYYY DDDDDMMM mmmHHHHH ssssUmmm ffffffss ffffffff ffffffff ffffffff
        "2023-10-15T01:00Z,              01110011 00110101 01111101 00000001 00001000",
        "2023-10-15T01:59Z,              01110011 00110101 01111101 01100001 00001111",
        "2023-10-15T11:22Z,              01110011 00110101 01111101 11001011 00001010",
        "2023-10-15T23:00Z,              01110011 00110101 01111101 00010111 00001000",
        "2023-10-15T23:59Z,              01110011 00110101 01111101 01110111 00001111",
        "2023-10-15T11:22:00Z,           01110100 00110101 01111101 11001011 00001010 00000000",
        "2023-10-15T11:22:33Z,           01110100 00110101 01111101 11001011 00011010 00000010",
        "2023-10-15T11:22:59Z,           01110100 00110101 01111101 11001011 10111010 00000011",
        "2023-10-15T11:22:33.000Z,       01110101 00110101 01111101 11001011 00011010 00000010 00000000",
        "2023-10-15T11:22:33.444Z,       01110101 00110101 01111101 11001011 00011010 11110010 00000110",
        "2023-10-15T11:22:33.999Z,       01110101 00110101 01111101 11001011 00011010 10011110 00001111",
        "2023-10-15T11:22:33.000000Z,    01110110 00110101 01111101 11001011 00011010 00000010 00000000 00000000",
        "2023-10-15T11:22:33.444555Z,    01110110 00110101 01111101 11001011 00011010 00101110 00100010 00011011",
        "2023-10-15T11:22:33.999999Z,    01110110 00110101 01111101 11001011 00011010 11111110 00001000 00111101",
        "2023-10-15T11:22:33.000000000Z, 01110111 00110101 01111101 11001011 00011010 00000010 00000000 00000000 00000000",
        "2023-10-15T11:22:33.444555666Z, 01110111 00110101 01111101 11001011 00011010 01001010 10000110 11111101 01101001",
        "2023-10-15T11:22:33.999999999Z, 01110111 00110101 01111101 11001011 00011010 11111110 00100111 01101011 11101110",
    })
    public void readTimestampValueWithUtcShortForm(@ConvertWith(StringToTimestamp.class) Timestamp expectedValue, String inputBits) throws Exception {
        assertIonTimestampCorrectlyParsed(true, expectedValue, inputBits);
        assertIonTimestampCorrectlyParsed(false, expectedValue, inputBits);
    }

    @ParameterizedTest
    @CsvSource({
        //                                    OpCode   MYYYYYYY DDDDDMMM mmmHHHHH ssssUmmm ffffffss ffffffff ffffffff ffffffff
        "1970T,                               01110000 00000000",
        "2023T,                               01110000 00110101",
        "2097T,                               01110000 01111111",
        "2023-01T,                            01110001 10110101 00000000",
        "2023-10T,                            01110001 00110101 00000101",
        "2023-12T,                            01110001 00110101 00000110",
        "2023-10-01T,                         01110010 00110101 00001101",
        "2023-10-15T,                         01110010 00110101 01111101",
        "2023-10-31T,                         01110010 00110101 11111101",
        "2023-10-15T01:00-00:00,              01110011 00110101 01111101 00000001 00000000",
        "2023-10-15T01:59-00:00,              01110011 00110101 01111101 01100001 00000111",
        "2023-10-15T11:22-00:00,              01110011 00110101 01111101 11001011 00000010",
        "2023-10-15T23:00-00:00,              01110011 00110101 01111101 00010111 00000000",
        "2023-10-15T23:59-00:00,              01110011 00110101 01111101 01110111 00000111",
        "2023-10-15T11:22:00-00:00,           01110100 00110101 01111101 11001011 00000010 00000000",
        "2023-10-15T11:22:33-00:00,           01110100 00110101 01111101 11001011 00010010 00000010",
        "2023-10-15T11:22:59-00:00,           01110100 00110101 01111101 11001011 10110010 00000011",
        "2023-10-15T11:22:33.000-00:00,       01110101 00110101 01111101 11001011 00010010 00000010 00000000",
        "2023-10-15T11:22:33.444-00:00,       01110101 00110101 01111101 11001011 00010010 11110010 00000110",
        "2023-10-15T11:22:33.999-00:00,       01110101 00110101 01111101 11001011 00010010 10011110 00001111",
        "2023-10-15T11:22:33.000000-00:00,    01110110 00110101 01111101 11001011 00010010 00000010 00000000 00000000",
        "2023-10-15T11:22:33.444555-00:00,    01110110 00110101 01111101 11001011 00010010 00101110 00100010 00011011",
        "2023-10-15T11:22:33.999999-00:00,    01110110 00110101 01111101 11001011 00010010 11111110 00001000 00111101",
        "2023-10-15T11:22:33.000000000-00:00, 01110111 00110101 01111101 11001011 00010010 00000010 00000000 00000000 00000000",
        "2023-10-15T11:22:33.444555666-00:00, 01110111 00110101 01111101 11001011 00010010 01001010 10000110 11111101 01101001",
        "2023-10-15T11:22:33.999999999-00:00, 01110111 00110101 01111101 11001011 00010010 11111110 00100111 01101011 11101110",
    })
    public void readTimestampValueWithUnknownOffsetShortForm(@ConvertWith(StringToTimestamp.class) Timestamp expectedValue, String inputBits) throws Exception {
        assertIonTimestampCorrectlyParsed(true, expectedValue, inputBits);
        assertIonTimestampCorrectlyParsed(false, expectedValue, inputBits);
    }

    @ParameterizedTest
    @CsvSource({
        //                                    OpCode   MYYYYYYY DDDDDMMM mmmHHHHH ooooommm ssssssoo ffffffff ffffffff ffffffff ..ffffff
        "2023-10-15T01:00-14:00,              01111000 00110101 01111101 00000001 00000000 00000000",
        "2023-10-15T01:00+14:00,              01111000 00110101 01111101 00000001 10000000 00000011",
        "2023-10-15T01:00-01:15,              01111000 00110101 01111101 00000001 10011000 00000001",
        "2023-10-15T01:00+01:15,              01111000 00110101 01111101 00000001 11101000 00000001",
        "2023-10-15T01:59+01:15,              01111000 00110101 01111101 01100001 11101111 00000001",
        "2023-10-15T11:22+01:15,              01111000 00110101 01111101 11001011 11101010 00000001",
        "2023-10-15T23:00+01:15,              01111000 00110101 01111101 00010111 11101000 00000001",
        "2023-10-15T23:59+01:15,              01111000 00110101 01111101 01110111 11101111 00000001",
        "2023-10-15T11:22:00+01:15,           01111001 00110101 01111101 11001011 11101010 00000001",
        "2023-10-15T11:22:33+01:15,           01111001 00110101 01111101 11001011 11101010 10000101",
        "2023-10-15T11:22:59+01:15,           01111001 00110101 01111101 11001011 11101010 11101101",
        "2023-10-15T11:22:33.000+01:15,       01111010 00110101 01111101 11001011 11101010 10000101 00000000 00000000",
        "2023-10-15T11:22:33.444+01:15,       01111010 00110101 01111101 11001011 11101010 10000101 10111100 00000001",
        "2023-10-15T11:22:33.999+01:15,       01111010 00110101 01111101 11001011 11101010 10000101 11100111 00000011",
        "2023-10-15T11:22:33.000000+01:15,    01111011 00110101 01111101 11001011 11101010 10000101 00000000 00000000 00000000",
        "2023-10-15T11:22:33.444555+01:15,    01111011 00110101 01111101 11001011 11101010 10000101 10001011 11001000 00000110",
        "2023-10-15T11:22:33.999999+01:15,    01111011 00110101 01111101 11001011 11101010 10000101 00111111 01000010 00001111",
        "2023-10-15T11:22:33.000000000+01:15, 01111100 00110101 01111101 11001011 11101010 10000101 00000000 00000000 00000000 00000000",
        "2023-10-15T11:22:33.444555666+01:15, 01111100 00110101 01111101 11001011 11101010 10000101 10010010 01100001 01111111 00011010",
        "2023-10-15T11:22:33.999999999+01:15, 01111100 00110101 01111101 11001011 11101010 10000101 11111111 11001001 10011010 00111011",

    })
    public void readTimestampValueWithKnownOffsetShortForm(@ConvertWith(StringToTimestamp.class) Timestamp expectedValue, String inputBits) throws Exception {
        assertIonTimestampCorrectlyParsed(true, expectedValue, inputBits);
        assertIonTimestampCorrectlyParsed(false, expectedValue, inputBits);
    }

    @ParameterizedTest
    @CsvSource({
        //                                    OpCode   Length   YYYYYYYY MMYYYYYY HDDDDDMM mmmmHHHH oooooomm ssoooooo ....ssss Scale+ Coefficient
        "0001T,                               11110111 00000101 00000001 00000000",
        "1947T,                               11110111 00000101 10011011 00000111",
        "9999T,                               11110111 00000101 00001111 00100111",
        "1947-01T,                            11110111 00000111 10011011 01000111 00000000",
        "1947-12T,                            11110111 00000111 10011011 00000111 00000011",
        "1947-01-01T,                         11110111 00000111 10011011 01000111 00000100",
        "1947-12-23T,                         11110111 00000111 10011011 00000111 01011111",
        "1947-12-31T,                         11110111 00000111 10011011 00000111 01111111",
        "1947-12-23T00:00Z,                   11110111 00001101 10011011 00000111 01011111 00000000 10000000 00010110",
        "1947-12-23T23:59Z,                   11110111 00001101 10011011 00000111 11011111 10111011 10000011 00010110",
        "1947-12-23T23:59:00Z,                11110111 00001111 10011011 00000111 11011111 10111011 10000011 00010110 00000000",
        "1947-12-23T23:59:59Z,                11110111 00001111 10011011 00000111 11011111 10111011 10000011 11010110 00001110",
        "1947-12-23T23:59:00.0Z,              11110111 00010001 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00000011",
        "1947-12-23T23:59:00.00Z,             11110111 00010001 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00000101",
        "1947-12-23T23:59:00.000Z,            11110111 00010001 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00000111",
        "1947-12-23T23:59:00.0000Z,           11110111 00010001 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00001001",
        "1947-12-23T23:59:00.00000Z,          11110111 00010001 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00001011",
        "1947-12-23T23:59:00.000000Z,         11110111 00010001 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00001101",
        "1947-12-23T23:59:00.0000000Z,        11110111 00010001 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00001111",
        "1947-12-23T23:59:00.00000000Z,       11110111 00010001 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00010001",
        "1947-12-23T23:59:00.9Z,              11110111 00010011 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00000011 00001001",
        "1947-12-23T23:59:00.99Z,             11110111 00010011 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00000101 01100011",
        "1947-12-23T23:59:00.999Z,            11110111 00010101 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00000111 11100111 00000011",
        "1947-12-23T23:59:00.9999Z,           11110111 00010101 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00001001 00001111 00100111",
        "1947-12-23T23:59:00.99999Z,          11110111 00010111 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00001011 10011111 10000110 00000001",
        "1947-12-23T23:59:00.999999Z,         11110111 00010111 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00001101 00111111 01000010 00001111",
        "1947-12-23T23:59:00.9999999Z,        11110111 00011001 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00001111 01111111 10010110 10011000 00000000",
        "1947-12-23T23:59:00.99999999Z,       11110111 00011001 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00010001 11111111 11100000 11110101 00000101",

        "1947-12-23T23:59:00.9223372036854775807Z, " + // Long.MAX_VALUE
            "11110111 00100001 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00100111 11111111 11111111 11111111 11111111 11111111 11111111 11111111 01111111",

        "1947-12-23T23:59:00.9223372036854775808Z, " + // Long.MAX_VALUE + 1 (unsigned)
            "11110111 00100001 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00100111 00000000 00000000 00000000 00000000 00000000 00000000 00000000 10000000",

        "1947-12-23T23:59:00.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000Z, " +
            "11110111 00010011 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00110110 00000010",

        "1947-12-23T23:59:00.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
            "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
            "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000Z, " +
            "11110111 00010011 10011011 00000111 11011111 10111011 10000011 00010110 00000000 10100010 00000101",

        "1947-12-23T23:59:00.999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999Z, " +
            "11110111 10001001 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00110110 00000010 11111111 11111111 11111111 11111111 11111111 11111111 " +
            "11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111 10011111 00110010 00110001 10001111 11001101 00011001 " +
            "01001111 00011110 10101000 11001111 11110100 00011000 11010101 00101000 00101011 10101110 00001001 10100100 11011110 01001101 10001111 00100001 11100001 " +
            "11111101 01101111 11011110 10000011 11100010 00011010 11101101 10001110 10010101 11001101 01010001 11100110 01010110 01010000 11011110 00000110 01001101 " +
            "11111110 00010100",

        // Offsets
        "2048-01-01T01:01-23:59,              11110111 00001101 00000000 01001000 10000100 00010000 00000100 00000000",
        "2048-01-01T01:01-00:02,              11110111 00001101 00000000 01001000 10000100 00010000 01111000 00010110",
        "2048-01-01T01:01-00:01,              11110111 00001101 00000000 01001000 10000100 00010000 01111100 00010110",
        "2048-01-01T01:01-00:00,              11110111 00001101 00000000 01001000 10000100 00010000 11111100 00111111",
        "2048-01-01T01:01+00:00,              11110111 00001101 00000000 01001000 10000100 00010000 10000000 00010110",
        "2048-01-01T01:01+00:01,              11110111 00001101 00000000 01001000 10000100 00010000 10000100 00010110",
        "2048-01-01T01:01+00:02,              11110111 00001101 00000000 01001000 10000100 00010000 10001000 00010110",
        "2048-01-01T01:01+23:59,              11110111 00001101 00000000 01001000 10000100 00010000 11111100 00101100",
    })
    public void readTimestampValueLongForm(@ConvertWith(StringToTimestamp.class) Timestamp expectedValue, String inputBits) throws Exception {
        assertIonTimestampCorrectlyParsed(true, expectedValue, inputBits);
        assertIonTimestampCorrectlyParsed(false, expectedValue, inputBits);
    }

    /**
     * Checks that the reader reads the expected string value from the given input bits.
     */
    private void assertIonStringCorrectlyParsed(boolean constructFromBytes, String expected, String inputBytes) throws Exception {
        reader = readerForIon11(hexStringToByteArray(inputBytes), constructFromBytes);
        assertSequence(
            next(IonType.STRING), stringValue(expected),
            next(null)
        );
        closeAndCount();
    }

    @ParameterizedTest
    @CsvSource({
        "'', 80",
        "'', F8 01",
        "'a', 81 61",
        "'a', F8 03 61",
        "'ab', 82 61 62",
        "'abc', 83 61 62 63",
        "'fourteen bytes', 8E 66 6F 75 72 74 65 65 6E 20 62 79 74 65 73",
        "'fourteen bytes', F8 1D 66 6F 75 72 74 65 65 6E 20 62 79 74 65 73",
        "'this has sixteen', F8 21 74 68 69 73 20 68 61 73 20 73 69 78 74 65 65 6E",
        "'variable length encoding', F8 31 76 61 72 69 61 62 6C 65 20 6C 65 6E 67 74 68 20 65 6E 63 6F 64 69 6E 67",
    })
    public void readStringValue(String expectedValue, String inputBytes) throws Exception {
        assertIonStringCorrectlyParsed(true, expectedValue, inputBytes);
        assertIonStringCorrectlyParsed(false, expectedValue, inputBytes);
    }

    /**
     * Checks that the reader reads the expected symbol text from the given input bits.
     */
    private void assertIonSymbolCorrectlyParsed(boolean constructFromBytes, String expected, String inputBytes) throws Exception {
        reader = readerForIon11(hexStringToByteArray(inputBytes), constructFromBytes);
        assertSequence(
            next(IonType.SYMBOL), stringValue(expected),
            next(null)
        );
        closeAndCount();
    }

    @ParameterizedTest
    @CsvSource({
        "'', 90",
        "'', F9 01",
        "'a', 91 61",
        "'a', F9 03 61",
        "'ab', 92 61 62",
        "'abc', 93 61 62 63",
        "'fourteen bytes', 9E 66 6F 75 72 74 65 65 6E 20 62 79 74 65 73",
        "'fourteen bytes', F9 1D 66 6F 75 72 74 65 65 6E 20 62 79 74 65 73",
        "'this has sixteen', F9 21 74 68 69 73 20 68 61 73 20 73 69 78 74 65 65 6E",
        "'variable length encoding', F9 31 76 61 72 69 61 62 6C 65 20 6C 65 6E 67 74 68 20 65 6E 63 6F 64 69 6E 67",
    })
    public void readSymbolValueWithInlineText(String expectedValue, String inputBytes) throws Exception {
        assertIonSymbolCorrectlyParsed(true, expectedValue, inputBytes);
        assertIonSymbolCorrectlyParsed(false, expectedValue, inputBytes);
    }

    /**
     * Checks that the reader reads the expected clob or blob value from the given input bits, using IonReader.newBytes.
     */
    private void assertIonLobCorrectlyParsedViaNewBytes(
        boolean constructFromBytes,
        IonType lobType,
        byte[] expected,
        String inputBytes
    ) throws Exception {
        reader = readerForIon11(hexStringToByteArray(inputBytes), constructFromBytes);
        assertSequence(
            next(lobType), newBytesValue(expected),
            next(null)
        );
        closeAndCount();
    }

    @ParameterizedTest
    @CsvSource({
        "'', FE 01",
        "20, FE 03 20",
        "49 20 61 70 70 6C 61 75 64 20 79 6F 75 72 20 63 75 72 69 6F 73 69 74 79, " +
            "FE 31 49 20 61 70 70 6C 61 75 64 20 79 6F 75 72 20 63 75 72 69 6F 73 69 74 79"
    })
    public void readBlobValueViaNewBytes(@ConvertWith(TestUtils.HexStringToByteArray.class) byte[] expectedBytes, String inputBytes) throws Exception {
        assertIonLobCorrectlyParsedViaNewBytes(true, IonType.BLOB, expectedBytes, inputBytes);
        assertIonLobCorrectlyParsedViaNewBytes(false, IonType.BLOB, expectedBytes, inputBytes);
    }

    @ParameterizedTest
    @CsvSource({
        "'', FF 01",
        "20, FF 03 20",
        "49 20 61 70 70 6C 61 75 64 20 79 6F 75 72 20 63 75 72 69 6F 73 69 74 79, " +
            "FF 31 49 20 61 70 70 6C 61 75 64 20 79 6F 75 72 20 63 75 72 69 6F 73 69 74 79"
    })
    public void readClobValueViaNewBytes(@ConvertWith(TestUtils.HexStringToByteArray.class) byte[] expectedBytes, String inputBytes) throws Exception {
        assertIonLobCorrectlyParsedViaNewBytes(true, IonType.CLOB, expectedBytes, inputBytes);
        assertIonLobCorrectlyParsedViaNewBytes(false, IonType.CLOB, expectedBytes, inputBytes);
    }

    /**
     * Checks that the reader reads the expected clob or blob value from the given input bits, using IonReader.getBytes.
     */
    private void assertIonLobCorrectlyParsedViaGetBytes(
        boolean constructFromBytes,
        IonType lobType,
        byte[] expected,
        String inputBytes
    ) throws Exception {
        reader = readerForIon11(hexStringToByteArray(inputBytes), constructFromBytes);
        byte[] shiftedBytes = new byte[expected.length + 7];
        System.arraycopy(expected, 0, shiftedBytes, 3, expected.length);
        assertSequence(
            next(lobType), getBytesValue(shiftedBytes, 3, expected.length, expected.length),
            next(null)
        );
        closeAndCount();
    }

    @ParameterizedTest
    @CsvSource({
        "'', FE 01",
        "20, FE 03 20",
        "49 20 61 70 70 6C 61 75 64 20 79 6F 75 72 20 63 75 72 69 6F 73 69 74 79, " +
            "FE 31 49 20 61 70 70 6C 61 75 64 20 79 6F 75 72 20 63 75 72 69 6F 73 69 74 79"
    })
    public void readBlobValueViaGetBytes(@ConvertWith(TestUtils.HexStringToByteArray.class) byte[] expectedBytes, String inputBytes) throws Exception {
        assertIonLobCorrectlyParsedViaGetBytes(true, IonType.BLOB, expectedBytes, inputBytes);
        assertIonLobCorrectlyParsedViaGetBytes(false, IonType.BLOB, expectedBytes, inputBytes);
    }

    @ParameterizedTest
    @CsvSource({
        "'', FF 01",
        "20, FF 03 20",
        "49 20 61 70 70 6C 61 75 64 20 79 6F 75 72 20 63 75 72 69 6F 73 69 74 79, " +
            "FF 31 49 20 61 70 70 6C 61 75 64 20 79 6F 75 72 20 63 75 72 69 6F 73 69 74 79"
    })
    public void readClobValueViaGetBytes(@ConvertWith(TestUtils.HexStringToByteArray.class) byte[] expectedBytes, String inputBytes) throws Exception {
        assertIonLobCorrectlyParsedViaGetBytes(true, IonType.CLOB, expectedBytes, inputBytes);
        assertIonLobCorrectlyParsedViaGetBytes(false, IonType.CLOB, expectedBytes, inputBytes);
    }
}
