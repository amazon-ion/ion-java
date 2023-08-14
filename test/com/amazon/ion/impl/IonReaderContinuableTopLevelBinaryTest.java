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
import com.amazon.ion.impl.bin._Private_IonManagedBinaryWriterBuilder;
import com.amazon.ion.impl.bin._Private_IonManagedWriter;
import com.amazon.ion.impl.bin._Private_IonRawWriter;
import com.amazon.ion.system.IonBinaryWriterBuilder;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazon.ion.system.SimpleCatalog;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static com.amazon.ion.BitUtils.bytes;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class IonReaderContinuableTopLevelBinaryTest {

    private static final IonSystem SYSTEM = IonSystemBuilder.standard().build();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Parameterized.Parameters(name = "constructWithBytes={0}")
    public static Object[] parameters() {
        return new Object[]{true, false};
    }

    @Parameterized.Parameter
    public boolean constructFromBytes;

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
            Assert.fail("Oversized symbol table not expected.");
        }

        @Override
        public void onOversizedValue() {
            Assert.fail("Oversized value not expected.");
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
    
    @Before
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
     * @param bytes the binary Ion data.
     * @return a new reader.
     */
    private IonReader readerFor(IonReaderBuilder builder, byte[] bytes) {
        totalBytesInStream = bytes.length;
        if (constructFromBytes) {
            return new IonReaderContinuableTopLevelBinary(builder, bytes, 0, bytes.length);
        }
        return new IonReaderContinuableTopLevelBinary(builder, new ByteArrayInputStream(bytes), null, 0, 0);
    }

    /**
     * Creates an incremental reader over the binary equivalent of the given text Ion.
     * @param ion text Ion data.
     * @return a new reader.
     */
    private IonReader readerFor(String ion) {
        byte[] binary = toBinary(ion);
        totalBytesInStream = binary.length;
        return readerFor(readerBuilder, binary);
    }

    /**
     * Creates an incremental reader over the binary Ion data created by invoking the given RawWriterFunction.
     * @param writerFunction the function used to generate the data.
     * @return a new reader.
     * @throws Exception if an exception is raised while writing the Ion data.
     */
    private IonReader readerFor(RawWriterFunction writerFunction) throws Exception {
        byte[] binary = writeRaw(writerFunction, true);
        totalBytesInStream = binary.length;
        return readerFor(readerBuilder, binary);
    }

    /**
     * Creates an incremental reader over the binary Ion data created by invoking the given WriterFunction.
     * @param writerFunction the function used to generate the data.
     * @return a new reader.
     * @throws Exception if an exception is raised while writing the Ion data.
     */
    private IonReader readerFor(WriterFunction writerFunction) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonWriter writer = writerBuilder.build(out);
        writerFunction.write(writer);
        writer.close();
        byte[] binary = out.toByteArray();
        totalBytesInStream = binary.length;
        return readerFor(readerBuilder, binary);
    }

    /**
     * Creates an incremental reader over the given bytes, prepended with the IVM.
     * @param ion binary Ion bytes without an IVM.
     * @return a new reader.
     */
    private IonReader readerFor(int... ion) throws Exception {
        byte[] binary = new TestUtils.BinaryIonAppender().append(ion).toByteArray();
        totalBytesInStream = binary.length;
        return readerFor(readerBuilder, binary);
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

    private void nextExpect(IonType type) {
        assertEquals(type, reader.next());
    }

    private void typeExpect(IonType type) {
        assertEquals(type, reader.getType());
    }

    private void stepIn() {
        reader.stepIn();
    }

    private void stepOut() {
        reader.stepOut();
    }

    private void expectAnnotations(String... annotations) {
        assertEquals(Arrays.asList(annotations), Arrays.asList(reader.getTypeAnnotations()));
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

    private void closeAndCount() throws IOException {
        reader.close();
        assertBytesConsumed();
    }

    @Test
    public void skipContainers() throws Exception {
        reader = readerFor(
            "[123] 456 {abc: foo::bar::123, def: baz::456} [123] 789 [foo::bar::123, baz::456] [123]"
        );
        nextExpect(IonType.LIST);
        typeExpect(IonType.LIST);
        nextExpect(IonType.INT);
        typeExpect(IonType.INT);
        nextExpect(IonType.STRUCT);
        typeExpect(IonType.STRUCT);
        nextExpect(IonType.LIST);
        typeExpect(IonType.LIST);
        stepIn();
        typeExpect(null);
        nextExpect(IonType.INT);
        typeExpect(IonType.INT);
        expectInt(123);
        stepOut();
        typeExpect(null);
        nextExpect(IonType.INT);
        typeExpect(IonType.INT);
        expectInt(789);
        nextExpect(IonType.LIST);
        typeExpect(IonType.LIST);
        nextExpect(IonType.LIST);
        typeExpect(IonType.LIST);
        nextExpect(null);
        typeExpect(null);
        closeAndCount();
    }

    @Test
    public void skipContainerAfterSteppingIn() throws Exception {
        reader = readerFor("{abc: foo::bar::123, def: baz::456} 789");
        nextExpect(IonType.STRUCT);
        typeExpect(IonType.STRUCT);
        stepIn();
        typeExpect(null);
        nextExpect(IonType.INT);
        typeExpect(IonType.INT);
        expectAnnotations("foo", "bar");
        expectInt(123);
        typeExpect(IonType.INT);
        // Step out before completing the value.
        stepOut();
        typeExpect(null);
        nextExpect(IonType.INT);
        typeExpect(IonType.INT);
        expectInt(789);
        nextExpect(null);
        typeExpect(null);
        closeAndCount();
    }

    @Test
    public void skipValueInContainer() throws Exception {
        reader = readerFor("{foo: \"bar\", abc: 123, baz: a}");
        nextExpect(IonType.STRUCT);
        stepIn();
        nextExpect(IonType.STRING);
        nextExpect(IonType.INT);
        expectField("abc");
        nextExpect(IonType.SYMBOL);
        expectField("baz");
        nextExpect(null);
        stepOut();
        nextExpect(null);
        closeAndCount();
    }

    @Test
    public void symbolsAsStrings() throws Exception {
        reader = readerFor("{foo: uvw::abc, bar: qrs::xyz::def}");
        nextExpect(IonType.STRUCT);
        stepIn();
        nextExpect(IonType.SYMBOL);
        expectField("foo");
        expectAnnotations("uvw");
        expectString("abc");
        nextExpect(IonType.SYMBOL);
        expectField("bar");
        expectAnnotations("qrs", "xyz");
        expectString("def");
        stepOut();
        nextExpect(null);
        closeAndCount();
    }

    @Test
    public void lstAppend() throws Exception {
        writerBuilder = writerBuilder.withLocalSymbolTableAppendEnabled();
        reader = readerFor(writer -> {
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
        });

        nextExpect(IonType.STRUCT);
        stepIn();
        nextExpect(IonType.SYMBOL);
        expectField("foo");
        expectAnnotations("uvw");
        expectString("abc");
        nextExpect(IonType.SYMBOL);
        expectField("bar");
        expectAnnotations("qrs", "xyz");
        expectString("def");
        stepOut();
        SymbolTable preAppend = reader.getSymbolTable();
        nextExpect(IonType.SYMBOL);
        SymbolTable postAppend = reader.getSymbolTable();
        expectString("orange");
        assertNull(preAppend.find("orange"));
        assertNotNull(postAppend.find("orange"));
        nextExpect(null);
        closeAndCount();
    }

    @Test
    public void lstNonAppend() throws Exception {
        writerBuilder = writerBuilder.withLocalSymbolTableAppendDisabled();
        reader = readerFor(writer -> {
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
        });

        nextExpect(IonType.STRUCT);
        stepIn();
        nextExpect(IonType.SYMBOL);
        expectField("foo");
        expectAnnotations("uvw");
        expectString("abc");
        nextExpect(IonType.SYMBOL);
        expectField("bar");
        expectAnnotations("qrs", "xyz");
        expectString("def");
        stepOut();
        nextExpect(IonType.SYMBOL);
        expectString("orange");
        nextExpect(null);
        closeAndCount();
    }

    @Test
    public void ivmBetweenValues() throws Exception {
        writerBuilder = writerBuilder.withLocalSymbolTableAppendDisabled();
        reader = readerFor(writer -> {
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
        });

        nextExpect(IonType.STRUCT);
        stepIn();
        nextExpect(IonType.SYMBOL);
        expectField("foo");
        expectAnnotations("uvw");
        expectString("abc");
        nextExpect(IonType.SYMBOL);
        expectField("bar");
        expectAnnotations("qrs", "xyz");
        expectString("def");
        stepOut();
        nextExpect(IonType.SYMBOL);
        expectString("orange");
        nextExpect(null);
        closeAndCount();
    }

    @Test
    public void ivmOnly() throws Exception {
        reader = readerFor();
        nextExpect(null);
        closeAndCount();
    }

    @Test
    public void twoIvmsOnly() throws Exception {
        reader = readerFor(0xE0, 0x01, 0x00, 0xEA);
        nextExpect(null);
        closeAndCount();
    }

    @Test
    public void multipleSymbolTablesBetweenValues() throws Exception {
        reader = readerFor(writer -> {
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
        });

        nextExpect(IonType.SYMBOL);
        expectString("abc");
        nextExpect(IonType.SYMBOL);
        expectString("def");
        nextExpect(IonType.SYMBOL);
        expectString("purple");
        nextExpect(null);
        closeAndCount();
    }

    @Test
    public void multipleIvmsBetweenValues() throws Exception  {
        reader = readerFor((writer, out) -> {
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
        });

        nextExpect(IonType.SYMBOL);
        expectString("def");
        nextExpect(IonType.SYMBOL);
        expectString("name");
        nextExpect(null);
        closeAndCount();
    }

    @Test
    public void invalidVersion() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(bytes(0xE0, 0x01, 0x74, 0xEA, 0x20));
        reader = readerFor(readerBuilder, out.toByteArray());
        thrown.expect(IonException.class);
        reader.next();
        reader.close();
    }

    @Test
    public void invalidVersionMarker() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(bytes(0xE0, 0x01, 0x00, 0xEB, 0x20));
        reader = readerFor(readerBuilder, out.toByteArray());
        thrown.expect(IonException.class);
        reader.next();
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
        nextExpect(IonType.STRING);
        expectString("StringValueLong");
        nextExpect(null);
        closeAndCount();
    }

    @Test
    public void incrementalMultipleValues() throws Exception {
        ResizingPipedInputStream pipe = new ResizingPipedInputStream(128);
        reader = readerFor(pipe);
        byte[] bytes = toBinary("value_type::\"StringValueLong\"");
        totalBytesInStream = bytes.length;
        feedBytesOneByOne(bytes, pipe, reader);
        nextExpect(IonType.STRING);
        expectString("StringValueLong");
        expectAnnotations("value_type");
        nextExpect(null);
        assertBytesConsumed();
        bytes = toBinary("{foobar: \"StringValueLong\"}");
        totalBytesInStream += bytes.length;
        feedBytesOneByOne(bytes, pipe, reader);
        nextExpect(IonType.STRUCT);
        stepIn();
        nextExpect(IonType.STRING);
        expectField("foobar");
        expectString("StringValueLong");
        nextExpect(null);
        stepOut();
        nextExpect(null);
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
        thrown.expect(IonException.class);
        loader.load(pipe);
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
        nextExpect(null);
        // Valid text Ion. Also hex 0x20, which is binary int 0. However, it is not preceded by the IVM, so it must be
        // interpreted as text. The binary reader must fail.
        pipe.receive(' ');
        thrown.expect(IonException.class);
        reader.next();
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
        nextExpect(IonType.STRUCT);
        stepIn();
        nextExpect(IonType.STRING);
        expectAnnotations("def");
        expectField("abcdefghijklmnopqrstuvwxyz");
        expectString("foo");
        nextExpect(null);
        stepOut();
        assertBytesConsumed();
        feedBytesOneByOne(secondValue, pipe, reader);
        totalBytesInStream += secondValue.length;
        nextExpect(IonType.STRUCT);
        stepIn();
        nextExpect(IonType.STRING);
        expectString("fairlyLongString");
        expectField("abcdefghijklmnopqrstuvwxyz");
        expectAnnotations("foo", "bar");
        nextExpect(null);
        stepOut();
        nextExpect(null);
        closeAndCount();
    }

    @Test
    public void lobsNewBytes() throws Exception {
        final byte[] blobBytes = "abcdef".getBytes(StandardCharsets.UTF_8);
        final byte[] clobBytes = "ghijklmnopqrstuv".getBytes(StandardCharsets.UTF_8);
        reader = readerFor(writer -> {
            writer.writeBlob(blobBytes);
            writer.writeClob(clobBytes);
            writer.setTypeAnnotations("foo");
            writer.writeBlob(blobBytes);
            writer.stepIn(IonType.STRUCT);
            writer.setFieldName("bar");
            writer.writeClob(clobBytes);
            writer.stepOut();
        });

        nextExpect(IonType.BLOB);
        assertArrayEquals(blobBytes, reader.newBytes());
        nextExpect(IonType.CLOB);
        assertArrayEquals(clobBytes, reader.newBytes());
        nextExpect(IonType.BLOB);
        expectAnnotations("foo");
        assertArrayEquals(blobBytes, reader.newBytes());
        nextExpect(IonType.STRUCT);
        stepIn();
        nextExpect(IonType.CLOB);
        expectField("bar");
        assertArrayEquals(clobBytes, reader.newBytes());
        nextExpect(null);
        stepOut();
        nextExpect(null);
        closeAndCount();
    }

    @Test
    public void lobsGetBytes() throws Exception {
        final byte[] blobBytes = "abcdef".getBytes(StandardCharsets.UTF_8);
        final byte[] clobBytes = "ghijklmnopqrstuv".getBytes(StandardCharsets.UTF_8);
        reader = readerFor(writer -> {
            writer.writeBlob(blobBytes);
            writer.writeClob(clobBytes);
            writer.setTypeAnnotations("foo");
            writer.writeBlob(blobBytes);
            writer.stepIn(IonType.STRUCT);
            writer.setFieldName("bar");
            writer.writeClob(clobBytes);
            writer.stepOut();
        });

        nextExpect(IonType.BLOB);
        byte[] fullBlob = new byte[blobBytes.length];
        assertEquals(fullBlob.length, reader.getBytes(fullBlob, 0, fullBlob.length));
        assertArrayEquals(blobBytes, fullBlob);
        nextExpect(IonType.CLOB);
        byte[] partialClob = new byte[clobBytes.length];
        assertEquals(3, reader.getBytes(partialClob, 0, 3));
        assertEquals(clobBytes.length - 3, reader.getBytes(partialClob, 3, clobBytes.length - 3));
        assertArrayEquals(clobBytes, partialClob);
        Arrays.fill(fullBlob, (byte) 0);
        nextExpect(IonType.BLOB);
        assertEquals(fullBlob.length, reader.getBytes(fullBlob, 0, 100000));
        expectAnnotations("foo");
        nextExpect(IonType.STRUCT);
        stepIn();
        Arrays.fill(partialClob, (byte) 0);
        nextExpect(IonType.CLOB);
        assertEquals(5, reader.getBytes(partialClob, 0, 5));
        assertEquals(clobBytes.length - 5, reader.getBytes(partialClob, 5, 100000));
        assertArrayEquals(clobBytes, partialClob);
        nextExpect(null);
        stepOut();
        nextExpect(null);
        closeAndCount();
    }

    @Test
    public void nopPad() throws Exception {
        reader = readerFor(
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

        nextExpect(IonType.INT);
        expectInt(0);
        nextExpect(IonType.INT);
        expectInt(1);
        nextExpect(IonType.STRUCT);
        stepIn();
        nextExpect(IonType.INT);
        expectInt(-1);
        nextExpect(null);
        stepOut();
        nextExpect(IonType.STRUCT);
        stepIn();
        nextExpect(null);
        stepOut();
        nextExpect(IonType.LIST);
        stepIn();
        nextExpect(null);
        stepOut();
        nextExpect(null);
        closeAndCount();
    }

    @Test
    public void symbolTableWithImportsThenSymbols() throws Exception {
        SimpleCatalog catalog = new SimpleCatalog();
        catalog.putTable(SYSTEM.newSharedSymbolTable("foo", 1, Arrays.asList("abc", "def").iterator()));
        readerBuilder = readerBuilder.withCatalog(catalog);
        writerBuilder = writerBuilder.withCatalog(catalog);

        reader = readerFor(writer -> {
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
        });

        nextExpect(IonType.SYMBOL);
        expectString("abc");
        nextExpect(IonType.SYMBOL);
        expectString("def");
        nextExpect(IonType.SYMBOL);
        expectString("ghi");
        nextExpect(null);
        closeAndCount();
    }

    @Test
    public void symbolTableWithSymbolsThenImports() throws Exception {
        SimpleCatalog catalog = new SimpleCatalog();
        catalog.putTable(SYSTEM.newSharedSymbolTable("foo", 1, Arrays.asList("abc", "def").iterator()));
        readerBuilder = readerBuilder.withCatalog(catalog);

        reader = readerFor((writer, out) -> {
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
        });

        nextExpect(IonType.SYMBOL);
        expectString("abc");
        nextExpect(IonType.SYMBOL);
        expectString("def");
        nextExpect(IonType.SYMBOL);
        expectString("ghi");
        nextExpect(null);
        closeAndCount();
    }

    @Test
    public void symbolTableWithManySymbolsThenImports() throws Exception {
        SimpleCatalog catalog = new SimpleCatalog();
        catalog.putTable(SYSTEM.newSharedSymbolTable("foo", 1, Arrays.asList("abc", "def").iterator()));
        readerBuilder = readerBuilder.withCatalog(catalog);

        reader = readerFor((writer, out) -> {
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
        });

        nextExpect(IonType.SYMBOL);
        expectString("abc");
        nextExpect(IonType.SYMBOL);
        expectString("def");
        nextExpect(IonType.SYMBOL);
        expectString("ghi");
        nextExpect(IonType.SYMBOL);
        expectString("jkl");
        nextExpect(IonType.SYMBOL);
        expectString("mno");
        nextExpect(IonType.SYMBOL);
        expectString("pqr");
        nextExpect(null);
        closeAndCount();
    }

    @Test
    public void multipleSymbolTablesWithSymbolsThenImports() throws Exception {
        SimpleCatalog catalog = new SimpleCatalog();
        catalog.putTable(SYSTEM.newSharedSymbolTable("foo", 1, Arrays.asList("abc", "def").iterator()));
        catalog.putTable(SYSTEM.newSharedSymbolTable("bar", 1, Collections.singletonList("baz").iterator()));
        readerBuilder = readerBuilder.withCatalog(catalog);

        reader = readerFor((writer, out) -> {
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
        });

        nextExpect(IonType.SYMBOL);
        expectString("abc");
        SymbolTable[] imports = reader.getSymbolTable().getImportedTables();
        assertEquals(1, imports.length);
        assertEquals("foo", imports[0].getName());
        nextExpect(IonType.SYMBOL);
        expectString("def");
        nextExpect(IonType.SYMBOL);
        expectString("ghi");
        nextExpect(IonType.SYMBOL);
        expectString("baz");
        nextExpect(IonType.SYMBOL);
        imports = reader.getSymbolTable().getImportedTables();
        assertEquals(1, imports.length);
        assertEquals("bar", imports[0].getName());
        expectString("xyz");
        nextExpect(IonType.SYMBOL);
        expectString("uvw");
        nextExpect(IonType.SYMBOL);
        expectString("rst");
        nextExpect(null);
        closeAndCount();
    }

    @Test
    public void ivmResetsImports() throws Exception {
        SimpleCatalog catalog = new SimpleCatalog();
        catalog.putTable(SYSTEM.newSharedSymbolTable("foo", 1, Arrays.asList("abc", "def").iterator()));
        readerBuilder = readerBuilder.withCatalog(catalog);

        reader = readerFor((writer, out) -> {
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
        });

        nextExpect(IonType.SYMBOL);
        expectString("abc");
        SymbolTable[] imports = reader.getSymbolTable().getImportedTables();
        assertEquals(1, imports.length);
        assertEquals("foo", imports[0].getName());
        nextExpect(IonType.SYMBOL);
        expectString("def");
        nextExpect(IonType.SYMBOL);
        expectString("ghi");
        nextExpect(IonType.INT);
        assertTrue(reader.getSymbolTable().isSystemTable());
        expectInt(0);
        nextExpect(null);
        closeAndCount();
    }

    private static void assertSymbolEquals(
        String expectedText,
        ImportLocation expectedImportLocation,
        SymbolToken actual
    ) {
        assertEquals(expectedText, actual.getText());
        SymbolTokenWithImportLocation impl = (SymbolTokenWithImportLocation) actual;
        assertEquals(expectedImportLocation, impl.getImportLocation());
    }

    @Test
    public void symbolsAsTokens() throws Exception {
        reader = readerFor("{foo: uvw::abc, bar: qrs::xyz::def}");
        nextExpect(IonType.STRUCT);
        stepIn();
        nextExpect(IonType.SYMBOL);
        assertSymbolEquals("foo", null, reader.getFieldNameSymbol());
        SymbolToken[] annotations = reader.getTypeAnnotationSymbols();
        assertEquals(1, annotations.length);
        assertSymbolEquals("uvw", null, annotations[0]);
        assertSymbolEquals("abc", null, reader.symbolValue());
        nextExpect(IonType.SYMBOL);
        assertSymbolEquals("bar", null, reader.getFieldNameSymbol());
        annotations = reader.getTypeAnnotationSymbols();
        assertEquals(2, annotations.length);
        assertSymbolEquals("qrs", null, annotations[0]);
        assertSymbolEquals("xyz", null, annotations[1]);
        assertSymbolEquals("def", null, reader.symbolValue());
        stepOut();
        nextExpect(null);
        closeAndCount();
    }

    @Test
    public void intNegativeZeroFails() throws Exception {
        reader = readerFor(0x31, 0x00);
        reader.next();
        thrown.expect(IonException.class);
        reader.longValue();
        reader.close();
    }

    @Test
    public void bigIntNegativeZeroFails() throws Exception {
        reader = readerFor(0x31, 0x00);
        reader.next();
        thrown.expect(IonException.class);
        reader.bigIntegerValue();
        reader.close();
    }

    @Test
    public void listWithLengthTooShortFails() throws Exception {
        reader = readerFor(0xB1, 0x21, 0x01);
        nextExpect(IonType.LIST);
        stepIn();
        thrown.expect(IonException.class);
        reader.next();
        reader.close();
    }

    @Test
    public void listWithContainerValueLengthTooShortFails() throws Exception {
        reader = readerFor(0xB2, 0xB2, 0x21, 0x01);
        nextExpect(IonType.LIST);
        stepIn();
        thrown.expect(IonException.class);
        reader.next();
        reader.close();
    }

    @Test
    public void listWithVariableLengthTooShortFails() throws Exception {
        reader = readerFor(0xBE, 0x81, 0x21, 0x01);
        nextExpect(IonType.LIST);
        stepIn();
        thrown.expect(IonException.class);
        reader.next();
        reader.close();
    }

    @Test(expected = IonException.class)
    public void noOpPadTooShort1() throws Exception {
        reader = readerFor(0x37, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01);
        nextExpect(IonType.INT);
        nextExpect(null);
        reader.close();
    }

    @Test(expected = IonException.class)
    public void noOpPadTooShort2() throws Exception {
        reader = readerFor(
            0x0e, 0x90, 0x00, 0xde, 0xad, 0xbe, 0xef, 0xca, 0xfe, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
        );
        nextExpect(null);
        reader.close();
    }

    @Test
    public void nopPadOneByte() throws Exception {
        reader = readerFor(0);
        nextExpect(null);
        closeAndCount();
        reader.close();
    }

    @Test
    public void localSidOutOfRangeStringValue() throws Exception {
        reader = readerFor(0x71, 0x0A); // SID 10
        nextExpect(IonType.SYMBOL);
        thrown.expect(IonException.class);
        reader.stringValue();
        reader.close();
    }

    @Test
    public void localSidOutOfRangeSymbolValue() throws Exception {
        reader = readerFor(0x71, 0x0A); // SID 10
        nextExpect(IonType.SYMBOL);
        thrown.expect(IonException.class);
        reader.symbolValue();
        reader.close();
    }

    @Test
    public void localSidOutOfRangeFieldName() throws Exception {
        reader = readerFor(
            0xD2, // Struct, length 2
            0x8A, // SID 10
            0x20 // int 0
        );
        nextExpect(IonType.STRUCT);
        stepIn();
        nextExpect(IonType.INT);
        thrown.expect(IonException.class);
        reader.getFieldName();
        reader.close();
    }

    @Test
    public void localSidOutOfRangeFieldNameSymbol() throws Exception {
        reader = readerFor(
            0xD2, // Struct, length 2
            0x8A, // SID 10
            0x20 // int 0
        );
        nextExpect(IonType.STRUCT);
        stepIn();
        nextExpect(IonType.INT);
        thrown.expect(IonException.class);
        reader.getFieldNameSymbol();
        reader.close();
    }

    @Test
    public void localSidOutOfRangeAnnotation() throws Exception {
        reader = readerFor(
            0xE3, // Annotation wrapper, length 3
            0x81, // annotation SID length 1
            0x8A, // SID 10
            0x20 // int 0
        );
        nextExpect(IonType.INT);
        thrown.expect(IonException.class);
        reader.getTypeAnnotations();
        reader.close();
    }

    @Test
    public void localSidOutOfRangeAnnotationSymbol() throws Exception {
        reader = readerFor(
            0xE3, // Annotation wrapper, length 3
            0x81, // annotation SID length 1
            0x8A, // SID 10
            0x20 // int 0
        );
        nextExpect(IonType.INT);
        thrown.expect(IonException.class);
        reader.getTypeAnnotationSymbols();
        reader.close();
    }

    @Test
    public void localSidOutOfRangeIterateAnnotations() throws Exception {
        reader = readerFor(
            0xE3, // Annotation wrapper, length 3
            0x81, // annotation SID length 1
            0x8A, // SID 10
            0x20 // int 0
        );
        nextExpect(IonType.INT);
        Iterator<String> annotationIterator = reader.iterateTypeAnnotations();
        thrown.expect(IonException.class);
        annotationIterator.next();
        reader.close();
    }

    @Test
    public void stepInOnScalarFails() throws Exception {
        reader = readerFor(0x20);
        nextExpect(IonType.INT);
        thrown.expect(IllegalStateException.class);
        stepIn();
        reader.close();
    }

    @Test
    public void stepInBeforeNextFails() throws Exception {
        reader = readerFor(0xD2, 0x84, 0xD0);
        reader.next();
        stepIn();
        thrown.expect(IllegalStateException.class);
        stepIn();
        reader.close();
    }

    @Test
    public void stepOutAtDepthZeroFails() throws Exception {
        reader = readerFor(0x20);
        reader.next();
        thrown.expect(IllegalStateException.class);
        stepOut();
        reader.close();
    }

    @Test
    public void byteSizeNotOnLobFails() throws Exception {
        reader = readerFor(0x20);
        reader.next();
        thrown.expect(IonException.class);
        reader.byteSize();
        reader.close();
    }

    @Test
    public void doubleValueOnIntFails() throws Exception {
        reader = readerFor(0x20);
        reader.next();
        thrown.expect(IllegalStateException.class);
        reader.doubleValue();
        reader.close();
    }

    @Test
    public void floatWithInvalidLengthFails() throws Exception {
        reader = readerFor(0x43, 0x01, 0x02, 0x03);
        thrown.expect(IonException.class);
        reader.next();
        reader.close();
    }

    @Test
    public void invalidTypeIdFFailsAtTopLevel() throws Exception {
        reader = readerFor(0xF0);
        thrown.expect(IonException.class);
        reader.next();
        reader.close();
    }

    @Test
    public void invalidTypeIdFFailsBelowTopLevel() throws Exception {
        reader = readerFor(0xB1, 0xF0);
        reader.next();
        stepIn();
        thrown.expect(IonException.class);
        reader.next();
        reader.close();
    }

    @Test
    public void reallyLargeString() throws Exception {
        StringBuilder sb = new StringBuilder();
        // 8192 is a arbitrarily large; it requires a couple bytes of length, and it doesn't fit in the preallocated
        // string decoding buffer of size 4096.
        for (int i = 0; i < 8192; i++) {
            sb.append('a');
        }
        String string = sb.toString();
        reader = readerFor("\"" + string + "\"");
        nextExpect(IonType.STRING);
        expectString(string);
        nextExpect(null);
        closeAndCount();
    }

    @Test
    public void nopPadInAnnotationWrapperFails() throws Exception {
        reader = readerFor(
            0xB5, // list
            0xE4, // annotation wrapper
            0x81, // 1 byte of annotations
            0x84, // annotation: "name"
            0x01, // 2-byte no-op pad
            0x00
        );
        reader.next();
        stepIn();
        thrown.expect(IonException.class);
        reader.next();
        reader.close();
    }

    @Test
    public void nestedAnnotationWrapperFails() throws Exception {
        reader = readerFor(
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
        thrown.expect(IonException.class);
        reader.next();
        reader.close();
    }

    @Test
    public void annotationWrapperLengthMismatchFails() throws Exception {
        reader = readerFor(
            0xB5, // list
            0xE4, // annotation wrapper
            0x81, // 1 byte of annotations
            0x84, // annotation: "name"
            0x20, // int 0
            0x20  // next value
        );
        reader.next();
        stepIn();
        thrown.expect(IonException.class);
        reader.next();
        reader.close();
    }

    @Test
    public void annotationWrapperVariableLengthMismatchFails() throws Exception {
        reader = readerFor(
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
        thrown.expect(IonException.class);
        reader.next();
        reader.close();
    }

    @Test
    public void multipleSymbolTableImportsFieldsFails() throws Exception {
        reader = readerFor((writer, out) -> {
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
        });

        thrown.expect(IonException.class);
        reader.next();
        reader.close();
    }

    @Test
    public void multipleSymbolTableSymbolsFieldsFails() throws Exception {
        reader = readerFor((writer, out) -> {
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
        });

        thrown.expect(IonException.class);
        reader.next();
        reader.close();
    }

    @Test
    public void nonStringInSymbolsListCreatesNullSlot() throws Exception {
        reader = readerFor((writer, out) -> {
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
        });

        nextExpect(IonType.SYMBOL);
        SymbolToken symbolValue = reader.symbolValue();
        assertNull(symbolValue.getText());
        assertEquals(0, symbolValue.getSid());
        nextExpect(IonType.SYMBOL);
        symbolValue = reader.symbolValue();
        assertEquals("abc", symbolValue.getText());
        nextExpect(IonType.SYMBOL);
        symbolValue = reader.symbolValue();
        assertNull(symbolValue.getText());
        assertEquals(0, symbolValue.getSid());
        nextExpect(null);
        closeAndCount();
    }

    @Test
    public void symbolTableWithMultipleImportsCorrectlyAssignsImportLocations() throws Exception {
        SimpleCatalog catalog = new SimpleCatalog();
        catalog.putTable(SYSTEM.newSharedSymbolTable("foo", 1, Arrays.asList("abc", "def").iterator()));
        catalog.putTable(SYSTEM.newSharedSymbolTable("bar", 1, Arrays.asList("123", "456").iterator()));
        readerBuilder = readerBuilder.withCatalog(catalog);

        reader = readerFor((writer, out) -> {
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
        });

        nextExpect(IonType.SYMBOL);
        expectString("abc");
        nextExpect(IonType.SYMBOL);
        expectString("def");
        nextExpect(IonType.SYMBOL);
        SymbolTokenWithImportLocation symbolValue =
            (SymbolTokenWithImportLocation) reader.symbolValue();
        assertNull(symbolValue.getText());
        assertEquals(new ImportLocation("foo", 3), symbolValue.getImportLocation());
        nextExpect(IonType.SYMBOL);
        symbolValue = (SymbolTokenWithImportLocation) reader.symbolValue();
        assertNull(symbolValue.getText());
        assertEquals(new ImportLocation("foo", 4), symbolValue.getImportLocation());
        nextExpect(IonType.SYMBOL);
        assertEquals("123", reader.stringValue());
        nextExpect(IonType.SYMBOL);
        symbolValue = (SymbolTokenWithImportLocation) reader.symbolValue();
        assertEquals(
            new SymbolTokenWithImportLocation(
                null,
                15,
                new ImportLocation("baz", 1)
            ),
            symbolValue
        );
        nextExpect(IonType.SYMBOL);
        symbolValue = (SymbolTokenWithImportLocation) reader.symbolValue();
        assertEquals(
            new SymbolTokenWithImportLocation(
                null,
                16,
                new ImportLocation("baz", 2)
            ),
            symbolValue
        );
        nextExpect(null);
        closeAndCount();
    }

    @Test
    public void symbolTableSnapshotImplementsBasicMethods() throws Exception {
        reader = readerFor("'abc'");
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
        thrown.expect(IllegalArgumentException.class);
        symbolTable.findKnownSymbol(-1);
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
     * @param initialBufferSize the initial buffer size.
     * @param maximumBufferSize the maximum size to which the buffer may grow.
     * @param handler the unified handler for byte counting and oversized value handling.
     */
    private IonReader boundedReaderFor(byte[] bytes, int initialBufferSize, int maximumBufferSize, UnifiedTestHandler handler) {
        byteCounter.set(0);
        setBufferBounds(initialBufferSize, maximumBufferSize, handler);
        return readerFor(readerBuilder, bytes);
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

    @Test
    public void singleValueExceedsInitialBufferSize() throws Exception {
        reader = boundedReaderFor(
            toBinary("\"abcdefghijklmnopqrstuvwxyz\""),
            8,
            Integer.MAX_VALUE,
            byteCountingHandler
        );
        nextExpect(IonType.STRING);
        expectString("abcdefghijklmnopqrstuvwxyz");
        nextExpect(null);
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
        thrown.expect(IllegalArgumentException.class);
        builder.build();
    }

    @Test
    public void maximumBufferSizeWithoutHandlerFails() {
        IonBufferConfiguration.Builder builder = IonBufferConfiguration.Builder.standard();
        builder
            .withMaximumBufferSize(9)
            .withInitialBufferSize(9);
        thrown.expect(IllegalArgumentException.class);
        builder.build();
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

        nextExpect(IonType.STRING);
        expectString("abc");
        expectOversized(1);
        nextExpect(IonType.STRING);
        expectString("def");
        nextExpect(null);
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
                    assertEquals(IonType.STRING, type);
                    expectString("abc");
                    expectOversized(1);
                } else {
                    assertEquals(2, valueCounter);
                    assertEquals(IonType.STRING, type);
                    expectString("def");
                    expectOversized(2);
                }
            }
        }
        assertEquals(2, valueCounter);
        nextExpect(null);
        reader.close();
        expectOversized(2);
        totalBytesInStream = bytes.length;
        assertBytesConsumed();
    }

    @Test
    public void oversizeValueDetectedDuringFillOfOnlyScalarInStream() throws Exception {
        // Unlike the previous test, where excessive size is detected when trying to skip past the value portion,
        // this test verifies that excessive size can be detected while reading a value header, which happens
        // byte-by-byte.
        byte[] bytes = toBinary("\"abcdefghijklmnopqrstuvwxyz\""); // Requires a 2-byte header.
        reader = boundedReaderFor(new ByteArrayInputStream(bytes), 5, 5, byteAndOversizedValueCountingHandler);

        // The maximum buffer size is 5, which will be exceeded after the IVM (4 bytes), the type ID (1 byte), and
        // the length byte (1 byte).
        nextExpect(null);
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
        // The maximum buffer size is 5, which will be exceeded after the IVM (4 bytes), the type ID (1 byte), and
        // the length byte (1 byte).
        nextExpect(null);
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
        reader = boundedReaderFor(new ByteArrayInputStream(bytes), 5, 5, byteAndOversizedValueCountingHandler);;

        // The maximum buffer size is 5, which will be exceeded after the annotation wrapper type ID
        // (1 byte), the annotations length (1 byte), and the annotation SID 3 (3 bytes). The next byte is the wrapped
        // value type ID byte.
        nextExpect(IonType.STRING);
        expectString("a");
        expectOversized(1);
        nextExpect(null);
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
        nextExpect(null);
        nextExpect(null);
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
        nextExpect(IonType.STRING);
        expectOversized(0);
        expectString("12345678");
        nextExpect(IonType.SYMBOL);
        expectOversized(0);
        expectString("abcdefghijklmnopqrstuvwxyz");
        nextExpect(null);
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
                    assertEquals(IonType.STRING, type);
                    expectString("12345678");
                    expectOversized(0);
                } else if (valueCounter == 2) {
                    assertEquals(IonType.SYMBOL, type);
                    expectString("abcdefghijklmnopqrstuvwxyz");
                    expectOversized(0);
                }
            }
        }
        assertEquals(2, valueCounter);
        nextExpect(null);
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
        nextExpect(null);
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
        nextExpect(null);
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
        nextExpect(IonType.STRING);
        expectString("12345678");
        nextExpect(null);
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
                assertEquals(IonType.STRING, type);
                expectString("12345678");
                foundValue = true;
            }
            pipe.receive(b);
        }
        assertTrue(foundValue);
        nextExpect(null);
        expectOversized(1);
        reader.close();
    }

    @Test
    public void skipOversizeScalarBelowTopLevelNonIncremental() throws Exception {
        byte[] bytes = toBinary("[\"abcdefg\", 123]");
        readerBuilder = readerBuilder.withIncrementalReadingEnabled(false);
        reader = boundedReaderFor(new ByteArrayInputStream(bytes), 5, 5, byteAndOversizedValueCountingHandler);
        nextExpect(IonType.LIST);
        stepIn();
        expectOversized(0);
        // This value is oversized, but since it is not filled, `onOversizedValue` does not need to be called.
        nextExpect(IonType.STRING);
        expectOversized(0);
        nextExpect(IonType.INT);
        expectOversized(0);
        expectInt(123);
        nextExpect(null);
        stepOut();
        nextExpect(null);
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
        nextExpect(IonType.LIST);
        stepIn();
        expectOversized(0);
        nextExpect(IonType.STRING);
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
        nextExpect(IonType.INT);
        expectOversized(1);
        expectInt(123);
        nextExpect(null);
        stepOut();
        nextExpect(null);
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
        nextExpect(IonType.SEXP);
        stepIn();
        expectOversized(0);
        nextExpect(null);
        expectOversized(1);
        stepOut();
        nextExpect(IonType.STRING);
        expectString("a");
        nextExpect(null);
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
        nextExpect(IonType.SEXP);
        stepIn();
        expectOversized(0);
        nextExpect(null);
        expectOversized(1);
        stepOut();
        nextExpect(IonType.STRING);
        expectString("a");
        nextExpect(null);
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
        nextExpect(IonType.STRING);
        expectString("12");
        nextExpect(IonType.SEXP);
        stepIn();
        nextExpect(IonType.SEXP);
        stepIn();
        nextExpect(IonType.SEXP);
        stepIn();
        expectOversized(0);
        nextExpect(IonType.STRING);
        expectOversized(0);
        expectString("1234");
        nextExpect(null);
        stepOut();
        stepOut();
        stepOut();
        nextExpect(IonType.INT);
        expectInt(0);
        nextExpect(null);
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

    private void assertFirstStruct(IonReader reader) {
        nextExpect(IonType.STRUCT);
        stepIn();
        nextExpect(IonType.SYMBOL);
        expectField("foo");
        expectString("bar");
        nextExpect(IonType.LIST);
        expectField("abc");
        stepIn();
        nextExpect(IonType.INT);
        expectInt(123);
        nextExpect(IonType.INT);
        expectInt(456);
        stepOut();
        stepOut();
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

    private void assertSecondStruct(IonReader reader) {
        nextExpect(IonType.STRUCT);
        stepIn();
        nextExpect(IonType.SYMBOL);
        expectField("foo");
        expectString("baz");
        nextExpect(IonType.LIST);
        expectField("abc");
        stepIn();
        assertEquals(IonType.DECIMAL, reader.next());
        assertEquals(new BigDecimal("42.0"), reader.decimalValue());
        assertEquals(IonType.FLOAT, reader.next());
        assertEquals(43., reader.doubleValue(), 1e-9);
        stepOut();
        stepOut();
        nextExpect(null);
    }

    @Test
    public void flushBetweenStructs() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonWriter writer = _Private_IonManagedBinaryWriterBuilder
            .create(_Private_IonManagedBinaryWriterBuilder.AllocatorMode.BASIC)
                .withLocalSymbolTableAppendEnabled()
                .newWriter(out);
        writeFirstStruct(writer);
        writer.flush();
        writeSecondStruct(writer);
        writer.close();

        reader = boundedReaderFor(out.toByteArray(), 64, 64, byteCountingHandler);
        assertFirstStruct(reader);
        assertSecondStruct(reader);
        closeAndCount();
    }

    @Test
    public void structsWithFloat32AndPreallocatedLength() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonWriter writer = _Private_IonManagedBinaryWriterBuilder
            .create(_Private_IonManagedBinaryWriterBuilder.AllocatorMode.BASIC)
                .withPaddedLengthPreallocation(2)
                .withFloatBinary32Enabled()
                .newWriter(out);
        writeFirstStruct(writer);
        writeSecondStruct(writer);
        writer.close();

        reader = boundedReaderFor(out.toByteArray(), 64, 64, byteCountingHandler);
        assertFirstStruct(reader);
        assertSecondStruct(reader);
        assertBytesConsumed();
    }

    @Test
    public void nopPadThatFillsBufferFollowedByValueNotOversized() throws Exception {
        TestUtils.BinaryIonAppender out = new TestUtils.BinaryIonAppender().append(
            0x03, 0x00, 0x00, 0x00, // 4 byte NOP pad.
            0x20 // Int 0.
        );
        // The IVM is 4 bytes and the NOP pad is 4 bytes. The first value is the 9th byte and should not be considered
        // oversize because the NOP pad can be discarded.
        reader = boundedReaderFor(out.toByteArray(), 8, 8, byteCountingHandler);
        nextExpect(IonType.INT);
        expectInt(0);
        nextExpect(null);
        closeAndCount();
    }

    @Test
    public void nopPadFollowedByValueThatOverflowsBufferNotOversized() throws Exception {
        TestUtils.BinaryIonAppender out = new TestUtils.BinaryIonAppender().append(
            0x03, 0x00, 0x00, 0x00, // 4 byte NOP pad.
            0x21, 0x01 // Int 1.
        );
        // The IVM is 4 bytes and the NOP pad is 4 bytes. The first byte of the value is the 9th byte and fits in the
        // buffer. Even though there is a 10th byte, the value should not be considered oversize because the NOP pad
        // can be discarded.
        reader = boundedReaderFor(out.toByteArray(), 9, 9, byteCountingHandler);
        nextExpect(IonType.INT);
        expectInt(1);
        nextExpect(null);
        closeAndCount();
    }

    @Test
    public void symbolTableFollowedByNopPadFollowedByValueThatOverflowsBufferNotOversized() throws Exception {
        TestUtils.BinaryIonAppender out = new TestUtils.BinaryIonAppender().append(
            // Symbol table with the symbol 'hello'.
            0xEB, 0x81, 0x83, 0xD8, 0x87, 0xB6, 0x85, 'h', 'e', 'l', 'l', 'o',
            0x00, // 1-byte NOP pad.
            0x71, 0x0A // SID 10 (hello).
        );
        // The IVM is 4 bytes, the symbol table is 12 bytes, and the symbol value is 2 bytes (total 18). The 1-byte NOP
        // pad needs to be reclaimed to make space for the value. Once that is done, the value will fit.
        reader = boundedReaderFor(out.toByteArray(), 18, 18, byteCountingHandler);
        nextExpect(IonType.SYMBOL);
        expectString("hello");
        nextExpect(null);
        closeAndCount();
    }

    @Test
    public void multipleNopPadsFollowedByValueThatOverflowsBufferNotOversized() throws Exception {
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

        reader = boundedReaderFor(out.toByteArray(), 11, 11, byteCountingHandler);
        nextExpect(IonType.INT);
        expectInt(1);
        nextExpect(IonType.INT);
        expectInt(2);
        nextExpect(null);
        closeAndCount();
    }

    @Test
    public void nopPadsInterspersedWithSystemValuesDoNotCauseOversizedErrors() throws Exception {
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
        reader = boundedReaderFor(out.toByteArray(), 22, 22, byteCountingHandler);
        nextExpect(IonType.SYMBOL);
        expectString("hello");
        nextExpect(null);
        closeAndCount();
    }

    @Test
    public void nopPadsInterspersedWithSystemValuesDoNotCauseOversizedErrors2() throws Exception {
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
        reader = boundedReaderFor(out.toByteArray(), 22, 22, byteCountingHandler);
        nextExpect(IonType.SYMBOL);
        expectString("hello");
        nextExpect(null);
        closeAndCount();
    }

    @Test
    public void nopPadsInterspersedWithSystemValuesDoNotCauseOversizedErrors3() throws Exception {
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
        reader = boundedReaderFor(out.toByteArray(), 22, 22, byteCountingHandler);
        nextExpect(IonType.SYMBOL);
        expectString("hello");
        nextExpect(null);
        closeAndCount();
    }

    @Test
    public void nopPadSurroundingSymbolTableThatFitsInBuffer() throws Exception {
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
        reader = boundedReaderFor(out.toByteArray(), 32, 32, byteCountingHandler);
        nextExpect(IonType.STRING);
        expectString("abcdefg");
        nextExpect(IonType.SYMBOL);
        expectString("hello");
        nextExpect(null);
        closeAndCount();
    }

    @Test
    public void nopPadInStructNonIncremental() throws Exception {
        TestUtils.BinaryIonAppender out = new TestUtils.BinaryIonAppender().append(
            0xD6, // Struct length 6
            0x80, // Field name SID 0
            0x04, 0x00, 0x00, 0x00, 0x00 // 5-byte NOP pad.
        );
        readerBuilder = readerBuilder.withIncrementalReadingEnabled(false);
        reader = boundedReaderFor(out.toByteArray(), 5, Integer.MAX_VALUE, byteCountingHandler);
        nextExpect(IonType.STRUCT);
        stepIn();
        nextExpect(null);
        stepOut();
        nextExpect(null);
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

    @Test
    public void annotationIteratorReuse() throws Exception {
        reader = readerFor("foo::bar::123 baz::456");
        nextExpect(IonType.INT);
        Iterator<String> firstValueAnnotationIterator = reader.iterateTypeAnnotations();
        expectInt(123);
        nextExpect(IonType.INT);
        compareIterator(Arrays.asList("foo", "bar"), firstValueAnnotationIterator);
        Iterator<String> secondValueAnnotationIterator = reader.iterateTypeAnnotations();
        expectInt(456);
        nextExpect(null);
        compareIterator(Collections.singletonList("baz"), secondValueAnnotationIterator);
        closeAndCount();
    }

    @Test(expected = IonException.class)
    public void failsOnMalformedSymbolTable() throws Exception {
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
        reader = boundedReaderFor(data, 1024, 1024, byteCountingHandler);
        nextExpect(null);
        nextExpect(null);
        reader.close();
    }

    @Test
    public void multiByteSymbolTokens() throws Exception {
        reader = readerFor((writer, out) -> {
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
        });
        nextExpect(IonType.STRUCT);
        stepIn();
        nextExpect(IonType.SYMBOL);
        SymbolToken[] annotations = reader.getTypeAnnotationSymbols();
        assertEquals(1, annotations.length);
        assertEquals("a", annotations[0].assumeText());
        Iterator<String> annotationsIterator = reader.iterateTypeAnnotations();
        assertTrue(annotationsIterator.hasNext());
        assertEquals("a", annotationsIterator.next());
        assertFalse(annotationsIterator.hasNext());
        assertEquals("b", reader.getFieldNameSymbol().assumeText());
        assertEquals("c", reader.symbolValue().assumeText());
        stepOut();
        nextExpect(null);
        closeAndCount();
    }

    @Test
    public void symbolTableWithOpenContentImportsListField() throws Exception {
        reader = readerFor((writer, out) -> {
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
        });
        nextExpect(IonType.SYMBOL);
        expectString("a");
        nextExpect(null);
        closeAndCount();
    }

    @Test
    public void symbolTableWithOpenContentImportsSymbolField() throws Exception {
        reader = readerFor((writer, out) -> {
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
        });
        nextExpect(IonType.SYMBOL);
        expectString("a");
        nextExpect(null);
        closeAndCount();
    }

    @Test
    public void symbolTableWithOpenContentSymbolField() throws Exception {
        reader = readerFor((writer, out) -> {
            writer.addTypeAnnotationSymbol(SystemSymbols.ION_SYMBOL_TABLE_SID);
            writer.stepIn(IonType.STRUCT);
            writer.setFieldNameSymbol(SystemSymbols.SYMBOLS_SID);
            writer.writeString("foo");
            writer.setFieldNameSymbol(SystemSymbols.IMPORTS_SID);
            writer.writeSymbolToken(SystemSymbols.NAME_SID);
            writer.stepOut();
            writer.writeSymbolToken(SystemSymbols.VERSION_SID);
        });
        nextExpect(IonType.SYMBOL);
        expectString(SystemSymbols.VERSION);
        nextExpect(null);
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
        nextExpect(IonType.LIST);
        stepIn();
        nextExpect(IonType.INT);
        expectInt(1);
        // Early step out. Not enough bytes to complete the value. Throw if the reader attempts
        // to advance the cursor.
        stepOut();
        thrown.expect(IonException.class);
        reader.next();
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
        nextExpect(IonType.STRING);
        thrown.expect(IonException.class);
        reader.next();
        reader.close();
    }

    @Test
    public void concatenatedAfterGZIPHeader() throws Exception {
        // Tests that a stream that initially contains only a GZIP header can be read successfully if more data
        // is later made available.
        final int gzipHeaderLength = 10; // Length of the GZIP header, as defined by the GZIP spec.
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (OutputStream gzip = new GZIPOutputStream(out)) {
            gzip.write(bytes(0xE0, 0x01, 0x00, 0xEA)); // IVM
        }
        ResizingPipedInputStream pipe = new ResizingPipedInputStream(128);

        // First, feed just the GZIP header bytes. On next(), the GZIPInputStream will throw EOFException, which is
        // handled by the reader.
        pipe.receive(out.toByteArray(), 0, gzipHeaderLength);
        reader = readerFor(new GZIPInputStream(pipe));
        nextExpect(null);

        // Now feed the bytes for an Ion value, spanning the value across two GZIP payloads.
        try (OutputStream gzip = new GZIPOutputStream(out)) {
            gzip.write(bytes(0x2E)); // Positive int with length subfield
        }
        try (OutputStream gzip = new GZIPOutputStream(out)) {
            gzip.write(bytes(0x81, 0x01)); // Length 1, value 1
        }
        byte[] bytes = out.toByteArray();
        byte[] withoutGzipHeader = new byte[bytes.length - gzipHeaderLength];
        System.arraycopy(bytes, gzipHeaderLength, withoutGzipHeader, 0, withoutGzipHeader.length);
        pipe.receive(withoutGzipHeader);
        nextExpect(IonType.INT);
        expectInt(1);
        nextExpect(null);
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
        ByteArrayOutputStream out1 = new ByteArrayOutputStream();
        try (OutputStream gzip = new GZIPOutputStream(out1)) {
            // IVM followed by an annotation wrapper that declares length 7, then three overpadded VarUInt bytes that
            // indicate two bytes of annotations follow, followed by two overpadded VarUInt bytes that indicate that
            // the value's only annotation has SID 4 ('name').
            gzip.write(bytes(0xE0, 0x01, 0x00, 0xEA, 0xE7, 0x00, 0x00, 0x82, 0x00, 0x84));
        }
        ResizingPipedInputStream pipe = new ResizingPipedInputStream(128);

        // First, feed the bytes (representing an incomplete Ion value) to the reader, but without a GZIP trailer.
        byte[] out1Bytes = out1.toByteArray();
        byte[] withoutTrailer = new byte[out1Bytes.length - gzipTrailerLength];
        System.arraycopy(out1Bytes, 0, withoutTrailer, 0, withoutTrailer.length);
        pipe.receive(withoutTrailer);
        reader = boundedReaderFor(new GZIPInputStream(pipe), maxBufferSize, maxBufferSize, byteAndOversizedValueCountingHandler);

        // During this next(), the reader will read and discard
        // the IVM, then hold all the rest of these bytes in its buffer, which is then at its maximum size. At that
        // point, the reader will attempt to read another byte (expecting a type ID byte), and determine that the
        // value is oversized because the buffer is already at its maximum size. Since the byte cannot be buffered,
        // the reader consumes it directly from the underlying InputStream using a single-byte read, which causes
        // GZIPInputStream to throw EOFException because the GZIP trailer is missing.
        nextExpect(null);

        // Now, finish the previous GZIP payload by appending the trailer, then append a concatenated GZIP payload
        // that contains the rest of the Ion value.
        pipe.receive(out1Bytes, out1Bytes.length - gzipTrailerLength, gzipTrailerLength);
        ByteArrayOutputStream out2 = new ByteArrayOutputStream();
        try (OutputStream gzip = new GZIPOutputStream(out2)) {
            // Positive integer length 1, with value 1 (the rest of the annotated value), then integer 0.
            gzip.write(bytes(0x21, 0x01, 0x20));
        }
        pipe.receive(out2.toByteArray());

        // On this next, the reader will finish skipping the oversized value by calling skip() on the underlying
        // GZIPInputStream, which is able to complete the request successfully now that the GZIP trailer is available.
        // The following value (integer 0) is then read successfully.
        nextExpect(IonType.INT);
        expectInt(0);
        assertEquals(1, oversizedCounter.get());
        nextExpect(null);
    }
}
