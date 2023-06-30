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
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
            .withBufferConfiguration(standardBufferConfiguration);
        writerBuilder = IonBinaryWriterBuilder.standard();
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

    @Test
    public void skipContainers() throws Exception {
        IonReader reader = readerFor(
            "[123] 456 {abc: foo::bar::123, def: baz::456} [123] 789 [foo::bar::123, baz::456] [123]"
        );
        assertEquals(IonType.LIST, reader.next());
        assertEquals(IonType.LIST, reader.getType());
        assertEquals(IonType.INT, reader.next());
        assertEquals(IonType.INT, reader.getType());
        assertEquals(IonType.STRUCT, reader.next());
        assertEquals(IonType.STRUCT, reader.getType());
        assertEquals(IonType.LIST, reader.next());
        assertEquals(IonType.LIST, reader.getType());
        reader.stepIn();
        assertNull(reader.getType());
        assertEquals(IonType.INT, reader.next());
        assertEquals(IonType.INT, reader.getType());
        assertEquals(123, reader.intValue());
        reader.stepOut();
        assertNull(reader.getType());
        assertEquals(IonType.INT, reader.next());
        assertEquals(IonType.INT, reader.getType());
        assertEquals(789, reader.intValue());
        assertEquals(IonType.LIST, reader.next());
        assertEquals(IonType.LIST, reader.getType());
        assertEquals(IonType.LIST, reader.next());
        assertEquals(IonType.LIST, reader.getType());
        assertNull(reader.next());
        assertNull(reader.getType());
        reader.close();
        assertBytesConsumed();
    }

    @Test
    public void skipContainerAfterSteppingIn() throws Exception {
        IonReader reader = readerFor("{abc: foo::bar::123, def: baz::456} 789");
        assertEquals(IonType.STRUCT, reader.next());
        assertEquals(IonType.STRUCT, reader.getType());
        reader.stepIn();
        assertNull(reader.getType());
        assertEquals(IonType.INT, reader.next());
        assertEquals(IonType.INT, reader.getType());
        assertEquals(Arrays.asList("foo", "bar"), Arrays.asList(reader.getTypeAnnotations()));
        assertEquals(123, reader.intValue());
        assertEquals(IonType.INT, reader.getType());
        // Step out before completing the value.
        reader.stepOut();
        assertNull(reader.getType());
        assertEquals(IonType.INT, reader.next());
        assertEquals(IonType.INT, reader.getType());
        assertEquals(789, reader.intValue());
        assertNull(reader.next());
        assertNull(reader.getType());
        reader.close();
        assertBytesConsumed();
    }

    @Test
    public void skipValueInContainer() throws Exception {
        IonReader reader = readerFor("{foo: \"bar\", abc: 123, baz: a}");
        assertEquals(IonType.STRUCT, reader.next());
        reader.stepIn();
        assertEquals(IonType.STRING, reader.next());
        assertEquals(IonType.INT, reader.next());
        assertEquals("abc", reader.getFieldName());
        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("baz", reader.getFieldName());
        assertNull(reader.next());
        reader.stepOut();
        assertNull(reader.next());
        reader.close();
        assertBytesConsumed();
    }

    @Test
    public void symbolsAsStrings() throws Exception {
        IonReader reader = readerFor("{foo: uvw::abc, bar: qrs::xyz::def}");
        assertEquals(IonType.STRUCT, reader.next());
        reader.stepIn();
        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("foo", reader.getFieldName());
        assertEquals(Collections.singletonList("uvw"), Arrays.asList(reader.getTypeAnnotations()));
        assertEquals("abc", reader.stringValue());
        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("bar", reader.getFieldName());
        assertEquals(Arrays.asList("qrs", "xyz"), Arrays.asList(reader.getTypeAnnotations()));
        assertEquals("def", reader.stringValue());
        reader.stepOut();
        assertNull(reader.next());
        reader.close();
        assertBytesConsumed();
    }

    @Test
    public void lstAppend() throws Exception {
        writerBuilder = writerBuilder.withLocalSymbolTableAppendEnabled();
        IonReader reader = readerFor(writer -> {
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

        assertEquals(IonType.STRUCT, reader.next());
        reader.stepIn();
        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("foo", reader.getFieldName());
        assertEquals(Collections.singletonList("uvw"), Arrays.asList(reader.getTypeAnnotations()));
        assertEquals("abc", reader.stringValue());
        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("bar", reader.getFieldName());
        assertEquals(Arrays.asList("qrs", "xyz"), Arrays.asList(reader.getTypeAnnotations()));
        assertEquals("def", reader.stringValue());
        reader.stepOut();
        SymbolTable preAppend = reader.getSymbolTable();
        assertEquals(IonType.SYMBOL, reader.next());
        SymbolTable postAppend = reader.getSymbolTable();
        assertEquals("orange", reader.stringValue());
        assertNull(preAppend.find("orange"));
        assertNotNull(postAppend.find("orange"));
        assertNull(reader.next());
        reader.close();
        assertBytesConsumed();
    }

    @Test
    public void lstNonAppend() throws Exception {
        writerBuilder = writerBuilder.withLocalSymbolTableAppendDisabled();
        IonReader reader = readerFor(writer -> {
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

        assertEquals(IonType.STRUCT, reader.next());
        reader.stepIn();
        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("foo", reader.getFieldName());
        assertEquals(Collections.singletonList("uvw"), Arrays.asList(reader.getTypeAnnotations()));
        assertEquals("abc", reader.stringValue());
        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("bar", reader.getFieldName());
        assertEquals(Arrays.asList("qrs", "xyz"), Arrays.asList(reader.getTypeAnnotations()));
        assertEquals("def", reader.stringValue());
        reader.stepOut();
        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("orange", reader.stringValue());
        assertNull(reader.next());
        reader.close();
        assertBytesConsumed();
    }

    @Test
    public void ivmBetweenValues() throws Exception {
        writerBuilder = writerBuilder.withLocalSymbolTableAppendDisabled();
        IonReader reader = readerFor(writer -> {
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

        assertEquals(IonType.STRUCT, reader.next());
        reader.stepIn();
        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("foo", reader.getFieldName());
        assertEquals(Collections.singletonList("uvw"), Arrays.asList(reader.getTypeAnnotations()));
        assertEquals("abc", reader.stringValue());
        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("bar", reader.getFieldName());
        assertEquals(Arrays.asList("qrs", "xyz"), Arrays.asList(reader.getTypeAnnotations()));
        assertEquals("def", reader.stringValue());
        reader.stepOut();
        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("orange", reader.stringValue());
        assertNull(reader.next());
        reader.close();
        assertBytesConsumed();
    }

    @Test
    public void ivmOnly() throws Exception {
        IonReader reader = readerFor();
        assertNull(reader.next());
        reader.close();
        assertBytesConsumed();
    }

    @Test
    public void twoIvmsOnly() throws Exception {
        IonReader reader = readerFor(0xE0, 0x01, 0x00, 0xEA);
        assertNull(reader.next());
        reader.close();
        assertBytesConsumed();
    }

    @Test
    public void multipleSymbolTablesBetweenValues() throws Exception {
        IonReader reader = readerFor(writer -> {
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

        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("abc", reader.stringValue());
        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("def", reader.stringValue());
        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("purple", reader.stringValue());
        assertNull(reader.next());
        reader.close();
        assertBytesConsumed();
    }

    @Test
    public void multipleIvmsBetweenValues() throws Exception  {
        IonReader reader = readerFor((writer, out) -> {
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

        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("def", reader.stringValue());
        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("name", reader.stringValue());
        assertNull(reader.next());
        reader.close();
        assertBytesConsumed();
    }

    @Test
    public void invalidVersion() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(bytes(0xE0, 0x01, 0x74, 0xEA, 0x20));
        IonReader reader = readerFor(readerBuilder, out.toByteArray());
        thrown.expect(IonException.class);
        reader.next();
        reader.close();
    }

    @Test
    public void invalidVersionMarker() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(bytes(0xE0, 0x01, 0x00, 0xEB, 0x20));
        IonReader reader = readerFor(readerBuilder, out.toByteArray());
        thrown.expect(IonException.class);
        reader.next();
        reader.close();
    }

    @Test
    public void lobsNewBytes() throws Exception {
        final byte[] blobBytes = "abcdef".getBytes(StandardCharsets.UTF_8);
        final byte[] clobBytes = "ghijklmnopqrstuv".getBytes(StandardCharsets.UTF_8);
        IonReader reader = readerFor(writer -> {
            writer.writeBlob(blobBytes);
            writer.writeClob(clobBytes);
            writer.setTypeAnnotations("foo");
            writer.writeBlob(blobBytes);
            writer.stepIn(IonType.STRUCT);
            writer.setFieldName("bar");
            writer.writeClob(clobBytes);
            writer.stepOut();
        });

        assertEquals(IonType.BLOB, reader.next());
        assertArrayEquals(blobBytes, reader.newBytes());
        assertEquals(IonType.CLOB, reader.next());
        assertArrayEquals(clobBytes, reader.newBytes());
        assertEquals(IonType.BLOB, reader.next());
        assertEquals(Collections.singletonList("foo"), Arrays.asList(reader.getTypeAnnotations()));
        assertArrayEquals(blobBytes, reader.newBytes());
        assertEquals(IonType.STRUCT, reader.next());
        reader.stepIn();
        assertEquals(IonType.CLOB, reader.next());
        assertEquals("bar", reader.getFieldName());
        assertArrayEquals(clobBytes, reader.newBytes());
        assertNull(reader.next());
        reader.stepOut();
        assertNull(reader.next());
        reader.close();
        assertBytesConsumed();
    }

    @Test
    public void lobsGetBytes() throws Exception {
        final byte[] blobBytes = "abcdef".getBytes(StandardCharsets.UTF_8);
        final byte[] clobBytes = "ghijklmnopqrstuv".getBytes(StandardCharsets.UTF_8);
        IonReader reader = readerFor(writer -> {
            writer.writeBlob(blobBytes);
            writer.writeClob(clobBytes);
            writer.setTypeAnnotations("foo");
            writer.writeBlob(blobBytes);
            writer.stepIn(IonType.STRUCT);
            writer.setFieldName("bar");
            writer.writeClob(clobBytes);
            writer.stepOut();
        });

        assertEquals(IonType.BLOB, reader.next());
        byte[] fullBlob = new byte[blobBytes.length];
        assertEquals(fullBlob.length, reader.getBytes(fullBlob, 0, fullBlob.length));
        assertArrayEquals(blobBytes, fullBlob);
        assertEquals(IonType.CLOB, reader.next());
        byte[] partialClob = new byte[clobBytes.length];
        assertEquals(3, reader.getBytes(partialClob, 0, 3));
        assertEquals(clobBytes.length - 3, reader.getBytes(partialClob, 3, clobBytes.length - 3));
        assertArrayEquals(clobBytes, partialClob);
        Arrays.fill(fullBlob, (byte) 0);
        assertEquals(IonType.BLOB, reader.next());
        assertEquals(fullBlob.length, reader.getBytes(fullBlob, 0, 100000));
        assertEquals(Collections.singletonList("foo"), Arrays.asList(reader.getTypeAnnotations()));
        assertEquals(IonType.STRUCT, reader.next());
        reader.stepIn();
        Arrays.fill(partialClob, (byte) 0);
        assertEquals(IonType.CLOB, reader.next());
        assertEquals(5, reader.getBytes(partialClob, 0, 5));
        assertEquals(clobBytes.length - 5, reader.getBytes(partialClob, 5, 100000));
        assertArrayEquals(clobBytes, partialClob);
        assertNull(reader.next());
        reader.stepOut();
        assertNull(reader.next());
        reader.close();
        assertBytesConsumed();
    }

    @Test
    public void nopPad() throws Exception {
        IonReader reader = readerFor(
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

        assertEquals(IonType.INT, reader.next());
        assertEquals(0, reader.intValue());
        assertEquals(IonType.INT, reader.next());
        assertEquals(1, reader.intValue());
        assertEquals(IonType.STRUCT, reader.next());
        reader.stepIn();
        assertEquals(IonType.INT, reader.next());
        assertEquals(-1, reader.intValue());
        assertNull(reader.next());
        reader.stepOut();
        assertEquals(IonType.STRUCT, reader.next());
        reader.stepIn();
        assertNull(reader.next());
        reader.stepOut();
        assertEquals(IonType.LIST, reader.next());
        reader.stepIn();
        assertNull(reader.next());
        reader.stepOut();
        assertNull(reader.next());
        reader.close();
        assertBytesConsumed();
    }

    @Test
    public void symbolTableWithImportsThenSymbols() throws Exception {
        SimpleCatalog catalog = new SimpleCatalog();
        catalog.putTable(SYSTEM.newSharedSymbolTable("foo", 1, Arrays.asList("abc", "def").iterator()));
        readerBuilder = readerBuilder.withCatalog(catalog);
        writerBuilder = writerBuilder.withCatalog(catalog);

        IonReader reader = readerFor(writer -> {
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

        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("abc", reader.stringValue());
        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("def", reader.stringValue());
        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("ghi", reader.stringValue());
        assertNull(reader.next());
        reader.close();
        assertBytesConsumed();
    }

    @Test
    public void symbolTableWithSymbolsThenImports() throws Exception {
        SimpleCatalog catalog = new SimpleCatalog();
        catalog.putTable(SYSTEM.newSharedSymbolTable("foo", 1, Arrays.asList("abc", "def").iterator()));
        readerBuilder = readerBuilder.withCatalog(catalog);

        IonReader reader = readerFor((writer, out) -> {
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

        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("abc", reader.stringValue());
        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("def", reader.stringValue());
        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("ghi", reader.stringValue());
        assertNull(reader.next());
        reader.close();
        assertBytesConsumed();
    }

    @Test
    public void symbolTableWithManySymbolsThenImports() throws Exception {
        SimpleCatalog catalog = new SimpleCatalog();
        catalog.putTable(SYSTEM.newSharedSymbolTable("foo", 1, Arrays.asList("abc", "def").iterator()));
        readerBuilder = readerBuilder.withCatalog(catalog);

        IonReader reader = readerFor((writer, out) -> {
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

        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("abc", reader.stringValue());
        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("def", reader.stringValue());
        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("ghi", reader.stringValue());
        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("jkl", reader.stringValue());
        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("mno", reader.stringValue());
        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("pqr", reader.stringValue());
        assertNull(reader.next());
        reader.close();
        assertBytesConsumed();
    }

    @Test
    public void multipleSymbolTablesWithSymbolsThenImports() throws Exception {
        SimpleCatalog catalog = new SimpleCatalog();
        catalog.putTable(SYSTEM.newSharedSymbolTable("foo", 1, Arrays.asList("abc", "def").iterator()));
        catalog.putTable(SYSTEM.newSharedSymbolTable("bar", 1, Collections.singletonList("baz").iterator()));
        readerBuilder = readerBuilder.withCatalog(catalog);

        IonReader reader = readerFor((writer, out) -> {
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

        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("abc", reader.stringValue());
        SymbolTable[] imports = reader.getSymbolTable().getImportedTables();
        assertEquals(1, imports.length);
        assertEquals("foo", imports[0].getName());
        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("def", reader.stringValue());
        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("ghi", reader.stringValue());
        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("baz", reader.stringValue());
        assertEquals(IonType.SYMBOL, reader.next());
        imports = reader.getSymbolTable().getImportedTables();
        assertEquals(1, imports.length);
        assertEquals("bar", imports[0].getName());
        assertEquals("xyz", reader.stringValue());
        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("uvw", reader.stringValue());
        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("rst", reader.stringValue());
        assertNull(reader.next());
        reader.close();
        assertBytesConsumed();
    }

    @Test
    public void ivmResetsImports() throws Exception {
        SimpleCatalog catalog = new SimpleCatalog();
        catalog.putTable(SYSTEM.newSharedSymbolTable("foo", 1, Arrays.asList("abc", "def").iterator()));
        readerBuilder = readerBuilder.withCatalog(catalog);

        IonReader reader = readerFor((writer, out) -> {
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

        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("abc", reader.stringValue());
        SymbolTable[] imports = reader.getSymbolTable().getImportedTables();
        assertEquals(1, imports.length);
        assertEquals("foo", imports[0].getName());
        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("def", reader.stringValue());
        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("ghi", reader.stringValue());
        assertEquals(IonType.INT, reader.next());
        assertTrue(reader.getSymbolTable().isSystemTable());
        assertEquals(0, reader.intValue());
        assertNull(reader.next());
        reader.close();
        assertBytesConsumed();
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
        IonReader reader = readerFor("{foo: uvw::abc, bar: qrs::xyz::def}");
        assertEquals(IonType.STRUCT, reader.next());
        reader.stepIn();
        assertEquals(IonType.SYMBOL, reader.next());
        assertSymbolEquals("foo", null, reader.getFieldNameSymbol());
        SymbolToken[] annotations = reader.getTypeAnnotationSymbols();
        assertEquals(1, annotations.length);
        assertSymbolEquals("uvw", null, annotations[0]);
        assertSymbolEquals("abc", null, reader.symbolValue());
        assertEquals(IonType.SYMBOL, reader.next());
        assertSymbolEquals("bar", null, reader.getFieldNameSymbol());
        annotations = reader.getTypeAnnotationSymbols();
        assertEquals(2, annotations.length);
        assertSymbolEquals("qrs", null, annotations[0]);
        assertSymbolEquals("xyz", null, annotations[1]);
        assertSymbolEquals("def", null, reader.symbolValue());
        reader.stepOut();
        assertNull(reader.next());
        reader.close();
        assertBytesConsumed();
    }

    @Test
    public void intNegativeZeroFails() throws Exception {
        IonReader reader = readerFor(0x31, 0x00);
        reader.next();
        thrown.expect(IonException.class);
        reader.longValue();
        reader.close();
    }

    @Test
    public void bigIntNegativeZeroFails() throws Exception {
        IonReader reader = readerFor(0x31, 0x00);
        reader.next();
        thrown.expect(IonException.class);
        reader.bigIntegerValue();
        reader.close();
    }

    @Test
    public void listWithLengthTooShortFails() throws Exception {
        IonReader reader = readerFor(0xB1, 0x21, 0x01);
        assertEquals(IonType.LIST, reader.next());
        reader.stepIn();
        thrown.expect(IonException.class);
        reader.next();
        reader.close();
    }

    @Test
    public void listWithContainerValueLengthTooShortFails() throws Exception {
        IonReader reader = readerFor(0xB2, 0xB2, 0x21, 0x01);
        assertEquals(IonType.LIST, reader.next());
        reader.stepIn();
        thrown.expect(IonException.class);
        reader.next();
        reader.close();
    }

    @Test
    public void listWithVariableLengthTooShortFails() throws Exception {
        IonReader reader = readerFor(0xBE, 0x81, 0x21, 0x01);
        assertEquals(IonType.LIST, reader.next());
        reader.stepIn();
        thrown.expect(IonException.class);
        reader.next();
        reader.close();
    }

    @Test(expected = IonException.class)
    public void noOpPadTooShort1() throws Exception {
        IonReader reader = readerFor(0x37, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01);
        assertEquals(IonType.INT, reader.next());
        assertNull(reader.next());
        reader.close();
    }

    @Test(expected = IonException.class)
    public void noOpPadTooShort2() throws Exception {
        IonReader reader = readerFor(
            0x0e, 0x90, 0x00, 0xde, 0xad, 0xbe, 0xef, 0xca, 0xfe, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
        );
        assertNull(reader.next());
        reader.close();
    }

    @Test
    public void nopPadOneByte() throws Exception {
        IonReader reader = readerFor(0);
        assertNull(reader.next());
        reader.close();
        assertBytesConsumed();
        reader.close();
    }

    @Test
    public void localSidOutOfRangeStringValue() throws Exception {
        IonReader reader = readerFor(0x71, 0x0A); // SID 10
        assertEquals(IonType.SYMBOL, reader.next());
        thrown.expect(IonException.class);
        reader.stringValue();
        reader.close();
    }

    @Test
    public void localSidOutOfRangeSymbolValue() throws Exception {
        IonReader reader = readerFor(0x71, 0x0A); // SID 10
        assertEquals(IonType.SYMBOL, reader.next());
        thrown.expect(IonException.class);
        reader.symbolValue();
        reader.close();
    }

    @Test
    public void localSidOutOfRangeFieldName() throws Exception {
        IonReader reader = readerFor(
            0xD2, // Struct, length 2
            0x8A, // SID 10
            0x20 // int 0
        );
        assertEquals(IonType.STRUCT, reader.next());
        reader.stepIn();
        assertEquals(IonType.INT, reader.next());
        thrown.expect(IonException.class);
        reader.getFieldName();
        reader.close();
    }

    @Test
    public void localSidOutOfRangeFieldNameSymbol() throws Exception {
        IonReader reader = readerFor(
            0xD2, // Struct, length 2
            0x8A, // SID 10
            0x20 // int 0
        );
        assertEquals(IonType.STRUCT, reader.next());
        reader.stepIn();
        assertEquals(IonType.INT, reader.next());
        thrown.expect(IonException.class);
        reader.getFieldNameSymbol();
        reader.close();
    }

    @Test
    public void localSidOutOfRangeAnnotation() throws Exception {
        IonReader reader = readerFor(
            0xE3, // Annotation wrapper, length 3
            0x81, // annotation SID length 1
            0x8A, // SID 10
            0x20 // int 0
        );
        assertEquals(IonType.INT, reader.next());
        thrown.expect(IonException.class);
        reader.getTypeAnnotations();
        reader.close();
    }

    @Test
    public void localSidOutOfRangeAnnotationSymbol() throws Exception {
        IonReader reader = readerFor(
            0xE3, // Annotation wrapper, length 3
            0x81, // annotation SID length 1
            0x8A, // SID 10
            0x20 // int 0
        );
        assertEquals(IonType.INT, reader.next());
        thrown.expect(IonException.class);
        reader.getTypeAnnotationSymbols();
        reader.close();
    }

    @Test
    public void localSidOutOfRangeIterateAnnotations() throws Exception {
        IonReader reader = readerFor(
            0xE3, // Annotation wrapper, length 3
            0x81, // annotation SID length 1
            0x8A, // SID 10
            0x20 // int 0
        );
        assertEquals(IonType.INT, reader.next());
        Iterator<String> annotationIterator = reader.iterateTypeAnnotations();
        thrown.expect(IonException.class);
        annotationIterator.next();
        reader.close();
    }

    @Test
    public void stepInOnScalarFails() throws Exception {
        IonReader reader = readerFor(0x20);
        assertEquals(IonType.INT, reader.next());
        thrown.expect(IonException.class);
        reader.stepIn();
        reader.close();
    }

    @Test
    public void stepInBeforeNextFails() throws Exception {
        IonReader reader = readerFor(0xD2, 0x84, 0xD0);
        reader.next();
        reader.stepIn();
        thrown.expect(IonException.class);
        reader.stepIn();
        reader.close();
    }

    @Test
    public void stepOutAtDepthZeroFails() throws Exception {
        IonReader reader = readerFor(0x20);
        reader.next();
        thrown.expect(IllegalStateException.class);
        reader.stepOut();
        reader.close();
    }

    @Test
    public void byteSizeNotOnLobFails() throws Exception {
        IonReader reader = readerFor(0x20);
        reader.next();
        thrown.expect(IonException.class);
        reader.byteSize();
        reader.close();
    }

    @Test
    public void doubleValueOnIntFails() throws Exception {
        IonReader reader = readerFor(0x20);
        reader.next();
        thrown.expect(IllegalStateException.class);
        reader.doubleValue();
        reader.close();
    }

    @Test
    public void floatWithInvalidLengthFails() throws Exception {
        IonReader reader = readerFor(0x43, 0x01, 0x02, 0x03);
        thrown.expect(IonException.class);
        reader.next();
        reader.close();
    }

    @Test
    public void invalidTypeIdFFailsAtTopLevel() throws Exception {
        IonReader reader = readerFor(0xF0);
        thrown.expect(IonException.class);
        reader.next();
        reader.close();
    }

    @Test
    public void invalidTypeIdFFailsBelowTopLevel() throws Exception {
        IonReader reader = readerFor(0xB1, 0xF0);
        reader.next();
        reader.stepIn();
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
        IonReader reader = readerFor("\"" + string + "\"");
        assertEquals(IonType.STRING, reader.next());
        assertEquals(string, reader.stringValue());
        assertNull(reader.next());
        reader.close();
        assertBytesConsumed();
    }

    @Test
    public void nopPadInAnnotationWrapperFails() throws Exception {
        IonReader reader = readerFor(
            0xB5, // list
            0xE4, // annotation wrapper
            0x81, // 1 byte of annotations
            0x84, // annotation: "name"
            0x01, // 2-byte no-op pad
            0x00
        );
        reader.next();
        reader.stepIn();
        thrown.expect(IonException.class);
        reader.next();
        reader.close();
    }

    @Test
    public void nestedAnnotationWrapperFails() throws Exception {
        IonReader reader = readerFor(
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
        reader.stepIn();
        thrown.expect(IonException.class);
        reader.next();
        reader.close();
    }

    @Test
    public void annotationWrapperLengthMismatchFails() throws Exception {
        IonReader reader = readerFor(
            0xB5, // list
            0xE4, // annotation wrapper
            0x81, // 1 byte of annotations
            0x84, // annotation: "name"
            0x20, // int 0
            0x20  // next value
        );
        reader.next();
        reader.stepIn();
        thrown.expect(IonException.class);
        reader.next();
        reader.close();
    }

    @Test
    public void annotationWrapperVariableLengthMismatchFails() throws Exception {
        IonReader reader = readerFor(
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
        reader.stepIn();
        thrown.expect(IonException.class);
        reader.next();
        reader.close();
    }

    @Test
    public void multipleSymbolTableImportsFieldsFails() throws Exception {
        IonReader reader = readerFor((writer, out) -> {
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
        IonReader reader = readerFor((writer, out) -> {
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
        IonReader reader = readerFor((writer, out) -> {
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

        assertEquals(IonType.SYMBOL, reader.next());
        SymbolToken symbolValue = reader.symbolValue();
        assertNull(symbolValue.getText());
        assertEquals(0, symbolValue.getSid());
        assertEquals(IonType.SYMBOL, reader.next());
        symbolValue = reader.symbolValue();
        assertEquals("abc", symbolValue.getText());
        assertEquals(IonType.SYMBOL, reader.next());
        symbolValue = reader.symbolValue();
        assertNull(symbolValue.getText());
        assertEquals(0, symbolValue.getSid());
        assertNull(reader.next());
        reader.close();
        assertBytesConsumed();
    }

    @Test
    public void symbolTableWithMultipleImportsCorrectlyAssignsImportLocations() throws Exception {
        SimpleCatalog catalog = new SimpleCatalog();
        catalog.putTable(SYSTEM.newSharedSymbolTable("foo", 1, Arrays.asList("abc", "def").iterator()));
        catalog.putTable(SYSTEM.newSharedSymbolTable("bar", 1, Arrays.asList("123", "456").iterator()));
        readerBuilder = readerBuilder.withCatalog(catalog);

        IonReader reader = readerFor((writer, out) -> {
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

        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("abc", reader.stringValue());
        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("def", reader.stringValue());
        assertEquals(IonType.SYMBOL, reader.next());
        SymbolTokenWithImportLocation symbolValue =
            (SymbolTokenWithImportLocation) reader.symbolValue();
        assertNull(symbolValue.getText());
        assertEquals(new ImportLocation("foo", 3), symbolValue.getImportLocation());
        assertEquals(IonType.SYMBOL, reader.next());
        symbolValue = (SymbolTokenWithImportLocation) reader.symbolValue();
        assertNull(symbolValue.getText());
        assertEquals(new ImportLocation("foo", 4), symbolValue.getImportLocation());
        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("123", reader.stringValue());
        assertEquals(IonType.SYMBOL, reader.next());
        symbolValue = (SymbolTokenWithImportLocation) reader.symbolValue();
        assertEquals(
            new SymbolTokenWithImportLocation(
                null,
                15,
                new ImportLocation("baz", 1)
            ),
            symbolValue
        );
        assertEquals(IonType.SYMBOL, reader.next());
        symbolValue = (SymbolTokenWithImportLocation) reader.symbolValue();
        assertEquals(
            new SymbolTokenWithImportLocation(
                null,
                16,
                new ImportLocation("baz", 2)
            ),
            symbolValue
        );
        assertNull(reader.next());
        reader.close();
        assertBytesConsumed();
    }

    @Test
    public void symbolTableSnapshotImplementsBasicMethods() throws Exception {
        IonReader reader = readerFor("'abc'");
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
        IonReader reader = readerFor("foo::bar::123 baz::456");

        assertEquals(IonType.INT, reader.next());
        Iterator<String> firstValueAnnotationIterator = reader.iterateTypeAnnotations();
        assertEquals(123, reader.intValue());
        assertEquals(IonType.INT, reader.next());
        compareIterator(Arrays.asList("foo", "bar"), firstValueAnnotationIterator);
        Iterator<String> secondValueAnnotationIterator = reader.iterateTypeAnnotations();
        assertEquals(456, reader.intValue());
        assertNull(reader.next());
        compareIterator(Collections.singletonList("baz"), secondValueAnnotationIterator);
        reader.close();
        assertBytesConsumed();
    }

    @Test
    public void multiByteSymbolTokens() throws Exception {
        IonReader reader = readerFor((writer, out) -> {
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
        assertEquals(IonType.STRUCT, reader.next());
        reader.stepIn();
        assertEquals(IonType.SYMBOL, reader.next());
        SymbolToken[] annotations = reader.getTypeAnnotationSymbols();
        assertEquals(1, annotations.length);
        assertEquals("a", annotations[0].assumeText());
        Iterator<String> annotationsIterator = reader.iterateTypeAnnotations();
        assertTrue(annotationsIterator.hasNext());
        assertEquals("a", annotationsIterator.next());
        assertFalse(annotationsIterator.hasNext());
        assertEquals("b", reader.getFieldNameSymbol().assumeText());
        assertEquals("c", reader.symbolValue().assumeText());
        reader.stepOut();
        assertNull(reader.next());
        reader.close();
        assertBytesConsumed();
    }

    @Test
    public void symbolTableWithOpenContentImportsListField() throws Exception {
        IonReader reader = readerFor((writer, out) -> {
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
        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("a", reader.stringValue());
        assertNull(reader.next());
        reader.close();
        assertBytesConsumed();
    }

    @Test
    public void symbolTableWithOpenContentImportsSymbolField() throws Exception {
        IonReader reader = readerFor((writer, out) -> {
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
        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("a", reader.stringValue());
        assertNull(reader.next());
        reader.close();
        assertBytesConsumed();
    }

    @Test
    public void symbolTableWithOpenContentSymbolField() throws Exception {
        IonReader reader = readerFor((writer, out) -> {
            writer.addTypeAnnotationSymbol(SystemSymbols.ION_SYMBOL_TABLE_SID);
            writer.stepIn(IonType.STRUCT);
            writer.setFieldNameSymbol(SystemSymbols.SYMBOLS_SID);
            writer.writeString("foo");
            writer.setFieldNameSymbol(SystemSymbols.IMPORTS_SID);
            writer.writeSymbolToken(SystemSymbols.NAME_SID);
            writer.stepOut();
            writer.writeSymbolToken(SystemSymbols.VERSION_SID);
        });
        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals(SystemSymbols.VERSION, reader.stringValue());
        assertNull(reader.next());
        reader.close();
        assertBytesConsumed();
    }

}
