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

}
