// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.system.IonBinaryWriterBuilder;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.system.IonTextWriterBuilder;
import com.amazon.ion.util.GzipStreamInterceptor;
import com.amazon.ion.util.InputStreamInterceptor;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Preservation tests verifying that normal Ion inputs are processed correctly after the
 * memory exhaustion fix. These tests must pass on both unfixed and fixed code.
 *
 * For all inputs within configured limits (normal value lengths, normal GZIP inflation
 * ratios, normal text sizes), the reader must produce correct results.
 */
@Tag("memory-exhaustion-preservation")
public class MemoryExhaustionPreservationTest {

    private static final Random RANDOM = new Random(42); // Fixed seed for reproducibility

    // --- Helper Methods ---

    /**
     * Generates a valid binary Ion payload containing random integers.
     * The payload size is approximately the target size in bytes.
     */
    private static byte[] generateBinaryIonPayload(int approximateTargetSize) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (IonWriter writer = IonBinaryWriterBuilder.standard().build(baos)) {
            int bytesWritten = 0;
            int valueIndex = 0;
            while (bytesWritten < approximateTargetSize) {
                // Mix different value types to make the payload realistic
                switch (valueIndex % 5) {
                    case 0:
                        writer.writeInt(RANDOM.nextInt());
                        bytesWritten += 5; // approximate
                        break;
                    case 1:
                        writer.writeString("test_string_" + valueIndex);
                        bytesWritten += 20; // approximate
                        break;
                    case 2:
                        writer.writeBool(valueIndex % 2 == 0);
                        bytesWritten += 2;
                        break;
                    case 3:
                        writer.writeFloat(RANDOM.nextDouble());
                        bytesWritten += 9;
                        break;
                    case 4:
                        writer.writeNull();
                        bytesWritten += 1;
                        break;
                }
                valueIndex++;
            }
        }
        return baos.toByteArray();
    }

    /**
     * Generates valid text Ion containing random values.
     */
    private static byte[] generateTextIonPayload(int approximateTargetSize) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (IonWriter writer = IonTextWriterBuilder.standard().build(baos)) {
            int bytesWritten = 0;
            int valueIndex = 0;
            while (bytesWritten < approximateTargetSize) {
                switch (valueIndex % 5) {
                    case 0:
                        writer.writeInt(RANDOM.nextInt());
                        bytesWritten += 12;
                        break;
                    case 1:
                        writer.writeString("preservation_test_" + valueIndex);
                        bytesWritten += 30;
                        break;
                    case 2:
                        writer.writeBool(valueIndex % 2 == 0);
                        bytesWritten += 5;
                        break;
                    case 3:
                        writer.writeFloat(RANDOM.nextDouble());
                        bytesWritten += 20;
                        break;
                    case 4:
                        writer.writeNull();
                        bytesWritten += 5;
                        break;
                }
                valueIndex++;
            }
        }
        return baos.toByteArray();
    }

    /**
     * GZIP-compresses the given data.
     */
    private static byte[] gzipCompress(byte[] data) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPOutputStream gzipOut = new GZIPOutputStream(baos)) {
            gzipOut.write(data);
        }
        return baos.toByteArray();
    }

    /**
     * Counts all top-level values in the given Ion data using the specified reader builder.
     * Returns the list of IonTypes encountered.
     */
    private static List<IonType> readAllValues(IonReaderBuilder builder, byte[] data) throws IOException {
        List<IonType> types = new ArrayList<>();
        try (IonReader reader = builder.build(data)) {
            IonType type;
            while ((type = reader.next()) != null) {
                types.add(type);
            }
        }
        return types;
    }

    /**
     * Counts all top-level values in the given Ion data using a stream-based reader.
     */
    private static List<IonType> readAllValuesFromStream(IonReaderBuilder builder, byte[] data) throws IOException {
        List<IonType> types = new ArrayList<>();
        try (IonReader reader = builder.build(new ByteArrayInputStream(data))) {
            IonType type;
            while ((type = reader.next()) != null) {
                types.add(type);
            }
        }
        return types;
    }

    // --- Property Tests: Binary Ion Preservation ---

    /**
     * For valid binary Ion payloads with various sizes within normal bounds,
     * the reader produces correct parsed results.
     */
    @ParameterizedTest
    @ValueSource(ints = {0, 1, 100, 1024, 10240, 102400, 1048576})
    public void binaryIonWithinBoundsReadsCorrectly(int approximateSize) throws IOException {
        byte[] binaryIon = generateBinaryIonPayload(approximateSize);

        // Read with standard builder (default configuration)
        IonReaderBuilder builder = IonReaderBuilder.standard();
        List<IonType> types = readAllValues(builder, binaryIon);

        // Verify: all values were read without error
        // For size 0, we still get the IVM but no values
        if (approximateSize == 0) {
            assertTrue(types.isEmpty());
        } else {
            assertTrue(types.size() > 0, "Should read at least one value from a non-empty payload");
        }

        // Verify: reading from stream produces same results as reading from bytes
        List<IonType> streamTypes = readAllValuesFromStream(builder, binaryIon);
        assertEquals(types, streamTypes,
            "Stream-based and byte-based readers should produce identical results");
    }

    /**
     * Binary Ion with incremental reading enabled also works correctly for normal payloads.
     */
    @ParameterizedTest
    @ValueSource(ints = {100, 1024, 10240, 102400})
    public void binaryIonIncrementalReadingPreserved(int approximateSize) throws IOException {
        byte[] binaryIon = generateBinaryIonPayload(approximateSize);

        IonReaderBuilder builder = IonReaderBuilder.standard()
            .withIncrementalReadingEnabled(true);

        List<IonType> types = readAllValuesFromStream(builder, binaryIon);
        assertTrue(types.size() > 0, "Should read at least one value");

        // Verify specific value types are present (our generator produces a mix)
        assertTrue(types.contains(IonType.INT), "Should contain INT values");
        assertTrue(types.contains(IonType.STRING), "Should contain STRING values");
    }

    // --- Property Tests: GZIP Preservation (Requirements 3.2, 3.4) ---

    /**
     * For valid Ion payloads GZIP-compressed with normal inflation ratios (< 10:1),
     * decompression + parsing produces correct results. GZIP auto-decompression
     * remains enabled by default.
     */
    @ParameterizedTest
    @ValueSource(ints = {100, 1024, 10240, 102400})
    public void gzipCompressedBinaryIonDecompressesAndParsesCorrectly(int approximateSize) throws IOException {
        // Generate binary Ion and compress with GZIP
        byte[] binaryIon = generateBinaryIonPayload(approximateSize);
        byte[] gzipped = gzipCompress(binaryIon);

        // Verify inflation ratio is reasonable (< 10:1)
        double inflationRatio = (double) binaryIon.length / gzipped.length;
        assertTrue(inflationRatio < 10.0,
            "Test data should have normal inflation ratio, got: " + inflationRatio);

        // Read the compressed data - GZIP auto-decompression should handle it transparently
        IonReaderBuilder builder = IonReaderBuilder.standard();
        List<IonType> compressedTypes = readAllValues(builder, gzipped);
        List<IonType> uncompressedTypes = readAllValues(builder, binaryIon);

        // The compressed and uncompressed reading should produce identical results
        assertEquals(uncompressedTypes, compressedTypes,
            "GZIP-compressed and uncompressed binary Ion should parse to same result");
    }

    /**
     * GZIP-compressed text Ion with normal inflation ratios is auto-decompressed
     * and parsed correctly.
     */
    @ParameterizedTest
    @ValueSource(ints = {100, 1024, 10240, 102400})
    public void gzipCompressedTextIonDecompressesAndParsesCorrectly(int approximateSize) throws IOException {
        // Generate text Ion and compress with GZIP
        byte[] textIon = generateTextIonPayload(approximateSize);
        byte[] gzipped = gzipCompress(textIon);

        // Read the compressed data
        IonReaderBuilder builder = IonReaderBuilder.standard();
        List<IonType> compressedTypes = readAllValues(builder, gzipped);
        List<IonType> uncompressedTypes = readAllValues(builder, textIon);

        assertEquals(uncompressedTypes, compressedTypes,
            "GZIP-compressed and uncompressed text Ion should parse to same result");
    }

    /**
     * GZIP-compressed Ion read via stream (not byte array) also decompresses correctly.
     */
    @Test
    public void gzipCompressedIonFromStreamDecompressesCorrectly() throws IOException {
        byte[] binaryIon = generateBinaryIonPayload(10240);
        byte[] gzipped = gzipCompress(binaryIon);

        IonReaderBuilder builder = IonReaderBuilder.standard();
        List<IonType> fromBytes = readAllValues(builder, gzipped);
        List<IonType> fromStream = readAllValuesFromStream(builder, gzipped);

        assertEquals(fromBytes, fromStream,
            "Stream and byte-array readers should produce identical results for GZIP data");
    }

    // --- Property Tests: Text Ion Preservation ---

    /**
     * For valid text Ion documents with sizes within normal bounds,
     * parsing produces correct results.
     */
    @ParameterizedTest
    @ValueSource(ints = {0, 1, 100, 1024, 10240, 102400, 1048576})
    public void textIonWithinBoundsReadsCorrectly(int approximateSize) throws IOException {
        byte[] textIon = generateTextIonPayload(approximateSize);

        IonReaderBuilder builder = IonReaderBuilder.standard();
        List<IonType> types = readAllValues(builder, textIon);

        if (approximateSize == 0) {
            assertTrue(types.isEmpty());
        } else {
            assertTrue(types.size() > 0, "Should read at least one value");
        }

        // Verify reading from stream produces same results
        List<IonType> streamTypes = readAllValuesFromStream(builder, textIon);
        assertEquals(types, streamTypes,
            "Stream-based and byte-based text Ion readers should produce identical results");
    }

    /**
     * Text Ion with various content types (structs, lists, annotations) parses correctly.
     */
    @Test
    public void textIonWithComplexStructuresReadsCorrectly() throws IOException {
        // Generate text Ion with complex structures
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (IonWriter writer = IonTextWriterBuilder.standard().build(baos)) {
            // Write a struct
            writer.stepIn(IonType.STRUCT);
            writer.setFieldName("name");
            writer.writeString("test");
            writer.setFieldName("value");
            writer.writeInt(42);
            writer.setFieldName("nested");
            writer.stepIn(IonType.LIST);
            writer.writeInt(1);
            writer.writeInt(2);
            writer.writeInt(3);
            writer.stepOut();
            writer.stepOut();

            // Write a list
            writer.stepIn(IonType.LIST);
            writer.writeString("a");
            writer.writeString("b");
            writer.stepOut();

            // Write annotated values
            writer.setTypeAnnotations("annotation1");
            writer.writeSymbol("sym");
        }
        byte[] textIon = baos.toByteArray();

        IonReaderBuilder builder = IonReaderBuilder.standard();
        try (IonReader reader = builder.build(textIon)) {
            // Verify struct
            assertEquals(IonType.STRUCT, reader.next());
            reader.stepIn();
            assertEquals(IonType.STRING, reader.next());
            assertEquals("name", reader.getFieldName());
            assertEquals("test", reader.stringValue());
            assertEquals(IonType.INT, reader.next());
            assertEquals("value", reader.getFieldName());
            assertEquals(42, reader.intValue());
            assertEquals(IonType.LIST, reader.next());
            assertEquals("nested", reader.getFieldName());
            reader.stepIn();
            assertEquals(IonType.INT, reader.next());
            assertEquals(1, reader.intValue());
            assertEquals(IonType.INT, reader.next());
            assertEquals(2, reader.intValue());
            assertEquals(IonType.INT, reader.next());
            assertEquals(3, reader.intValue());
            assertNull(reader.next());
            reader.stepOut();
            assertNull(reader.next());
            reader.stepOut();

            // Verify list
            assertEquals(IonType.LIST, reader.next());
            reader.stepIn();
            assertEquals(IonType.STRING, reader.next());
            assertEquals("a", reader.stringValue());
            assertEquals(IonType.STRING, reader.next());
            assertEquals("b", reader.stringValue());
            assertNull(reader.next());
            reader.stepOut();

            // Verify annotated value
            assertEquals(IonType.SYMBOL, reader.next());
            String[] annotations = reader.getTypeAnnotations();
            assertEquals(1, annotations.length);
            assertEquals("annotation1", annotations[0]);

            assertNull(reader.next());
        }
    }

    // --- Property Tests: Interceptor Ordering Preservation ---

    /**
     * Custom InputStreamInterceptor instances added via addInputStreamInterceptor()
     * are applied in documented order: GZIP first, then detected, then user-added.
     */
    @Test
    public void interceptorOrderingPreserved() {
        // Create custom interceptors
        InputStreamInterceptor customInterceptor1 = new TestInterceptor("custom1");
        InputStreamInterceptor customInterceptor2 = new TestInterceptor("custom2");

        IonReaderBuilder builder = IonReaderBuilder.standard()
            .addInputStreamInterceptor(customInterceptor1)
            .addInputStreamInterceptor(customInterceptor2);

        List<InputStreamInterceptor> interceptors = builder.getInputStreamInterceptors();

        // Verify: GZIP is always first
        assertSame(GzipStreamInterceptor.INSTANCE, interceptors.get(0),
            "GzipStreamInterceptor must always be the first interceptor");

        // Verify: user-added interceptors come after GZIP (and any detected ones)
        // The exact position depends on whether any classpath-detected interceptors exist,
        // but custom ones must come at the end in order added
        int custom1Index = interceptors.indexOf(customInterceptor1);
        int custom2Index = interceptors.indexOf(customInterceptor2);
        assertTrue(custom1Index > 0, "Custom interceptor 1 should come after GZIP");
        assertTrue(custom2Index > custom1Index, "Custom interceptor 2 should come after custom 1");
    }

    /**
     * Default builder (no custom interceptors) has only GZIP interceptor.
     */
    @Test
    public void defaultBuilderHasGzipInterceptorFirst() {
        IonReaderBuilder builder = IonReaderBuilder.standard();
        List<InputStreamInterceptor> interceptors = builder.getInputStreamInterceptors();

        // GZIP should be present
        assertTrue(interceptors.size() >= 1, "Should have at least one interceptor (GZIP)");
        assertSame(GzipStreamInterceptor.INSTANCE, interceptors.get(0),
            "First interceptor must be GzipStreamInterceptor");
    }

    // --- Property Tests: IonReaderBuilder.standard() Preservation (Requirements 3.4, 3.5) ---

    /**
     * IonReaderBuilder.standard() with default configuration reads well-formed
     * binary Ion without error.
     */
    @Test
    public void standardBuilderReadsBinaryIonWithoutError() throws IOException {
        byte[] binaryIon = generateBinaryIonPayload(10240);

        // Standard builder with no custom configuration
        IonReaderBuilder builder = IonReaderBuilder.standard();
        List<IonType> types = readAllValues(builder, binaryIon);
        assertTrue(types.size() > 0);
    }

    /**
     * IonReaderBuilder.standard() with default configuration reads well-formed
     * text Ion without error.
     */
    @Test
    public void standardBuilderReadsTextIonWithoutError() throws IOException {
        byte[] textIon = generateTextIonPayload(10240);

        IonReaderBuilder builder = IonReaderBuilder.standard();
        List<IonType> types = readAllValues(builder, textIon);
        assertTrue(types.size() > 0);
    }

    /**
     * IonReaderBuilder.standard() auto-detects and decompresses GZIP by default.
     */
    @Test
    public void standardBuilderAutoDecompressesGzip() throws IOException {
        byte[] binaryIon = generateBinaryIonPayload(1024);
        byte[] gzipped = gzipCompress(binaryIon);

        IonReaderBuilder builder = IonReaderBuilder.standard();

        // Should auto-detect GZIP and decompress, then parse binary Ion
        List<IonType> types = readAllValues(builder, gzipped);
        assertTrue(types.size() > 0,
            "Standard builder should auto-decompress GZIP and read binary Ion");
    }

    /**
     * Reading binary Ion data with specific value types preserves exact values.
     */
    @Test
    public void binaryIonValueIntegrityPreserved() throws IOException {
        // Write specific values
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (IonWriter writer = IonBinaryWriterBuilder.standard().build(baos)) {
            writer.writeInt(Integer.MAX_VALUE);
            writer.writeInt(Integer.MIN_VALUE);
            writer.writeInt(0);
            writer.writeFloat(3.14159);
            writer.writeString("Hello, Ion!");
            writer.writeBool(true);
            writer.writeBool(false);
            writer.writeNull();
        }
        byte[] binaryIon = baos.toByteArray();

        // Read and verify exact values
        IonReaderBuilder builder = IonReaderBuilder.standard();
        try (IonReader reader = builder.build(binaryIon)) {
            assertEquals(IonType.INT, reader.next());
            assertEquals(Integer.MAX_VALUE, reader.intValue());

            assertEquals(IonType.INT, reader.next());
            assertEquals(Integer.MIN_VALUE, reader.intValue());

            assertEquals(IonType.INT, reader.next());
            assertEquals(0, reader.intValue());

            assertEquals(IonType.FLOAT, reader.next());
            assertEquals(3.14159, reader.doubleValue(), 0.00001);

            assertEquals(IonType.STRING, reader.next());
            assertEquals("Hello, Ion!", reader.stringValue());

            assertEquals(IonType.BOOL, reader.next());
            assertTrue(reader.booleanValue());

            assertEquals(IonType.BOOL, reader.next());
            assertEquals(false, reader.booleanValue());

            assertEquals(IonType.NULL, reader.next());
            assertTrue(reader.isNullValue());

            assertNull(reader.next());
        }
    }

    /**
     * GZIP-compressed binary Ion preserves exact values after decompression.
     */
    @Test
    public void gzipCompressedBinaryIonPreservesValues() throws IOException {
        // Write specific values
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (IonWriter writer = IonBinaryWriterBuilder.standard().build(baos)) {
            writer.writeInt(123456789);
            writer.writeString("GZIP preserved");
            writer.writeFloat(2.71828);
        }
        byte[] binaryIon = baos.toByteArray();
        byte[] gzipped = gzipCompress(binaryIon);

        // Read from GZIP-compressed data and verify values
        IonReaderBuilder builder = IonReaderBuilder.standard();
        try (IonReader reader = builder.build(gzipped)) {
            assertEquals(IonType.INT, reader.next());
            assertEquals(123456789, reader.intValue());

            assertEquals(IonType.STRING, reader.next());
            assertEquals("GZIP preserved", reader.stringValue());

            assertEquals(IonType.FLOAT, reader.next());
            assertEquals(2.71828, reader.doubleValue(), 0.00001);

            assertNull(reader.next());
        }
    }

    /**
     * Text Ion string parsing with various sizes produces correct results.
     */
    @Test
    public void textIonStringParsingPreserved() throws IOException {
        String textIon = "123 \"hello\" true null 3.14e0 [1, 2, 3]";

        IonReaderBuilder builder = IonReaderBuilder.standard();
        try (IonReader reader = builder.build(textIon)) {
            assertEquals(IonType.INT, reader.next());
            assertEquals(123, reader.intValue());

            assertEquals(IonType.STRING, reader.next());
            assertEquals("hello", reader.stringValue());

            assertEquals(IonType.BOOL, reader.next());
            assertTrue(reader.booleanValue());

            assertEquals(IonType.NULL, reader.next());
            assertTrue(reader.isNullValue());

            assertEquals(IonType.FLOAT, reader.next());
            assertEquals(3.14, reader.doubleValue(), 0.001);

            assertEquals(IonType.LIST, reader.next());
            reader.stepIn();
            assertEquals(IonType.INT, reader.next());
            assertEquals(1, reader.intValue());
            assertEquals(IonType.INT, reader.next());
            assertEquals(2, reader.intValue());
            assertEquals(IonType.INT, reader.next());
            assertEquals(3, reader.intValue());
            assertNull(reader.next());
            reader.stepOut();

            assertNull(reader.next());
        }
    }

    /**
     * Verifies GZIP-compressed text Ion with stream-based reader decompresses correctly.
     */
    @Test
    public void gzipCompressedTextIonFromStreamPreserved() throws IOException {
        String ionText = "42 \"gzip_text_test\" true";
        byte[] textBytes = ionText.getBytes(StandardCharsets.UTF_8);
        byte[] gzipped = gzipCompress(textBytes);

        IonReaderBuilder builder = IonReaderBuilder.standard();
        try (IonReader reader = builder.build(new ByteArrayInputStream(gzipped))) {
            assertEquals(IonType.INT, reader.next());
            assertEquals(42, reader.intValue());

            assertEquals(IonType.STRING, reader.next());
            assertEquals("gzip_text_test", reader.stringValue());

            assertEquals(IonType.BOOL, reader.next());
            assertTrue(reader.booleanValue());

            assertNull(reader.next());
        }
    }

    /**
     * Property-based: for multiple random binary Ion payloads of different sizes,
     * byte-array and stream-based readers produce identical results.
     */
    @Test
    public void binaryIonConsistencyAcrossReaderModes() throws IOException {
        int[] sizes = {64, 256, 1024, 4096, 16384, 65536, 262144};
        IonReaderBuilder builder = IonReaderBuilder.standard();

        for (int size : sizes) {
            byte[] binaryIon = generateBinaryIonPayload(size);
            List<IonType> fromBytes = readAllValues(builder, binaryIon);
            List<IonType> fromStream = readAllValuesFromStream(builder, binaryIon);
            assertEquals(fromBytes, fromStream,
                "Byte and stream readers should be consistent for size " + size);
            assertTrue(fromBytes.size() > 0);
        }
    }

    /**
     * Property-based: for multiple random text Ion payloads of different sizes,
     * byte-array and stream-based readers produce identical results.
     */
    @Test
    public void textIonConsistencyAcrossReaderModes() throws IOException {
        int[] sizes = {64, 256, 1024, 4096, 16384, 65536, 262144};
        IonReaderBuilder builder = IonReaderBuilder.standard();

        for (int size : sizes) {
            byte[] textIon = generateTextIonPayload(size);
            List<IonType> fromBytes = readAllValues(builder, textIon);
            List<IonType> fromStream = readAllValuesFromStream(builder, textIon);
            assertEquals(fromBytes, fromStream,
                "Byte and stream readers should be consistent for text Ion size " + size);
            assertTrue(fromBytes.size() > 0);
        }
    }

    /**
     * Property-based: for multiple GZIP-compressed payloads of different sizes,
     * decompressed reading produces same result as direct reading.
     */
    @Test
    public void gzipPreservationAcrossMultipleSizes() throws IOException {
        int[] sizes = {64, 256, 1024, 4096, 16384, 65536};
        IonReaderBuilder builder = IonReaderBuilder.standard();

        for (int size : sizes) {
            byte[] binaryIon = generateBinaryIonPayload(size);
            byte[] gzipped = gzipCompress(binaryIon);

            List<IonType> direct = readAllValues(builder, binaryIon);
            List<IonType> fromGzip = readAllValues(builder, gzipped);

            assertEquals(direct, fromGzip,
                "GZIP-compressed reading should match direct reading for size " + size);
        }
    }

    // --- Test Interceptor Implementation ---

    /**
     * A simple test interceptor for verifying ordering behavior.
     */
    private static class TestInterceptor implements InputStreamInterceptor {
        private final String name;

        TestInterceptor(String name) {
            this.name = name;
        }

        @Override
        public String formatName() {
            return name;
        }

        @Override
        public int numberOfBytesNeededToDetermineMatch() {
            return 4;
        }

        @Override
        public boolean isMatch(byte[] candidate, int offset, int length) {
            return false; // Never matches - used only for ordering tests
        }

        @Override
        public InputStream newInputStream(InputStream interceptedStream) {
            return interceptedStream;
        }
    }
}
