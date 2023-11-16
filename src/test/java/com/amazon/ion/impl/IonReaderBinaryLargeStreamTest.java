// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ion.impl;

import com.amazon.ion.BufferConfiguration;
import com.amazon.ion.IonBufferConfiguration;
import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.Timestamp;
import com.amazon.ion.system.IonBinaryWriterBuilder;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.util.RepeatInputStream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.math.BigDecimal;

import static com.amazon.ion.impl._Private_IonConstants.BINARY_VERSION_MARKER_1_0;
import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;

// NOTE: these tests each take several seconds to complete.
public class IonReaderBinaryLargeStreamTest {

    private byte[] testData(Timestamp timestamp) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonWriter writer = IonBinaryWriterBuilder.standard().build(out);
        writer.writeString("foo");
        writer.writeDecimal(BigDecimal.TEN);
        writer.writeTimestamp(timestamp);
        writer.close();
        byte[] dataWithIvm = out.toByteArray();
        // Strip the IVM, as this needs to be one continuous stream to avoid resetting the reader's internals.
        byte[] data = new byte[dataWithIvm.length - BINARY_VERSION_MARKER_1_0.length];
        System.arraycopy(dataWithIvm, BINARY_VERSION_MARKER_1_0.length, data, 0, data.length);
        return data;
    }

    private static IonReaderBuilder newReaderBuilderThatThrowsOnOversizedValues(boolean enableIncremental) {
        return IonReaderBuilder.standard()
            .withIncrementalReadingEnabled(enableIncremental)
            .withBufferConfiguration(
                IonBufferConfiguration.Builder.standard()
                    .onOversizedValue(new BufferConfiguration.OversizedValueHandler() {
                        @Override
                        public void onOversizedValue() {
                            throw new IonException("oversized");
                        }
                    })
                    .build()
            );
    }

    public void readLargeScalarStream(IonReaderBuilder readerBuilder) throws Exception {
        final Timestamp timestamp = Timestamp.forDay(2000, 1, 1);
        byte[] data = testData(timestamp);
        // Repeat the batch a sufficient number of times to exceed a total stream length of Integer.MAX_VALUE, plus
        // a few more to make sure batches continue to be read correctly.
        final int totalNumberOfBatches = (Integer.MAX_VALUE / data.length) + 7; // 7 makes the value exceed Integer.MAX_VALUE by an arbitrary amount.
        InputStream inputStream = new SequenceInputStream(
            new ByteArrayInputStream(BINARY_VERSION_MARKER_1_0),
            new RepeatInputStream(data, totalNumberOfBatches - 1) // This will provide the data 'totalNumberOfBatches' times
        );
        IonReader reader = readerBuilder.build(inputStream);
        reader.next();
        assertEquals("foo", reader.stringValue());
        reader.next();
        assertEquals(BigDecimal.TEN, reader.decimalValue());
        reader.next();
        assertEquals(timestamp, reader.timestampValue());
        int batchesRead = 1;
        while (reader.next() != null) {
            assertEquals(IonType.STRING, reader.getType());
            assertEquals(IonType.DECIMAL, reader.next());
            assertEquals(IonType.TIMESTAMP, reader.next());
            batchesRead++;
        }
        assertEquals(totalNumberOfBatches, batchesRead);
    }

    @Test
    public void readLargeScalarStreamNonIncremental() throws Exception {
        readLargeScalarStream(newReaderBuilderThatThrowsOnOversizedValues(false));
    }

    @Test
    public void readLargeScalarStreamIncremental() throws Exception {
        readLargeScalarStream(newReaderBuilderThatThrowsOnOversizedValues(true));
    }

    @Test
    public void readLargeContainer() throws Exception {
        final Timestamp timestamp = Timestamp.forDay(2000, 1, 1);

        byte[] data = testData(timestamp);

        final int totalNumberOfBatches = (Integer.MAX_VALUE / data.length) + 32768; // 32768 makes the value exceed Integer.MAX_VALUE by an arbitrary amount.
        ByteArrayOutputStream header = new ByteArrayOutputStream();
        header.write(BINARY_VERSION_MARKER_1_0);
        header.write(0xBE); // List with length subfield.
        IonBinary.writeVarUInt(header, (long) data.length * totalNumberOfBatches); // Length
        InputStream inputStream = new SequenceInputStream(
            new ByteArrayInputStream(header.toByteArray()),
            new RepeatInputStream(data, totalNumberOfBatches - 1) // This will provide the data 'totalNumberOfBatches' times
        );

        IonReader reader = IonReaderBuilder.standard().build(inputStream);
        assertEquals(IonType.LIST, reader.next());
        reader.stepIn();
        int batchesRead = 0;
        while (reader.next() != null) {
            // Materializing on every iteration makes the tests take too long. Do it on every 100.
            boolean materializeValues = (batchesRead % 100) == 0;
            assertEquals(IonType.STRING, reader.getType());
            if (materializeValues) {
                assertEquals("foo", reader.stringValue());
            }
            assertEquals(IonType.DECIMAL, reader.next());
            if (materializeValues) {
                assertEquals(BigDecimal.TEN, reader.decimalValue());
            }
            assertEquals(IonType.TIMESTAMP, reader.next());
            if (materializeValues) {
                assertEquals(timestamp, reader.timestampValue());
            }
            batchesRead++;
        }
        assertNull(reader.next());
        reader.stepOut();
        assertNull(reader.next());
        assertEquals(totalNumberOfBatches, batchesRead);
    }

    @Test
    public void skipLargeContainer() throws Exception {
        final Timestamp timestamp = Timestamp.forDay(2000, 1, 1);

        byte[] data = testData(timestamp);

        final int totalNumberOfBatches = (Integer.MAX_VALUE / data.length) + 17; // 17 makes the value exceed Integer.MAX_VALUE by an arbitrary amount.
        ByteArrayOutputStream header = new ByteArrayOutputStream();
        header.write(BINARY_VERSION_MARKER_1_0);
        header.write(0xCE); // S-exp with length subfield.
        IonBinary.writeVarUInt(header, (long) data.length * totalNumberOfBatches); // Length
        InputStream inputStream = new SequenceInputStream(
            new ByteArrayInputStream(header.toByteArray()),
            new SequenceInputStream(
                new RepeatInputStream(data, totalNumberOfBatches - 1), // This will provide the data 'totalNumberOfBatches' times
                new ByteArrayInputStream(new byte[]{(byte) 0x83, 'b', 'a', 'r'}) // The string "bar"
            )
        );

        IonReader reader = IonReaderBuilder.standard().build(inputStream);
        assertEquals(IonType.SEXP, reader.next());
        assertEquals(IonType.STRING, reader.next());
        assertEquals("bar", reader.stringValue());
        assertNull(reader.next());
    }

    @Test
    public void skipLargeNestedContainer() throws Exception {
        final Timestamp timestamp = Timestamp.forDay(2000, 1, 1);

        byte[] data = testData(timestamp);

        final int totalNumberOfBatches = (Integer.MAX_VALUE / data.length) + 512; // 512 makes the value exceed Integer.MAX_VALUE by an arbitrary amount.
        final long nestedDataLength = (long) data.length * totalNumberOfBatches;
        ByteArrayOutputStream header = new ByteArrayOutputStream();
        header.write(BINARY_VERSION_MARKER_1_0);
        header.write(0xCE); // S-exp with length subfield.
        IonBinary.writeVarUInt(header, 1 + IonBinary.lenVarUInt(nestedDataLength) + nestedDataLength); // Length
        header.write(0xBE); // List with length subfield.
        IonBinary.writeVarUInt(header, nestedDataLength);
        InputStream inputStream = new SequenceInputStream(
            new ByteArrayInputStream(header.toByteArray()),
            new SequenceInputStream(
                new RepeatInputStream(data, totalNumberOfBatches - 1), // This will provide the data 'totalNumberOfBatches' times
                new ByteArrayInputStream(new byte[]{(byte) 0x83, 'b', 'a', 'r'}) // The string "bar"
            )
        );

        IonReader reader = IonReaderBuilder.standard().build(inputStream);
        assertEquals(IonType.SEXP, reader.next());
        reader.stepIn();
        assertEquals(IonType.LIST, reader.next());
        assertNull(reader.next());
        reader.stepOut();
        assertEquals(IonType.STRING, reader.next());
        assertEquals("bar", reader.stringValue());
        assertNull(reader.next());
    }

    @Test
    public void readLargeAnnotatedContainer() throws Exception {
        final Timestamp timestamp = Timestamp.forDay(2000, 1, 1);
        byte[] raw = testData(timestamp);
        ByteArrayOutputStream dataBuilder = new ByteArrayOutputStream();
        dataBuilder.write(0x85); // Field name. Conveniently use SID 5 ("version"), which is in the system symbol table.
        dataBuilder.write(0xBE); // List with length subfield.
        IonBinary.writeVarUInt(dataBuilder, raw.length);
        dataBuilder.write(raw);
        byte[] data = dataBuilder.toByteArray();

        final int totalNumberOfBatches = (Integer.MAX_VALUE / data.length) + 100000; // 100000 makes the value exceed Integer.MAX_VALUE by an arbitrary amount.
        ByteArrayOutputStream header = new ByteArrayOutputStream();
        header.write(BINARY_VERSION_MARKER_1_0);
        long containerLength = (long) data.length * totalNumberOfBatches;
        header.write(0xEE); // Annotation wrapper with length subfield.
        IonBinary.writeVarUInt(header, containerLength + 3 + IonBinary.lenVarUInt(containerLength));
        header.write(0x81); // One byte of annotations.
        header.write(0x84); // Conveniently use SID 4 ("name"), which is in the system symbol table.
        header.write(0xDE); // Struct with length subfield.
        IonBinary.writeVarUInt(header, containerLength); // Length
        InputStream inputStream = new SequenceInputStream(
            new ByteArrayInputStream(header.toByteArray()),
            new RepeatInputStream(data, totalNumberOfBatches - 1) // This will provide the data 'totalNumberOfBatches' times
        );
        IonReader reader = IonReaderBuilder.standard().build(inputStream);
        assertEquals(IonType.STRUCT, reader.next());
        String[] annotations = reader.getTypeAnnotations();
        assertEquals(1, annotations.length);
        assertEquals("name", annotations[0]);
        reader.stepIn();
        int batchesRead = 0;
        while (reader.next() != null) {
            assertEquals(IonType.LIST, reader.getType());
            // Materializing on every iteration makes the tests take too long. Do it on every 100.
            boolean materializeValues = (batchesRead % 100) == 0;
            if (materializeValues) {
                assertEquals("version", reader.getFieldName());
                reader.stepIn();
                assertEquals(IonType.STRING, reader.next());
                assertEquals("foo", reader.stringValue());
                assertEquals(IonType.DECIMAL, reader.next());
                assertEquals(BigDecimal.TEN, reader.decimalValue());
                assertEquals(IonType.TIMESTAMP, reader.next());
                assertEquals(timestamp, reader.timestampValue());
                assertNull(reader.next());
                reader.stepOut();
            }
            batchesRead++;
        }
        assertNull(reader.next());
        reader.stepOut();
        assertNull(reader.next());
        assertEquals(totalNumberOfBatches, batchesRead);
    }

    @Test
    public void skipLargeAnnotatedContainer() throws Exception {
        final Timestamp timestamp = Timestamp.forDay(2000, 1, 1);
        byte[] raw = testData(timestamp);
        ByteArrayOutputStream dataBuilder = new ByteArrayOutputStream();
        dataBuilder.write(0xEE); // Annotation wrapper with length subfield.
        IonBinary.writeVarUInt(dataBuilder, 3 + IonBinary.lenVarUInt(raw.length) + raw.length);
        dataBuilder.write(0x81); // One byte of annotations.
        dataBuilder.write(0x85); // Annotation. Conveniently use SID 5 ("version"), which is in the system symbol table.
        dataBuilder.write(0xCE); // S-exp with length subfield.
        IonBinary.writeVarUInt(dataBuilder, raw.length);
        dataBuilder.write(raw);
        byte[] data = dataBuilder.toByteArray();

        final int totalNumberOfBatches = (Integer.MAX_VALUE / data.length) + 1000000; // 1000000 makes the value exceed Integer.MAX_VALUE by an arbitrary amount.
        ByteArrayOutputStream header = new ByteArrayOutputStream();
        header.write(BINARY_VERSION_MARKER_1_0);
        long containerLength = (long) data.length * totalNumberOfBatches;
        header.write(0xEE); // Annotation wrapper with length subfield.
        IonBinary.writeVarUInt(header, containerLength + 3 + IonBinary.lenVarUInt(containerLength));
        header.write(0x81); // One byte of annotations.
        header.write(0x84); // Conveniently use SID 4 ("name"), which is in the system symbol table.
        header.write(0xBE); // List with length subfield.
        IonBinary.writeVarUInt(header, containerLength); // Length
        InputStream inputStream = new SequenceInputStream(
            new ByteArrayInputStream(header.toByteArray()),
            new RepeatInputStream(data, totalNumberOfBatches - 1) // This will provide the data 'totalNumberOfBatches' times
        );
        IonReader reader = IonReaderBuilder.standard().build(inputStream);
        assertEquals(IonType.LIST, reader.next());
        String[] annotations = reader.getTypeAnnotations();
        assertEquals(1, annotations.length);
        assertEquals("name", annotations[0]);
        assertNull(reader.next());
    }

    // Note: the objective of the following tests is not to assert that large scalars *should* fail, but rather that
    // when they *do* fail due to limitations of the current implementation, they fail by throwing an IonException
    // and not something unexpected and ugly.

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private void cleanlyFailsOnLargeScalar(IonReaderBuilder readerBuilder) throws Exception {
        byte[] data = "foobarbaz".getBytes("UTF-8");
        final int totalNumberOfBatches = (Integer.MAX_VALUE / data.length) + 123; // 123 makes the value exceed Integer.MAX_VALUE by an arbitrary amount.
        ByteArrayOutputStream header = new ByteArrayOutputStream();
        header.write(BINARY_VERSION_MARKER_1_0);
        header.write(0x8E); // String with length subfield.
        IonBinary.writeVarUInt(header, (long) totalNumberOfBatches * data.length);
        InputStream inputStream = new SequenceInputStream(
            new ByteArrayInputStream(header.toByteArray()),
            new RepeatInputStream(data, totalNumberOfBatches - 1) // This will provide the data 'totalNumberOfBatches' times
        );
        IonReader reader = readerBuilder.build(inputStream);
        // If support for large scalars is added, the following will be deleted and the rest of the test
        // completed to assert the correctness of the value.
        if (readerBuilder.isIncrementalReadingEnabled()) {
            thrown.expect(IonException.class);
            reader.next();
        } else {
            assertEquals(IonType.STRING, reader.next());
            thrown.expect(IonException.class);
            reader.stringValue();
        }
    }

    @Test
    public void cleanlyFailsOnLargeScalarNonIncremental() throws Exception {
        cleanlyFailsOnLargeScalar(newReaderBuilderThatThrowsOnOversizedValues(false));
    }

    @Test
    public void cleanlyFailsOnLargeScalarIncremental() throws Exception {
        cleanlyFailsOnLargeScalar(newReaderBuilderThatThrowsOnOversizedValues(true));
    }

    private void cleanlyFailsOnLargeAnnotatedScalar(IonReaderBuilder readerBuilder) throws Exception {
        byte[] data = "foobarbaz".getBytes("UTF-8");
        final int totalNumberOfBatches = (Integer.MAX_VALUE / data.length) + 9999; // 9999 makes the value exceed Integer.MAX_VALUE by an arbitrary amount.
        final long stringLength = (long) totalNumberOfBatches * data.length;
        ByteArrayOutputStream header = new ByteArrayOutputStream();
        header.write(BINARY_VERSION_MARKER_1_0);
        header.write(0xEE); // Annotation wrapper with length subfield.
        IonBinary.writeVarUInt(header, 3 + IonBinary.lenVarUInt(stringLength) + stringLength);
        header.write(0x81); // One byte of annotations.
        header.write(0x84); // Conveniently use SID 4 ("name"), which is in the system symbol table.
        header.write(0x8E); // String with length subfield.
        IonBinary.writeVarUInt(header, stringLength);
        InputStream inputStream = new SequenceInputStream(
            new ByteArrayInputStream(header.toByteArray()),
            new RepeatInputStream(data, totalNumberOfBatches - 1) // This will provide the data 'totalNumberOfBatches' times
        );
        IonReader reader = readerBuilder.build(inputStream);
        // If support for large scalars is added, the following will be deleted and the rest of the test
        // completed to assert the correctness of the value.
        if (readerBuilder.isIncrementalReadingEnabled()) {
            thrown.expect(IonException.class);
            reader.next();
        } else {
            assertEquals(IonType.STRING, reader.next());
            thrown.expect(IonException.class);
            reader.stringValue();
        }
    }

    @Test
    public void cleanlyFailsOnLargeAnnotatedScalarNonIncremental() throws Exception {
        cleanlyFailsOnLargeAnnotatedScalar(newReaderBuilderThatThrowsOnOversizedValues(false));
    }

    @Test
    public void cleanlyFailsOnLargeAnnotatedScalarIncremental() throws Exception {
        cleanlyFailsOnLargeAnnotatedScalar(newReaderBuilderThatThrowsOnOversizedValues(true));
    }

    @Test
    public void cleanlyFailsOnLargeContainerIncremental() throws Exception {
        final Timestamp timestamp = Timestamp.forDay(2000, 1, 1);

        byte[] data = testData(timestamp);

        final int totalNumberOfBatches = (Integer.MAX_VALUE / data.length) + 42; // 42 makes the value exceed Integer.MAX_VALUE by an arbitrary amount.
        ByteArrayOutputStream header = new ByteArrayOutputStream();
        header.write(BINARY_VERSION_MARKER_1_0);
        header.write(0xCE); // S-exp with length subfield.
        IonBinary.writeVarUInt(header, (long) data.length * totalNumberOfBatches); // Length
        InputStream inputStream = new SequenceInputStream(
            new ByteArrayInputStream(header.toByteArray()),
            new RepeatInputStream(data, totalNumberOfBatches - 1) // This will provide the data 'totalNumberOfBatches' times
        );

        IonReader reader = newReaderBuilderThatThrowsOnOversizedValues(true).build(inputStream);
        thrown.expect(IonException.class);
        reader.next();
    }

}
