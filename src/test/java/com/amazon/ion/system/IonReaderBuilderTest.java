// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.system;

import static com.amazon.ion.TestUtils.gzippedBytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.amazon.ion.BitUtils;
import com.amazon.ion.GZIPStreamInterceptor;
import com.amazon.ion.IonBufferConfiguration;
import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.StreamInterceptor;
import com.amazon.ion.impl.ResizingPipedInputStream;
import com.amazon.ion.impl._Private_IonBinaryWriterBuilder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import com.amazon.ion.impl._Private_IonConstants;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.rules.ExpectedException;

/**
 * Note: because the IonReaderBuilder is used by IonSystem.newReader(...),
 * its build() methods are well-exercised elsewhere. See: ReaderMaker.
 */
public class IonReaderBuilderTest
{

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testMutable()
    {
        IonCatalog catalog = new SimpleCatalog();
        IonReaderBuilder mutable = IonReaderBuilder.standard().withCatalog(catalog);
        assertSame(catalog, mutable.getCatalog());
        IonReaderBuilder mutableSame = mutable.withCatalog(new SimpleCatalog());
        assertNotSame(catalog, mutable.getCatalog());
        assertSame(mutable, mutableSame);
    }

    @Test
    public void testImmutable()
    {
        IonCatalog catalog = new SimpleCatalog();
        IonReaderBuilder mutable = IonReaderBuilder.standard().withCatalog(catalog);
        IonReaderBuilder immutable = mutable.immutable();
        mutable.withCatalog(new SimpleCatalog());
        assertNotSame(catalog, mutable.getCatalog());
        assertSame(catalog, immutable.getCatalog());
    }

    @Test
    public void testMutatingImmutableFails()
    {
        IonReaderBuilder immutable = IonReaderBuilder.standard().immutable();
        thrown.expect(UnsupportedOperationException.class);
        immutable.setCatalog(new SimpleCatalog());
    }

    @Test
    public void testMutateCopiedImmutable()
    {
        IonCatalog catalog = new SimpleCatalog();
        IonReaderBuilder immutable = IonReaderBuilder.standard().withCatalog(catalog).immutable();
        IonReaderBuilder mutableCopy = immutable.copy();
        assertSame(immutable, immutable.immutable());
        assertNotSame(immutable, mutableCopy);
        assertSame(catalog, mutableCopy.getCatalog());
        mutableCopy.withCatalog(new SimpleCatalog());
        assertNotSame(catalog, mutableCopy.getCatalog());
    }

    @Test
    public void testMutateCopiedMutable()
    {
        IonCatalog catalog = new SimpleCatalog();
        IonReaderBuilder mutable = IonReaderBuilder.standard().withCatalog(catalog);
        IonReaderBuilder mutableCopy = mutable.copy();
        assertNotSame(mutable, mutable.immutable());
        assertNotSame(mutable, mutableCopy);
        assertSame(mutable, mutable.mutable());
        assertSame(catalog, mutableCopy.getCatalog());
        IonReaderBuilder mutableSame = mutableCopy.withCatalog(new SimpleCatalog());
        assertNotSame(catalog, mutableCopy.getCatalog());
        assertSame(mutableCopy, mutableSame);
    }

    @Test
    public void testSystemFreeRoundtrip() throws IOException
    {
        // No IonSystem in sight.
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonWriter writer = _Private_IonBinaryWriterBuilder.standard().build(out);
        writer.writeInt(42);
        writer.finish();
        IonReader reader = IonReaderBuilder.standard().build(out.toByteArray());
        assertEquals(IonType.INT, reader.next());
        assertEquals(42, reader.intValue());
    }

    @Test
    public void testEnableIncrementalReading() throws IOException
    {
        IonReaderBuilder builder = IonReaderBuilder.standard();
        assertFalse(builder.isIncrementalReadingEnabled());
        builder.withIncrementalReadingEnabled(true);
        assertTrue(builder.isIncrementalReadingEnabled());
        builder.setIncrementalReadingDisabled();
        assertFalse(builder.isIncrementalReadingEnabled());
        builder.setIncrementalReadingEnabled();
        assertTrue(builder.isIncrementalReadingEnabled());
        builder.withIncrementalReadingEnabled(false);
        assertFalse(builder.isIncrementalReadingEnabled());
        ByteArrayOutputStream data = new ByteArrayOutputStream();
        data.write(_Private_IonConstants.BINARY_VERSION_MARKER_1_0);
        data.write(0xE5); // 5-byte annotation wrapper (incomplete).
        IonReader reader1 = builder.build(data.toByteArray());
        try {
            reader1.next();
            fail();
        } catch (IonException e) {
            // Expected; this is a non-incremental reader, but a complete value was not available.
        }
        builder.withIncrementalReadingEnabled(true);
        // Even though incremental reading is enabled, when a byte array is provided an error should be raised on the
        // incomplete input because additional data can never be provided.
        IonReader reader2 = builder.build(data.toByteArray());
        assertThrows(IonException.class, reader2::next);
        IonReader reader3 = builder.build(new ByteArrayInputStream(data.toByteArray()));
        assertNull(reader3.next());
    }

    @Test
    public void testBufferConfiguration()
    {
        IonBufferConfiguration configuration1 = IonBufferConfiguration.Builder.standard().build();
        IonBufferConfiguration configuration2 = IonBufferConfiguration.Builder.standard().build();
        IonReaderBuilder builder = IonReaderBuilder.standard();
        assertSame(IonBufferConfiguration.DEFAULT, builder.getBufferConfiguration());
        builder.withBufferConfiguration(configuration1);
        assertSame(configuration1, builder.getBufferConfiguration());
        builder.setBufferConfiguration(configuration2);
        assertSame(configuration2, builder.getBufferConfiguration());
        builder.withBufferConfiguration(IonBufferConfiguration.DEFAULT);
        assertSame(IonBufferConfiguration.DEFAULT, builder.getBufferConfiguration());
    }

    @Test
    public void testNullBufferConfigurationThrows() {
        IonReaderBuilder builder = IonReaderBuilder.standard();
        assertThrows(IllegalArgumentException.class, () -> builder.withBufferConfiguration(null));
    }

    @Test
    public void testIncrementalReadingSupportsAutoGzip() throws IOException
    {
        IonReaderBuilder builder = IonReaderBuilder.standard();
        builder.withIncrementalReadingEnabled(true);
        ByteArrayOutputStream data = new ByteArrayOutputStream();
        GZIPOutputStream gzip = new GZIPOutputStream(data);
        gzip.write(_Private_IonConstants.BINARY_VERSION_MARKER_1_0);
        gzip.write(0x20); // int 0.
        gzip.close();
        IonReader reader1 = builder.build(data.toByteArray());
        assertEquals(IonType.INT, reader1.next());
        assertEquals(0, reader1.intValue());
        reader1.close();
        IonReader reader2 = builder.build(new ByteArrayInputStream(data.toByteArray()));
        assertEquals(IonType.INT, reader2.next());
        assertEquals(0, reader2.intValue());
        reader2.close();
    }

    @Test
    public void concatenatedAfterGZIPHeader() throws Exception {
        // Tests that a stream that initially contains only a GZIP header can be read successfully if more data
        // is later made available.
        IonReaderBuilder builder = IonReaderBuilder.standard();
        builder.withIncrementalReadingEnabled(true);
        final int gzipHeaderLength = 10; // Length of the GZIP header, as defined by the GZIP spec.
        byte[] gzIVM = gzippedBytes(0xE0, 0x01, 0x00, 0xEA);  // IVM
        ResizingPipedInputStream pipe = new ResizingPipedInputStream(128);
        // First, feed just the GZIP header bytes.
        pipe.receive(gzIVM, 0, gzipHeaderLength); // Just the GZIP header
        // We must build the reader after the input stream has some content
        IonReader reader = builder.build(pipe);
        //  On next(), the GZIPInputStream will throw EOFException, which is handled by the reader.
        assertNull(reader.next());
        // Finish feeding the gzipped IVM payload
        pipe.receive(gzIVM, gzipHeaderLength, gzIVM.length - gzipHeaderLength);
        // Now feed the bytes for an Ion value, spanning the value across two GZIP payloads.
        pipe.receive(gzippedBytes(0x2E)); // Positive int with length subfield
        pipe.receive(gzippedBytes(0x81, 0x01)); // Length 1, value 1
        assertEquals(IonType.INT, reader.next());
        assertEquals(1, reader.intValue());
        assertNull(reader.next());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void incompleteIvmFailsCleanly(boolean isIncremental) throws Exception {
        IonReader reader = IonReaderBuilder.standard().withIncrementalReadingEnabled(isIncremental).build(BitUtils.bytes(0xE0, 0x01, 0x00));
        assertThrows(IonException.class, reader::next);
        reader.close();
    }

    @Test
    public void gzipInterceptorEnabledByDefault() {
        IonReaderBuilder builder = IonReaderBuilder.standard();
        List<StreamInterceptor> interceptors = builder.getStreamInterceptors();
        assertEquals(1, interceptors.size());
        assertEquals(GZIPStreamInterceptor.INSTANCE.formatName(), interceptors.get(0).formatName());
        // The list returned from IonReaderBuilder.getStreamInterceptors() is unmodifiable.
        assertThrows(UnsupportedOperationException.class, () -> interceptors.add(GZIPStreamInterceptor.INSTANCE));
    }

}
