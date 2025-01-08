// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.util;

import com.amazon.ion.IonException;
import com.amazon.ion.system.IonBinaryWriterBuilder;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazon.ion.system.IonTextWriterBuilder;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

import com.amazon.ion.impl._Private_IonReaderBuilder;
import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.Assert.assertSame;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Demonstrates how a StreamInterceptor that recognizes Zstd streams can be plugged into the IonReaderBuilder and
 * IonSystem.
 */
public class ZstdStreamInterceptorTest {

    private static final byte[] ZSTD_HEADER = {(byte) 0x28, (byte) 0xB5, (byte) 0x2F, (byte) 0xFD};

    public static class ZstdStreamInterceptor implements InputStreamInterceptor {

        @Override
        public String formatName() {
            return "Zstd";
        }

        @Override
        public int headerMatchLength() {
            return ZSTD_HEADER.length;
        }

        @Override
        public boolean matchesHeader(byte[] candidate, int offset, int length) {
            if (candidate == null || length < ZSTD_HEADER.length) {
                return false;
            }

            for (int i = 0; i < ZSTD_HEADER.length; i++) {
                if (ZSTD_HEADER[i] != candidate[offset + i]) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public InputStream newInputStream(InputStream interceptedStream) throws IOException {
            return new ZstdInputStream(interceptedStream).setContinuous(true);
        }
    }

    public enum ZstdStream {
        BINARY_STREAM_READER {
            @Override
            IonReader newReader(IonReaderBuilder builder) {
                return builder.build(new ByteArrayInputStream(BINARY_BYTES));
            }
        },
        TEXT_STREAM_READER {
            @Override
            IonReader newReader(IonReaderBuilder builder) {
                return builder.build(new ByteArrayInputStream(TEXT_BYTES));
            }
        },
        BINARY_BYTES_READER {
            @Override
            IonReader newReader(IonReaderBuilder builder) {
                return builder.build(BINARY_BYTES);
            }
        },
        TEXT_BYTES_READER {
            @Override
            IonReader newReader(IonReaderBuilder builder) {
                return builder.build(TEXT_BYTES);
            }
        },
        BINARY_STREAM_SYSTEM {
            @Override
            IonReader newReader(IonReaderBuilder builder) {
                return IonSystemBuilder.standard()
                    .withReaderBuilder(builder)
                    .build()
                    .newReader(new ByteArrayInputStream(BINARY_BYTES));
            }
        },
        TEXT_STREAM_SYSTEM {
            @Override
            IonReader newReader(IonReaderBuilder builder) {
                return IonSystemBuilder.standard()
                    .withReaderBuilder(builder)
                    .build()
                    .newReader(new ByteArrayInputStream(TEXT_BYTES));
            }
        },
        BINARY_BYTES_SYSTEM {
            @Override
            IonReader newReader(IonReaderBuilder builder) {
                return IonSystemBuilder.standard()
                    .withReaderBuilder(builder)
                    .build()
                    .newReader(BINARY_BYTES);
            }
        },
        TEXT_BYTES_SYSTEM {
            @Override
            IonReader newReader(IonReaderBuilder builder) {
                return IonSystemBuilder.standard()
                    .withReaderBuilder(builder)
                    .build()
                    .newReader(TEXT_BYTES);
            }
        };

        abstract IonReader newReader(IonReaderBuilder builder);

        private static byte[] writeCompressedStream(boolean isText) {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (IonWriter writer = isText
                ? IonTextWriterBuilder.standard().build(new ZstdOutputStream(bytes))
                : IonBinaryWriterBuilder.standard().build(new ZstdOutputStream(bytes))
            ) {
                writer.writeInt(123);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            return bytes.toByteArray();
        }

        private static final byte[] TEXT_BYTES = writeCompressedStream(true);
        private static final byte[] BINARY_BYTES = writeCompressedStream(false);
    }

    @ParameterizedTest
    @EnumSource(ZstdStream.class)
    public void interceptedWhenAddedManually(ZstdStream stream) throws IOException {
        IonReaderBuilder builder = IonReaderBuilder.standard().addInputStreamInterceptor(new ZstdStreamInterceptor());
        try (IonReader reader = stream.newReader(builder)) {
            assertEquals(IonType.INT, reader.next());
            assertEquals(123, reader.intValue());
        }
    }

    public static class CustomInterceptorClassLoader extends URLClassLoader {

        public CustomInterceptorClassLoader() {
            super(new URL[0], InputStreamInterceptor.class.getClassLoader());
        }

        @Override
        public Enumeration<URL> getResources(String name) throws IOException {
            if (name.equals("META-INF/services/" + InputStreamInterceptor.class.getName())) {
                URL dummyUrl = new URL("unused", "unused", 42, "unused", new URLStreamHandler() {
                    @Override
                    protected URLConnection openConnection(URL url) {
                        return new URLConnection(url) {
                            @Override
                            public void connect() {
                                // Nothing to do.
                            }

                            @Override
                            public InputStream getInputStream() {
                                return new ByteArrayInputStream(ZstdStreamInterceptor.class.getName().getBytes(StandardCharsets.UTF_8));
                            }
                        };
                    }
                });
                return Collections.enumeration(Collections.singletonList(dummyUrl));
            }
            return super.getResources(name);
        }
    }

    @ParameterizedTest
    @EnumSource(ZstdStream.class)
    public void interceptedWhenDetectedOnClasspath(ZstdStream stream) throws IOException {
        IonReaderBuilder builder = ((_Private_IonReaderBuilder) IonReaderBuilder.standard())
            .withCustomClassLoader(new CustomInterceptorClassLoader());
        try (IonReader reader = stream.newReader(builder)) {
            assertEquals(IonType.INT, reader.next());
            assertEquals(123, reader.intValue());
        }
    }

    private static void assertGzipThen(IonReaderBuilder readerBuilder, Class<? extends InputStreamInterceptor> interceptorType) {
        List<InputStreamInterceptor> streamInterceptorsAddedManually = readerBuilder.getInputStreamInterceptors();
        assertEquals(2, streamInterceptorsAddedManually.size());
        assertSame(GZIPStreamInterceptor.INSTANCE, streamInterceptorsAddedManually.get(0));
        assertTrue(streamInterceptorsAddedManually.get(1).getClass().isAssignableFrom(interceptorType));
    }

    @Test
    public void gzipAlwaysAvailableWhenZstdAddedManually() {
        IonReaderBuilder builder = IonReaderBuilder.standard().addInputStreamInterceptor(new ZstdStreamInterceptor());
        assertGzipThen(builder, ZstdStreamInterceptor.class);
    }

    @Test
    public void gzipAlwaysAvailableWhenZstdDetectedOnClasspath() {
        IonReaderBuilder builder = ((_Private_IonReaderBuilder) IonReaderBuilder.standard())
            .withCustomClassLoader(new CustomInterceptorClassLoader());
        assertGzipThen(builder, ZstdStreamInterceptor.class);
    }

    private static class DummyInterceptor implements InputStreamInterceptor {

        @Override
        public String formatName() {
            return null;
        }

        @Override
        public int headerMatchLength() {
            return 0;
        }

        @Override
        public boolean matchesHeader(byte[] candidate, int offset, int length) {
            return false;
        }

        @Override
        public InputStream newInputStream(InputStream interceptedStream) {
            return null;
        }
    }

    @Test
    public void addingManuallyTakesPrecedenceOverClasspath() {
        IonReaderBuilder builder = ((_Private_IonReaderBuilder) IonReaderBuilder.standard())
            .withCustomClassLoader(new CustomInterceptorClassLoader()) // This would add Zstd if classpath detection were used.
            .addInputStreamInterceptor(new DummyInterceptor());
        assertGzipThen(builder, DummyInterceptor.class);
    }

    @Test
    public void differentClassLoadersInEachThread() throws Exception {
        Thread withDefaultClassLoader = new Thread(() -> {
            IonReaderBuilder readerBuilder = IonReaderBuilder.standard();
            List<InputStreamInterceptor> streamInterceptors = readerBuilder.getInputStreamInterceptors();
            assertEquals(1, streamInterceptors.size());
            assertSame(GZIPStreamInterceptor.INSTANCE, streamInterceptors.get(0));
        });
        Thread withCustomClassLoader = new Thread(() -> {
            IonReaderBuilder readerBuilder = IonReaderBuilder.standard();
            List<InputStreamInterceptor> streamInterceptors = readerBuilder.getInputStreamInterceptors();
            assertGzipThen(readerBuilder, ZstdStreamInterceptor.class);
            // Verify that a new IonReaderBuilder instance does not re-detect the interceptors applicable to this
            // thread.
            readerBuilder = IonReaderBuilder.standard();
            assertSame(streamInterceptors, readerBuilder.getInputStreamInterceptors());
        });
        withCustomClassLoader.setContextClassLoader(new CustomInterceptorClassLoader());

        withDefaultClassLoader.start();
        withCustomClassLoader.start();

        // While the spawned threads are working, verify they do not affect the parent thread.
        IonReaderBuilder builder = IonReaderBuilder.standard().addInputStreamInterceptor(new DummyInterceptor());
        assertGzipThen(builder, DummyInterceptor.class);

        withDefaultClassLoader.join();
        withCustomClassLoader.join();
    }

    private static class LengthTooLongInterceptor implements InputStreamInterceptor {

        private final int length;

        LengthTooLongInterceptor(int length) {
            this.length = length;
        }

        @Override
        public String formatName() {
            return null;
        }

        @Override
        public int headerMatchLength() {
            return length;
        }

        @Override
        public boolean matchesHeader(byte[] candidate, int offset, int length) {
            return Assertions.fail("This method should be unreachable.");
        }

        @Override
        public InputStream newInputStream(InputStream interceptedStream) {
            return Assertions.fail("This method should be unreachable.");
        }
    }

    @ParameterizedTest
    @EnumSource(ZstdStream.class)
    public void notInterceptedWhenStreamLengthIsLessThanHeaderLength(ZstdStream stream) throws IOException {
        IonReaderBuilder builder = IonReaderBuilder.standard()
            // The LengthTooLongInterceptor should be skipped, then the ZstdStreamInterceptor matched.
            .addInputStreamInterceptor(new LengthTooLongInterceptor(1000)) // None of the test data is this long.
            .addInputStreamInterceptor(new ZstdStreamInterceptor());
        try (IonReader reader = stream.newReader(builder)) {
            assertEquals(IonType.INT, reader.next());
            assertEquals(123, reader.intValue());
        }
    }

    @ParameterizedTest
    @EnumSource(ZstdStream.class)
    public void expectFailureWhenHeaderLengthIsInvalid(ZstdStream stream) {
        IonReaderBuilder builder = IonReaderBuilder.standard()
            // This header length is invalid because an array of that size cannot be allocated.
            .addInputStreamInterceptor(new LengthTooLongInterceptor(Integer.MAX_VALUE))
            .addInputStreamInterceptor(new ZstdStreamInterceptor());
        assertThrows(IonException.class, () -> stream.newReader(builder));
    }

    private static class OneBytePerReadInputStream extends InputStream {

        private final InputStream delegate;

        OneBytePerReadInputStream(InputStream delegate) {
            this.delegate = delegate;
        }

        @Override
        public int read() throws IOException {
            return delegate.read();
        }

        @Override
        public int read(byte[] bytes, int off, int len) throws IOException {
            int b = delegate.read();
            if (b < 0) {
                return -1;
            }
            bytes[off] = (byte) b;
            return 1;
        }
    }

    @Test
    public void headerRequiresMultipleInputStreamReads() throws IOException {
        IonReaderBuilder builder = IonReaderBuilder.standard()
            .addInputStreamInterceptor(new ZstdStreamInterceptor());
        try (IonReader reader = builder.build(new OneBytePerReadInputStream(new ByteArrayInputStream(ZstdStream.BINARY_BYTES)))) {
            assertEquals(IonType.INT, reader.next());
            assertEquals(123, reader.intValue());
        }
    }
}
