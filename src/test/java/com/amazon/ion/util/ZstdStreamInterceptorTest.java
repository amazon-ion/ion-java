// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.util;

import com.amazon.ion.IonException;
import com.amazon.ion.IonSystem;
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
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;
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
        BINARY_STREAM_READER(builder -> builder.build(stream(binaryBytes()))),
        TEXT_STREAM_READER(builder -> builder.build(stream(textBytes()))),
        BINARY_BYTES_READER(builder -> builder.build(binaryBytes())),
        TEXT_BYTES_READER(builder -> builder.build(textBytes())),
        BINARY_STREAM_SYSTEM(builder -> system(builder).newReader((stream(binaryBytes())))),
        TEXT_STREAM_SYSTEM(builder -> system(builder).newReader((stream(textBytes())))),
        BINARY_BYTES_SYSTEM(builder -> system(builder).newReader(binaryBytes())),
        TEXT_BYTES_SYSTEM(builder -> system(builder).newReader(textBytes()));

        private final Function<IonReaderBuilder, IonReader> readerFactory;

        ZstdStream(Function<IonReaderBuilder, IonReader> readerFactory) {
            this.readerFactory = readerFactory;
        }

        IonReader newReader(IonReaderBuilder builder) {
            return readerFactory.apply(builder);
        }

        static IonSystem system(IonReaderBuilder builder) {
            return IonSystemBuilder.standard().withReaderBuilder(builder).build();
        }

        static byte[] textBytes() {
            return writeCompressedStream(IonTextWriterBuilder.standard()::build);
        }

        static byte[] binaryBytes() {
            return writeCompressedStream(IonBinaryWriterBuilder.standard()::build);
        }

        static InputStream stream(byte[] bytes) {
            return new ByteArrayInputStream(bytes);
        }

        private static byte[] writeCompressedStream(Function<OutputStream, IonWriter> writerBuilder) {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (IonWriter writer = writerBuilder.apply(new ZstdOutputStream(bytes))) {
                writer.writeInt(123);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            return bytes.toByteArray();
        }
    }

    @ParameterizedTest
    @EnumSource(ZstdStream.class)
    public void interceptorsFunctionProperly(ZstdStream stream) throws IOException {
        IonReaderBuilder builder = IonReaderBuilder.standard().addInputStreamInterceptor(new ZstdStreamInterceptor());
        try (IonReader reader = stream.newReader(builder)) {
            assertEquals(IonType.INT, reader.next());
            assertEquals(123, reader.intValue());
        }
    }

    public static class CustomInterceptorClassLoader extends URLClassLoader {

        public CustomInterceptorClassLoader() {
            // Allow this ClassLoader to load all classes relevant to the builder and custom InputStreamInterceptor
            // implementations being tested.
            super(
                new URL[] {
                    IonReaderBuilder.class.getProtectionDomain().getCodeSource().getLocation(),
                    ZstdStreamInterceptor.class.getProtectionDomain().getCodeSource().getLocation(),
                    ZstdInputStream.class.getProtectionDomain().getCodeSource().getLocation()
                },
                IonReaderBuilder.class.getClassLoader().getParent()
            );
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

    /**
     * Asserts that the given IonReaderBuilder has a GzipStreamInterceptor followed an InputStreamInterceptor of the
     * given type.
     * @param readerBuilder the builder.
     * @param interceptorType the type of the InputStreamInterceptor to follow GzipStreamInterceptor.
     */
    private static void assertGzipThen(IonReaderBuilder readerBuilder, Class<? extends InputStreamInterceptor> interceptorType) {
        List<InputStreamInterceptor> streamInterceptors = readerBuilder.getInputStreamInterceptors();
        assertEquals(2, streamInterceptors.size());
        assertSame(GzipStreamInterceptor.INSTANCE, streamInterceptors.get(0));
        assertTrue(streamInterceptors.get(1).getClass().isAssignableFrom(interceptorType));
    }

    /**
     * Asserts that the given IonReaderBuilder has a GzipStreamInterceptor followed an InputStreamInterceptor of the
     * given type, performing all accesses via reflection. This must be used when an instance has been manually
     * loaded by a custom ClassLoader, as such instances are not compatible with vanilla Java written in this context
     * (for example, a manually loaded `IonReaderBuilder` would throw a `ClassCastException` if assigned to
     * `IonReaderBuilder` in this test).
     * @param builderClass the Class that represents the custom-loaded IonReaderBuilder.
     * @param builderInstance the builder instance.
     * @param interceptorTypes the type of the InputStreamInterceptor(s) to follow GzipStreamInterceptor.
     */
    private static List<?> assertGzipThen(Class<?> builderClass, Object builderInstance, Class<?>... interceptorTypes) {
        try {
            Method getInputStreamInterceptorsMethod = builderClass.getMethod("getInputStreamInterceptors");
            List<?> streamInterceptors = (List<?>) getInputStreamInterceptorsMethod.invoke(builderInstance);
            assertEquals(interceptorTypes.length + 1, streamInterceptors.size());
            assertEquals(GzipStreamInterceptor.class.getName(), streamInterceptors.get(0).getClass().getName());
            for (int i = 0; i < interceptorTypes.length; i++) {
                Class<?> interceptorType = interceptorTypes[i];
                assertEquals(interceptorType.getName(), streamInterceptors.get(i + 1).getClass().getName());
            }
            return streamInterceptors;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Executes the given target using an IonReaderBuilder instance loaded from the given custom ClassLoader.
     * @param classLoader the custom ClassLoader to use.
     * @param target the code to execute.
     */
    private void executeWithCustomClassLoader(ClassLoader classLoader, BiConsumer<Class<?>, Object> target) {
        Class<?> ionReaderBuilderClass;
        Object ionReaderBuilderInstance;
        try {
            // Note: below, 'true' forces static initialization.
            ionReaderBuilderClass = Class.forName(IonReaderBuilder.class.getName(), true, classLoader);
            Method factoryMethod = ionReaderBuilderClass.getMethod("standard");
            ionReaderBuilderInstance = factoryMethod.invoke(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        target.accept(ionReaderBuilderClass, ionReaderBuilderInstance);
    }

    @Test
    public void gzipAlwaysAvailableWhenZstdAddedManually() {
        IonReaderBuilder builder = IonReaderBuilder.standard().addInputStreamInterceptor(new ZstdStreamInterceptor());
        assertGzipThen(builder, ZstdStreamInterceptor.class);
    }

    @Test
    public void gzipAlwaysAvailableWhenZstdDetectedOnClasspath() {
        executeWithCustomClassLoader(
            new CustomInterceptorClassLoader(),
            (builderClass, builder) -> assertGzipThen(builderClass, builder, ZstdStreamInterceptor.class)
        );
    }

    @Test
    public void manuallyAddedInterceptorsComeAfterDetectedInterceptors() {
        executeWithCustomClassLoader(
            new CustomInterceptorClassLoader(),
            (builderClass, builder) -> {
                // The custom ClassLoader adds Zstd. This should occur after GZIP (the default) but before the one
                // added manually.
                try {
                    Class<?> zstdInterceptorClass = builderClass.getClassLoader().loadClass(ZstdStreamInterceptor.class.getName());
                    Class<?> dummyInterceptorClass = builderClass.getClassLoader().loadClass(LengthTooLongInterceptor.class.getName());
                    Object dummyInterceptorInstance = dummyInterceptorClass.getConstructor(int.class).newInstance(0);
                    // Manually load the interface with the same ClassLoader so that it's compatible with the instance.
                    Class<?> inputStreamInterceptorInterface = builderClass.getClassLoader().loadClass(InputStreamInterceptor.class.getName());
                    Method addInterceptor = builderClass.getMethod("addInputStreamInterceptor", inputStreamInterceptorInterface);
                    assertGzipThen(
                        builderClass,
                        addInterceptor.invoke(builder, dummyInterceptorInstance),
                        zstdInterceptorClass, // Zstd is the first to follow GZIP because it was detected on the classpath.
                        dummyInterceptorClass // Manually added interceptors come last.
                    );
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
    }

    /**
     * Configures the given thread to capture any uncaught exceptions thrown during execution of the thread.
     * @param thread the thread to configure.
     * @return a reference to the exception thrown (if any) during execution of the thread.
     */
    private AtomicReference<Throwable> registerExceptionHandler(Thread thread) {
        AtomicReference<Throwable> error = new AtomicReference<>();
        thread.setUncaughtExceptionHandler((t, e) -> error.set(e));
        return error;
    }

    /**
     * Fails the test if the given reference points to any exception.
     * @param error an error reference.
     */
    private void failOnAnyError(AtomicReference<Throwable> error) {
        Throwable t = error.get();
        if (t != null) {
            Assertions.fail(t);
        }
    }

    // This is a multithreaded test; execute multiple times to increase the chances of triggering a race condition if
    // one exists.
    @RepeatedTest(100)
    public void differentClassLoadersInEachThread() throws Exception {
        Thread withDefaultClassLoader = new Thread(() -> {
            IonReaderBuilder readerBuilder = IonReaderBuilder.standard();
            List<InputStreamInterceptor> streamInterceptors = readerBuilder.getInputStreamInterceptors();
            assertEquals(1, streamInterceptors.size());
            assertSame(GzipStreamInterceptor.INSTANCE, streamInterceptors.get(0));
        });
        AtomicReference<Throwable> withDefaultClassLoaderError = registerExceptionHandler(withDefaultClassLoader);
        Thread withCustomClassLoader = new Thread(() -> {
            AtomicReference<List<?>> streamInterceptors = new AtomicReference<>();
            executeWithCustomClassLoader(
                Thread.currentThread().getContextClassLoader(), // This will be our custom ClassLoader (set below)
                (builderClass, builder) -> {
                    streamInterceptors.set(assertGzipThen(builderClass, builder, ZstdStreamInterceptor.class));
                }
            );
            // Verify that a new IonReaderBuilder instance does not re-detect the interceptors applicable to this
            // ClassLoader.
            executeWithCustomClassLoader(
                Thread.currentThread().getContextClassLoader(), // This will be our custom ClassLoader (set below)
                (builderClass, builder) -> {
                    assertSame(
                        streamInterceptors.get(),
                        assertGzipThen(builderClass, builder, ZstdStreamInterceptor.class)
                    );
                }
            );
        });
        AtomicReference<Throwable> withCustomClassLoaderError = registerExceptionHandler(withCustomClassLoader);
        withCustomClassLoader.setContextClassLoader(new CustomInterceptorClassLoader());

        withDefaultClassLoader.start();
        withCustomClassLoader.start();

        // While the spawned threads are working, verify they do not affect the parent thread.
        IonReaderBuilder builder = IonReaderBuilder.standard().addInputStreamInterceptor(new LengthTooLongInterceptor(0));
        assertGzipThen(builder, LengthTooLongInterceptor.class);

        withDefaultClassLoader.join();
        withCustomClassLoader.join();

        failOnAnyError(withDefaultClassLoaderError);
        failOnAnyError(withCustomClassLoaderError);
    }

    public static class LengthTooLongInterceptor implements InputStreamInterceptor {

        private final int length;

        public LengthTooLongInterceptor(int length) {
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
        try (IonReader reader = builder.build(new OneBytePerReadInputStream(new ByteArrayInputStream(ZstdStream.binaryBytes())))) {
            assertEquals(IonType.INT, reader.next());
            assertEquals(123, reader.intValue());
        }
    }
}
