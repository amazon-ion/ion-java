// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.system;

import com.amazon.ion.util.GzipStreamInterceptor;
import com.amazon.ion.util.InputStreamInterceptor;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the GZIP decompression toggle API on {@link IonReaderBuilder}.
 */
class IonReaderBuilderGzipToggleTest {

    @Test
    void defaultBuilderHasGzipEnabled() {
        IonReaderBuilder builder = IonReaderBuilder.standard();
        assertTrue(builder.isGzipDecompressionEnabled());
    }

    @Test
    void getInputStreamInterceptorsIncludesGzipWhenEnabled() {
        IonReaderBuilder builder = IonReaderBuilder.standard();
        List<InputStreamInterceptor> interceptors = builder.getInputStreamInterceptors();
        boolean hasGzip = interceptors.stream().anyMatch(i -> i instanceof GzipStreamInterceptor);
        assertTrue(hasGzip, "Expected GzipStreamInterceptor in interceptor list when GZIP is enabled");
    }

    @Test
    void getInputStreamInterceptorsExcludesGzipWhenDisabled() {
        IonReaderBuilder builder = IonReaderBuilder.standard()
                .withGzipDecompressionEnabled(false);
        List<InputStreamInterceptor> interceptors = builder.getInputStreamInterceptors();
        boolean hasGzip = interceptors.stream().anyMatch(i -> i instanceof GzipStreamInterceptor);
        assertFalse(hasGzip, "Expected no GzipStreamInterceptor in interceptor list when GZIP is disabled");
    }

    @Test
    void customInterceptorsPreservedWhenGzipDisabled() {
        InputStreamInterceptor customInterceptor = new InputStreamInterceptor() {
            @Override
            public String formatName() {
                return "custom";
            }

            @Override
            public int numberOfBytesNeededToDetermineMatch() {
                return 2;
            }

            @Override
            public boolean isMatch(byte[] candidate, int offset, int length) {
                return false;
            }

            @Override
            public InputStream newInputStream(InputStream interceptedStream) throws IOException {
                return interceptedStream;
            }
        };

        IonReaderBuilder builder = IonReaderBuilder.standard()
                .addInputStreamInterceptor(customInterceptor)
                .withGzipDecompressionEnabled(false);

        List<InputStreamInterceptor> interceptors = builder.getInputStreamInterceptors();

        // Custom interceptor should still be present
        assertTrue(interceptors.contains(customInterceptor),
                "Custom interceptor should be preserved when GZIP is disabled");

        // GZIP interceptor should be excluded
        boolean hasGzip = interceptors.stream().anyMatch(i -> i instanceof GzipStreamInterceptor);
        assertFalse(hasGzip, "GzipStreamInterceptor should be excluded when GZIP is disabled");
    }

    @Test
    void customInterceptorsPreservedWhenGzipEnabled() {
        InputStreamInterceptor customInterceptor = new InputStreamInterceptor() {
            @Override
            public String formatName() {
                return "custom";
            }

            @Override
            public int numberOfBytesNeededToDetermineMatch() {
                return 2;
            }

            @Override
            public boolean isMatch(byte[] candidate, int offset, int length) {
                return false;
            }

            @Override
            public InputStream newInputStream(InputStream interceptedStream) throws IOException {
                return interceptedStream;
            }
        };

        IonReaderBuilder builder = IonReaderBuilder.standard()
                .addInputStreamInterceptor(customInterceptor)
                .withGzipDecompressionEnabled(true);

        List<InputStreamInterceptor> interceptors = builder.getInputStreamInterceptors();

        // Custom interceptor should be present
        assertTrue(interceptors.contains(customInterceptor),
                "Custom interceptor should be preserved when GZIP is enabled");

        // GZIP interceptor should also be present
        boolean hasGzip = interceptors.stream().anyMatch(i -> i instanceof GzipStreamInterceptor);
        assertTrue(hasGzip, "GzipStreamInterceptor should be present when GZIP is enabled");
    }

    @Test
    void withGzipDecompressionEnabledOnImmutableBuilderReturnsMutableCopy() {
        IonReaderBuilder immutableBuilder = IonReaderBuilder.standard().immutable();
        IonReaderBuilder result = immutableBuilder.withGzipDecompressionEnabled(false);

        // The result should be a different instance (mutable copy)
        assertNotSame(immutableBuilder, result);
        // The original should still have GZIP enabled
        assertTrue(immutableBuilder.isGzipDecompressionEnabled());
        // The new builder should have GZIP disabled
        assertFalse(result.isGzipDecompressionEnabled());
    }

    @Test
    void copyConstructorPreservesGzipFlag() {
        IonReaderBuilder original = IonReaderBuilder.standard()
                .withGzipDecompressionEnabled(false);
        IonReaderBuilder copy = original.copy();

        assertFalse(copy.isGzipDecompressionEnabled(),
                "Copy should preserve the gzipDecompressionEnabled flag");
    }

    @Test
    void setGzipDecompressionEnabledOnMutableBuilder() {
        IonReaderBuilder builder = IonReaderBuilder.standard();
        builder.setGzipDecompressionEnabled(false);
        assertFalse(builder.isGzipDecompressionEnabled());
    }

    @Test
    void setGzipDecompressionEnabledOnImmutableBuilderThrows() {
        IonReaderBuilder immutableBuilder = IonReaderBuilder.standard().immutable();
        assertThrows(UnsupportedOperationException.class, () -> {
            immutableBuilder.setGzipDecompressionEnabled(false);
        });
    }
}
