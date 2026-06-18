// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.util;

import com.amazon.ion.IonException;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * An InputStream wrapper that tracks the total number of decompressed bytes read from a GZIP stream
 * and throws an {@link IonException} when the configured maximum is exceeded. This provides protection
 * against GZIP bomb attacks where a small compressed payload decompresses to an extremely large output.
 */
public final class LimitedInflaterInputStream extends FilterInputStream {

    private final long maxDecompressedBytes;
    private long totalBytesRead;

    /**
     * Creates a new LimitedInflaterInputStream.
     *
     * @param in the underlying InputStream (typically a GZIPInputStream) to wrap.
     * @param maxDecompressedBytes the maximum number of decompressed bytes allowed to be read
     *                             before throwing an {@link IonException}.
     * @throws IllegalArgumentException if maxDecompressedBytes is not positive.
     */
    public LimitedInflaterInputStream(InputStream in, long maxDecompressedBytes) {
        super(in);
        if (maxDecompressedBytes <= 0) {
            throw new IllegalArgumentException("maxDecompressedBytes must be positive, got: " + maxDecompressedBytes);
        }
        this.maxDecompressedBytes = maxDecompressedBytes;
        this.totalBytesRead = 0;
    }

    @Override
    public int read() throws IOException {
        int b = in.read();
        if (b >= 0) {
            totalBytesRead++;
            checkLimit();
        }
        return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int bytesRead = in.read(b, off, len);
        if (bytesRead > 0) {
            totalBytesRead += bytesRead;
            checkLimit();
        }
        return bytesRead;
    }

    private void checkLimit() {
        if (totalBytesRead > maxDecompressedBytes) {
            throw new IonException(
                String.format(
                    "GZIP decompressed size %d exceeds the configured maximum of %d bytes. " +
                    "This may indicate a GZIP bomb attack. To increase the limit, configure a larger " +
                    "maximumBufferSize in the IonBufferConfiguration.",
                    totalBytesRead,
                    maxDecompressedBytes
                )
            );
        }
    }
}
