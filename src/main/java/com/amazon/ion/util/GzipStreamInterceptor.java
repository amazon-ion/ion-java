// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

/**
 * The interceptor for GZIP streams. This is a singleton that may be accessed using {@link #INSTANCE}.
 */
public enum GzipStreamInterceptor implements InputStreamInterceptor {

    INSTANCE;

    private static final byte[] GZIP_HEADER = {0x1F, (byte) 0x8B};

    @Override
    public String formatName() {
        return "gzip";
    }

    @Override
    public int headerMatchLength() {
        return GZIP_HEADER.length;
    }

    @Override
    public boolean matchesHeader(byte[] candidate, int offset, int length) {
        if (candidate == null || length < GZIP_HEADER.length) {
            return false;
        }

        for (int i = 0; i < GZIP_HEADER.length; i++) {
            if (GZIP_HEADER[i] != candidate[offset + i]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public InputStream newInputStream(InputStream interceptedStream) throws IOException {
        return new GZIPInputStream(interceptedStream);
    }
}
