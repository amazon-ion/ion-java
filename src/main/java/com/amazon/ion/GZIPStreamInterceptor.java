// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

/**
 * The interceptor for GZIP streams.
 */
public enum GZIPStreamInterceptor implements StreamInterceptor {

    INSTANCE;

    private static final byte[] GZIP_HEADER = {0x1F, (byte) 0x8B};

    @Override
    public String formatName() {
        return "GZIP";
    }

    @Override
    public int headerLength() {
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
