/*
 * Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package software.amazon.ion.util;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;

/**
 * Represents an {@link InputStream} that may be GZIP compressed or not.
 * This stream assumes that the GZIP header is unique to the raw content.
 * That is, one should not use this on a stream that may have raw data
 * in the begining of the stream that collides with the GZIP header.
 * In the case of Ion binary, Ion text, and XML, a GZIP stream is unambiguous
 * from them.
 */
class GzipOrRawInputStream
    extends FilterInputStream
{
    /** GZIP magic cookie. */
    static final byte[] GZIP_HEADER = {0x1F, (byte) 0x8B};

    /**
     * Wraps the given stream determining based on the GZIP header
     * whether or not the stream is compressed and should be dynamically
     * de-compressed.
     *
     * @param raw The input stream to wrap.
     *
     * @throws IOException
     *         Thrown if there is a problem reading from the underlying stream.
     */
    GzipOrRawInputStream(final InputStream raw)
        throws IOException
    {
        this(raw, 512); // 512 is the default buffer size in GZIPInputStream
    }

    /**
     * Wraps the given stream determining based on the GZIP header
     * whether or not the stream is compressed and should be dynamically
     * de-compressed.
     *
     * @param raw The input stream to wrap.
     * @param bufferSize internal buffer size to use for decompression.
     *
     * @throws IOException
     *         Thrown if there is a problem reading from the underlying stream.
     */
    GzipOrRawInputStream(final InputStream raw, final int bufferSize)
        throws IOException
    {
        super(null);
        final byte[] header = new byte[GZIP_HEADER.length];
        final PushbackInputStream input = new PushbackInputStream(raw, 2);

        // fetch the header and determine length
        int size = 0;
        while (size < header.length) {
            int octet = input.read();
            if (octet == -1) break;

            header[size] = (byte) octet;
            size++;
        }
        assert size <= 2;
        // unread the header
        input.unread(header, 0, size);
        // determine the underlying stream
        if (size == 2 && Arrays.equals(header, GZIP_HEADER)) {
            in = new GZIPInputStream(input, bufferSize);
        } else {
            in = input;
        }
    }
}
