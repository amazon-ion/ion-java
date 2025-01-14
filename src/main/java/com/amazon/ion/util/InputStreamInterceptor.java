// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.util;

import com.amazon.ion.IonReader;
import com.amazon.ion.system.IonReaderBuilder;

import java.io.IOException;
import java.io.InputStream;

/**
 * An interceptor to be consulted by the {@link com.amazon.ion.system.IonReaderBuilder} when creating an
 * {@link IonReader} over a user-provided stream. This allows users to transform a stream's raw bytes into
 * valid text or binary Ion.
 *
 * @see com.amazon.ion.system.IonReaderBuilder#addInputStreamInterceptor(InputStreamInterceptor)
 * @see com.amazon.ion.system.IonSystemBuilder#withReaderBuilder(IonReaderBuilder)
 */
public interface InputStreamInterceptor {

    /**
     * The name of the format the interceptor recognizes.
     * @return a constant String.
     */
    String formatName();

    /**
     * The number of bytes required to be read from the beginning of the stream in order to determine whether
     * it matches the format relevant to this interceptor. If a stream contains fewer than the number of bytes
     * returned by this method, then this interceptor will not be considered a match and
     * {@link #isMatch(byte[], int, int)} will not be called.
     * @return the length in bytes.
     */
    int numberOfBytesNeededToDetermineMatch();

    /**
     * Determines whether the given candidate byte sequence matches this format.
     * @param candidate the candidate byte sequence.
     * @param offset the offset into the candidate bytes to begin matching.
     * @param length the number of bytes (beginning at 'offset') in `candidate`. Must be greater than or equal to
     *               {@link #numberOfBytesNeededToDetermineMatch()}.
     * @return true if the candidate byte sequence matches; otherwise, false.
     */
    boolean isMatch(byte[] candidate, int offset, int length);

    /**
     * Creates a new InputStream that transforms the bytes in the given InputStream into valid text or binary Ion.
     * @param interceptedStream the stream containing bytes in this format.
     * @return a new InputStream.
     * @throws IOException if thrown when constructing the new InputStream.
     */
    InputStream newInputStream(InputStream interceptedStream) throws IOException;
}
