/*
 * Copyright 2007-2026 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.ion.impl;

import java.nio.ByteBuffer;

/**
 * Internal helpers for adapting {@link java.nio.ByteBuffer} inputs to the
 * {@code byte[]}-based factory methods on {@link com.amazon.ion.IonSystem}.
 * <p>
 * <b>This is an internal API and is subject to change without notice.</b>
 */
public final class _Private_ByteBufferUtils
{
    private _Private_ByteBufferUtils() {}

    /**
     * Copies the readable region of the given buffer (the bytes between its
     * current {@code position} and its {@code limit}) into a newly allocated
     * {@code byte[]}, advancing the buffer's {@code position} to its
     * {@code limit} as a side effect (i.e. the remaining bytes are consumed).
     * <p>
     * This uses a relative bulk {@link ByteBuffer#get(byte[])}, which works for
     * every kind of {@link ByteBuffer} &mdash; array-backed, direct (off-heap),
     * and read-only &mdash; and never throws {@link java.nio.ReadOnlyBufferException}.
     * The returned array is an independent copy, so subsequent mutations of the
     * buffer do not affect it.
     *
     * @param buffer the buffer to read from; must not be null.
     * @return a new array containing the buffer's (former) remaining bytes.
     * @throws NullPointerException if {@code buffer} is null.
     */
    public static byte[] toByteArrayConsuming(ByteBuffer buffer)
    {
        if (buffer == null)
        {
            throw new NullPointerException("ionData");
        }
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }
}
