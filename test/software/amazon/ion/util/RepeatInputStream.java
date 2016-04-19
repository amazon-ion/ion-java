/*
 * Copyright 2011-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Streams a fixed sequence of bytes repeated numerous times.
 * Useful for synthesizing very long streams of data.
 */
public final class RepeatInputStream
    extends InputStream
{
    private final byte[] bytes;
    private final long times;
    private long remainder;
    private ByteBuffer buf;

    /**
     * @param bytes the data to be repeated.
     * @param times the number of times to repeat the data.
     */
    public RepeatInputStream(byte[] bytes, long times)
    {
        this.bytes = bytes;
        this.times = times;
        remainder = times;
        buf = ByteBuffer.wrap(bytes);
    }

    private boolean isDone() {
        return remainder == 0 && !buf.hasRemaining();
    }

    private void checkBuf() {
        if (!isDone() && !buf.hasRemaining()) {
            remainder--;
            buf.clear();
        }
    }

    @Override
    public int read() throws IOException
    {
        if (isDone())
        {
            return -1;
        }

        int octet = buf.get() & 0xFF;
        checkBuf();
        return octet;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException
    {
        if (isDone())
        {
            return -1;
        }

        int rem = len - off;
        int consumed = 0;
        while (rem > 0 && !isDone())
        {
            int amount = Math.min(rem, buf.remaining());
            buf.get(b, off, amount);
            off += amount;
            rem -= amount;
            consumed += amount;
            checkBuf();
        }
        return consumed;
    }
}