/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates.  All rights reserved.
 */

package software.amazon.ion.impl;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

public class UnifiedInputStreamXTest extends Assert {
    @Test
    public void testReadExactlyAvailable() throws Exception {
        class TestInputStream extends InputStream {
            int remaining = 10;

            @Override
            public int read() throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                if (remaining == 0) {
                    throw new AssertionError("Unexpected call to read");
                }
                int amount = len;
                if (amount > remaining) {
                    amount = remaining;
                }

                for (int i = off; i < (off + amount); i++) {
                    b[i] = (byte) 0xFF;
                }
                remaining -= amount;
                return amount;
            }
        }

        TestInputStream is = new TestInputStream();
        UnifiedInputStreamX uix = UnifiedInputStreamX.makeStream(is);

        byte[] expected = new byte[10];
        Arrays.fill(expected, (byte) 0xFF);
        byte[] actual = new byte[10];
        int read = uix.read(actual, 0, actual.length);

        assertArrayEquals(expected, actual);
    }
}
