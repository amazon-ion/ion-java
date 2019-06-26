/*
 * Copyright 2007-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
