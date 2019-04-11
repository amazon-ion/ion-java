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

package com.amazon.ion.impl;

import org.junit.Test;
import com.amazon.ion.IonException;
import com.amazon.ion.IonTestCase;
import com.amazon.ion.system.SimpleCatalog;

import java.io.ByteArrayInputStream;
import java.math.BigInteger;

public class VarIntTest extends IonTestCase {

    @Test
    public void readMaxVarUInt() throws Exception {
        assertEquals(Integer.MAX_VALUE, makeReader("077F7F7FFF").readVarUInt());
    }

    @Test(expected = IonException.class)
    public void overflowVarUInt() throws Exception {
        makeReader("0800000080").readVarUInt(); // Integer.MAX_VALUE + 1
    }

    @Test(expected = IonException.class)
    public void readEOFVarUInt() throws Exception {
        makeReader("").readVarUInt();
    }

    @Test
    public void readMaxVarUIntOrEOF() throws Exception {
        assertEquals(Integer.MAX_VALUE, makeReader("077F7F7FFF").readVarUIntOrEOF());
    }

    @Test(expected = IonException.class)
    public void overflowVarUIntOrEOF() throws Exception {
        makeReader("0800000080").readVarUIntOrEOF(); // Integer.MAX_VALUE + 1
    }

    @Test
    public void readEOFVarUIntOrEOF() throws Exception {
        assertEquals(UnifiedInputStreamX.EOF, makeReader("").readVarUIntOrEOF());
    }

    @Test
    public void readMaxVarInt() throws Exception {
        assertEquals(Integer.MAX_VALUE, makeReader("077F7F7FFF").readVarInt());
    }

    @Test
    public void readMinVarInt() throws Exception {
        assertEquals(Integer.MIN_VALUE, makeReader("4800000080").readVarInt());
    }

    @Test(expected = IonException.class)
    public void readVarIntOverflow() throws Exception {
        makeReader("0800000080").readVarInt(); // Integer.MAX_VALUE + 1
    }

    @Test(expected = IonException.class)
    public void readVarIntUnderflow() throws Exception {
        makeReader("4800000081").readVarInt(); // Integer.MIN_VALUE - 1
    }

    @Test
    public void readMaxVarInteger() throws Exception {
        assertEquals(Integer.MAX_VALUE, (int) makeReader("077F7F7FFF").readVarInteger());
    }

    @Test
    public void readMinVarInteger() throws Exception {
        assertEquals(Integer.MIN_VALUE, (int) makeReader("4800000080").readVarInteger());
    }

    @Test(expected = IonException.class)
    public void readVarIntegerOverflow() throws Exception {
        makeReader("0800000080").readVarInteger(); // Integer.MAX_VALUE + 1
    }

    @Test(expected = IonException.class)
    public void readVarIntegerUnderflow() throws Exception {
        makeReader("4800000081").readVarInt(); // Integer.MIN_VALUE - 1
    }

    @Test
    public void readVarIntegerNegativeZero() throws Exception {
        assertNull(makeReader("C0").readVarInteger());
    }

    private IonReaderBinaryUserX makeReader(String hex) throws Exception {
        ByteArrayInputStream input = new ByteArrayInputStream(parseHexBinary("E00100EA" + hex));
        UnifiedInputStreamX uis = UnifiedInputStreamX.makeStream(input);
        uis.skip(4);

        return new IonReaderBinaryUserX(new SimpleCatalog(), LocalSymbolTable.DEFAULT_LST_FACTORY, uis, 0);
    }

    private static byte[] parseHexBinary(String str) {
        int len = str.length();
        if (len % 2 != 0) {
            throw new IllegalArgumentException("str must have even length");
        }

        byte[] result = new byte[len / 2];

        for (int i = 0; i < len; i += 2) {
            int high = parseHexChar(str.charAt(i));
            int low  = parseHexChar(str.charAt(i + 1));
            result[i / 2] = (byte) ((high << 4) | low);
        }

        return result;
    }

    private static int parseHexChar(char c) {
        if (c >= '0' && c <= '9') {
            return c - '0';
        }
        if (c >= 'a' && c <= 'f') {
            return (c - 'a') + 10;
        }
        if (c >= 'A' && c <= 'F') {
            return (c - 'A') + 10;
        }
        throw new IllegalArgumentException("invalid hex character: " + c);
    }
}
