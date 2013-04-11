// Copyright (c) 2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonException;
import com.amazon.ion.IonTestCase;
import java.io.ByteArrayOutputStream;
import java.util.Random;
import org.junit.Test;

/**
 *
 */
public class UTF8ConverterTest extends IonTestCase
{
    private Random rand = new Random();

    private void assertConversion(String str, byte[] out) throws Exception {
        UTF8Converter conv = new UTF8Converter();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        conv.write(baos, str);
        byte[] bytes = baos.toByteArray();
        assertEqualBytes(bytes, 0, bytes.length, out);
    }

    @Test
    public void testLatin1Conversion() throws Exception {
        assertConversion("", new byte[] {} );
        assertConversion("abcd1234", new byte[] { 97, 98, 99, 100, 49, 50, 51, 52 });

        StringBuffer sb = new StringBuffer();
        byte[] test = new byte[2000];
        for (int i = 0; i < 2000; ++i) {
            sb.append((char)(i % 128));
            test[i] = (byte)(i % 128);
        }
        assertConversion(sb.toString(), test);

        byte[] partial = new byte[1234];
        System.arraycopy(test, 123, partial, 0, 1234);
        assertConversion(sb.substring(123, 1234), partial);

        assertConversion("հայ Ḁℐ", new byte[] { (byte)0xD5, (byte)0xB0, (byte)0xD5, (byte)0xA1, (byte)0xD5, (byte)0xB5, (byte)0x20,
                                                (byte)0xE1, (byte)0xB8, (byte)0x80, (byte)0xE2, (byte)0x84, (byte)0x90 });
    }

    @Test
    public void testSurrogates() throws Exception {
        assertConversion(new String(new char[] { 0xD834, 0xDD1E} ), new byte[] { (byte)0xF0, (byte)0x9D, (byte)0x84, (byte)0x9E });
    }

    @Test
    public void testSurrogatesOnBufferEdge() throws Exception {
        // test surrogate in between the 1024 byte char buffer
        StringBuffer sb = new StringBuffer();
        byte[] test = new byte[1300];
        // fill with latin-1 data
        for (int i = 0; i < 1023; ++i) {
            sb.append((char)(i % 128));
            test[i] = (byte)(i % 128);
        }
        // add surrogate on the edge
        sb.append((char)0xD834);
        sb.append((char)0xDD1E);
        test[1023] = (byte)0xF0;
        test[1024] = (byte)0x9D;
        test[1025] = (byte)0x84;
        test[1026] = (byte)0x9E;
        // append more latin-1 data
        for (int i = 1027; i < 1200; ++i) {
            sb.append((char)(i % 128));
            test[i] = (byte)(i % 128);
        }
        assertConversion(sb.toString(), test);
    }

    @Test(expected = IonException.class)
    public void testSurrogatesFailure() throws Exception {
        new UTF8Converter().write(new ByteArrayOutputStream(), new String(new char[] { 0xDD1E} ));
    }
}
