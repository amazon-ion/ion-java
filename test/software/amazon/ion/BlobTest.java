/*
 * Copyright 2007-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.ion;

import static software.amazon.ion.TestUtils.US_ASCII_CHARSET;
import static software.amazon.ion.impl.PrivateUtils.encode;

import java.io.IOException;
import java.io.InputStream;
import org.junit.Test;
import software.amazon.ion.IonBlob;
import software.amazon.ion.IonType;
import software.amazon.ion.NullValueException;
import software.amazon.ion.UnexpectedEofException;
import software.amazon.ion.impl.PrivateUtils;


public class BlobTest
    extends IonTestCase
{
    public void checkNullBlob(IonBlob value)
        throws IOException
    {
        assertSame(IonType.BLOB, value.getType());
        assertTrue(value.isNullValue());
        assertNull(value.newInputStream());
        assertNull(value.getBytes());

        try
        {
            value.byteSize();
            fail("expected NullValueException");
        }
        catch (NullValueException e)
        {
        }

        StringBuilder buf = new StringBuilder();
        try
        {
            value.printBase64(buf);
            fail("expected NullValueException");
        }
        catch (NullValueException e) { /* ok */ }
        assertEquals(0, buf.length());
    }



    public void checkBlob(int[] expectedBytes, IonBlob value)
        throws NullValueException, IOException
    {
        assertSame(IonType.BLOB, value.getType());

        byte[] bytes = value.getBytes();
        assertNotSame(expectedBytes, bytes);
        assertEquals(expectedBytes.length, bytes.length);

        InputStream in = value.newInputStream();
        for (int i = 0; i < expectedBytes.length; i++)
        {
            String msg = "index " + i;
            assertEquals(msg, (byte) expectedBytes[i], bytes[i]);
            assertEquals(msg, expectedBytes[i], in.read());
        }
        assertEquals("should be at EOF", -1, in.read());
        assertEquals("should be at EOF", -1, in.read());
        assertEquals("should be at EOF", -1, in.read());
    }

    public void checkBlob(byte[] expectedBytes, IonBlob value)
        throws NullValueException, IOException
    {
        assertSame(IonType.BLOB, value.getType());

        byte[] bytes = value.getBytes();
        assertEquals(expectedBytes.length, bytes.length);

        InputStream in = value.newInputStream();
        for (int i = 0; i < expectedBytes.length; i++)
        {
            String msg = "byte " + i;
            assertEquals(msg, expectedBytes[i], bytes[i]);
            assertEquals(msg, expectedBytes[i], (byte) in.read());
        }
        assertEquals("should be at EOF", -1, in.read());
        assertEquals("should be at EOF", -1, in.read());
        assertEquals("should be at EOF", -1, in.read());
    }


    /**
     * @param expectedBytes
     * @param base64 is the content of a blob
     * @throws NullValueException
     * @throws IOException
     */
    public void checkBlob(byte[] expectedBytes, String base64)
        throws NullValueException, IOException
    {
        IonBlob value = (IonBlob) oneValue("{{" + base64 + "}}");
        checkBlob(expectedBytes, value);

        StringBuilder buf = new StringBuilder();
        value.printBase64(buf);
        assertEquals(base64, buf.toString());
    }


    public void modifyBlob(IonBlob value)
        throws Exception
    {
        byte[] bytes = new byte[]{ 1, 2, 3, 4, 5 };
        value.setBytes(bytes);
        assertFalse(value.isNullValue());
        checkBlob(bytes, value);

        value.setBytes(null);
        checkNullBlob(value);

        // TODO empty blob
    }


    //=========================================================================
    // Test cases

    @Test
    public void testFactoryBlob()
        throws Exception
    {
        IonBlob value = system().newNullBlob();
        checkNullBlob(value);
        modifyBlob(value);
    }

    @Test
    public void testTextNullBlob()
        throws Exception
    {
        IonBlob value = (IonBlob) oneValue("null.blob");
        checkNullBlob(value);
        assertArrayEquals(new String[0], value.getTypeAnnotations());
        modifyBlob(value);
        assertArrayEquals(new String[0], value.getTypeAnnotations());

        value = (IonBlob) oneValue("a::null.blob");
        checkNullBlob(value);
        checkAnnotation("a", value);
        modifyBlob(value);
        checkAnnotation("a", value);
    }

    @Test
    public void testEmptyBlob()
        throws IOException
    {
        IonBlob value = (IonBlob) oneValue("{{ }}");
        assertFalse(value.isNullValue());

        InputStream in = value.newInputStream();
        assertEquals("should be at EOF", -1, in.read());

        byte[] bytes = value.getBytes();
        assertEquals(0, bytes.length);

        StringBuilder buf = new StringBuilder();
        value.printBase64(buf);
        assertEquals(0, buf.length());
    }

    @Test
    public void testByteSize()
    {
        for (int i = 0; i < TEST_DATA.length; i++)
        {
            IonBlob value = system().newNullBlob();
            byte[] testBytes = TEST_DATA[i].bytes;
            value.setBytes(testBytes);
            assertEquals("unexpected byte size", testBytes.length,
                         value.byteSize());
        }
    }


    public static final class TestData
    {
        public TestData(byte[] bytes, String base64)
        {
            this.bytes  = bytes;
            this.base64 = base64;
        }

        public TestData(int[] bytes, String base64)
        {
            this.bytes = new byte[bytes.length];
            for (int i = 0; i < bytes.length; i++)
            {
                this.bytes[i] = (byte) bytes[i];
            }
            this.base64 = base64;
        }

        public byte[] bytes;
        public String base64;
    }

    private static byte[] EncodeAscii(String ascii)
    {
        return PrivateUtils.encode(ascii, US_ASCII_CHARSET);
    }


    /**
     * Test cases, mostly from RFC 4648.
     *
     * @see <a href="http://tools.ietf.org/html/rfc4648">RFC 4648</a>
     */
    public static final TestData[] TEST_DATA =
    {
        // Empty blob
        new TestData(new byte[]{ }, ""),

        // Test both sides of the 13-byte threshold.
        new TestData(new int[]{ 0x14, 0xfb, 0x9c, 0x03, 0xd9, 0x7e,
                                0x14, 0xfb, 0x9c, 0x03, 0xd9, 0x7e,
                                'f', },
                     "FPucA9l+FPucA9l+Zg=="),
        new TestData(new int[]{ 0x14, 0xfb, 0x9c, 0x03, 0xd9, 0x7e,
                                0x14, 0xfb, 0x9c, 0x03, 0xd9, 0x7e,
                                'f', 'o' },
                     "FPucA9l+FPucA9l+Zm8="),

        new TestData(new byte[]{ 'f' },
                     "Zg=="),
        new TestData(new byte[]{ 'f', 'o' },
                     "Zm8="),
        new TestData(new byte[]{ 'f', 'o', 'o' },
                     "Zm9v"),
        new TestData(new byte[]{ 'f', 'o', 'o', 'b' },
                     "Zm9vYg=="),
        new TestData(new byte[]{ 'f', 'o', 'o', 'b', 'a' },
                     "Zm9vYmE="),
        new TestData(new byte[]{ 'f', 'o', 'o', 'b', 'a', 'r' },
                     "Zm9vYmFy"),
        new TestData(new int[]{ 0x14, 0xfb, 0x9c, 0x03, 0xd9, 0x7e },
                     "FPucA9l+"),
        new TestData(new int[]{ 0x14, 0xfb, 0x9c, 0x03, 0xd9 },
                     "FPucA9k="),
        new TestData(new int[]{ 0x14, 0xfb, 0x9c, 0x03 },
                     "FPucAw=="),
        // Test a blob size that exceeds a single base 64 encoding buffer
        new TestData(EncodeAscii(
                     "We the People of the United States, in Order to form a more perfect Union, " +
                     "establish Justice, insure domestic Tranquility, provide for the common defence, " +
                     "promote the general Welfare, and secure the Blessings of Liberty to ourselves " +
                     "and our Posterity, do ordain and establish this Constitution for the United " +
                     "States of America.\nArticle I\n\nSection 1. All legislative Powers herein " +
                     "granted shall be vested in a Congress of the United States, which shall consist " +
                     "of a Senate and House of Representatives.\n"),
                     "V2UgdGhlIFBlb3BsZSBvZiB0aGUgVW5pdGVkIFN0YXRlcywgaW4gT3JkZXIgdG8g" +
                     "Zm9ybSBhIG1vcmUgcGVyZmVjdCBVbmlvbiwgZXN0YWJsaXNoIEp1c3RpY2UsIGlu" +
                     "c3VyZSBkb21lc3RpYyBUcmFucXVpbGl0eSwgcHJvdmlkZSBmb3IgdGhlIGNvbW1v" +
                     "biBkZWZlbmNlLCBwcm9tb3RlIHRoZSBnZW5lcmFsIFdlbGZhcmUsIGFuZCBzZWN1" +
                     "cmUgdGhlIEJsZXNzaW5ncyBvZiBMaWJlcnR5IHRvIG91cnNlbHZlcyBhbmQgb3Vy" +
                     "IFBvc3Rlcml0eSwgZG8gb3JkYWluIGFuZCBlc3RhYmxpc2ggdGhpcyBDb25zdGl0" +
                     "dXRpb24gZm9yIHRoZSBVbml0ZWQgU3RhdGVzIG9mIEFtZXJpY2EuCkFydGljbGUg" +
                     "SQoKU2VjdGlvbiAxLiBBbGwgbGVnaXNsYXRpdmUgUG93ZXJzIGhlcmVpbiBncmFu" +
                     "dGVkIHNoYWxsIGJlIHZlc3RlZCBpbiBhIENvbmdyZXNzIG9mIHRoZSBVbml0ZWQg" +
                     "U3RhdGVzLCB3aGljaCBzaGFsbCBjb25zaXN0IG9mIGEgU2VuYXRlIGFuZCBIb3Vz" +
                     "ZSBvZiBSZXByZXNlbnRhdGl2ZXMuCg==")
    };


    @Test
    public void testBlobData()
        throws IOException
    {
        for (int i = 0; i < TEST_DATA.length; i++)
        {
            TestData td = TEST_DATA[i];
//            IonBlob value = (IonBlob) oneValue("{{ " + td.base64 + "}}");
//            checkBlob(td.bytes, value);
            checkBlob(td.bytes, td.base64);
        }
    }


    @Test
    public void testUnterminatedBlob()
        throws IOException
    {
        try {
            oneValue("{{ FPuc");
            fail("Expected UnexpectedEofException");
        }
        catch (UnexpectedEofException e) { }

        try {
            oneValue("{{ FPuc }");
            fail("Expected UnexpectedEofException");
        }
        catch (UnexpectedEofException e) { }
    }


    @Test
    public void testBlobWithSlashes()
        throws IOException
    {
        oneValue("{{ //79/PsAAQIDBAU= }}");
        oneValue("{{ /PsAAQID//79BAU= }}");
    }

    @Test
    public void testBlobWithTooMuchPadding()
    {
        badValue("{{ zg=== }}");
        badValue("{{ nonsense= }}");
    }

    @Test
    public void testBlobWithTooLittlePadding()
    {
        badValue("{{ Zg  }}");
        badValue("{{ Zg= }}");
        badValue("{{ Zm8 }}");
    }
}
