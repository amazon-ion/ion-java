/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;

import java.io.IOException;
import java.io.InputStream;



public class BlobTest
    extends IonTestCase
{
    public void checkNullBlob(IonBlob value)
        throws IOException
    {
        assertSame(IonType.BLOB, value.getType());
        assertTrue(value.isNullValue());
        assertNull(value.newInputStream());
        assertNull(value.newBytes());

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
            value.appendBase64(buf);
            fail("expected NullValueException");
        }
        catch (NullValueException e) { /* ok */ }
        assertEquals(0, buf.length());
    }



    public void checkBlob(int[] expectedBytes, IonBlob value)
        throws NullValueException, IOException
    {
        assertSame(IonType.BLOB, value.getType());

        byte[] bytes = value.newBytes();
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

        byte[] bytes = value.newBytes();
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
        value.appendBase64(buf);
        assertEquals(base64, buf.toString());
    }


    public void modifyBlob(IonBlob value)
        throws Exception
    {
        byte[] bytes = new byte[]{ 1, 2, 3, 4, 5 };
        value.setBytes(bytes);
        assertFalse(value.isNullValue());
        checkBlob(new int[]{ 1, 2, 3, 4, 5 }, value);

        value.setBytes(null);
        checkNullBlob(value);

        // TODO empty blob
    }


    //=========================================================================
    // Test cases

    public void testFactoryBlob()
        throws Exception
    {
        IonBlob value = system().newNullBlob();
        checkNullBlob(value);
        modifyBlob(value);
    }

    public void testTextNullBlob()
        throws Exception
    {
        IonBlob value = (IonBlob) oneValue("null.blob");
        checkNullBlob(value);
        assertNull(value.getTypeAnnotations());
        modifyBlob(value);
        assertNull(value.getTypeAnnotations());

        value = (IonBlob) oneValue("a::null.blob");
        checkNullBlob(value);
        checkAnnotation("a", value);
        modifyBlob(value);
        checkAnnotation("a", value);
    }

    public void testEmptyBlob()
        throws IOException
    {
        IonBlob value = (IonBlob) oneValue("{{ }}");
        assertFalse(value.isNullValue());

        InputStream in = value.newInputStream();
        assertEquals("should be at EOF", -1, in.read());

        byte[] bytes = value.newBytes();
        assertEquals(0, bytes.length);

        StringBuilder buf = new StringBuilder();
        value.appendBase64(buf);
        assertEquals(0, buf.length());
    }

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
    };


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


    public void testBlobWithSlashes()
        throws IOException
    {
        oneValue("{{ //79/PsAAQIDBAU= }}");
        oneValue("{{ /PsAAQID//79BAU= }}");
    }

    public void testBlobWithTooMuchPadding()
    {
        badValue("{{ zg=== }}");
        badValue("{{ nonsense= }}");
    }

    public void testBlobWithTooLittlePadding()
    {
        badValue("{{ Zg  }}");
        badValue("{{ Zg= }}");
        badValue("{{ Zm8 }}");
    }
}
