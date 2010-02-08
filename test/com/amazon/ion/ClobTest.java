/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;



public class ClobTest
    extends IonTestCase
{
    private static final Charset UTF8 = Charset.forName("UTF-8");


    public static final String SAMPLE_ASCII = "Wow!";
    public static final byte[] SAMPLE_ASCII_AS_UTF8 =
        stringToBytes(SAMPLE_ASCII, UTF8);


    public static byte[] stringToBytes(String string, Charset charset)
    {
        ByteBuffer buffer = charset.encode(string);
        byte[] bytes = new byte[buffer.limit()];
        buffer.get(bytes);
        return bytes;
    }

    public void checkNullClob(IonClob value)
    {
        assertSame(IonType.CLOB, value.getType());
        assertTrue(value.isNullValue());
        assertNull(value.newInputStream());
        assertNull(value.newReader(UTF8));
        assertNull(value.stringValue(UTF8));

        try
        {
            value.byteSize();
            fail("expected NullValueException");
        }
        catch(NullValueException e)
        {
        }
    }


    /**
     * @param expectedString
     * @param text is ion text for a blob.
     * @throws NullValueException
     * @throws IOException
     */
    private void checkUtf8(String expectedString, String text)
        throws NullValueException, IOException
    {
        IonClob value = (IonClob) oneValue(text);
        checkUtf8(expectedString, value);
    }

    private void checkUtf8(String expectedString, IonClob value)
        throws NullValueException, IOException
    {
        assertSame(IonType.CLOB, value.getType());
        assertFalse(value.isNullValue());

        // TODO byte-based access to clobs
//        byte[] bytes = value.newBytes();
//        assertEquals(expectedBytes.length(), bytes.length);

        InputStream in = value.newInputStream();
        Reader rd = value.newReader(UTF8);

        for (int i = 0; i < expectedString.length(); i++)
        {
            char c = expectedString.charAt(i);
            String msg = "index " + i;

            assertEquals(msg, c, (char) in.read());
            assertEquals(msg, c, (char) rd.read());
        }

        // Make sure streams stay at EOF
        assertEquals("should be at EOF", -1, in.read());
        assertEquals("should be at EOF", -1, in.read());
        assertEquals("should be at EOF", -1, in.read());

        assertEquals("should be at EOF", -1, rd.read());
        assertEquals("should be at EOF", -1, rd.read());
        assertEquals("should be at EOF", -1, rd.read());

        String stringValue = value.stringValue(UTF8);
        assertEquals(expectedString, stringValue);
    }


    public void modifyClob(IonClob value)
        throws Exception
    {
        value.setBytes(SAMPLE_ASCII_AS_UTF8);
        checkUtf8(SAMPLE_ASCII, value);

        value.setBytes(null);
        checkNullClob(value);

        value.setBytes(new byte[]{});
        checkUtf8("", value);
    }


    //=========================================================================
    // Test cases

    public void testFactoryClob()
        throws Exception
    {
        IonClob value = system().newNullClob();
        checkNullClob(value);
        modifyClob(value);

        value = system().newClob(null);
        checkNullClob(value);
        modifyClob(value);
    }


    public void testNullClob()
        throws Exception
    {
        IonClob value = (IonClob) oneValue("null.clob");
        checkNullClob(value);
        assertArrayEquals(new String[0], value.getTypeAnnotations());

        value = (IonClob) oneValue("a::null.clob");
        checkNullClob(value);
        checkAnnotation("a", value);
    }

    public void testEmptyClob()
        throws IOException
    {
        IonClob value = (IonClob) oneValue("{{\"\"}}");
        assertFalse(value.isNullValue());
        checkUtf8("", value);

        // There was once a problem with whitespace between {{ "
        value = (IonClob) oneValue("{{ \"\" }}");
        assertFalse(value.isNullValue());
        checkUtf8("", value);
    }

    public void testEmptyClobLongString()
        throws Exception
    {
        IonClob value = (IonClob) oneValue("{{ '''''' }}");
        assertFalse(value.isNullValue());
        checkUtf8("", value);
    }

    public void testConcatenatedClob()
        throws Exception
    {
        IonClob value = (IonClob) oneValue("{{ '''a''' \n '''b'''}}");
        assertFalse(value.isNullValue());
        checkUtf8("ab", value);
    }

    public void testClobData()
        throws Exception
    {
        checkUtf8("abc", "{{\"abc\"}}");
    }

    public void testByteSize()
    {
        IonClob value = system().newNullClob();
        value.setBytes(SAMPLE_ASCII_AS_UTF8);
        assertEquals("unexpected byte size", value.byteSize(),
                     SAMPLE_ASCII_AS_UTF8.length);

        value = system().newClob(SAMPLE_ASCII_AS_UTF8);
        assertEquals("unexpected byte size", value.byteSize(),
                     SAMPLE_ASCII_AS_UTF8.length);
    }

    public void testHighUnicodeEscapes()
    {
        badValue("{{\"\\U0000003f\"}}");
    }

    public void testParseTripleQuoteLoader() {
        IonValue c = oneValue("{aws_id:a, aws_key:b, aes_key:{{'''c'''}}, bucket:d, bucket_suffix:e, flush_timeout:1}");
    }

    // TODO  test use of other encodings
    // TODO test long strings and concatenation
}
