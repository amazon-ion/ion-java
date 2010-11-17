// Copyright (c) 2008-2010 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import static com.amazon.ion.impl.IonImplUtils.READER_HASNEXT_REMOVED;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.Iterator;
import junit.framework.Assert;

/**
 *
 */
public class TestUtils
{

    /**
     * Reads everything until the end of the current container, traversing
     * down nested containers.
     *
     * @param reader
     */
    public static void deepRead(IonReader reader)
    {
        deepRead( reader, true );
    }
    public static void deepRead(IonReader reader, boolean flgMaterializeScalars)
    {
        IonType t = null;
        while ((t = doNext(reader)) != null )
        {
            switch (t)
            {
                case NULL:
                case BOOL:
                case INT:
                case FLOAT:
                case DECIMAL:
                case TIMESTAMP:
                case STRING:
                case SYMBOL:
                case BLOB:
                case CLOB:
                    if ( flgMaterializeScalars )
                        materializeScalar(reader);
                    break;

                case STRUCT:
                case LIST:
                case SEXP:
                    reader.stepIn();
                    deepRead(reader);
                    reader.stepOut();
                    break;

                default:
                    Assert.fail("unexpected type: " + t);
            }
        }
    }

    private static IonType doNext(IonReader reader)
    {
        boolean hasnext = true;
        IonType t = null;

        if (READER_HASNEXT_REMOVED == false) {
            hasnext = reader.hasNext();
        }
        if (hasnext) {
            t = reader.next();
        }
        return t;
    }


    private static void materializeScalar(IonReader reader)
    {
        IonType t = reader.getType();

        if (t == null) {
            return;
        }
        if (reader.isNullValue()) {
            return;
        }

        switch (t)
        {
            case NULL:
                // we really shouldn't get here, but it's not really an issue
                reader.isNullValue();
                break;
            case BOOL:
                boolean b = reader.booleanValue();
                break;
            case INT:
                long l = reader.longValue();
                break;
            case FLOAT:
                double f = reader.doubleValue();
                break;
            case DECIMAL:
                BigDecimal bd = reader.bigDecimalValue();
                break;
            case TIMESTAMP:
                Timestamp time = reader.timestampValue();
                break;
            case STRING:
                String s = reader.stringValue();
                break;
            case SYMBOL:
                int sid = reader.getSymbolId();
                String sy = reader.stringValue();
                break;
            case BLOB:
            case CLOB:
                // getting the size is close enough since the
                // reader has to evaluate the contents to know
                // the length the user needs for new.
                int bs = reader.byteSize();
                break;

            case STRUCT:
            case LIST:
            case SEXP:
                break;

            default:
                Assert.fail("unexpected type: " + t);
        }
    }


    public static void assertEqualValues(Iterator<?> expectedValues,
                                         Iterator<?> actualValues)
    {
        Object expected;
        Object actual;
        while (expectedValues.hasNext())
        {
            expected = expectedValues.next();
            actual = actualValues.next();
            Assert.assertEquals(expected, actual);
        }
        Assert.assertFalse("unexpected next value", actualValues.hasNext());
    }


    public static String hexDump(final String str)
    {
        try {
            final byte[] utf16Bytes = str.getBytes("UTF-16BE");
            StringBuilder buf = new StringBuilder(utf16Bytes.length * 4);
            for (byte b : utf16Bytes) {
                buf.append(Integer.toString(0x00FF & b, 16));
                buf.append(' ');
            }
            return buf.toString();
        }
        catch (final UnsupportedEncodingException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * U+00A5 YEN SIGN
     * UTF-8 (hex)      0xC2 0xA5 (c2a5)
     * UTF-8 (binary)  11000010:10100101
     * UTF-16 (hex)    0x00A5 (00a5)
     * UTF-32 (hex)    0x000000A5 (00a5)
     */
    public static final String YEN_SIGN = "\u00a5";

    /**
     * U+1D110 MUSICAL SYMBOL FERMATA
     * <pre>
     * UTF-8 (hex)     0xF0 0x9D 0x84 0x90 (f09d8490)
     * UTF-8 (binary)  11110000:10011101:10000100:10010000
     * UTF-16 (hex)    0xD834 0xDD10 (d834dd10)
     * UTF-32 (hex)    0x0001D110 (1d110)
     * </pre>
     */
    public static final String FERMATA = "\ud834\udd10";

    public static final byte[] FERMATA_UTF8 =
    {
        (byte) 0xF0, (byte) 0x9D, (byte) 0x84, (byte) 0x90
    };

    static
    {
        try
        {
            Assert.assertEquals(FERMATA, new String(FERMATA_UTF8, "UTF-8"));
        }
        catch (UnsupportedEncodingException e)
        {
            throw new RuntimeException(e);
        }
    }
}
