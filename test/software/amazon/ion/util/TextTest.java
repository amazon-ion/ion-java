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

package software.amazon.ion.util;

import java.math.BigDecimal;
import org.junit.Test;
import software.amazon.ion.BlobTest;
import software.amazon.ion.Decimal;
import software.amazon.ion.IonTestCase;
import software.amazon.ion.BlobTest.TestData;
import software.amazon.ion.impl.PrivateIonTextAppender;
import software.amazon.ion.util.IonTextUtils;

public class TextTest
    extends IonTestCase
{
    @Test
    public void testSymbolNeedsQuoting()
    {
        unquotedAnywhere("hello");
        unquotedAnywhere("$hello");
        unquotedAnywhere("$$123");
        unquotedAnywhere("$1234d678");
        unquotedAnywhere("$");

        quotedEverywhere("$0");
        quotedEverywhere("$00000");
        quotedEverywhere("$123");
        quotedEverywhere("$1234567890");

        quotedEverywhere("hi there");
        quotedEverywhere("'hi there'");
        quotedEverywhere("\"hi there\"");
        quotedEverywhere("123");
        quotedEverywhere("hi!");
        quotedEverywhere("hi:");

        // Keywords
        quotedEverywhere("true");
        quotedEverywhere("false");
        quotedEverywhere("null");
        quotedEverywhere("null.int");

        // Operators
        unquotedInSexp("!");
        unquotedInSexp("<");
        unquotedInSexp("<===");

        quotedEverywhere("<abc");
        quotedEverywhere("<abc>");
        quotedEverywhere("abc>");
        quotedEverywhere("< ");
        quotedEverywhere("<12");
        quotedEverywhere("<{");
        quotedEverywhere("{");
        quotedEverywhere("}");
        quotedEverywhere("[");
        quotedEverywhere("]");
        quotedEverywhere(",");
        quotedEverywhere("'");
        quotedEverywhere("\"");
        quotedEverywhere(":");
        quotedEverywhere("::");
        quotedEverywhere(":a");
    }

    @Test
    public void testPrintLongString()
        throws Exception
    {
        final String LQ = "'''";

        checkLongString("null.string", null);
        checkLongString(LQ + LQ, "");
        checkLongString(LQ + "a" + LQ, "a");
        checkLongString(LQ + "a\n" + LQ, "a\n");

        // Tricky escapes
        checkLongString(LQ + "\\0\\a\n" + LQ, "\u0000\u0007\n");
        checkLongString(LQ + "1\\r\n2" + LQ, "1\r\n2");

        // Now the big ones
        checkLongString(LQ + "\\'" + LQ, "'");
        checkLongString(LQ + "\\'\\'" + LQ, "''");
        checkLongString(LQ + "\\'\\'\\'" + LQ, "'''");

        // TODO minimize escaping of single-quotes
//        checkLongString(LQ + "a'b" + LQ, "a'b");
    }

    private void checkLongString(String expected, String value)
        throws Exception
    {
        String rendered = IonTextUtils.printLongString(value);
        assertEquals(expected, rendered);
        checkString(value, oneValue(rendered));

        StringBuilder buf = new StringBuilder();
        IonTextUtils.printLongString(buf, value);
        rendered = buf.toString();
        assertEquals(expected, rendered);
        checkString(value, oneValue(rendered));
    }

    private void unquotedAnywhere(String symbol)
    {
        assertEquals(IonTextUtils.SymbolVariant.IDENTIFIER,
                     IonTextUtils.symbolVariant(symbol));

        // unquoted in sexp
        assertFalse(PrivateIonTextAppender.symbolNeedsQuoting(symbol, false));
        // unquoted elsewhere
        assertFalse(PrivateIonTextAppender.symbolNeedsQuoting(symbol, true));
    }

    private void quotedEverywhere(String symbol)
    {
        assertEquals(IonTextUtils.SymbolVariant.QUOTED,
                     IonTextUtils.symbolVariant(symbol));

        // Quoted in sexp
        assertTrue(PrivateIonTextAppender.symbolNeedsQuoting(symbol, false));
        // Quoted elsewhere
        assertTrue(PrivateIonTextAppender.symbolNeedsQuoting(symbol, true));
    }

    private void unquotedInSexp(String symbol)
    {
        assertEquals(IonTextUtils.SymbolVariant.OPERATOR,
                     IonTextUtils.symbolVariant(symbol));

        // unquoted in sexp
        assertFalse(PrivateIonTextAppender.symbolNeedsQuoting(symbol, false));
        // quoted elsewheres
        assertTrue(PrivateIonTextAppender.symbolNeedsQuoting(symbol, true));
    }



    private void checkDecimal(String expected, BigDecimal value)
        throws Exception
    {
        StringBuilder buf = new StringBuilder();
        IonTextUtils.printDecimal(buf, value);
        assertEquals(expected, buf.toString());

        assertEquals(expected, IonTextUtils.printDecimal(value));
    }

    @Test
    public void testPrintDecimal()
        throws Exception
    {
        checkDecimal("null.decimal", null);
        checkDecimal("-0.", Decimal.NEGATIVE_ZERO);
        checkDecimal("0.",  Decimal.ZERO);
        checkDecimal("1.",  Decimal.ONE);
    }



    private void checkFloat(String expected, Double value)
        throws Exception
    {
        StringBuilder buf = new StringBuilder();
        IonTextUtils.printFloat(buf, value);
        assertEquals(expected, buf.toString());

        assertEquals(expected, IonTextUtils.printFloat(value));

        if (value != null)
        {
            double d = value.doubleValue();

            buf.setLength(0);
            IonTextUtils.printFloat(buf, d);
            assertEquals(expected, buf.toString());

            assertEquals(expected, IonTextUtils.printFloat(d));
        }
    }

    @Test
    public void testPrintFloat()
        throws Exception
    {
        checkFloat("null.float", null);
        checkFloat("0e0", 0.0);
        checkFloat("1e0", 1.0);
    }





    private void checkBlob(String expected, byte[] value)
        throws Exception
    {
        StringBuilder buf = new StringBuilder();
        IonTextUtils.printBlob(buf, value);
        assertEquals(expected, buf.toString());

        assertEquals(expected, IonTextUtils.printBlob(value));
    }

    @Test
    public void testPrintBlob()
        throws Exception
    {
        checkBlob("null.blob", null);

        for (TestData data : BlobTest.TEST_DATA)
        {
            checkBlob("{{" + data.base64 + "}}", data.bytes);
        }
    }



    private void checkClob(String expected, byte[] value)
        throws Exception
    {
        StringBuilder buf = new StringBuilder();
        IonTextUtils.printClob(buf, value);
        assertEquals(expected, buf.toString());

        assertEquals(expected, IonTextUtils.printClob(value));
    }

    @Test
    public void testPrintClob()
        throws Exception
    {
        checkClob("null.clob", null);
        checkClob("{{\"\"}}", new byte[0]);
        checkClob("{{\"\\0a\\xff\"}}", new byte[]{ 0, 'a', (byte) 0xff });
    }
}
