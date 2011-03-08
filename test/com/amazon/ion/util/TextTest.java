// Copyright (c) 2007-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.util;

import com.amazon.ion.IonTestCase;
import org.junit.Test;

/**
 *
 */
public class TextTest
    extends IonTestCase
{
    @Test
    public void testSymbolNeedsQuoting()
    {
        unquotedAnywhere("hello");
        unquotedAnywhere("$hello");
        unquotedAnywhere("$123");
        unquotedAnywhere("$$123");

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


    @SuppressWarnings("deprecation")
    private void unquotedAnywhere(String symbol)
    {
        assertEquals(IonTextUtils.SymbolVariant.IDENTIFIER,
                     IonTextUtils.symbolVariant(symbol));

        // unquoted in sexp
        assertFalse(Text.symbolNeedsQuoting(symbol, false));
        // unquoted elsewhere
        assertFalse(Text.symbolNeedsQuoting(symbol, true));
    }

    @SuppressWarnings("deprecation")
    private void quotedEverywhere(String symbol)
    {
        assertEquals(IonTextUtils.SymbolVariant.QUOTED,
                     IonTextUtils.symbolVariant(symbol));

        // Quoted in sexp
        assertTrue(Text.symbolNeedsQuoting(symbol, false));
        // Quoted elsewhere
        assertTrue(Text.symbolNeedsQuoting(symbol, true));
    }

    @SuppressWarnings("deprecation")
    private void unquotedInSexp(String symbol)
    {
        assertEquals(IonTextUtils.SymbolVariant.OPERATOR,
                     IonTextUtils.symbolVariant(symbol));

        // unquoted in sexp
        assertFalse(Text.symbolNeedsQuoting(symbol, false));
        // quoted elsewheres
        assertTrue(Text.symbolNeedsQuoting(symbol, true));
    }
}
