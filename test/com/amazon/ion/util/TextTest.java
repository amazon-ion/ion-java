// Copyright (c) 2007-2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.util;

import com.amazon.ion.IonTestCase;

/**
 *
 */
public class TextTest
    extends IonTestCase
{

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
