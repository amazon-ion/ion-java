// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import static com.amazon.ion.IonTestCase.checkSymbol;
import static com.amazon.ion.SymbolTable.UNKNOWN_SYMBOL_ID;
import static com.amazon.ion.junit.IonAssert.assertSymbolEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 *
 */
public class ReaderChecker
    implements Checker
{
    private IonReader myReader;

    public ReaderChecker(IonReader reader)
    {
        myReader = reader;
    }


    public ReaderChecker next()
    {
        myReader.next();
        return this;
    }


    public ReaderChecker noFieldName()
    {
        assertEquals(false, myReader.isInStruct());

        assertEquals("IonReader.getFieldName()",
                     null, myReader.getFieldName());

        assertEquals("IonReader.getFieldId()",
                     UNKNOWN_SYMBOL_ID, myReader.getFieldId());

        assertEquals("IonReader.getFieldNameSymbol()",
                     null, myReader.getFieldNameSymbol());

        return this;
    }

    public ReaderChecker fieldName(String expectedText, int expectedSid)
    {
        String expectedStringValue =
            (expectedText == null ? "$" + expectedSid : expectedText);

        assertEquals("IonReader.getFieldName()",
                     expectedStringValue, myReader.getFieldName());

        assertEquals("IonReader.getFieldId()",
                     expectedSid, myReader.getFieldId());

        InternedSymbol sym = myReader.getFieldNameSymbol();
        checkSymbol(expectedText, expectedSid, sym);

        return this;
    }

    /**
     * @param name null means no name expected.
     */
    public ReaderChecker fieldName(String name)
    {
        InternedSymbol is = myReader.getFieldNameSymbol();

        assertEquals("field name", name, myReader.getFieldName());

        if (name == null)
        {
            assertEquals("Unexpected InternedSymbol", null, is);
        }
        else
        {
            assertEquals("field name InternedSymbol text",
                         name, is.getText());
        }
        // TODO check sid

        return this;
    }

    public ReaderChecker fieldName(InternedSymbol sym)
    {
        InternedSymbol is = myReader.getFieldNameSymbol();
        if (sym == null)
        {
            assertEquals("IonReader.getFieldName()",
                         null, myReader.getFieldName());
            assertEquals("IonReader.getFieldNameSymbol()",
                         null, is);
        }
        else
        {
            assertEquals("field name InternedSymbol text",
                         sym.getText(), is.getText());
        }
        // TODO check sid

        return this;
    }


    public ReaderChecker annotation(String expectedText, int expectedSid)
    {
        String expectedStringValue =
            (expectedText == null ? "$" + expectedSid : expectedText);

        String[] typeAnnotations = myReader.getTypeAnnotations();
        assertEquals("symbol text", expectedStringValue, typeAnnotations[0]);

        int[] sids = myReader.getTypeAnnotationIds();
        assertEquals("sid", expectedSid, sids[0]);

        InternedSymbol[] annSyms = myReader.getTypeAnnotationSymbols();
        IonTestCase.checkSymbol(expectedText, expectedSid, annSyms[0]);

        return this;
    }

    public ReaderChecker annotations(String[] expectedTexts, int[] expectedSids)
    {
        String[] typeAnnotations = myReader.getTypeAnnotations();
        assertArrayEquals(expectedTexts, typeAnnotations);

        int[] sids = myReader.getTypeAnnotationIds();
        assertArrayEquals(expectedSids, sids);

        return this;
    }

    public ReaderChecker annotations(InternedSymbol[] expecteds)
    {
        InternedSymbol[] actuals = myReader.getTypeAnnotationSymbols();

        assertSymbolEquals("annotation", expecteds, actuals);

        return this;
    }
}
