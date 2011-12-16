// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import static com.amazon.ion.IonTestCase.checkSymbol;
import static com.amazon.ion.SymbolTable.UNKNOWN_SYMBOL_ID;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 *
 */
public class IonValueChecker
    implements Checker
{
    private IonValue myCurrentValue;

    public IonValueChecker(IonValue value)
    {
        myCurrentValue = value;
    }


    public IonValueChecker fieldName(String expectedText, int expectedSid)
    {
        String expectedStringValue =
            (expectedText == null ? "$" + expectedSid : expectedText);

        assertEquals("IonValue.getFieldName()",
                     expectedStringValue, myCurrentValue.getFieldName());

        assertEquals("IonValue.getFieldId",
                     expectedSid, myCurrentValue.getFieldId());

        InternedSymbol sym = myCurrentValue.getFieldNameSymbol();
        checkSymbol(expectedText, expectedSid, sym);

        return this;
    }


    public IonValueChecker annotation(String expectedText, int expectedSid)
    {
        String expectedStringValue =
            (expectedText == null ? "$" + expectedSid : expectedText);

        String[] typeAnnotations = myCurrentValue.getTypeAnnotations();
        assertEquals(expectedStringValue, typeAnnotations[0]);

        InternedSymbol[] annSyms = myCurrentValue.getTypeAnnotationSymbols();
        checkSymbol(expectedText, expectedSid, annSyms[0]);

        if (expectedText != null
            && ! myCurrentValue.hasTypeAnnotation(expectedStringValue))
        {
            fail("Didn't find expected annotation: " + expectedStringValue);
        }
        myCurrentValue.hasTypeAnnotation("just make sure it doesn't blow up");

        return this;
    }

    public IonValueChecker annotations(String[] expectedTexts, int[] expectedSids)
    {
        String[] typeAnnotations = myCurrentValue.getTypeAnnotations();
        assertArrayEquals(expectedTexts, typeAnnotations);

        SymbolTable symtab = myCurrentValue.getSymbolTable();
        for (int i = 0; i < expectedTexts.length; i++)
        {
            int foundSid = symtab.findSymbol(expectedTexts[i]);
            if (foundSid != UNKNOWN_SYMBOL_ID)
            {
                assertEquals("symbol id", expectedSids[i], foundSid);
            }
        }

        return this;
    }
}
