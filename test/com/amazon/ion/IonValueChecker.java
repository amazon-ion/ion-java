// Copyright (c) 2011-2012 Amazon.com, Inc.  All rights reserved.

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
        if (expectedText == null)
        {
            try
            {
                myCurrentValue.getFieldName();
                fail("Expected " + UnknownSymbolException.class);
            }
            catch (UnknownSymbolException e)
            {
                assertEquals(expectedSid, e.getSid());
            }
        }
        else
        {
            assertEquals("IonValue.getFieldName()",
                         expectedText, myCurrentValue.getFieldName());
        }

        SymbolToken sym = myCurrentValue.getFieldNameSymbol();
        checkSymbol(expectedText, expectedSid, sym);

        return this;
    }


    public IonValueChecker annotation(String expectedText)
    {
        String[] typeAnnotations = myCurrentValue.getTypeAnnotations();
        assertEquals(expectedText, typeAnnotations[0]);

        return this;
    }


    public IonValueChecker annotation(String expectedText, int expectedSid)
    {
        if (expectedText == null)
        {
            try
            {
                myCurrentValue.getTypeAnnotations();
                fail("Expected " + UnknownSymbolException.class);
            }
            catch (UnknownSymbolException e)
            {
                assertEquals(expectedSid, e.getSid());
            }
        }
        else
        {
            String[] typeAnnotations = myCurrentValue.getTypeAnnotations();
            assertEquals(expectedText, typeAnnotations[0]);
        }

        SymbolToken[] annSyms = myCurrentValue.getTypeAnnotationSymbols();
        checkSymbol(expectedText, expectedSid, annSyms[0]);

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
