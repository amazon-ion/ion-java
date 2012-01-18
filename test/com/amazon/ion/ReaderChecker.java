// Copyright (c) 2011-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import static com.amazon.ion.IonTestCase.checkSymbol;
import static com.amazon.ion.SymbolTable.UNKNOWN_SYMBOL_ID;
import static com.amazon.ion.impl._Private_Utils.stringIterator;
import static com.amazon.ion.junit.IonAssert.assertIteratorEquals;
import static com.amazon.ion.junit.IonAssert.assertSymbolEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.util.Iterator;

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
        if (expectedText == null)
        {
            try {
                myReader.getFieldName();
                fail("Expected " + UnknownSymbolException.class);
            }
            catch (UnknownSymbolException e)
            {
                assertEquals(expectedSid, e.getSid());
            }
        }
        else
        {
            assertEquals("IonReader.getFieldName()",
                         expectedText, myReader.getFieldName());
        }

        assertEquals("IonReader.getFieldId()",
                     expectedSid, myReader.getFieldId());

        SymbolToken sym = myReader.getFieldNameSymbol();
        checkSymbol(expectedText, expectedSid, sym);

        return this;
    }

    /**
     * @param name null means no field name expected.
     */
    public ReaderChecker fieldName(String name)
    {
        SymbolToken tok = myReader.getFieldNameSymbol();

        assertEquals("field name", name, myReader.getFieldName());

        if (name == null)
        {
            assertEquals("Unexpected SymbolToken", null, tok);
        }
        else
        {
            assertEquals("field name SymbolToken text",
                         name, tok.getText());
        }
        // TODO check sid

        return this;
    }

    /**
     * @param name null means no field name expected.
     */
    public ReaderChecker fieldName(SymbolToken name)
    {
        if (name == null)
        {
            assertEquals("IonReader.getFieldName()",
                         null, myReader.getFieldName());
            assertEquals("IonReader.getFieldNameSymbol()",
                         null, myReader.getFieldNameSymbol());
        }
        else
        {
            fieldName(name.getText(), name.getSid());
        }

        return this;
    }


    public ReaderChecker annotation(String expectedText, int expectedSid)
    {
        if (expectedText == null)
        {
            try {
                myReader.getTypeAnnotations();
                fail("Expected " + UnknownSymbolException.class);
            }
            catch (UnknownSymbolException e)
            {
                assertEquals(expectedSid, e.getSid());
            }

            try {
                myReader.iterateTypeAnnotations();
                fail("Expected " + UnknownSymbolException.class);
            }
            catch (UnknownSymbolException e)
            {
                assertEquals(expectedSid, e.getSid());
            }
        }
        else
        {
            String[] typeAnnotations = myReader.getTypeAnnotations();
            assertEquals("symbol text", expectedText, typeAnnotations[0]);

            Iterator<String> anns = myReader.iterateTypeAnnotations();
            assertEquals("symbol text", expectedText, anns.next());
            assertFalse(anns.hasNext());
        }

        int[] sids = myReader.getTypeAnnotationIds();
        assertEquals("sid", expectedSid, sids[0]);

        SymbolToken[] annSyms = myReader.getTypeAnnotationSymbols();
        IonTestCase.checkSymbol(expectedText, expectedSid, annSyms[0]);

        return this;
    }

    public ReaderChecker annotations(String[] expectedTexts, int[] expectedSids)
    {
        String[] typeAnnotations = myReader.getTypeAnnotations();
        assertArrayEquals(expectedTexts, typeAnnotations);

        Iterator<String> anns = myReader.iterateTypeAnnotations();
        assertIteratorEquals(stringIterator(expectedTexts), anns);

        int[] sids = myReader.getTypeAnnotationIds();
        assertArrayEquals(expectedSids, sids);

        return this;
    }

    public ReaderChecker annotations(SymbolToken[] expecteds)
    {
        SymbolToken[] actuals = myReader.getTypeAnnotationSymbols();

        assertSymbolEquals("annotation", expecteds, actuals);

        return this;
    }
}
