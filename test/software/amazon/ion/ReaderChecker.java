/*
 * Copyright 2011-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.ion;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static software.amazon.ion.IonTestCase.checkSymbol;
import static software.amazon.ion.impl.PrivateUtils.stringIterator;
import static software.amazon.ion.junit.IonAssert.assertIteratorEquals;
import static software.amazon.ion.junit.IonAssert.assertSymbolEquals;

import java.util.Iterator;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonType;
import software.amazon.ion.SymbolToken;
import software.amazon.ion.UnknownSymbolException;

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


    public ReaderChecker annotation(String expectedText)
    {
        Iterator<String> anns = myReader.iterateTypeAnnotations();
        assertEquals("symbol text", expectedText, anns.next());
        assertFalse(anns.hasNext());

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

        SymbolToken[] tokens = myReader.getTypeAnnotationSymbols();
        assertEquals(expectedSids.length, tokens.length);
        int i = 0;
        for (SymbolToken token : tokens)
        {
            assertEquals(expectedSids[i++], token.getSid());
        }

        return this;
    }

    public ReaderChecker annotations(SymbolToken[] expecteds)
    {
        SymbolToken[] actuals = myReader.getTypeAnnotationSymbols();

        assertSymbolEquals("annotation", expecteds, actuals);

        return this;
    }

    public ReaderChecker type(IonType expected)
    {
        IonType actual = myReader.getType();
        assertEquals("IonReader.getType()", expected, actual);
        return this;
    }

    public ReaderChecker isInt(long expected)
    {
        type(IonType.INT);
        long actual = myReader.longValue();
        assertEquals("IonReader.longValue()", expected, actual);
        return this;
    }
}
