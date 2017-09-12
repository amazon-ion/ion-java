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

package software.amazon.ion.junit;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static software.amazon.ion.impl.PrivateIonConstants.UNKNOWN_SYMBOL_TEXT_PREFIX;
import static software.amazon.ion.util.IonTextUtils.printSymbol;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import software.amazon.ion.IonLob;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonSequence;
import software.amazon.ion.IonStruct;
import software.amazon.ion.IonTestCase;
import software.amazon.ion.IonType;
import software.amazon.ion.IonValue;
import software.amazon.ion.ReaderChecker;
import software.amazon.ion.SymbolToken;
import software.amazon.ion.UnknownSymbolException;
import software.amazon.ion.util.Equivalence;

public final class IonAssert
{

    //========================================================================
    // IonReader assertions

    public static void assertTopLevel(IonReader in)
    {
        assertTopLevel(in, false);
    }

    public static void assertTopLevel(IonReader in, boolean inStruct)
    {
        assertEquals("reader depth", 0, in.getDepth());
        assertEquals("reader inStruct", inStruct, in.isInStruct());

        if (! inStruct) {
            assertNull("reader field name", in.getFieldName());
            assertNull("reader field symbol", in.getFieldNameSymbol());
        }

        try {
            in.stepOut();
            fail("Expected exception stepping out");
        }
        catch (IllegalStateException e) {
            // TODO compare to IonMessages.CANNOT_STEP_OUT
            // Can't do that right now due to permissions
        }
    }


    public static void assertNoCurrentValue(IonReader in)
    {
        assertNull(in.getType());

        assertNull(in.getFieldName());
        assertNull(in.getFieldNameSymbol());

        // TODO amzn/ion-java#13 Text reader doesn't throw, but others do.
        try {
            String[] ann = in.getTypeAnnotations();
            assertEquals(0, ann.length);
//            fail("expected exception");
        }
        catch (IllegalStateException e) { }

        try {
            Iterator<String> ann = in.iterateTypeAnnotations();
            assertEquals(false, ann.hasNext());
//            fail("expected exception");
        }
        catch (IllegalStateException e) { }

        try {
            in.stringValue();
            fail("expected exception");
        }
        catch (IllegalStateException e) { }
    }


    public static void assertEof(IonReader in)
    {
        assertNull(in.next());
        assertNoCurrentValue(in);
        assertNull(in.next());
        assertNull(in.next());
    }


    public static void assertTopEof(IonReader in)
    {
        assertTopLevel(in);
        assertEof(in);
        assertTopLevel(in);
    }


    /**
     * @deprecated Use {@link ReaderChecker}.
     */
    @Deprecated
    public static void expectField(IonReader in, String name)
    {
        IonTestCase.check(in).fieldName(name);
    }

    /**
     * Move to the next value and check the field name.
     * @deprecated Use {@link ReaderChecker}.
     */
    @Deprecated
    public static void expectNextField(IonReader in, String name)
    {
        in.next();
        expectField(in, name);
    }


    /**
     * @param expectedText null means absent
     */
    public static void checkSymbol(IonReader in,
                                   String expectedText,
                                   int expectedSid)
    {
        assertSame(IonType.SYMBOL, in.getType());

        assertFalse(in.isNullValue());

        if (expectedText == null)
        {
            try {
                in.stringValue();
                fail("Expected " + UnknownSymbolException.class);
            }
            catch (UnknownSymbolException e)
            {
                assertEquals(expectedSid, e.getSid());
            }
        }
        else
        {
            assertEquals("IonReader.stringValue()",
                         expectedText, in.stringValue());
        }

        SymbolToken sym = in.symbolValue();
        IonTestCase.checkSymbol(expectedText, expectedSid, sym);
    }

    /**
     * @param expectedText null means null.symbol
     */
    public static void checkSymbol(String expectedText,
                                   IonReader in)
    {
        assertEquals("getType", IonType.SYMBOL, in.getType());
        assertEquals("isNullValue", expectedText == null, in.isNullValue());
        assertEquals("stringValue", expectedText, in.stringValue());

        SymbolToken symTok = in.symbolValue();
        if (expectedText == null)
        {
            assertEquals("symbolValue", null, symTok);
        }
        else
        {
            assertEquals("symbolValue.text", expectedText, symTok.getText());
        }
    }

    public static void checkNullSymbol(IonReader in)
    {
        checkSymbol(null, in);
    }


    public static void assertSymbolEquals(String path,
                                          SymbolToken expected,
                                          SymbolToken actual)
    {
        String expectedText = expected.getText();
        String actualText   = actual.getText();
        assertEquals(path + " text", expectedText, actualText);

        if (expectedText == null)
        {
            assertEquals(path + " sid", expected.getSid(), actual.getSid());
        }
    }

    public static void assertSymbolEquals(String path,
                                          SymbolToken[] expecteds,
                                          SymbolToken[] actuals)
    {
        assertEquals(path + " count", expecteds.length, actuals.length);

        for (int i = 0; i < expecteds.length; i++)
        {
            assertSymbolEquals(path + "[" + i + "]",
                               expecteds[i], actuals[i]);
        }
    }


    //========================================================================
    // DOM assertions

    /**
     * Verifies that the given value has exactly the given annotations, in the
     * same order.
     *
     * @param actual must not be null.
     * @param expectedAnns may be empty to expect no annotations.
     */
    public static void assertAnnotations(IonValue actual,
                                         String... expectedAnns)
    {
        String[] actualAnns = actual.getTypeAnnotations();
        assertArrayEquals("Ion annotations", expectedAnns, actualAnns);
    }


    private static void assertEqualAnnotations(String path,
                                               IonValue expected,
                                               IonValue actual)
    {
        if (expected == actual) return;

        SymbolToken[] expecteds = expected.getTypeAnnotationSymbols();
        SymbolToken[] actuals   = actual.getTypeAnnotationSymbols();

        assertSymbolEquals(path + " annotation", expecteds, actuals);
    }


    public static void assertIonEquals(IonValue expected, IonValue actual)
    {
        doAssertIonEquals("root", expected, actual);

        // Finally, cross-check against IonValue.equals()
        // If this fails, something is wrong somewhere in here.
        assertEquals("Failure in assertIonEquals!", expected, actual);
    }

    public static void assertIonEquals(String message,
                                       IonValue expected, IonValue actual)
    {
        doAssertIonEquals(message + " root", expected, actual);

        // Finally, cross-check against IonValue.equals()
        // If this fails, something is wrong somewhere in here.
        assertEquals("Failure in assertIonEquals!", expected, actual);
    }


    public static void assertIteratorEquals(Iterator<?> expected,
                                            Iterator<?> actual)
    {
        while (expected.hasNext())
        {
            Object expectedValue = expected.next();
            Object actualValue = actual.next();
            assertEquals(expectedValue, actualValue);
        }
        assertFalse("unexpected next value", actual.hasNext());
    }


    public static void assertIonIteratorEquals(Iterator<IonValue> expected,
                                               Iterator<IonValue> actual)
    {
        int i = 0;
        while (expected.hasNext())
        {
            IonValue expectedValue = expected.next();
            if (! actual.hasNext())
            {
                fail("actual iteration ends before [" + i + "]=" + expectedValue);
            }

            IonValue actualValue   = actual.next();
            doAssertIonEquals("iterator[" + i + ']',
                              expectedValue, actualValue);
            i++;
        }

        assertFalse("unexpected next value [" + i + ']',
                    actual.hasNext());
    }


    //========================================================================

    private static void doAssertIonEquals(String path,
                                          IonValue expected,
                                          IonValue actual)
    {
        if (expected == actual) return;

        IonType expectedType = expected.getType();
        assertSame(path + " type", expectedType, actual.getType());

        assertEqualAnnotations(path, expected, actual);

        if (expected.isNullValue() || actual.isNullValue())
        {
            assertEquals(path, expected, actual);
            return;
        }

        switch (expectedType)
        {
            case BOOL:
            case DECIMAL:
            case FLOAT:
            case INT:
            case NULL:
            case STRING:
            case SYMBOL:
            case TIMESTAMP:
            {
                // "Normal" IonValue.equals()
                assertEquals(path + " IonValue", expected, actual);
                break;
            }

            case BLOB:
            case CLOB:
            {
                assertArrayEquals(path,
                                  ((IonLob)expected).getBytes(),
                                  ((IonLob)actual).getBytes());
                break;
            }

            // NOTE: Datagram equality is currently only based on
            // user data, not system data.
            case DATAGRAM:
            case LIST:
            case SEXP:
            {
                assertSequenceEquals(path,
                                     (IonSequence)expected,
                                     (IonSequence)actual);
                break;
            }

            case STRUCT:
            {
                assertStructEquals(path,
                                   (IonStruct)expected,
                                   (IonStruct)actual);
                break;
            }
        }
    }




    private static void assertSequenceEquals(String path,
                                             IonSequence expected,
                                             IonSequence actual)
    {
        int expectedSize = expected.size();
        int actualSize = actual.size();
        if (expectedSize != actualSize)
        {
            fail(path + " length, expected:" + expectedSize
                 + " actual:" + actualSize);
        }

        for (int i = 0; i < expectedSize; i++)
        {
            String childPath = path + '[' + i + ']';

            doAssertIonEquals(childPath, expected.get(i), actual.get(i));
        }
    }


    private static void assertStructEquals(String path,
                                           IonStruct expected,
                                           IonStruct actual)
    {
        int expectedSize = expected.size();
        int actualSize = actual.size();
        if (expectedSize != actualSize)
        {
            fail(path + " size, expected:" + expectedSize
                 + " actual:" + actualSize);
        }

        Map<String, List<IonValue>> expectedFields = sortFields(expected);
        Map<String, List<IonValue>> actualFields   = sortFields(actual);

        for (Entry<String,List<IonValue>> expectedEntry
                : expectedFields.entrySet())
        {
            String fieldName = expectedEntry.getKey();
            String fieldPath = path + '.' + printSymbol(fieldName);

            List<IonValue> actualList = actualFields.get(fieldName);
            if (actualList == null)
            {
                fail("Missing field " + fieldPath
                     + ", expected: " + expected
                     + " actual: " + actual);
            }

            assertFieldEquals(fieldPath,
                              expectedEntry.getValue(),
                              actual,
                              actualList);
        }
    }

    /**
     * Problematic with unknown field names.
     * See {@link Equivalence} for another use of this idiom.
     */
    private static HashMap<String,List<IonValue>> sortFields(IonStruct s)
    {
        HashMap<String,List<IonValue>> sorted =
            new HashMap<String,List<IonValue>>();
        for (IonValue v : s)
        {
            SymbolToken tok = v.getFieldNameSymbol();
            String text = tok.getText();
            if (text == null) {
                text = UNKNOWN_SYMBOL_TEXT_PREFIX + tok.getSid(); // TODO amzn/ion-java#23
            }
            List<IonValue> fields = sorted.get(text);
            if (fields == null)
            {
                fields = new ArrayList<IonValue>(2);
                sorted.put(text, fields);
            }
            fields.add(v);
        }
        return sorted;
    }


    private static void assertFieldEquals(String path,
                                          List<IonValue> expectedFieldValues,
                                          IonStruct actual,
                                          List<IonValue> actualFieldValues)
    {
        if (expectedFieldValues.size() == 1 && actualFieldValues.size() == 1)
        {
            // Easy squeezy
            doAssertIonEquals(path,
                              expectedFieldValues.get(0),
                              actualFieldValues.get(0));
        }
        else
        {
            for (IonValue expectedChild : expectedFieldValues)
            {
                if (! actualFieldValues.remove(expectedChild))
                {
                    fail("No match for field " + path);
                }
            }

            if (!actualFieldValues.isEmpty())
            {
                fail("Extra copies of field " + path
                     + " in struct: " + actual);
            }
        }
    }
}
