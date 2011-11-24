// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.junit;

import static com.amazon.ion.util.IonTextUtils.printSymbol;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.amazon.ion.InternedSymbol;
import com.amazon.ion.IonLob;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSequence;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

/**
 *
 */
public class IonAssert
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
            assertEquals("reader field name", null, in.getFieldName());
            assertTrue("reader shouldn't have fieldId", in.getFieldId() < 1);
            assertEquals("reader field symbol", null, in.getFieldNameSymbol());
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
        assertEquals(null, in.getType());

        assertEquals(null, in.getFieldName());
        assertTrue(in.getFieldId() < 0);
        assertEquals(null, in.getFieldNameSymbol());

        // TODO ION-213 Text reader doesn't throw, but others do.
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
            int[] ann = in.getTypeAnnotationIds();
            assertEquals(0, ann.length);
//            fail("expected exception");
        }
        catch (IllegalStateException e) { }

        try {
            Iterator<Integer> ann = in.iterateTypeAnnotationIds();
            assertEquals(false, ann.hasNext());
//            fail("expected exception");
        }
        catch (IllegalStateException e) { }
    }


    @SuppressWarnings("deprecation")
    public static void assertEof(IonReader in)
    {
        assertFalse(in.hasNext());
        assertFalse(in.hasNext());
        assertEquals(null, in.next());
        assertNoCurrentValue(in);
        assertEquals(null, in.next());
        assertFalse(in.hasNext());
        assertEquals(null, in.next());
    }


    public static void assertTopEof(IonReader in)
    {
        assertTopLevel(in);
        assertEof(in);
        assertTopLevel(in);
    }

    public static void expectField(IonReader in, String name)
    {
        assertEquals("field name", name, in.getFieldName());
        InternedSymbol is = in.getFieldNameSymbol();
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
    }

    /**
     * Move to the next value and check the field name.
     */
    public static void expectNextField(IonReader in, String name)
    {
        in.next();
        expectField(in, name);
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


    public static void assertEqualAnnotations(IonValue expected,
                                              IonValue actual)
    {
        if (expected == actual) return;

        String[] expectedAnn = expected.getTypeAnnotations();
        String[] actualAnn   = actual.getTypeAnnotations();

        assertArrayEquals("Ion annotations", expectedAnn, actualAnn);
    }

    private static void assertEqualAnnotations(String path,
                                               IonValue expected,
                                               IonValue actual)
    {
        if (expected == actual) return;

        String[] expectedAnn = expected.getTypeAnnotations();
        String[] actualAnn   = actual.getTypeAnnotations();

        assertArrayEquals(path + " annotations",
                          expectedAnn, actualAnn);
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
                assertEquals(path, expected, actual);
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
        HashMap<String,List<IonValue>> expectedFields = sortFields(expected);
        HashMap<String,List<IonValue>> actualFields   = sortFields(actual);

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
                              expected, expectedEntry.getValue(),
                              actual, actualList);
        }
    }

    private static HashMap<String,List<IonValue>> sortFields(IonStruct s)
    {
        HashMap<String,List<IonValue>> sorted =
            new HashMap<String,List<IonValue>>();
        for (IonValue v : s)
        {
            List<IonValue> fields = sorted.get(v.getFieldName());
            if (fields == null)
            {
                fields = new ArrayList<IonValue>(2);
                sorted.put(v.getFieldName(), fields);
            }
            fields.add(v);
        }
        return sorted;
    }


    private static void assertFieldEquals(String path,
                                          IonStruct expected,
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
                    fail("No match for field " + path + ":" + expectedChild
                         + " in struct: " + actual);
                }
            }

            if (actualFieldValues.size() != 0)
            {
                fail("Extra copies of field " + path
                     + " in struct: " + actual);
            }
        }
    }
}
