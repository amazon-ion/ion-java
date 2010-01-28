/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;

import java.util.Iterator;



public class NullTest
    extends IonTestCase
{
    public void checkNull(IonNull value)
    {
        assertSame(IonType.NULL, value.getType());
        assertTrue("isNullValue() is false", value.isNullValue());
    }


    //=========================================================================
    // Test cases

    public void testFactoryNull()
    {
        IonNull value = system().newNull();
        checkNull(value);

        value.addTypeAnnotation("scram");
        assertEquals(value.hasTypeAnnotation("scram"), true);
    }

    public void testTextNull()
    {
        IonNull value = (IonNull) oneValue("null");
        checkNull(value);

        value = (IonNull) oneValue("null.null");
        checkNull(value);
    }


    public void testAllNulls()
        throws Exception
    {
        IonDatagram values = loadTestFile("good/allNulls.ion");
        // File contains a list of all the null values.

        assertEquals(1, values.size());
        IonList listOfNulls = (IonList) values.get(0);
        for (Iterator<IonValue> i = listOfNulls.iterator(); i.hasNext(); )
        {
            IonValue value = i.next();
            if (! value.isNullValue())
            {
                fail("Expected a null value, found " + value);
            }
        }
    }


    public void testNonNulls()
        throws Exception
    {
        IonDatagram values = loadTestFile("good/nonNulls.ion");
        // File contains a list of non-null values.

        int idx = 0;
        for (IonValue value : values)
        {
            if (value.isNullValue())
            {
                fail("Value should not be null: " + value);
            }
            idx++;
        }
    }
}
