/*
 * Copyright 2007-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import java.util.Iterator;
import org.junit.Test;
import software.amazon.ion.IonDatagram;
import software.amazon.ion.IonList;
import software.amazon.ion.IonNull;
import software.amazon.ion.IonType;
import software.amazon.ion.IonValue;



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

    @Test
    public void testFactoryNull()
    {
        IonNull value = system().newNull();
        checkNull(value);

        value.addTypeAnnotation("scram");
        assertEquals(value.hasTypeAnnotation("scram"), true);
    }

    @Test
    public void testTextNull()
    {
        IonNull value = (IonNull) oneValue("null");
        checkNull(value);

        value = (IonNull) oneValue("null.null");
        checkNull(value);
    }


    @Test
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


    @Test
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
