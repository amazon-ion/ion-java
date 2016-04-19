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
package software.amazon.ion.impl;

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.junit.Test;
import software.amazon.ion.IonSymbol;
import software.amazon.ion.IonTestCase;
import software.amazon.ion.IonValue;

public class IterationTest
    extends IonTestCase
{
    /**
     * Fails if the iterator has a next element.
     */
    private void checkEmptyIterator(Iterator i)
    {
        try
        {
            i.next();
            fail("Expected NoSuchElementException");
        }
        catch (NoSuchElementException e) { /* good */ }
        assertFalse("iterator should be empty", i.hasNext());
    }


    //=========================================================================
    // Test cases

    @Test
    public void testSimpleIteration()
    {
        Iterator<IonValue> i = system().iterate("abc");
        assertTrue(i.hasNext());

        IonSymbol value = (IonSymbol) i.next();
        checkSymbol("abc", value);
        assertFalse(i.hasNext());

        checkEmptyIterator(i);
    }


    @Test
    public void testIteratorTermination()
    {
        Iterator<IonValue> scanner = system().iterate("");
        checkEmptyIterator(scanner);

        // Try calling next before hasNext
        scanner = system().iterate("1");
        scanner.next();
        checkEmptyIterator(scanner);


        Iterator<IonValue> iterator = system().systemIterate("");
        checkEmptyIterator(iterator);

        // Try calling next before hasNext
        iterator = system().systemIterate("1");
        iterator.next();
        checkEmptyIterator(iterator);
    }
}
