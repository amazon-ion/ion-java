// Copyright (c) 2007-2011 Amazon.com, Inc.  All rights reserved.
package com.amazon.ion.impl;

import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonTestCase;
import com.amazon.ion.IonValue;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.junit.Test;

/**
 *
 */
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
// TODO ION-262 shouldn't change the stream
iterator.next(); // skip $ion_1_0, this is a system iterator after all
        checkEmptyIterator(iterator);

        // Try calling next before hasNext
        iterator = system().systemIterate("1");
// TODO ION-262 shouldn't change the stream
iterator.next(); // skip $ion_1_0, this is a system iterator after all
        iterator.next();
        checkEmptyIterator(iterator);
    }
}
