/*
 * Copyright (c) 2007-2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.impl;

import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonTestCase;
import com.amazon.ion.IonValue;
import java.util.Iterator;
import java.util.NoSuchElementException;

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

    public void testSimpleIteration()
    {
        Iterator<IonValue> i = system().iterate("abc");
        assertTrue(i.hasNext());

        IonSymbol value = (IonSymbol) i.next();
        checkSymbol("abc", value);
        assertFalse(i.hasNext());

        checkEmptyIterator(i);
    }


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
