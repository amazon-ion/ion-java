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


        // TODO imlement raw Scanning via public API
        Iterator<IonValue> iterator = new SystemReader(system(), "");
        checkEmptyIterator(iterator);

        // Try calling next before hasNext
        iterator = new SystemReader(system(), "1");
        iterator.next();
        checkEmptyIterator(iterator);
    }
}
