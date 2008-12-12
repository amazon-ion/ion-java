/*
 * Copyright (c) 2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;

import java.util.Iterator;

/**
 *
 */
public class IteratorSystemProcessingTest
    extends SystemProcessingTestCase
{
    private Iterator<IonValue> myIterator;
    private IonValue myCurrentValue;


    protected Iterator<IonValue> iterate(String text)
    {
        return system().iterate(text);
    }

    @Override
    protected void startIteration(String text) throws Exception
    {
        myIterator = iterate(text);
    }

    @Override
    protected void checkInt(long expected) throws Exception
    {
        checkInt(expected, myCurrentValue);
    }

    @Override
    protected void checkSymbol(String expected) throws Exception
    {
        checkSymbol(expected, myCurrentValue);
    }

    @Override
    protected void checkSymbol(String expected, int expectedSid)
        throws Exception
    {
        checkSymbol(expected, expectedSid, myCurrentValue);
    }

    @Override
    protected SymbolTable currentSymtab() throws Exception
    {
        return myCurrentValue.getSymbolTable();
    }

    @Override
    protected void nextUserValue() throws Exception
    {
        myCurrentValue = myIterator.next();
    }

}
