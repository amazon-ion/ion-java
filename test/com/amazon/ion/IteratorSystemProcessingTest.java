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
        throws Exception
    {
        return system().iterate(text);
    }

    protected Iterator<IonValue> systemIterate(String text)
        throws Exception
    {
        return system().systemIterate(text);
    }


    @Override
    protected void startIteration(String text) throws Exception
    {
        myIterator = iterate(text);
    }

    @Override
    protected void startSystemIteration(String text) throws Exception
    {
        myIterator = systemIterate(text);

    }

    @Override
    protected void nextValue() throws Exception
    {
        myCurrentValue = myIterator.next();
    }

    @Override
    protected void checkAnnotation(String expected)
    {
        if (! myCurrentValue.hasTypeAnnotation(expected))
        {
            fail("Didn't find expected annotation: " + expected);
        }
    }

    @Override
    protected void checkType(IonType expected)
    {
        assertSame(expected, myCurrentValue.getType());
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
    protected void checkEof() throws Exception
    {
        if (myIterator.hasNext())
        {
            fail("expected EOF, found " +  myIterator.next());
        }
    }
}
