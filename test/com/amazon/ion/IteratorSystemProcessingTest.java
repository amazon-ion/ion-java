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
    private String myText;
    private Iterator<IonValue> myIterator;
    private IonValue myCurrentValue;

    protected Iterator<IonValue> iterate()
        throws Exception
    {
        return system().iterate(myText);
    }

    protected Iterator<IonValue> systemIterate()
        throws Exception
    {
        return system().systemIterate(myText);
    }


    @Override
    protected void prepare(String text)
        throws Exception
    {
        myText = text;
    }

    @Override
    protected void startIteration() throws Exception
    {
        myIterator = iterate();
    }

    @Override
    protected void startSystemIteration() throws Exception
    {
        myIterator = systemIterate();
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
    protected void checkDecimal(double expected) throws Exception
    {
        checkDecimal(expected, myCurrentValue);
    }

    @Override
    protected void checkFloat(double expected) throws Exception
    {
        checkFloat(expected, myCurrentValue);
    }

    @Override
    protected void checkString(String expected) throws Exception
    {
        checkString(expected, myCurrentValue);
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
    protected boolean checkMissingSymbol(String expected, int expectedSymbolTableSid, int expectedLocalSid)
        throws Exception
    {
        checkSymbol(expected, myCurrentValue);

        // when missing from a shared table the symbol
        // will have been added to the local symbols
        return true;
    }

    @Override
    protected void checkTimestamp(Timestamp expected) throws Exception
    {
        checkTimestamp(expected, myCurrentValue);
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

    @Override
    protected void testString(String expectedValue,
                              String expectedRendering,
                              String ionData)
        throws Exception
    {
        super.testString(expectedValue, expectedRendering, ionData);
        assertEquals(expectedRendering, myCurrentValue.toString());
    }
}
