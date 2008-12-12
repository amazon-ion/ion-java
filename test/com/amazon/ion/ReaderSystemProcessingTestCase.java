/*
 * Copyright (c) 2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;


/**
 *
 */
public abstract class ReaderSystemProcessingTestCase
    extends SystemProcessingTestCase
{
    private IonReader myReader;


    protected abstract IonReader read(String text)
        throws Exception;


    @Override
    protected void startIteration(String text) throws Exception
    {
        myReader = read(text);
    }

    @Override
    protected void nextUserValue() throws Exception
    {
        myReader.next();
    }

    @Override
    protected SymbolTable currentSymtab() throws Exception
    {
        return myReader.getSymbolTable();
    }


    @Override
    protected void checkInt(long expected) throws Exception
    {
        assertSame(IonType.INT, myReader.getType());
        assertEquals(expected, myReader.longValue());
    }

    @Override
    protected void checkSymbol(String expected) throws Exception
    {
        assertSame(IonType.SYMBOL, myReader.getType());
        assertEquals(expected, myReader.stringValue());
    }

    @Override
    protected void checkSymbol(String expected, int expectedSid)
        throws Exception
    {
        assertSame(IonType.SYMBOL, myReader.getType());
        assertEquals(expected, myReader.stringValue());
        assertEquals(expectedSid, myReader.getSymbolId());
    }

}
