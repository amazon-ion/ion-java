// Copyright (c) 2008-2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import com.amazon.ion.impl.IonBinaryReader;
import com.amazon.ion.impl.TreeReaderTest;
import java.util.NoSuchElementException;


/**
 *
 */
public abstract class ReaderSystemProcessingTestCase
    extends SystemProcessingTestCase
{
    private IonReader myReader;


    protected abstract IonReader read()
        throws Exception;

    protected abstract IonReader systemRead()
        throws Exception;


    @Override
    protected void startIteration() throws Exception
    {
        myReader = read();
    }

    @Override
    protected void startSystemIteration() throws Exception
    {
        myReader = systemRead();
    }

    @Override
    protected void nextValue() throws Exception
    {
        myReader.next();
    }

    @Override
    protected SymbolTable currentSymtab() throws Exception
    {
        return myReader.getSymbolTable();
    }

    @Override
    protected void checkAnnotation(String expected)
    {
        String[] typeAnnotations = myReader.getTypeAnnotations();
        for (int i = 0; i < typeAnnotations.length; i++)
        {
            if (typeAnnotations[i].equals(expected)) return;
        }
        fail("Didn't find expected annotation: " + expected);
    }

    @Override
    protected void checkType(IonType expected)
    {
        assertSame(expected, myReader.getType());
    }

    @Override
    protected void checkInt(long expected) throws Exception
    {
        assertSame(IonType.INT, myReader.getType());
        assertEquals("int content", expected, myReader.longValue());
    }

    @Override
    protected void checkDecimal(double expected) throws Exception
    {
        assertSame(IonType.DECIMAL, myReader.getType());
        // TODO also test longValue, bigDecimalValue, etc.
//        assertEquals("decimal content", expected, myReader.longValue());
        assertEquals("decimal content", expected, myReader.doubleValue());
    }

    @Override
    protected void checkFloat(double expected) throws Exception
    {
        assertSame(IonType.FLOAT, myReader.getType());
        assertEquals("float content", expected, myReader.doubleValue());
    }

    @Override
    protected void checkString(String expected) throws Exception
    {
        assertSame(IonType.STRING, myReader.getType());
        assertEquals(expected, myReader.stringValue());
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

        // FIXME this is a bug in binary reader
        if (!(myReader instanceof IonBinaryReader)) {
            assertEquals(expectedSid, myReader.getSymbolId());
        }
    }


    @Override
    protected void checkTimestamp(String expected) throws Exception
    {
        assertSame(IonType.TIMESTAMP, myReader.getType());
        // TODO handle null.timestamp
        assertEquals("timestamp",
                     expected, myReader.timestampValue().toString());
    }

    @Override
    protected void checkEof()
    {
        assertFalse("not at eof", myReader.hasNext());
    }

    protected void badNext()
    {
        try {
            myReader.next();
            fail("expected exception");
        }
        catch (NoSuchElementException e) { }
    }


    //=========================================================================

    public void testNextAtEnd()
        throws Exception
    {
        String text = "[]";
        startIteration(text);
        myReader.next();
        myReader.stepIn();
        badNext();
        myReader.stepOut();
        badNext();

        text = "[1]";
        startIteration(text);
        myReader.next();
        myReader.stepIn();
        myReader.next();
        badNext();
        myReader.stepOut();
        badNext();
    }

    /**
     * When this is working,
     * remove {@link TreeReaderTest#testInitialStateForStruct()}
     */
    public void testIsInStruct()
        throws Exception
    {
        String text = "{}";
        startIteration(text);
        assertFalse(myReader.isInStruct());

        assertTrue(myReader.hasNext());
//        assertFalse(myReader.isInStruct()); // FIXME text reader is broken

        assertEquals(IonType.STRUCT, myReader.next());
        assertEquals(0, myReader.getDepth());
//        assertFalse(myReader.isInStruct()); // text reader is broken

        myReader.stepIn();
        assertTrue(myReader.isInStruct());
        assertEquals(1, myReader.getDepth());

        assertFalse(myReader.hasNext());
        assertTrue(myReader.isInStruct());

        myReader.stepOut();
//        assertFalse(myReader.isInStruct()); // text reader is broken
        assertFalse(myReader.hasNext());
    }


    public void testHasNextLeavesCurrentData()
        throws Exception
    {
        String text = "hello 2";
        startIteration(text);

        assertTrue(myReader.hasNext());
        assertEquals(IonType.SYMBOL, myReader.next());
        assertEquals(IonType.SYMBOL, myReader.getType());
        assertTrue(myReader.hasNext());
        // FIXME text reader is broken
//        assertEquals(IonType.SYMBOL, myReader.getType());
        assertEquals(IonType.INT, myReader.next());
    }

}
