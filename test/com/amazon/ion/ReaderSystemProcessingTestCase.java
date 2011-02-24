// Copyright (c) 2008-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import static com.amazon.ion.SymbolTable.UNKNOWN_SYMBOL_ID;
import static com.amazon.ion.impl.IonImplUtils.EMPTY_INT_ARRAY;
import static com.amazon.ion.impl.IonImplUtils.EMPTY_STRING_ARRAY;

import com.amazon.ion.impl.IonImplUtils;
import com.amazon.ion.impl.TreeReaderTest;
import java.util.Date;
import org.junit.Assert;
import org.junit.Test;


/**
 *
 */
public abstract class ReaderSystemProcessingTestCase
    extends SystemProcessingTestCase
{
    private IonReader myReader;
    private IonType   myValueType;


    protected abstract IonReader read()
        throws Exception;

    protected abstract IonReader systemRead()
        throws Exception;


    @Override
    protected void startIteration() throws Exception
    {
        myReader = read();
        myValueType = null;
    }

    @Override
    protected void startSystemIteration() throws Exception
    {
        myReader = systemRead();
        myValueType = null;
    }

    @Override
    protected void nextValue() throws Exception
    {
        myValueType = myReader.next();
    }

    @Override
    protected IonType currentValueType() throws Exception
    {
        return myValueType;
    }

    @Override
    protected SymbolTable currentSymtab() throws Exception
    {
        return myReader.getSymbolTable();
    }

    @Override
    protected void checkAnnotation(String expected, int expectedSid)
    {
        String[] typeAnnotations = myReader.getTypeAnnotations();
        int[] sids = myReader.getTypeAnnotationIds();

        for (int i = 0; i < typeAnnotations.length; i++)
        {
            // FIXME ION-172 this assumes all annotations are known.
            if (typeAnnotations[i].equals(expected))
            {
                assertEquals("symbol id", expectedSid, sids[i]);
                return;
            }
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
        Decimal dec = myReader.decimalValue();

        assertEquals("decimal content", (long)expected, myReader.longValue());
        assertEquals("decimal content", expected, myReader.doubleValue());
        assertEquals("float value compared",
                     0, Float.compare((float)expected, dec.floatValue()));
        assertEquals("double value compared",
                     0, Double.compare(expected, dec.doubleValue()));
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
        // we don't really care what this returns, but it forces
        // any symbol table processing to occur, if necessary
        myReader.getSymbolId();
    }

    @Override
    protected void checkSymbol(String expected, int expectedSid)
        throws Exception
    {
        assertSame(IonType.SYMBOL, myReader.getType());

        // we'll use this to make sure we did check something here
        boolean was_checked = false;

        // first we check the text representation which all
        // user readers and any non-binary readers will have
        try {
            String reader_name = myReader.stringValue();
            assertEquals(expected, reader_name);
            was_checked = true;
        }
        catch (UnsupportedOperationException e) { }

        // now we check the binary value, which user readers
        // and any non-text readers should understand
        int sid = myReader.getSymbolId();
        if (sid != UNKNOWN_SYMBOL_ID) {
            if (expectedSid != sid) {
                int reader_sid = myReader.getSymbolId();
                assertEquals(expectedSid, reader_sid);
            }
            was_checked = true;
        }

        // finally we make sure we checked at least one of the
        // two representations (symbol text or symbol id)
        assertTrue("Didn't check symbol text or id", was_checked);
    }


    @Override
    protected void checkTimestamp(Timestamp expected) throws Exception
    {
        assertSame(IonType.TIMESTAMP, myReader.getType());
        assertEquals("timestamp", expected, myReader.timestampValue());

        Date expectedDate = (expected == null ? null : expected.dateValue());
        assertEquals("date", expectedDate, myReader.dateValue());
    }

    @Override
    @SuppressWarnings("deprecation")
    protected void checkEof()
    {
        // Doing this twice is intentional.
        assertEquals("next() at eof", null, myReader.next());

        if (!IonImplUtils.READER_HASNEXT_REMOVED) {
        assertFalse("not at eof", myReader.hasNext());
    }

        assertEquals("next() at eof", null, myReader.next());
    }

    protected void badNext()
    {
        assertEquals("result from IonReader.next()", null, myReader.next());
    }


    //=========================================================================


    @Test
    public void testNoAnnotations()
    throws Exception
    {
        startIteration("null");
        myReader.next();
        assertArrayEquals(EMPTY_STRING_ARRAY,
                          myReader.getTypeAnnotations());
        Assert.assertArrayEquals(EMPTY_INT_ARRAY,
                                 myReader.getTypeAnnotationIds());
    }


    @Test
    public void testNextAtEnd()
        throws Exception
    {
        startTestCheckpoint("testNextAtEnd"); // uses simple constants since the number just has to be unique for matching on the break point

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
    @Test
    public void testIsInStruct()
        throws Exception
    {
        startTestCheckpoint("testIsInStruct"); // uses simple constants since the number just has to be unique for matching on the break point

        String text = "{}";
        startIteration(text);
        assertFalse(myReader.isInStruct());

        // hasNext is depreciated: assertTrue(myReader.hasNext());
        assertFalse(myReader.isInStruct());


        assertEquals(IonType.STRUCT, myReader.next());
        if (0 != myReader.getDepth()) {
            assertEquals(0, myReader.getDepth());
        }
        assertFalse(myReader.isInStruct());

        myReader.stepIn();
        assertTrue(myReader.isInStruct());
        assertEquals(1, myReader.getDepth());

        // assertFalse(myReader.hasNext());
        assertTrue(myReader.isInStruct());

        myReader.stepOut();
        assertFalse(myReader.isInStruct());

        // hasNext is depreciated: assertFalse(myReader.hasNext());
    }


    @Test
    @SuppressWarnings("deprecation")
    public void testHasNextLeavesCurrentData()
        throws Exception
    {
        startTestCheckpoint("testHasNextLeavesCurrentData"); // uses simple constants since the number just has to be unique for matching on the break point

        String text = "hello 2";
        startIteration(text);

        assertTrue(myReader.hasNext());
        assertEquals(IonType.SYMBOL, myReader.next());
        assertEquals(IonType.SYMBOL, myReader.getType());
        assertTrue(myReader.hasNext());

        // FIXed ME text reader was broken, now fixed
        // really the binary readers were returning the
        // ion version marker which is a symbol and the
        // first (0th) value in a datagram.
        //if (!(myReader instanceof IonTextReaderImpl)) {
        //    assertEquals(IonType.SYMBOL, myReader.getType());
        //}
        assertEquals(IonType.INT, myReader.next());
    }

    // JIRA ION-79 reported by Scott Barber
    @Test
    @SuppressWarnings("deprecation")
    public void testDeepNesting()
        throws Exception
    {
        startTestCheckpoint("testDeepNesting"); // uses simple constants since the number just has to be unique for matching on the break point

        String text =
            "A::{data:B::{items:[C::{itemPromos:[D::{f4:['''12.5''']}]}]}}";
        startIteration(text);

        IonReader reader = myReader;
//        reader.hasNext();
        IonType ts = reader.next();
        assertEquals(IonType.STRUCT, ts);
        assertEquals("A", reader.getTypeAnnotations()[0]);
        reader.stepIn();
        {
//            reader.hasNext();
            reader.next();
            assertEquals("B", reader.getTypeAnnotations()[0]);
            reader.stepIn();
            {
                reader.next();
                reader.stepIn(); // list
                {
                    reader.next();
                    assertEquals("C", reader.getTypeAnnotations()[0]);
                    reader.stepIn();
                    {
                        reader.next();
                        reader.stepIn();
                        { // list
                            reader.next();
                            assertEquals("D", reader.getTypeAnnotations()[0]);
                            reader.stepIn();
                            {
                                reader.next();
                                assertEquals("f4", reader.getFieldName());
                                reader.stepIn();
                                {
                                    reader.next();
                                    assertEquals("12.5", reader.stringValue());
                                    assertFalse(reader.hasNext());
                                    assertNull(reader.next());
                                }
                                reader.stepOut();
                                assertFalse(reader.hasNext());
                                assertNull(reader.next());
                            }
                            reader.stepOut();
                            assertFalse(reader.hasNext());
                            IonType t = reader.next();
                            assertNull("reader should not find a value", t);
                            assertFalse(reader.hasNext());
                        }
                        reader.stepOut();
                        assertFalse(reader.hasNext());
                        assertNull(reader.next());
                    }
                    reader.stepOut(); //
                    assertFalse(reader.hasNext());
                    assertNull(reader.next());
                }
                reader.stepOut();
                assertFalse(reader.hasNext());
                assertNull(reader.next());
            }
            reader.stepOut();
            assertFalse(reader.hasNext());
            assertNull(reader.next());
        }
        reader.stepOut();
        assertFalse(reader.hasNext());
        assertNull(reader.next());
        assertEquals(0, reader.getDepth());
        try {
            reader.stepOut();
            fail("expected exception");
        }
        catch (IllegalStateException e) {
            // TODO compare to IonMessages.CANNOT_STEP_OUT
            // Can't do that right now due to permissions
        }
    }
}
