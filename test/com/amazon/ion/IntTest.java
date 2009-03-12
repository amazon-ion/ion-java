/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;

import java.math.BigInteger;




public class IntTest
    extends IonTestCase
{
    public static final long A_LONG_INT = 1L + Integer.MAX_VALUE;


    public static void checkNullInt(IonInt value)
    {
        assertSame(IonType.INT, value.getType());
        assertTrue("isNullValue() is false",   value.isNullValue());

        try
        {
            value.intValue();
            fail("Expected NullValueException");
        }
        catch (NullValueException e) { }

        try
        {
            value.longValue();
            fail("Expected NullValueException");
        }
        catch (NullValueException e) { }

        assertNull("toBigInteger() isn't null", value.bigIntegerValue());
    }


    public void modifyInt(IonInt value)
    {
        assertSame(IonType.INT, value.getType());
        value.setValue(123);
        assertFalse(value.isNullValue());
        assertEquals(123, value.intValue());
        assertEquals(123L, value.longValue());
        assertEquals(BigInteger.valueOf(123), value.bigIntegerValue());

        value.setValue(A_LONG_INT);
        assertEquals(A_LONG_INT, value.longValue());
        assertEquals(BigInteger.valueOf(A_LONG_INT), value.bigIntegerValue());

        value.setValue(null);
        checkNullInt(value);
    }


    //=========================================================================
    // Test cases

    public void testFactoryInt()
    {
        IonInt value = system().newNullInt();
        checkNullInt(value);
        modifyInt(value);
    }

    public void testTextNullInt()
    {
        IonInt value = (IonInt) oneValue("null.int");
        checkNullInt(value);
        modifyInt(value);
    }



    public void testNegInt()
    {
        IonInt value = (IonInt) oneValue("-1");
        assertSame(IonType.INT, value.getType());
        assertFalse(value.isNullValue());
        assertNull(value.getTypeAnnotations());
        assertEquals(-1, value.intValue());
        assertEquals(-1L, value.longValue());

        value = (IonInt)oneValue("-1999");
        assertSame(IonType.INT, value.getType());
        assertFalse(value.isNullValue());
        assertNull(value.getTypeAnnotations());
        assertEquals(-1999, value.intValue());
        assertEquals(-1999L, value.longValue());
        value.toString();
    }


    public void testInts()
    {
        IonInt value = (IonInt) oneValue("1");
        assertFalse(value.isNullValue());
        assertNull(value.getTypeAnnotations());
        assertEquals(1, value.intValue());
        assertEquals(1L, value.longValue());

        value = (IonInt) oneValue("a::" + Integer.MAX_VALUE);
        assertFalse(value.isNullValue());
        checkAnnotation("a", value);
        assertEquals(Integer.MAX_VALUE, value.intValue());
        assertEquals(Integer.MAX_VALUE, value.longValue());

        // Ensure that annotation makes it through value mods
        modifyInt(value);
        checkAnnotation("a", value);

        value = (IonInt) oneValue("" + Integer.MIN_VALUE);
        assertFalse(value.isNullValue());
        assertNull(value.getTypeAnnotations());
        assertEquals(Integer.MIN_VALUE, value.intValue());
    }


    public void testPositiveSign()
    {
        // Array keeps this from parsing as datagram "+ 1"
        badValue("[+1]");
        badValue("[+0]");
    }


    public void testNegativeIntRoundTrip()
    {
        IonInt i = system().newInt(-20);
        IonInt result = (IonInt) reload(i);
        assertEquals(-20, result.intValue());
    }


    public void testNegativeLongRoundTrip()
    {
        // FIXME: encoder can't handle Long.MIN_VALUE
        final long v = Long.MIN_VALUE + 1;

        IonInt i = system().newInt(v);
        IonInt result = (IonInt) reload(i);
        assertEquals(v, result.longValue());
    }


    public void testRoundTrip(BigInteger v)
    {
        IonInt i = system().newInt(v);
        IonInt result = (IonInt) reload(i);
        assertEquals(v, result.bigIntegerValue());
    }


    public void testNegNumberRoundTrip()
    {
        testRoundTrip(BigInteger.valueOf(Long.MAX_VALUE));
        testRoundTrip(BigInteger.valueOf(0));
        testRoundTrip(BigInteger.valueOf(-98102));
        testRoundTrip(BigInteger.valueOf(Long.MIN_VALUE+1));
        // FIXME: encoder can't handle Long.MIN_VALUE
        // testRoundTrip(BigInteger.valueOf(Long.MIN_VALUE));
    }

    public void testLongs()
    {
        IonInt value = (IonInt) oneValue(String.valueOf(A_LONG_INT));
        assertFalse(value.isNullValue());
        assertNull(value.getTypeAnnotations());
        assertEquals(A_LONG_INT, value.longValue());

        value = (IonInt) oneValue("a::" + Long.MAX_VALUE);
        assertFalse(value.isNullValue());
        checkAnnotation("a", value);
        assertEquals(Long.MAX_VALUE, value.longValue());

        // Ensure that annotation makes it through value mods
        modifyInt(value);
        checkAnnotation("a", value);
    }

    // TODO test BigInteger


    public void testStopChars()
    {
        badValue("12/");
    }

    public void testHexadecimal()
    {
        checkInt(-3, oneValue("-0x3"));
        checkInt(-3, oneValue("-0x0003"));
    }

    public void testIntsFromSuite()
        throws Exception
    {
        IonDatagram values = loadTestFile("good/integer_values.ion");
        // File is a sequence of many timestamp values.

        for (IonValue value : values)
        {
            assertTrue(value instanceof IonInt);
        }

    }


    public void testReadOnlyInt()
    {
        IonInt v = system().newInt(1);
        v.makeReadOnly();
        assertEquals(1, v.intValue());

        try {
            v.setValue(2);
            fail("Expected exception for modifying read-only value");
        }
        catch (IonException e) { }
        assertEquals(1, v.intValue());

        try {
            v.setValue(2L);
            fail("Expected exception for modifying read-only value");
        }
        catch (IonException e) { }
        assertEquals(1, v.intValue());

        try {
            v.setValue(new Long(2));
            fail("Expected exception for modifying read-only value");
        }
        catch (IonException e) { }
        assertEquals(1, v.intValue());
    }
}
