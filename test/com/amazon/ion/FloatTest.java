/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;


/**
 *
 */
public class FloatTest
    extends IonTestCase
{
    /** A double that's too big for a float */
    public static final double A_DOUBLE = 1D + Float.MAX_VALUE;

    public static void checkNullFloat(IonFloat value)
    {
        assertSame(IonType.FLOAT, value.getType());
        assertTrue("isNullValue is false", value.isNullValue());

        try
        {
            value.floatValue();
            fail("Expected NullValueException");
        }
        catch (NullValueException e) { }

        try
        {
            value.doubleValue();
            fail("Expected NullValueException");
        }
        catch (NullValueException e) { }

        assertNull("toBigDecimal() isn't null", value.toBigDecimal());
    }


    public void modifyFloat(IonFloat value)
    {
        float  fVal = 123.45F;

        value.setValue(fVal);
        assertEquals(fVal, value.floatValue());
        assertEquals((double) fVal, value.doubleValue());
        assertEquals(fVal, value.toBigDecimal().floatValue());

        value.setValue(A_DOUBLE);
        assertEquals(A_DOUBLE, value.doubleValue());
        assertEquals(A_DOUBLE, value.toBigDecimal().doubleValue());

        value.setValue(null);
        checkNullFloat(value);
    }


    //=========================================================================
    // Test cases

    public void testFactoryFloat()
    {
        IonFloat value = system().newNullFloat();
        checkNullFloat(value);
        modifyFloat(value);
    }

    public void testTextNullFloat()
    {
        IonFloat value = (IonFloat) oneValue("null.float");
        checkNullFloat(value);
        modifyFloat(value);
    }

    public void testFloats()
    {
        IonFloat value = (IonFloat) oneValue("1.0e0");
        assertSame(IonType.FLOAT, value.getType());
        assertFalse(value.isNullValue());
        assertNull(value.getTypeAnnotations());
        assertEquals(1.0F, value.floatValue());
        assertEquals(1.0D, value.doubleValue());

        // TODO more...

        value = (IonFloat) oneValue("a::1.0e0");
        assertFalse(value.isNullValue());
        checkAnnotation("a", value);

        // Ensure that annotation makes it through value mods
        modifyFloat(value);
        checkAnnotation("a", value);

        // TODO test BigDecimal
    }
}
