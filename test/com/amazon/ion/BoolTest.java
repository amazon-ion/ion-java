/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;



public class BoolTest
    extends IonTestCase
{
    public static void checkNullBool(IonBool value)
    {
        assertSame(IonType.BOOL, value.getType());

        assertTrue(value.isNullValue());
        try
        {
            value.booleanValue();
            fail("Expected NullValueException");
        }
        catch (NullValueException e) { }
    }

    public static void checkBool(boolean expected, IonBool value)
    {
        assertSame(IonType.BOOL, value.getType());
        assertFalse(value.isNullValue());
        assertEquals(expected, value.booleanValue());
    }


    public void testNullBool()
    {
        IonBool value = (IonBool) oneValue("null.bool");
        checkNullBool(value);
        assertNull(value.getTypeAnnotations());
        assertEquals("null.bool", value.toString());

        value = (IonBool) oneValue("a::null.bool");
        checkNullBool(value);
        checkAnnotation("a", value);
        assertEquals("a::null.bool", value.toString());
    }


    public void testBools()
    {
        IonBool value = (IonBool) oneValue("true");
        assertFalse(value.isNullValue());
        assertNull(value.getTypeAnnotations());
        assertTrue(value.booleanValue());
        assertEquals("true", value.toString());

        value = (IonBool) oneValue("false");
        assertFalse(value.isNullValue());
        assertNull(value.getTypeAnnotations());
        assertFalse(value.booleanValue());
        assertEquals("false", value.toString());

        value = (IonBool) oneValue("a::true");
        assertFalse(value.isNullValue());
        checkAnnotation("a", value);
        assertTrue(value.booleanValue());
        assertEquals("a::true", value.toString());

        value = (IonBool) oneValue("a::false");
        assertFalse(value.isNullValue());
        checkAnnotation("a", value);
        assertFalse(value.booleanValue());
        assertEquals("a::false", value.toString());

    }

    public void testBoolChanges()
    {
        IonBool v = system().newNullBool();
        checkNullBool(v);

        v.setValue(true);
        checkBool(true, v);

        v.setValue(false);
        checkBool(false, v);

        v.setValue(null);
        checkNullBool(v);

        v.setValue(Boolean.TRUE);
        checkBool(true, v);

        v.setValue(Boolean.FALSE);
        checkBool(false, v);
    }
}
