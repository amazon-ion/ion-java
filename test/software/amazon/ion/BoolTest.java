/*
 * Copyright 2007-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package software.amazon.ion;

import org.junit.Test;
import software.amazon.ion.IonBool;
import software.amazon.ion.IonType;
import software.amazon.ion.NullValueException;
import software.amazon.ion.ReadOnlyValueException;



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


    @Test
    public void testNullBool()
    {
        IonBool value = (IonBool) oneValue("null.bool");
        checkNullBool(value);
        assertArrayEquals(new String[0], value.getTypeAnnotations());
        assertEquals("null.bool", value.toString());

        value = (IonBool) oneValue("a::null.bool");
        checkNullBool(value);
        checkAnnotation("a", value);
        assertEquals("a::null.bool", value.toString());
    }


    @Test
    public void testBools()
    {
        IonBool value = (IonBool) oneValue("true");
        assertFalse(value.isNullValue());
        assertArrayEquals(new String[0], value.getTypeAnnotations());
        assertTrue(value.booleanValue());
        assertEquals("true", value.toString());

        value = (IonBool) oneValue("false");
        assertFalse(value.isNullValue());
        assertArrayEquals(new String[0], value.getTypeAnnotations());
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


    @Test
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


    /**
     * Thanks to Mark Tomko for reporting this bug.
     */
    @Test
    public void testReadOnlyBool()
    {
        IonBool v = system().newBool(true);
        v.makeReadOnly();
        assertTrue(v.booleanValue());

        try {
            v.setValue(false);
            fail("Expected exception for modifying read-only value");
        }
        catch (ReadOnlyValueException e) { }
        assertTrue(v.booleanValue());

        try {
            v.setValue(Boolean.FALSE);
            fail("Expected exception for modifying read-only value");
        }
        catch (ReadOnlyValueException e) { }
        assertTrue(v.booleanValue());

        try {
            v.setValue((Boolean) null);
            fail("Expected exception for modifying read-only value");
        }
        catch (ReadOnlyValueException e) { }
        assertTrue(v.booleanValue());
    }
}
