/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;



public class StructTest
    extends ContainerTestCase
{
    public static void checkNullStruct(IonStruct value)
    {
        checkNullStruct(value, "");
    }

    public static void checkNullStruct(IonStruct value, String annotationText)
    {
        assertSame(IonType.STRUCT, value.getType());
        assertTrue(value.isNullValue());

        try
        {
            value.size();
            fail("Expected NullValueException");
        }
        catch (NullValueException e) { }

        try
        {
            value.get("f");
            fail("Expected NullValueException");
        }
        catch (NullValueException e) { }

        try
        {
            value.iterator();
            fail("Expected NullValueException");
        }
        catch (NullValueException e) { }

        try
        {
            value.remove(null);
            fail("Expected NullValueException");
        }
        catch (NullValueException e) { }

        try
        {
            value.isEmpty();
            fail("Expected NullValueException");
        }
        catch (NullValueException e) { }

        assertEquals(annotationText + "null.struct", value.toString());
    }


    /**
     * @param value must be null.struct or empty
     */
    public void modifyStruct(IonStruct value)
    {
        IonBool nullBool0 = system().newNullBool();
        value.put("f", nullBool0);
        assertEquals("size", 1, value.size());
        assertSame(nullBool0, value.get("f"));
        assertEquals("f", nullBool0.getFieldName());
        assertSame(value, nullBool0.getContainer());

        IonBool nullBool1 = system().newNullBool();
        value.add("g", nullBool1);
        assertEquals("size", 2, value.size());
        assertSame(nullBool1, value.get("g"));
        assertEquals("g", nullBool1.getFieldName());
        assertSame(value, nullBool1.getContainer());

        // Repeated field name, which one do we get?
        IonBool nullBool2 = system().newNullBool();
        value.add("f", nullBool2);
        assertEquals("size", 3, value.size());
        assertEquals("f", nullBool2.getFieldName());
        assertSame(value, nullBool2.getContainer());

        IonBool someBool = (IonBool) value.get("f");
        assertTrue((someBool == nullBool0) || (someBool == nullBool2));

        try
        {
            value.put("h", nullBool0);
            fail("Expected ContainedValueException");
        }
        catch (ContainedValueException e) { }
        // Make sure the element hasn't changed
        assertEquals("f", nullBool0.getFieldName());
        assertSame(value, nullBool0.getContainer());

        // Cannot put a datagram
        try
        {
            IonDatagram dg = loader().load("hi");
            value.put("g", dg);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) { }
        assertEquals("g", nullBool1.getFieldName());

        // Cannot add a datagram
        try
        {
            IonDatagram dg = loader().load("hi");
            value.add("g", dg);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) { }
        assertEquals("g", nullBool1.getFieldName());


        // Make sure put replaces doubled field.
        IonBool nullBool3 = system().newNullBool();
        value.put("f", nullBool3);
        assertEquals(2, value.size());
        assertNull(nullBool0.getContainer());
        assertNull(nullBool2.getContainer());

        // Now remove an element
        value.put("h", nullBool0);
        boolean removed = value.remove(nullBool3);
        assertTrue(removed);
        assertNull(nullBool3.getFieldName());
        assertNull(nullBool3.getContainer());
        assertEquals("size", 2, value.size());
        assertSame(nullBool1, value.get("g"));
        assertSame(nullBool0, value.get("h"));

        // Clear the struct
        testClearContainer(value);
    }


    //=========================================================================
    // Test cases

    public void testFactoryNullStruct()
    {
        IonStruct value = system().newNullStruct();
        assertNull(value.getFieldName());
        assertNull(value.getContainer());
        checkNullStruct(value);
        modifyStruct(value);
    }

    public void testTextNullStruct()
    {
        IonStruct value = (IonStruct) oneValue("null.struct");
        assertSame(IonType.STRUCT, value.getType());
        checkNullStruct(value);
        modifyStruct(value);
    }

    public void testTextAnnotatedNullStruct()
    {
        IonStruct value = (IonStruct) oneValue("test::null.struct");
        assertSame(IonType.STRUCT, value.getType());
        checkNullStruct(value, "test::");
        modifyStruct(value);
    }

    public void testMakeNullStruct()
    {
        IonStruct value = (IonStruct) oneValue("{foo:bar}");
        assertSame(IonType.STRUCT, value.getType());
        assertFalse(value.isNullValue());
        value.makeNull();
        checkNullStruct(value);
    }

    public void testClearNonMaterializedStruct()
    {
        IonStruct value = (IonStruct) oneValue("{foo:bar}");
        testClearContainer(value);
    }

    public void testEmptyStruct()
    {
        IonStruct value = (IonStruct) oneValue("{}");
        assertSame(IonType.STRUCT, value.getType());
        assertFalse(value.isNullValue());
        assertNull("annotation should be null", value.getTypeAnnotations());
        assertEquals(0, value.size());
        assertTrue(value.isEmpty());
        assertFalse(value.iterator().hasNext());
        assertNull(value.get("not"));
        assertEquals("{}", value.toString());
    }

    public void testStructs()
    {
        IonStruct value = (IonStruct) oneValue("{a:b}");
        assertEquals(1, value.size());
        IonSymbol fieldA = (IonSymbol) value.get("a");
        assertEquals("a", fieldA.getFieldName());
        assertEquals("b", fieldA.stringValue());
        assertNull(value.get("not"));
        assertEquals("{a:b}", value.toString());
    }


    /**
     * This looks for a subtle encoding problem. If a value has its header
     * widened enough to overlap clean content, we must be careful to not
     * overwrite the content while writing the header.  This can happen when
     * adding annotations.
     * @see ListTest#testModsCausingHeaderOverlap()
     */
    public void testModsCausingHeaderOverlap()
        throws Exception
    {
        IonDatagram dg = values("{f:\"this is a string to overlap\"}");
        IonStruct v = (IonStruct) dg.get(0);
        v.addTypeAnnotation("one");
        v.addTypeAnnotation("two");
        v.addTypeAnnotation("three");
        v.addTypeAnnotation("four");
        v.addTypeAnnotation("five");
        v.addTypeAnnotation("six");

        dg = reload(dg);
        v = (IonStruct) dg.get(0);
        checkString("this is a string to overlap", v.get("f"));
    }


    public void testGetTwiceReturnsSame()
    {
        IonStruct value = (IonStruct) oneValue("{a:b}");
        IonValue fieldA1 = value.get("a");
        IonValue fieldA2 = value.get("a");
        assertSame(fieldA1, fieldA2);
    }

    public void testDeepPut()
    {
        IonStruct value = (IonStruct) oneValue("{a:{b:bv}}");
        IonStruct nested = (IonStruct) value.get("a");
        IonBool inserted = system().newNullBool();
        nested.put("c", inserted);
        assertSame(inserted, ((IonStruct)value.get("a")).get("c"));
    }

    public void testExtraCommas()
    {
        IonStruct value = (IonStruct) oneValue("{a:b,}");
        assertEquals(1, value.size());
        assertEquals("{a:b}", value.toString());

        // Leading and lonely commas not allowed.
        badValue("{,}");
        badValue("{,3}");
    }


    public void testLongFieldName()
    {
        IonStruct value = (IonStruct) oneValue("{ '''a''' : b}");
        assertEquals(1, value.size());
        IonSymbol fieldA = (IonSymbol) value.get("a");
        assertEquals("a", fieldA.getFieldName());
        assertEquals("b", fieldA.stringValue());
        assertEquals("{a:b}", value.toString());
    }

    public void testConcatenatedFieldName()
    {
        IonStruct value = (IonStruct) oneValue("{ '''a''' '''a''' : b}");
        assertEquals(1, value.size());
        IonSymbol fieldA = (IonSymbol) value.get("aa");
        assertEquals("aa", fieldA.getFieldName());
        assertEquals("b",  fieldA.stringValue());
        assertEquals("{aa:b}", value.toString());
    }


    public void testMediumSymbolFieldNames()
    {
        IonStruct value = (IonStruct) oneValue("{'123456789ABCDEF':b}");
        assertEquals(1, value.size());
        IonSymbol fieldA = (IonSymbol) value.get("123456789ABCDEF");
        assertEquals("123456789ABCDEF", fieldA.getFieldName());
        assertEquals("b", fieldA.stringValue());
    }


    public void testMediumStringFieldNames()
    {
        IonStruct value = (IonStruct) oneValue("{\"123456789ABCDEF\":b}");
        assertEquals(1, value.size());
        IonSymbol fieldA = (IonSymbol) value.get("123456789ABCDEF");
        assertEquals("123456789ABCDEF", fieldA.getFieldName());
        assertEquals("b", fieldA.stringValue());
    }


    public void testNewlineInStringFieldName()
    {
        badValue("{ \"123\n4\" : v }");
        // Get beyond the 13-char threshold.
        badValue("{ \"123456789ABCDEF\nGHI\" : v }");
    }


    public void testNewlineInLongStringFieldName()
    {
        IonStruct value = (IonStruct) oneValue("{ '''123\n4''' : v }");
        checkSymbol("v", value.get("123\n4"));

        // Get beyond the 13-char threshold.
        value = (IonStruct) oneValue("{ '''123456789ABCDEF\nGHI''' : v }");
        checkSymbol("v", value.get("123456789ABCDEF\nGHI"));
    }

    public void testConcatenatedFieldValue()
    {
        IonStruct value = (IonStruct) oneValue("{a:'''a''' '''a'''}");
        assertEquals(1, value.size());
        IonString fieldA = (IonString) value.get("a");
        assertEquals("a", fieldA.getFieldName());
        assertEquals("aa",  fieldA.stringValue());
        assertEquals("{a:\"aa\"}", value.toString());
    }

    public void testBadGets()
    {
        IonStruct value = (IonStruct) oneValue("{a:b}");

        try {
            value.get(null);
            fail("Expected NullPointerException");
        }
        catch (NullPointerException e) { }

        try {
            value.get("");
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) { }
    }

    public void testBadPuts()
    {
        IonStruct value = system().newNullStruct();
        IonBool nullBool = system().newNullBool();

        try {
            value.put(null, nullBool);
            fail("Expected NullPointerException");
        }
        catch (NullPointerException e) { }

        try {
            value.put("", nullBool);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) { }

        try {
            value.put("f", null);
            fail("Expected NullPointerException");
        }
        catch (NullPointerException e) { }
    }

    public void testBadAddss()
    {
        IonStruct value = system().newNullStruct();
        IonBool nullBool = system().newNullBool();

        try {
            value.add(null, nullBool);
            fail("Expected NullPointerException");
        }
        catch (NullPointerException e) { }

        try {
            value.add("", nullBool);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) { }

        try {
            value.add("f", null);
            fail("Expected NullPointerException");
        }
        catch (NullPointerException e) { }
    }

    public void testStructIteratorRemove()
    {
        IonStruct value = (IonStruct) oneValue("{a:b,c:d,e:f}");
        testIteratorRemove(value);
    }
}
