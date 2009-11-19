// Copyright (c) 2007-2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import java.util.HashMap;
import java.util.Iterator;



public class StructTest
    extends ContainerTestCase
{

    @Override
    protected IonStruct makeNull()
    {
        return system().newNullStruct();
    }

    @Override
    protected IonStruct makeEmpty()
    {
        return system().newEmptyStruct();
    }

    @Override
    protected void add(IonContainer container, IonValue child)
    {
        ((IonStruct) container).add("f", child);
    }

    @Override
    protected IonStruct wrapAndParse(String... children)
    {
        StringBuilder buf = new StringBuilder();
        buf.append('{');
        if (children != null)
        {
            for (int i = 0; i < children.length; i++)
            {
                buf.append('f');
                buf.append(i);
                buf.append(':');
                buf.append(children[i]);
                buf.append(',');
            }
        }
        buf.append('}');
        String text = buf.toString();
        return (IonStruct) system().singleValue(text);
    }

    @Override
    protected IonStruct wrap(IonValue... children)
    {
        IonStruct value = system().newNullStruct();
        for (IonValue child : children)
        {
            value.add("f", child);
        }
        return value;
    }

    public void checkNullStruct(IonStruct value)
    {
        checkNullStruct(value, "");
    }

    public void checkNullStruct(IonStruct value, String annotationText)
    {
        assertSame(IonType.STRUCT, value.getType());

        checkNullContainer(value);

        assertNull(value.get("f"));
        try
        {
            value.get(null);
            fail("Expected NullPointerException");
        }
        catch (NullPointerException e) { }

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
        assertArrayEquals(new String[0], value.getTypeAnnotations());
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

        IonDatagram dg2 = reload(dg);
        IonStruct v2 = (IonStruct) dg2.get(0);
        IonValue f = v2.get("f");
        checkString("this is a string to overlap", f);
    }

    public void testContainsKey()
    {
        IonStruct value = struct("{a:b}");
        assertTrue(value.containsKey("a"));
        assertFalse(value.containsKey("b"));
        testBadContainsKey(value);

        value = struct("{}");
        testBadContainsKey(value);

        value = struct("null.struct");
        testBadContainsKey(value);
    }

    private void testBadContainsKey(IonStruct value)
    {
        try {
            value.containsKey(null);
            fail("expected exception");
        }
        catch (NullPointerException e) { }

        try {
            value.containsKey(12);
            fail("expected exception");
        }
        catch (ClassCastException e) { }
    }

    public void testContainsValue()
    {
        IonStruct value = struct("{a:b}");
        IonValue b = value.get("a");
        assertTrue(value.containsValue(b));
        assertFalse(value.containsValue(b.clone()));
        testBadContainsValue(value);

        value = struct("{}");
        testBadContainsValue(value);

        value = struct("null.struct");
        testBadContainsValue(value);
    }

    private void testBadContainsValue(IonStruct value)
    {
        try {
            value.containsValue(null);
            fail("expected exception");
        }
        catch (NullPointerException e) { }

        try {
            value.containsValue(12);
            fail("expected exception");
        }
        catch (ClassCastException e) { }
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

    public void testPutAll()
    {
        IonStruct value = struct("{}");
        HashMap<String,IonValue> m = new HashMap<String,IonValue>();
        m.put("a", system().newInt(1));
        m.put("b", system().newInt(2));

        value.putAll(m);
        assertEquals(2, value.size());
        assertSame(m.get("a"), value.get("a"));
        assertSame(m.get("b"), value.get("b"));

        try {
            value.putAll(m);
            fail("expected exception");
        }
        catch (ContainedValueException e) { }

        try {
            value.putAll(null);
            fail("expected exception");
        }
        catch (NullPointerException e) { }

        m.clear();
        m.put("a", null);
        value.putAll(m);
        assertEquals(null, value.get("a"));
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

    public void testGetFromNull()
    {
        IonValue n = system().newNull();
        IonStruct s = system().newEmptyStruct();
        s.put("f", n);

        assertSame(n, s.get("f"));
        assertNull(s.get("g"));

        s.makeNull();
        assertNull(s.get("f"));
        assertNull(s.get("g"));
    }

    public void testPutNull()
    {
        IonStruct value = system().newNullStruct();
        value.put("f", null);
        assertTrue(value.isNullValue());

        value.clear();
        value.put("f", null);
        assertTrue(value.isEmpty());

        value.put("f", system().newInt(1));
        value.put("f", null);
        assertTrue(value.isEmpty());

        value.put("g", system().newInt(1));
        value.put("f", null);
        assertEquals(1, value.size());

        value.add("f", system().newInt(2));
        value.add("f", system().newInt(3));
        value.put("f", null);
        assertEquals(1, value.size());
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
    }

    public void testBadAdds()
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


    public void testClearRemovesChildsContainer()
    {
        IonValue val = system().newString("test");
        IonStruct s = system().newNullStruct();
        s.put("f", val);
        s.clear();
        assertNull("Removed value should have null container",
                   val.getContainer());
    }

    public void testMakeNullRemovesChildsContainer()
    {
        IonValue val = system().newString("test");
        IonStruct s = system().newNullStruct();
        s.put("f", val);
        s.makeNull();
        assertNull("Removed value should have null container",
                   val.getContainer());
    }

    public void testRemoveAfterClone()
    {
        IonStruct s1 = (IonStruct) oneValue("{a:1,b:2}");
        IonStruct s2 = system().clone(s1);

        IonValue v = s2.get("b");
        s2.remove(v);
        assertNull(s2.get("b"));
    }

    public void testPutOfClone()
    {
        IonStruct s = system().newEmptyStruct();

        IonList v1 = system().newEmptyList();
        IonList v2 = system().clone(v1);
        s.put("f", v2);

        v1 = system().newList(system().newInt(12));
        v1.addTypeAnnotation("hi");
        v2 = system().clone(v1);
        s.put("g", v2);
    }


    public void testRemove()
    {
        IonStruct s = (IonStruct) oneValue("{a:1,b:2,b:3,c:4}");

        IonValue v = s.remove("c");
        checkInt(4, v);
        assertEquals(3, s.size());
        assertNull(s.get("c"));

        v = s.remove("q");
        assertNull(v);
        assertEquals(3, s.size());
        assertNull(s.get("q"));

        v = s.remove("a");
        checkInt(1, v);

        v = s.remove("b");
        assertTrue(v.getType() == IonType.INT);
        assertEquals(1, s.size());
        long leftoverB = ((IonInt)s.get("b")).longValue();
        assertTrue(leftoverB == 2 || leftoverB == 3);

        try {
            s.remove((String) null);
            fail("Expected NullPointerException");
        }
        catch (NullPointerException e) { }

        s = system().newEmptyStruct();
        assertNull(s.remove("something"));
        assertTrue(s.isEmpty());
        assertFalse(s.isNullValue());

        s = system().newNullStruct();
        assertNull(s.remove("something"));
        assertTrue(s.isNullValue());
    }

    public void testRemoveOnReadOnlyStruct()
    {
        IonStruct s = (IonStruct) oneValue("{a:1}");
        s.makeReadOnly();

        try {
            s.remove("a");
            fail("expected exception");
        }
        catch (ReadOnlyValueException e) { }
        checkInt(1, s.get("a"));
    }

    public void testRemoveAll()
    {
        IonStruct s = (IonStruct) oneValue("{a:1,b:2,b:3,c:4,d:5}");

        boolean changed = s.removeAll("d");
        assertTrue(changed);
        assertEquals(4, s.size());
        assertNull(s.get("d"));

        changed = s.removeAll("q");
        assertFalse(changed);
        assertEquals(4, s.size());
        assertNull(s.get("d"));

        changed = s.removeAll("b", "a");
        assertTrue(changed);
        assertEquals(1, s.size());
        checkInt(4, s.get("c"));

        try {
            s.removeAll((String[]) null);
            fail("Expected NullPointerException");
        }
        catch (NullPointerException e) { }

        try {
            s.removeAll("1", null, "2");
            fail("Expected NullPointerException");
        }
        catch (NullPointerException e) { }

        s = system().newEmptyStruct();
        changed = s.removeAll((String[]) null);
        assertFalse(changed);
        assertTrue(s.isEmpty());
        assertFalse(s.isNullValue());

        s = system().newNullStruct();
        changed = s.removeAll((String[]) null);
        assertFalse(changed);
        assertTrue(s.isNullValue());
    }

    public void testRemoveAllOnReadOnlyStruct()
    {
        IonStruct s = (IonStruct) oneValue("{a:1}");
        s.makeReadOnly();

        try {
            s.removeAll("a");
            fail("expected exception");
        }
        catch (ReadOnlyValueException e) { }
        checkInt(1, s.get("a"));
    }

    public void testRetainAll()
    {
        IonStruct s = (IonStruct) oneValue("{a:1,b:2,b:3,c:4,d:5}");

        boolean changed = s.retainAll("d", "b", "a", "c");
        assertFalse(changed);
        assertEquals(5, s.size());

        changed = s.retainAll("b", "c");
        assertTrue(changed);
        assertEquals(3, s.size());
        assertNull(s.get("a"));
        assertNull(s.get("d"));

        changed = s.retainAll("b", "c");
        assertFalse(changed);

        changed = s.retainAll("b", "a");
        assertTrue(changed);
        assertEquals(2, s.size());

        try {
            s.retainAll((String[]) null);
            fail("Expected NullPointerException");
        }
        catch (NullPointerException e) { }

        try {
            s.retainAll("1", null, "2");
            fail("Expected NullPointerException");
        }
        catch (NullPointerException e) { }

        s = system().newEmptyStruct();
        changed = s.retainAll("holla");
        assertFalse(changed);
        assertTrue(s.isEmpty());
        assertFalse(s.isNullValue());

        s = system().newNullStruct();
        changed = s.removeAll("yo");
        assertFalse(changed);
        assertTrue(s.isNullValue());
    }

    public void testRetainAllOnReadOnlyStruct()
    {
        IonStruct s = (IonStruct) oneValue("{a:1}");
        s.makeReadOnly();

        try {
            s.retainAll("X");
            fail("expected exception");
        }
        catch (ReadOnlyValueException e) { }
        checkInt(1, s.get("a"));
    }

    public void testRemoveViaIteratorThenDirect()
    {
        IonStruct s = system().newEmptyStruct();
        IonInt v0 = system().newInt(0);
        IonInt v1 = system().newInt(1);
        s.put("a", v0);
        s.put("b", v1);

        // Remove first elt via iteration, then second directly
        Iterator<IonValue> i = s.iterator();
        assertSame(v0, i.next());
        i.remove();
        assertSame(v1, i.next());
        s.remove(v1);

        // Remove second elt via iteration, then first directly
        s.put("a", v0);
        s.put("b", v1);
        i = s.iterator();
        i.next();
        i.next();
        i.remove();
        s.remove(v0);
    }

    public void testPutMaintainsIndexes()
    {
        IonStruct s = system().newEmptyStruct();
        s.add("a", system().newInt(1));
        s.add("b", system().newNull());
        s.add("a", system().newInt(2));
        assertEquals(3, s.size());  // repeated field

        // This removes both fields above.  Ensure we don't screw up b's index
        s.put("a", system().newNull());
        s.remove(s.get("b"));
    }

    public void testBinaryStruct()
    {
        IonSystem ionSystem = system();

        IonStruct s1 = (IonStruct)ionSystem.singleValue("{a:0,b:1,j:{type:\"item\",region:\"NA\"}}");
        String    i1 = s1.toString();
        //System.out.println(i1);

        IonStruct s2 = (IonStruct)s1.get("j");
        IonStruct s3 = ionSystem.clone(s2);
        s1.put("j", s3);
        String i2 = s1.toString();
        //System.out.println(i2);

        // what happened to j?
        IonDatagram dg = ionSystem.newDatagram(s1);
        // Do this before toString, ensuring we have local symtab
        byte[] bytes = dg.getBytes();

        String i3 = dg.toString();
        //System.out.println(i3);

        IonLoader loader = ionSystem.getLoader();
        IonDatagram v = loader.load(bytes);
        assertIonEquals(s1, v.get(0));
        String i4 = v.toString();
        //System.out.println(i4);

        assertEquals(i1, i2);
//        assertEquals(i2, i3);  // Not true, i3 has system stuff
        assertEquals(i3, i4);
    }

    public void testStructClone()
        throws Exception
    {
        testSimpleClone("null.struct");
        testSimpleClone("{}");

        testSimpleClone("{f:{{}}}");           // blob
        testSimpleClone("{f:true}");           // bool
        testSimpleClone("{f:{{\"\"}}}");       // clob
        testSimpleClone("{f:1.}");             // decimal
        testSimpleClone("{f:1e0}");            // float
        testSimpleClone("{f:1}");              // int
        testSimpleClone("{f:[]}");             // list
        testSimpleClone("{f:null}");           // null
        testSimpleClone("{f:()}");             // sexp
        testSimpleClone("{f:\"s\"}");          // string
        testSimpleClone("{f:{}}");             // struct
        testSimpleClone("{f:sym}");            // symbol
        testSimpleClone("{f:2008-07-11T14:49:26.000-07:00}"); // timestamp
    }

    public void testClonedFieldHasNoName()
    {
        IonStruct s = (IonStruct) oneValue("{f:12}");
        IonValue f = s.get("f");
        assertEquals("f", f.getFieldName());
        assertTrue(0 < f.getFieldId());

        IonValue clone = f.clone();
        assertNull("field name shouldn't be cloned", clone.getFieldName());
        assertTrue(clone.getFieldId() < 1);
    }


    public void testPutFactory()
    {
        IonStruct s = system().newNullStruct();

        IonInt i = s.put("f").newInt(23);
        checkInt(23, i);
        assertSame(i, s.get("f"));

        IonString str = s.put("f").newString("g");
        checkString("g", str);
        assertSame(str, s.get("f"));
    }


    public void testAddFactory()
    {
        IonStruct s = system().newNullStruct();

        IonInt i = s.add("f").newInt(23);
        checkInt(23, i);
        assertSame(i, s.get("f"));

        IonString str = s.add("f").newString("g");
        checkString("g", str);
        assertEquals(2, s.size());

        IonValue f = s.get("f");
        assertTrue((f == i) || (f == str));
    }

    public void testReplacingReadOnlyChild()
    {
        IonStruct c = system().newEmptyStruct();

        IonNull n1 = system().newNull();
        c.put("f", n1);
        n1.makeReadOnly();

        IonNull n2 = system().newNull();

        try
        {
            c.put("f", n2);
            fail("expected exception");
        }
        catch (ReadOnlyValueException e) { }

        assertSame(n1, c.get("f"));
        assertSame(c, n1.getContainer());
        assertSame(n1, c.iterator().next());

        assertEquals(null, n2.getContainer());
    }
}
