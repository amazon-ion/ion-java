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

import static software.amazon.ion.CloneTest.DEFECTIVE_CLONE_OF_UNKNOWN_ANNOTATION_TEXT;
import static software.amazon.ion.SymbolTable.UNKNOWN_SYMBOL_ID;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.Random;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import software.amazon.ion.ContainedValueException;
import software.amazon.ion.IonBool;
import software.amazon.ion.IonContainer;
import software.amazon.ion.IonDatagram;
import software.amazon.ion.IonInt;
import software.amazon.ion.IonList;
import software.amazon.ion.IonNull;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonString;
import software.amazon.ion.IonStruct;
import software.amazon.ion.IonSymbol;
import software.amazon.ion.IonSystem;
import software.amazon.ion.IonType;
import software.amazon.ion.IonValue;
import software.amazon.ion.ReadOnlyValueException;
import software.amazon.ion.SymbolToken;
import software.amazon.ion.UnknownSymbolException;
import software.amazon.ion.impl.PrivateIonValue;
import software.amazon.ion.impl.PrivateUtils;


public class StructTest
    extends ContainerTestCase
{
    @Rule
    public ExpectedException thrown = ExpectedException.none();


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
    protected String wrap(String... children)
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
        return buf.toString();
    }

    @Override
    protected IonStruct wrapAndParse(String... children)
    {
        String text = wrap(children);
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

    @Test
    public void testPutAfterUnknownFieldName()
    {
        IonStruct value = struct("{$99:null}");
        value.put("hi", system().newNull());
    }

    //=========================================================================
    // Test cases

    @Test
    public void testFactoryNullStruct()
    {
        IonStruct value = system().newNullStruct();
        assertNull(value.getFieldName());
        assertNull(value.getContainer());
        checkNullStruct(value);
        modifyStruct(value);
    }

    @Test
    public void testTextNullStruct()
    {
        IonStruct value = (IonStruct) oneValue("null.struct");
        assertSame(IonType.STRUCT, value.getType());
        checkNullStruct(value);
        modifyStruct(value);
    }

    @Test
    public void testTextAnnotatedNullStruct()
    {
        IonStruct value = (IonStruct) oneValue("test::null.struct");
        assertSame(IonType.STRUCT, value.getType());
        checkNullStruct(value, "test::");
        modifyStruct(value);
    }

    @Test
    public void testMakeNullStruct()
    {
        IonStruct value = (IonStruct) oneValue("{foo:bar}");
        assertSame(IonType.STRUCT, value.getType());
        assertFalse(value.isNullValue());
        value.makeNull();
        checkNullStruct(value);
    }

    @Test
    public void testClearNonMaterializedStruct()
    {
        IonStruct value = (IonStruct) oneValue("{foo:bar}");
        testClearContainer(value);
    }

    @Test
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

    @Test
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
    @Test
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

    @Test
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

    @Test
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

    @Test
    public void testGetTwiceReturnsSame()
    {
        IonStruct value = (IonStruct) oneValue("{a:b}");
        IonValue fieldA1 = value.get("a");
        IonValue fieldA2 = value.get("a");
        assertSame(fieldA1, fieldA2);
    }

    @Test
    public void testDeepPut()
    {
        IonStruct value = (IonStruct) oneValue("{a:{b:bv}}");
        IonStruct nested = (IonStruct) value.get("a");
        IonBool inserted = system().newNullBool();
        nested.put("c", inserted);
        assertSame(inserted, ((IonStruct)value.get("a")).get("c"));
    }

    @Test
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


    @Test
    public void testExtraCommas()
    {
        IonStruct value = (IonStruct) oneValue("{a:b,}");
        assertEquals(1, value.size());
        assertEquals("{a:b}", value.toString());

        // Leading and lonely commas not allowed.
        badValue("{,}");
        badValue("{,3}");
    }


    @Test
    public void testLongFieldName()
    {
        IonStruct value = (IonStruct) oneValue("{ '''a''' : b}");
        assertEquals(1, value.size());
        IonSymbol fieldA = (IonSymbol) value.get("a");
        assertEquals("a", fieldA.getFieldName());
        checkSymbol("b", fieldA);
        assertEquals("{a:b}", value.toString());
    }

    @Test
    public void testConcatenatedFieldName()
    {
        IonStruct value = (IonStruct) oneValue("{ '''a''' '''a''' : b}");
        assertEquals(1, value.size());
        IonSymbol fieldA = (IonSymbol) value.get("aa");
        assertEquals("aa", fieldA.getFieldName());
        checkSymbol("b", fieldA);
        assertEquals("{aa:b}", value.toString());
    }


    @Test
    public void testMediumSymbolFieldNames()
    {
        IonStruct value = (IonStruct) oneValue("{'123456789ABCDEF':b}");
        assertEquals(1, value.size());
        IonSymbol fieldA = (IonSymbol) value.get("123456789ABCDEF");
        assertEquals("123456789ABCDEF", fieldA.getFieldName());
        assertEquals("123456789ABCDEF", fieldA.getFieldName());
        checkSymbol("b", fieldA);
    }


    @Test
    public void testMediumStringFieldNames()
    {
        IonStruct value = (IonStruct) oneValue("{\"123456789ABCDEF\":b}");
        assertEquals(1, value.size());
        IonSymbol fieldA = (IonSymbol) value.get("123456789ABCDEF");
        assertEquals("123456789ABCDEF", fieldA.getFieldName());
        checkSymbol("b", fieldA);
    }


    @Test
    public void testNewlineInStringFieldName()
    {
        badValue("{ \"123\n4\" : v }");
        // Get beyond the 13-char threshold.
        badValue("{ \"123456789ABCDEF\nGHI\" : v }");
    }


    @Test
    public void testNewlineInLongStringFieldName()
    {
        IonStruct value = (IonStruct) oneValue("{ '''123\n4''' : v }");
        checkSymbol("v", value.get("123\n4"));

        // Get beyond the 13-char threshold.
        value = (IonStruct) oneValue("{ '''123456789ABCDEF\nGHI''' : v }");
        checkSymbol("v", value.get("123456789ABCDEF\nGHI"));
    }

    @Test
    public void testConcatenatedFieldValue()
    {
        IonStruct value = (IonStruct) oneValue("{a:'''a''' '''a'''}");
        assertEquals(1, value.size());
        IonString fieldA = (IonString) value.get("a");
        assertEquals("a", fieldA.getFieldName());
        assertEquals("aa",  fieldA.stringValue());
        assertEquals("{a:\"aa\"}", value.toString());
    }

    @Test
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

    @Test
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

    @Test
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

    @Test
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

    @Test
    public void testBadAdds()
    {
        IonStruct value = system().newNullStruct();
        IonBool nullBool = system().newNullBool();

        try {
            value.add((String) null, nullBool);
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


    @Test
    public void testAddSymbolTokenWithBadSid()
    {
        IonStruct struct = system().newNullStruct();
        IonBool nullBool = system().newNullBool();

        SymbolToken is = PrivateUtils.newSymbolToken("f", 1);
        struct.add(is, nullBool);
        is = nullBool.getFieldNameSymbol();
        checkSymbol("f", UNKNOWN_SYMBOL_ID, is);

        nullBool = system().newNullBool();
        is = new FakeSymbolToken("g", 99);
        struct.add(is, nullBool);
        is = nullBool.getFieldNameSymbol();
        checkSymbol("g", UNKNOWN_SYMBOL_ID, is);

        IonBool nullBool2 = system().newNullBool();
        is = new FakeSymbolToken("h", -2);
        struct.add(is, nullBool2);
        is = nullBool2.getFieldNameSymbol();
        checkSymbol("h", UNKNOWN_SYMBOL_ID, is);
    }

    @Test
    public void testBadAddSymbolToken()
    {
        IonStruct value = system().newNullStruct();
        IonBool nullBool = system().newNullBool();

        try {
            value.add((SymbolToken) null, nullBool);
            fail("Expected NullPointerException");
        }
        catch (NullPointerException e) { }

        SymbolToken is = PrivateUtils.newSymbolToken("f", 1);
        try {
            value.add(is, null);
            fail("Expected NullPointerException");
        }
        catch (NullPointerException e) { }

        IonValue contained = value.add("g").newNull();
        try {
            value.add(is, contained);
            fail("Expected exception");
        }
        catch (ContainedValueException e) { }

        IonValue dg = system().newDatagram();
        try {
            value.add(is, dg);
            fail("Expected exception");
        }
        catch (IllegalArgumentException e) { }

        is = new FakeSymbolToken(null, UNKNOWN_SYMBOL_ID);
        try {
            value.add(is, nullBool);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) { }

        is = new FakeSymbolToken(null, -2);
        try {
            value.add(is, nullBool);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) { }

        value.remove(contained);
        assertTrue(value.isEmpty());
    }

    @Test
    public void testStructIteratorRemove()
    {
        IonStruct value = (IonStruct) oneValue("{a:b,c:d,e:f}");
        testIteratorRemove(value);
    }


    @Test
    public void testClearRemovesChildsContainer()
    {
        IonValue val = system().newString("test");
        IonStruct s = system().newNullStruct();
        s.put("f", val);
        s.clear();
        assertNull("Removed value should have null container",
                   val.getContainer());
    }

    @Test
    public void testMakeNullRemovesChildsContainer()
    {
        IonValue val = system().newString("test");
        IonStruct s = system().newNullStruct();
        s.put("f", val);
        s.makeNull();
        assertNull("Removed value should have null container",
                   val.getContainer());
    }

    @Test
    public void testRemoveAfterClone()
    {
        IonStruct s1 = (IonStruct) oneValue("{a:1,b:2}");
        IonStruct s2 = system().clone(s1);

        IonValue v = s2.get("b");
        s2.remove(v);
        assertNull(s2.get("b"));
    }

    @Test
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


    @Test
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

    public void testRemoveClearsFieldName(IonStruct s)
    {
        IonValue f = s.get("f");

        f.removeFromContainer();
        assertEquals(null, f.getFieldName());
        assertEquals(null, f.getFieldNameSymbol());
    }

    @Test
    public void testRemoveClearsFieldName()
    {
        IonDatagram dg = loader().load("{f:null}");
        dg = reload(dg);
        byte[] bytes = dg.getBytes(); // Force symbol interning

        IonStruct s = (IonStruct) dg.get(0);
        testRemoveClearsFieldName(s);

        IonReader r = system().newReader(bytes);
        r.next();
        s = (IonStruct) system().newValue(r);
        testRemoveClearsFieldName(s);
    }


    @Test
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

    @Test
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

    @Test
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

    @Test
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

    @Test
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

    @Test
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

    @Test
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

    @Test
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

    @Test
    public void testClonedFieldHasNoFieldName()
    {
        IonStruct origStruct = (IonStruct) oneValue("{f:12}");
        IonValue origStructField = origStruct.get("f");
        assertEquals("f", origStructField.getFieldName());

        IonValue clone = origStructField.clone();
        assertNull("field name shouldn't be cloned", clone.getFieldName());
        assertTrue(clone.getFieldNameSymbol() == null);
    }


    @Test
    public void testPutFactory()
    {
        IonStruct s = system().newNullStruct();

        IonInt i = s.put("f").newInt(23);
        checkInt(23, i);
        assertSame(i, s.get("f"));

        IonString str = s.put("f").newString("g");
        checkString("g", str);
        assertSame(str, s.get("f"));

        IonString str2 = s.put("h").clone(str);
        assertNotSame(str, str2);
        assertEquals(str, str2);
        assertEquals(2, s.size());
        checkString("g", str2);
        assertSame(str, s.get("f"));
        assertSame(str2, s.get("h"));
    }


    @Test
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

    @Test
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

    //-------------------------------------------------------------------------

    @Test
    public void testCloneAndRemove()
    {
        IonStruct s1 = struct("a::b::{c:1,d:2,e:3,d:3}");
        IonStruct actual = s1.cloneAndRemove("c", "e");
        IonStruct expected = struct("a::b::{d:2,d:3}");
        assertEquals(expected, actual);

        assertEquals(struct("a::b::{}"), actual.cloneAndRemove("d"));

        IonStruct n = struct("x::y::null.struct");
        IonStruct n2 = n.cloneAndRemove("a");
        assertEquals(struct("x::y::null.struct"), n2);
    }

    @Test
    public void testCloneAndRemoveWithSpecialFieldNames()
    {
        IonStruct s1 = struct("{c:1,$99:2,'$19':3,'$20':3}");

        // Unlike cloneAndRetain(), cloneAndRemove() allows null args since it
        // makes sense to request removal of unknown field names.
        IonStruct actual = s1.cloneAndRemove("$19", null);
        IonStruct expected = struct("{c:1,'$20':3}");
        assertEquals(expected, actual);

        assertEquals(struct("{}"), actual.cloneAndRemove("$20", "c"));

        IonStruct n = struct("x::y::null.struct");
        IonStruct n2 = n.cloneAndRemove("$20");
        assertEquals(struct("x::y::null.struct"), n2);
    }

    @Test
    public void testCloneAndRemoveWithUnknownFieldNameOnRoot()
    {
        IonStruct s1 = struct("{$99:a::{c:1,d:2,d:3}}");

        IonStruct root = (IonStruct) s1.iterator().next();

        IonStruct actual = root.cloneAndRemove("c");
        IonStruct expected = struct("a::{d:2,d:3}");
        assertEquals(expected, actual);
    }

    @Test
    public void testCloneAndRemoveWithUnknownFieldNameOnField()
    {
        IonStruct s1 = struct("a::{$99:1,d:2,e:3,d:3}");

        // OK if the unknown symbol is on a removed node.
        IonStruct actual = s1.cloneAndRemove(null, "e");
        IonStruct expected = struct("a::{d:2,d:3}");
        assertEquals(expected, actual);

        // Not OK if the unknown symbol is on a retained node.
        s1 = struct("a::{c:1,$99:2,e:3,d:3}");
        thrown.expect(UnknownSymbolException.class);
        thrown.expectMessage("$99");
        s1.cloneAndRemove("c", "e");
    }


    @Test
    public void testCloneAndRemoveWithUnknownSymbolTextOnField()
    {
        IonStruct s1 = struct("a::{c:$99,d:2,e:3,d:3}");

        // OK if the unknown symbol is on a removed node.
        IonStruct actual = s1.cloneAndRemove("c", "e");
        IonStruct expected = struct("a::{d:2,d:3}");
        assertEquals(expected, actual);

        // Not OK if the unknown symbol is on a retained node.
        s1 = struct("a::{c:1,d:$99,e:3,d:3}");
        thrown.expect(UnknownSymbolException.class);
        thrown.expectMessage("$99");
        s1.cloneAndRemove("c", "e");
    }

    @Test
    public void testCloneAndRemoveWithUnknownAnnotationTextOnRoot()
    {
        IonStruct s1 = struct("$99::{c:1,d:2,e:3,d:3}");
        if (! DEFECTIVE_CLONE_OF_UNKNOWN_ANNOTATION_TEXT) {
            thrown.expect(UnknownSymbolException.class);
            thrown.expectMessage("$99");
        }
        IonStruct actual = s1.cloneAndRemove("c", "e");
        if (DEFECTIVE_CLONE_OF_UNKNOWN_ANNOTATION_TEXT) {
            // If we don't fail we should at least retain the SID.
            IonStruct expected = struct("$99::{d:2,d:3}");
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testCloneAndRemoveWithUnknownAnnotationTextOnField()
    {
        IonStruct s1 = struct("a::{c:$99::1,d:2,e:3,d:3}");

        // OK if the unknown symbol is on a removed node.
        IonStruct actual = s1.cloneAndRemove("c", "e");
        IonStruct expected = struct("a::{d:2,d:3}");
        assertEquals(expected, actual);

        // Not OK if the unknown symbol is on a retained node.
        s1 = struct("a::{c:1,d:$99::2,e:3}");
        if (! DEFECTIVE_CLONE_OF_UNKNOWN_ANNOTATION_TEXT) {
            thrown.expect(UnknownSymbolException.class);
            thrown.expectMessage("$99");
        }
        actual = s1.cloneAndRemove("c", "e");
        if (DEFECTIVE_CLONE_OF_UNKNOWN_ANNOTATION_TEXT) {
            // If we don't fail we should at least retain the SID.
            expected = struct("a::{d:$99::2}");
            assertEquals(expected, actual);
        }
    }

    //-------------------------------------------------------------------------

    @Test
    public void testCloneAndRetain()
    {
        IonStruct s1 = struct("a::{c:1,d:2,e:3,d:3}");
        IonStruct actual = s1.cloneAndRetain("c", "d");
        IonStruct expected = struct("a::{c:1,d:2,d:3}");
        assertEquals(expected, actual);

        assertEquals(struct("a::{}"), actual.cloneAndRetain("e"));

        IonStruct n = struct("y::null.struct");
        IonStruct n2 = n.cloneAndRetain("a");
        assertEquals(struct("y::null.struct"), n2);

        // Not cool to ask to retain an unknown field name.
        thrown.expect(NullPointerException.class);
        s1.cloneAndRetain("c", null);
    }

    @Test
    public void testCloneAndRetainWithSpecialFieldNames()
    {
        IonStruct s1 = struct("{c:1,$99:2,'$19':3,'$20':3}");
        IonStruct actual = s1.cloneAndRetain("c", "$20");
        IonStruct expected = struct("{c:1,'$20':3}");
        assertEquals(expected, actual);

        assertEquals(struct("{}"), s1.cloneAndRetain("$99"));

        IonStruct n = struct("x::y::null.struct");
        IonStruct n2 = n.cloneAndRetain("$20");
        assertEquals(struct("x::y::null.struct"), n2);
    }


    @Test
    public void testCloneAndRetainWithUnknownFieldNameOnRoot()
    {
        IonStruct s1 = struct("{$99:a::{c:1,d:2,d:3}}");

        IonStruct root = (IonStruct) s1.iterator().next();

        IonStruct actual = root.cloneAndRetain("d");
        IonStruct expected = struct("a::{d:2,d:3}");
        assertEquals(expected, actual);
    }

    @Test
    public void testCloneAndRetainWithUnknownFieldNameOnField()
    {
        IonStruct s1 = struct("a::{$99:1,d:2,e:3,d:3}");

        // OK if the unknown symbol is on a removed node.
        IonStruct actual = s1.cloneAndRetain("d", "e");
        IonStruct expected = struct("a::{d:2,e:3,d:3}");
        assertEquals(expected, actual);

        // Not OK if the unknown symbol is on a retained node.
        s1 = struct("a::{c:1,$99:2,e:3,d:3}");
        thrown.expect(NullPointerException.class);
        s1.cloneAndRetain(null, "e");
    }


    @Test
    public void testCloneAndRetainWithUnknownSymbolTextOnField()
    {
        IonStruct s1 = struct("a::{c:$99,d:2,e:3,d:3}");

        // OK if the unknown symbol is on a removed node.
        IonStruct actual = s1.cloneAndRetain("d");
        IonStruct expected = struct("a::{d:2,d:3}");
        assertEquals(expected, actual);

        // Not OK if the unknown symbol is on a retained node.
        s1 = struct("a::{c:1,d:$99,e:3,d:3}");
        thrown.expect(UnknownSymbolException.class);
        thrown.expectMessage("$99");
        s1.cloneAndRetain("c", "d");
    }

    @Test
    public void testCloneAndRetainWithUnknownAnnotationTextOnRoot()
    {
        IonStruct s1 = struct("$99::{c:1,d:2,e:3,d:3}");
        if (! DEFECTIVE_CLONE_OF_UNKNOWN_ANNOTATION_TEXT) {
            thrown.expect(UnknownSymbolException.class);
            thrown.expectMessage("$99");
        }
        IonStruct actual = s1.cloneAndRetain("c", "e");
        if (DEFECTIVE_CLONE_OF_UNKNOWN_ANNOTATION_TEXT) {
            // If we don't fail we should at least retain the SID.
            IonStruct expected = struct("$99::{c:1,e:3}");
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testCloneAndRetainWithUnknownAnnotationTextOnField()
    {
        IonStruct s1 = struct("a::{c:$99::1,d:2,e:3,d:3}");

        // OK if the unknown symbol is on a removed node.
        IonStruct actual = s1.cloneAndRetain("d");
        IonStruct expected = struct("a::{d:2,d:3}");
        assertEquals(expected, actual);

        // Not OK if the unknown symbol is on a retained node.
        if (! DEFECTIVE_CLONE_OF_UNKNOWN_ANNOTATION_TEXT) {
            thrown.expect(UnknownSymbolException.class);
            thrown.expectMessage("$99");
        }
        actual = s1.cloneAndRetain("c", "e");
        if (DEFECTIVE_CLONE_OF_UNKNOWN_ANNOTATION_TEXT) {
            // If we don't fail we should at least retain the SID.
            expected = struct("a::{c:$99::1,e:3}");
            assertEquals(expected, actual);
        }
    }


    //-------------------------------------------------------------------------


    private static class TestField
    {
        final  String _fieldName;
        final  IonInt _value;
        static int    _next_value = 1;

        TestField(String name, IonSystem sys) {
            _fieldName = name;
            _value = sys.newInt(_next_value++);
        }
        TestField(String name, IonInt value) {
            _fieldName = name;
            _value = value;
        }
        @Override
        public String toString()
        {
            String s = _fieldName+":"+_value.intValue();
            return s;
        }
    }


    static final boolean _debug_print_flag = false;


    static final int COMMAND_EXECUTION_COUNTER_MAX = 10000;
    static final int C_ADD_UNIQUE         =  1;
    static final int C_ADD_DUPLICATE      =  2;
    static final int C_DELETE_UNIQUE      =  3;
    static final int C_DELETE_DUPLICATE   =  4;
    static final int C_COMPARE            =  5;
    static final int C_CHANGE_UNIQUE      =  6;
    static final int C_CHANGE_DUPLICATE   =  7;
    static final int C_PUT_NULL_UNIQUE    =  8;
    static final int C_PUT_NULL_DUPLICATE =  9;
    static final int C_PUT_INVALID        = 10;
    static final int C_PUT_NULL_INVALID   = 11;
    static final int C_DELETE_ITERATOR    = 12;
    static final int C_CLONE              = 13;
    static final int C_CLEAR              = 14;
    static final int COMMAND_MAX          = 14;

    @Test
    public void testRandomChanges()
    {
        IonSystem           sys = system();
        IonStruct            s1 = sys.newEmptyStruct();
        ArrayList<TestField> s2 = new ArrayList<TestField>();

        long seed = System.currentTimeMillis() | System.nanoTime();
        Random r = new java.util.Random(seed);
        TestField field;

        if (_debug_print_flag) {
            System.out.println("testRandomChanges.992: random seed "+seed);
        }

        try {
            for (int command_counter = 0
                ; command_counter < COMMAND_EXECUTION_COUNTER_MAX
                ; command_counter++
            ) {
                int command = pick_command(r, Math.max(s1.size(), s2.size()));
                if (_debug_print_flag) {
                    System.out.println("\nCMD "+command_counter+": "+command_name(command));
                    dump(s1, s2);
                    System.out.println("before ---");
                }
                switch (command) {
                case C_ADD_UNIQUE:
                    field = make_unique(s1);
                    s1.add(field._fieldName, field._value);
                    s2.add(field);
                    break;
                case C_ADD_DUPLICATE:
                    field = make_duplicate(r, sys, s2);
                    if (field != null) {
                        // we can see a null when the struct is empty
                        s1.add(field._fieldName, field._value);
                        s2.add(field);
                    }
                    break;
                case C_DELETE_UNIQUE:
                    field = choose_unique(r, s2);
                    if (field != null) {
                        s1.remove(field._value);
                        s2.remove(field);
                    }
                    break;
                case C_DELETE_DUPLICATE:
                    field = choose_duplicate(r, s2);
                    if (field != null) {
                        s1.remove(field._value);
                        s2.remove(field);
                    }
                    break;
                case C_COMPARE:
                    compare_field_lists(s1, s2, seed);
                    break;
                case C_CHANGE_UNIQUE:
                    field = choose_unique(r, s2);
                    if (field != null) {
                        String fieldName = field._fieldName;
                        TestField other = new TestField(fieldName, sys);
                        s1.put(fieldName, other._value);
                        remove_all_copies(s2, fieldName); // s2.remove(field);
                        s2.add(other);
                    }
                    break;
                case C_CHANGE_DUPLICATE:
                    field = choose_duplicate(r, s2);
                    if (field != null) {
                        String fieldName = field._fieldName;
                        TestField other = new TestField(fieldName, sys);
                        s1.put(fieldName, other._value);
                        remove_all_copies(s2, fieldName);
                        s2.add(other);
                    }
                    break;
                case C_PUT_NULL_UNIQUE:
                    field = choose_unique(r, s2);
                    if (field != null) {
                        String fieldName = field._fieldName;
                        s1.put(fieldName, null);
                        remove_all_copies(s2, fieldName); // s2.remove(field);
                    }
                    break;
                case C_PUT_NULL_DUPLICATE:
                    field = choose_duplicate(r, s2);
                    if (field != null) {
                        String fieldName = field._fieldName;
                        s1.put(fieldName, null);
                        remove_all_copies(s2, fieldName);
                    }
                    break;
                case C_PUT_INVALID:
                    field = make_unique(s1);
                    s1.put(field._fieldName, field._value);
                    s2.add(field);
                    break;
                case C_PUT_NULL_INVALID:
                    field = make_unique(s1);
                    s1.put(field._fieldName, null);
                    // s2. nothing
                    break;
                case C_DELETE_ITERATOR:
                    field = choose_any(r, s2);
                    if (field != null) {
                        int removed = iterator_delete(r, field, s1);
                        int idx = s2.indexOf(field);
                        for (int ii = 0; ii < removed; ii++) {
                            field = s2.get(idx);
                            s2.remove(field);
                        }
                    }
                    break;
                case C_CLONE:
                    IonStruct struct_clone = s1.clone();
                    ArrayList<TestField> array_clone = clone_array_list(s2, s1, struct_clone);
                    s1 = struct_clone;
                    s2 = array_clone;
                    break;
                case C_CLEAR:
                    s1.clear();
                    s2.clear();
                    break;
                default:
                    assertEquals("we've encounterd an unexpeced", "command id"+command);
                    break;
                }
    // check every time
                if (_debug_print_flag) {
                    System.out.println("after ---");
                    dump(s1, s2);
                    System.out.println("---");
                }
                compare_field_lists(s1, s2, seed);
            }
        }
        finally {
         // check every time
            if (_debug_print_flag) {
                System.out.println("FINALLY ---");
                dump(s1, s2);
                System.out.println("---");
            }
        }
    }
    void dump(IonStruct s1_temp, ArrayList<TestField> s2) {
        PrivateIonValue s1 = ((PrivateIonValue)s1_temp);
        s1.dump(new PrintWriter(System.out));
        System.out.println("array: "+s2.toString());
    }
    int pick_command(Random r, int size)
    {
        int cmd;
    loop:
        for (;;) {
            // eventually we'll pick compare if nothing else
            cmd = r.nextInt(COMMAND_MAX);
            switch (cmd) {
            case C_ADD_UNIQUE:
            case C_COMPARE:
            case C_PUT_INVALID:
            case C_PUT_NULL_INVALID:
            case C_CLONE:
            case C_CLEAR:
            default: // who knows?
                break loop;
            case C_ADD_DUPLICATE:
            case C_DELETE_UNIQUE:
            case C_DELETE_DUPLICATE:
            case C_CHANGE_UNIQUE:
            case C_CHANGE_DUPLICATE:
            case C_PUT_NULL_UNIQUE:
            case C_PUT_NULL_DUPLICATE:
            case C_DELETE_ITERATOR:
                if (size > 0) break loop;
                break; // continue;
            }
        }
        return cmd + 1;
    }

    static int _next_field_name_id = 1000;
    TestField make_unique(IonStruct s)
    {
        String name = "F_"+_next_field_name_id;
        _next_field_name_id++;
        TestField field = new TestField(name, s.getSystem());
        if (_debug_print_flag) {
            System.out.println("new unique: "+field.toString());
        }
        return field;
    }
    TestField make_duplicate(Random r, IonSystem sys, ArrayList<TestField> s)
    {
        if (s.size() < 1) {
            if (_debug_print_flag) {
                System.out.println("new duplicate: null");
            }
            return null;
        }

        int idx = r.nextInt(s.size());
        TestField field = s.get(idx);
        assert(field != null);

        TestField dup = new TestField(field._fieldName, sys);
        if (_debug_print_flag) {
            System.out.println("new duplicate: "+dup.toString());
        }
        return dup;
    }
    TestField choose_any(Random r, ArrayList<TestField> s)
    {
        if (s.size() < 1) {
            if (_debug_print_flag) {
                System.out.println("choose any : null");
            }
            return null;
        }

        int idx = r.nextInt(s.size());
        TestField field = s.get(idx);

        if (_debug_print_flag) {
            System.out.println("choose any: "+field);
        }
        return field;
    }
    TestField choose_unique(Random r, ArrayList<TestField> s)
    {
        if (s.size() < 1) {
            if (_debug_print_flag) {
                System.out.println("choose unique : null");
            }
            return null;
        }

        int start = r.nextInt(s.size());
        int end = start - 1;
        if (end < 0) {
            end = s.size() - 1;
        }

        TestField field;
        int idx = start;
        for (;;) {
            TestField temp = s.get(idx);
            if (is_duplicate(s, temp._fieldName, idx) == false) {
                field = temp;
                break;
            }
            idx++;
            if (idx >= s.size()) {
                idx = 0;
            }
            if (idx == end) {
                field = null;
                break;
            }
        }
        if (_debug_print_flag) {
            System.out.println("choose unique: "+field);
        }
        return field;
    }
    TestField choose_duplicate(Random r, ArrayList<TestField> s)
    {
        if (s.size() < 1) {
            if (_debug_print_flag) {
                System.out.println("choose duplicate: null");
            }
            return null;
        }

        int start = r.nextInt(s.size());
        int end = start - 1;
        if (end < 0) {
            end = s.size() - 1;
        }

        TestField field;
        int idx = start;
        for (;;) {
            TestField temp = s.get(idx);
            if (is_duplicate(s, temp._fieldName, idx) == true) {
                field = temp;
                break;
            }
            idx++;
            if (idx >= s.size()) {
                idx = 0;
            }
            if (idx == end) {
                field = null;
                break;
            }
        }
        if (_debug_print_flag) {
            System.out.println("choose duplicate: "+field);
        }
        return field;

    }
    boolean is_duplicate(ArrayList<TestField> s, String fieldName, int orig_idx)
    {
        for (int ii=0; ii<s.size(); ii++) {
            if (ii == orig_idx) {
                continue;
            }
            TestField temp = s.get(ii);
            if (temp._fieldName.equals(fieldName)) {
                return true;
            }
        }
        return false;
    }
    int iterator_delete(Random r, TestField field, IonStruct s1)
    {
        int removed = -1, count = 0;
        Iterator<IonValue> it = s1.iterator();
        while (it.hasNext()) {
            IonValue v = it.next();
            count++;
            if (v == field._value) {
                removed = remove_some(r, s1, count, it);
                return removed;
            }
        }
        assertEquals("the iteration didn't find field ", field._fieldName);
        return -1; // we can't get here, but Eclipse doesn't know that
    }
    int remove_some(Random r, IonStruct s1, int pos, Iterator<IonValue>it)
    {
        int size = s1.size();
        assert(pos <= size);

        int how_many = r.nextInt(size - pos + 1) + 1;
        int removed = 0;

        for(;;) {
            it.remove();
            removed++;
            if (how_many <= removed) {
                break;
            }
            assertTrue(it.hasNext());
            it.next();
        }
        return removed;
    }

    void compare_field_lists(IonStruct s1, ArrayList<TestField> s2, long seed)
    {
        String  errors = "";
        boolean difference = false;

        // quick check - are the sizes the same?
        if (s1.size() != s2.size()) {
            errors += "sizes differ:";
            errors += "struct = "+s1.size();
            errors += " vs array = "+s2.size();
            errors += "\n";
        }

        // first see if every value in the struct is in the array
        Iterator<IonValue> it = s1.iterator();
        int struct_idx = 0;
        while (it.hasNext()) {
            IonValue v = it.next();

            if (find_field(s2, v) == false) {
                difference = true;
                errors += "extra field in struct "+v;
                errors += "\n";
            }
            if (v instanceof PrivateIonValue) {
                int eid = ((PrivateIonValue)v).getElementId();
                if (eid != struct_idx) {
                    difference = true;
                    errors += "index of struct field "+struct_idx+" doesn't match array index "+eid+": "+v;
                    errors += "\n";
                }
            }
            struct_idx++;
        }

        // now check if every value in the array is in the struct
        for (int ii=0; ii<s2.size(); ii++) {
            TestField fld = s2.get(ii);
            IonValue    v = fld._value;
            if (s1.containsValue(v) == false) {
                difference = true;
                errors += "field missing from struct "+v.toString();
                errors += "\n";
            }
        }

        if (_debug_print_flag) {
            // now check the map, if there is one
            PrivateIonValue l = (PrivateIonValue)s1;
            String map_error = l.validate();
            if (map_error != null) {
                errors += map_error;
                difference = true;
            }
        }

        // if so we're equal and everything's ok
        if (difference) {
            if (_debug_print_flag) {
                System.out.println(errors);
            }
            if (!difference) {
                this.compare_field_lists(s1, s2, seed);
            }
            errors += "\nSEED: "+ seed;
            assertFalse(errors, difference);
        }
        return;
    }
    boolean find_field(ArrayList<TestField> s, IonValue v)
    {
        for (int ii=0; ii<s.size(); ii++) {
            TestField f = s.get(ii);
            if (f._value == v) {
                return true;
            }
        }
        return false;
    }
    void remove_all_copies(ArrayList<TestField> s, String fieldName)
    {
        ListIterator<TestField> it = s.listIterator();
        while (it.hasNext()) {
            TestField field = it.next();
            if (field._fieldName.equals(fieldName)) {
                it.remove();
            }
        }
    }

    ArrayList<TestField> clone_array_list(ArrayList<TestField> s2, IonStruct s1, IonStruct struct_clone)
    {
        ArrayList<TestField> array_clone = new ArrayList<TestField>();

        Iterator<IonValue> it1 = s1.iterator();
        Iterator<IonValue> itc = struct_clone.iterator();

        // this tests the comparison between the struct and its clone
        // while is populates a new array with the updated IonValues
        // from the struct clone so that comparisons between the
        // struct clone and array clone match with object identity
        // which is needed for operations like remove from arraylist
        while (it1.hasNext()) {
            assertTrue(itc.hasNext());
            IonValue v1 = it1.next();
            IonValue vc = itc.next();
            assertTrue(v1.equals(vc));
            assertTrue(vc instanceof IonInt);

            int idx = find_index(s2, v1);
            if (idx < 0) {
                assertTrue(idx >= 0);
            }
            TestField f = s2.get(idx);
            TestField fc = new TestField(f._fieldName, (IonInt)vc);
            array_clone.add(fc);
            assertEquals(array_clone.size(), idx + 1);

        }
        assertFalse(itc.hasNext());

        return array_clone;
    }

    int find_index(ArrayList<TestField> s2, IonValue v1)
    {
        for (int idx = 0; idx < s2.size(); idx++) {
            TestField f = s2.get(idx);
            if (f._value == v1) {
                return idx;
            }
        }
        return -1;
    }

    @Test
    public void testMultipleRandomChanges()
    {
        for (int ii=0; ii<20; ii++) {
            testRandomChanges();
        }
    }
    String command_name(int cmd) {
        switch (cmd) {
        case C_ADD_UNIQUE:         return "C_ADD_UNIQUE";
        case C_ADD_DUPLICATE:      return "C_ADD_DUPLICATE";
        case C_DELETE_UNIQUE:      return "C_DELETE_UNIQUE";
        case C_DELETE_DUPLICATE:   return "C_DELETE_DUPLICATE";
        case C_COMPARE:            return "C_COMPARE";
        case C_CHANGE_UNIQUE:      return "C_CHANGE_UNIQUE";
        case C_CHANGE_DUPLICATE:   return "C_CHANGE_DUPLICATE";
        case C_PUT_NULL_UNIQUE:    return "C_PUT_NULL_UNIQUE";
        case C_PUT_NULL_DUPLICATE: return "C_PUT_NULL_DUPLICATE";
        case C_PUT_INVALID:        return "C_PUT_INVALID";
        case C_PUT_NULL_INVALID:   return "C_PUT_NULL_INVALID";
        case C_DELETE_ITERATOR:    return "C_DELETE_ITERATOR";
        case C_CLONE:              return "C_CLONE";
        case C_CLEAR:              return "C_CLEAR";
        default:                   return "<cmd: "+cmd+">";
        }
    }

    @Test
    public void testStructClear() throws Exception
    {
        IonStruct data = (IonStruct) oneValue("{a:1,b:2,c:2,d:3,e:4,f:5,g:6}");
        data.clear();
        data.put("z").newInt(100);
        assertNull(data.get("a"));
    }

    @Test
    public void testStructRemove() throws Exception
    {
        IonStruct data = (IonStruct) oneValue("{a:1,b:2,c:2,d:3,e:4,f:5,g:6}");
        data.remove("d");
        assertEquals(system().newInt(5), data.get("f"));
    }

    @Test
    public void testRemoveIter() throws Exception
    {
        IonStruct data = (IonStruct) oneValue("{a:1,b:2,c:2,d:3,e:4,f:5,g:6}");
        final Iterator<IonValue> iter = data.iterator();
        iter.next();
        iter.remove();
        assertNull(data.get("a"));
        assertNotNull(data.get("b"));
    }

    @Test
    public void testGetContainer() {
        final IonStruct container = system().newEmptyStruct();
        final IonValue child = oneValue("{}");
        container.put("a", child);
        child.getContainer().remove(child);
    }

}
