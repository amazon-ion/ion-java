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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.junit.Test;
import software.amazon.ion.ContainedValueException;
import software.amazon.ion.IonInt;
import software.amazon.ion.IonList;
import software.amazon.ion.IonSystem;
import software.amazon.ion.IonType;
import software.amazon.ion.IonValue;


public class ListTest
    extends TrueSequenceTestCase
{
    @Override
    protected IonList makeNull()
    {
        return system().newNullList();
    }

    @Override
    protected IonList makeEmpty()
    {
        return system().newEmptyList();
    }

    @Override
    protected IonList newSequence(Collection<? extends IonValue> children)
    {
        IonList list = system().newEmptyList();
        list.addAll(children);
        return list;
    }

    @Override
    protected <T extends IonValue> IonList newSequence(T... elements)
    {
        return system().newList(elements);
    }


    @Override
    protected String wrap(String... children)
    {
        StringBuilder buf = new StringBuilder();
        buf.append('[');
        if (children != null)
        {
            for (String child : children)
            {
                buf.append(child);
                buf.append(',');
            }
        }
        buf.append(']');
        return buf.toString();
    }

    @Override
    protected IonList wrapAndParse(String... children)
    {
        String text = wrap(children);
        return (IonList) system().singleValue(text);
    }

    @Override
    protected IonList wrap(IonValue... children)
    {
        return system().newList(children);
    }

    //=========================================================================
    // Test cases

    @Test
    public void testFactoryNullList()
    {
        IonList value = system().newNullList();
        assertSame(IonType.LIST, value.getType());
        assertNull(value.getContainer());
        testFreshNullSequence(value);
    }

    @Test
    public void testTextNullList()
    {
        IonList value = (IonList) oneValue("null.list");
        assertSame(IonType.LIST, value.getType());
        testFreshNullSequence(value);
    }

    @Test
    public void testMakeNullList()
    {
        IonList value = (IonList) oneValue("[6,9]");
        assertSame(IonType.LIST, value.getType());
        assertFalse(value.isNullValue());
        value.makeNull();
        testFreshNullSequence(value);
    }

    @Test
    public void testClearNonMaterializedList()
    {
        IonList value = (IonList) oneValue("[6,9]");
        testClearContainer(value);
    }

    @Test
    public void testEmptyList()
    {
        IonList value = (IonList) oneValue("[]");
        assertSame(IonType.LIST, value.getType());
        testEmptySequence(value);
    }


    @Test
    public void testListParsing()
    {
        IonValue nine = oneValue("9");
        IonValue four = oneValue("4");

        IonList value = (IonList) oneValue("[9,4]");
        assertEquals(2, value.size());
        assertEquals(nine, value.get(0));
        assertEquals(four, value.get(1));

        try
        {
            value.get(2);
            fail("Expected IndexOutOfBoundsException");
        }
        catch (IndexOutOfBoundsException e) { }

        Iterator<IonValue> i = value.iterator();
        assertTrue(i.hasNext());
        assertEquals(nine, i.next());
        assertTrue(i.hasNext());
        assertEquals(four, i.next());
        assertFalse(i.hasNext());
    }


    @Test
    public void testGetTwiceReturnsSame()
    {
        IonList value = (IonList) oneValue("[a,b]");
        IonValue elt1 = value.get(1);
        IonValue elt2 = value.get(1);
        assertSame(elt1, elt2);
    }

    @Test
    public void testRemovalFromParsedWithAnnot()
    {
        IonList value = (IonList) oneValue("[a::4]");
        IonInt four = (IonInt) value.get(0);

        // Don't touch anything until we remove it!

        value.remove(four);
        assertEquals(4, four.intValue());
        checkAnnotation("a", four);
    }


    /**
     * Ensure that removal from a parsed list maintains its state.
     * It can no longer share the same backing store.
     */
    @Test
    public void testRemoveFromParsedListKeepsState()
    {
        IonList value = (IonList) oneValue("[1]");
        IonInt elt = (IonInt) value.get(0);
        value.remove(elt);
        assertNull(elt.getContainer());
        assertEquals(1, elt.intValue());
    }

    /** Ensure that triple-quote concatenation works inside lists. */
    @Test
    public void testConcatenation()
    {
        IonList value = (IonList) oneValue("[a, '''a''' '''b''', '''c''']");
        checkSymbol("a",  value.get(0));
        checkString("ab", value.get(1));
        checkString("c",  value.get(2));
        assertEquals(3, value.size());
    }

    @Test
    public void testListIteratorRemove()
    {
        IonList value = (IonList) oneValue("[a,b,c]");
        testIteratorRemove(value);
    }

    @Test
    public void testCreatingNullList()
    {
        IonList list1 = system().newNullList();
        IonValue list2 = reload(list1);

        // FIXME ensure list1._isPositionLoaded && _isMaterialized
        assertEquals(list1, list2);
    }

    @Test
    public void testCreatingListFromCollection()
    {
        IonSystem system = system();
        List<IonValue> elements = null;

        IonList v;
        try {
            v = newSequence(elements);
            fail("Expected NullPointerException");
        }
        catch (NullPointerException e) {}

        elements = new ArrayList<IonValue>();
        v = newSequence(elements);
        testEmptySequence(v);

        elements.add(system.newString("hi"));
        elements.add(system.newInt(1776));
        v = newSequence(elements);
        assertEquals(2, v.size());
        checkString("hi", v.get(0));
        checkInt(1776, v.get(1));

        try {
            v = newSequence(elements);
            fail("Expected ContainedValueException");
        }
        catch (ContainedValueException e) { }

        elements = new ArrayList<IonValue>();
        elements.add(system.newInt(1776));
        elements.add(null);
        try {
            newSequence(elements);
            fail("Expected NullPointerException");
        }
        catch (NullPointerException e) { }
    }

    @Test
    public void testCreatingListFromIntArray()
    {
        IonSystem system = system();
        int[] elements = null;

        IonList v = system.newList(elements);
        testFreshNullSequence(v);

        elements = new int[0];
        v = system.newList(elements);
        testEmptySequence(v);

        elements = new int[]{ 12, 13, 14 };
        v = system.newList(elements);
        assertEquals(3, v.size());
        checkInt(12, v.get(0));
        checkInt(13, v.get(1));
        checkInt(14, v.get(2));
        elements = new int[]{ 12, 13, 14 };
    }

    @Test
    public void testCreatingListFromLongArray()
    {
        IonSystem system = system();
        long[] elements = null;

        IonList v = system.newList(elements);
        testFreshNullSequence(v);

        elements = new long[0];
        v = system.newList(elements);
        testEmptySequence(v);

        elements = new long[]{ 12, 13, 14 };
        v = system.newList(elements);
        assertEquals(3, v.size());
        checkInt(12, v.get(0));
        checkInt(13, v.get(1));
        checkInt(14, v.get(2));
    }

    @Test
    public void testCreatingListFromValueArray()
    {
        IonSystem system = system();
        IonValue[] elements = null;

        IonList v = system.newList(elements);
        testFreshNullSequence(v);

        elements = new IonValue[0];
        v = system.newList(elements);
        testEmptySequence(v);

        elements = new IonValue[]{ system.newInt(12), system.newString("hi") };
        v = system.newList(elements);
        assertEquals(2, v.size());
        checkInt(12, v.get(0));
        checkString("hi", v.get(1));

        try {
            v = system.newList(elements);
            fail("Expected ContainedValueException");
        }
        catch (ContainedValueException e) { }

        // varargs usage
        v = system.newList(system.newInt(12), system.newString("hi"));
        assertEquals(2, v.size());
        checkInt(12, v.get(0));
        checkString("hi", v.get(1));

        try {
            v = system.newList(system.newInt(12), null);
            fail("Expected NullPointerException");
        }
        catch (NullPointerException e) { }
    }

    @Test
    public void testCreatingListWithString()
    {
        IonList list1 = system().newNullList();
        list1.add(system().newString("Hello"));

        IonValue list2 = reload(list1);

        assertEquals(list1, list2);

        // Again, starting from [] instead of null.list
        list1 = system().newEmptyList();
        list1.add(system().newString("Hello"));

        list2 = reload(list1);
        assertEquals(list1, list2);
    }
}
