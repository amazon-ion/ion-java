// Copyright (c) 2007-2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;



public abstract class SequenceTestCase
    extends ContainerTestCase
{
    /**
     * @return a new null sequence.
     */
    @Override
    protected abstract IonSequence makeNull();

    @Override
    protected abstract IonSequence makeEmpty();

    protected abstract
    IonSequence newSequence(Collection<? extends IonValue> children);

    protected abstract
    <T extends IonValue> IonSequence newSequence(T... elements);


    @Override
    protected void add(IonContainer container, IonValue child)
    {
        ((IonSequence) container).add(child);
    }


    /**
     * Wrap a single value with a sequence of the class under test.
     */
    protected abstract String wrap(String v);

    public void checkNullSequence(IonSequence value)
    {
        checkNullContainer(value);

        try
        {
            value.get(0);
            fail("Expected NullValueException");
        }
        catch (NullValueException e) { }
    }


    public void modifySequence(IonSequence value)
    {
        IonBool nullBool0 = system().newNullBool();
        value.add(nullBool0);
        assertNull(nullBool0.getFieldName());
        assertSame(value, nullBool0.getContainer());
        assertEquals("size", 1, value.size());
        assertSame(nullBool0, value.get(0));

        IonBool nullBool1 = system().newNullBool();
        value.add(nullBool1);
        assertNull(nullBool1.getFieldName());
        assertSame(value, nullBool1.getContainer());
        assertEquals("size", 2, value.size());
        assertSame(nullBool0, value.get(0));
        assertSame(nullBool1, value.get(1));

        try
        {
            value.add(nullBool0);
            fail("Expected ContainedValueException");
        }
        catch (ContainedValueException e) { }
        // Make sure the element hasn't changed
        assertNull(nullBool0.getFieldName());
        assertSame(value, nullBool0.getContainer());

        // Cannot append a datagram
        try
        {
            IonDatagram dg = loader().load("hi");
            value.add(dg);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) { }

        // Cannot insert a datagram
        try
        {
            IonDatagram dg = loader().load("hi");
            value.add(1, dg);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) { }

        // Now remove an element
        boolean removed = value.remove(nullBool0);
        assertTrue(removed);
        assertNull(nullBool0.getFieldName());
        assertNull(nullBool0.getContainer());
        assertEquals("size", 1, value.size());
        assertSame(nullBool1, value.get(0));

        // How about insert?
        IonInt nullInt = system().newNullInt();
        value.add(0, nullInt);
        assertSame(value, nullInt.getContainer());
        assertEquals("size", 2, value.size());
        assertSame(nullInt, value.get(0));
        assertSame(nullBool1, value.get(1));

        // Clear the sequence
        testClearContainer(value);
    }


    public void testFreshNullSequence(IonSequence value)
    {
        assertNull(value.getFieldName());
        checkNullSequence(value);
        modifySequence(value);
    }


    public void testEmptySequence(IonSequence value)
    {
        assertFalse(value.isNullValue());
        assertEquals(0, value.size());
        assertFalse(value.iterator().hasNext());
        assertTrue(value.isEmpty());

        try
        {
            value.get(0);
            fail("Expected IndexOutOfBoundsException");
        }
        catch (IndexOutOfBoundsException e) { }

        modifySequence(value);
    }


    //=========================================================================
    // Test cases

    /**
     * This looks for a subtle encoding problem. If a value has its header
     * widened enough to overlap clean content, we must be careful to not
     * overwrite the content while writing the header.  This can happen when
     * adding annotations.
     *
     * @see StructTest#testModsCausingHeaderOverlap()
     */
    public void testModsCausingHeaderOverlap()
        throws Exception
    {
        IonDatagram dg = values("[\"this is a string to overlap\"]");
        IonList v = (IonList) dg.get(0);
        v.addTypeAnnotation("one");
        v.addTypeAnnotation("two");
        v.addTypeAnnotation("three");
        v.addTypeAnnotation("four");
        v.addTypeAnnotation("five");
        v.addTypeAnnotation("six");

        dg = reload(dg);
        v = (IonList) dg.get(0);
        checkString("this is a string to overlap", v.get(0));
    }

    public void testBadAdds()
    {
        IonSequence value = makeNull();

        try {
            value.add(null);
            fail("Expected NullPointerException");
        }
        catch (NullPointerException e) { }
    }

    public void testBadRemoves()
    {
        IonSequence value = makeNull();
        IonBool nullBool0 = system().newNullBool();
        value.add(nullBool0);

        try {
            value.remove(null);
            fail("Expected NullPointerException");
        }
        catch (NullPointerException e) { }

        IonBool nullBool1 = system().newNullBool();
        assertFalse(value.remove(nullBool1));

        IonSequence otherSeq = makeNull();
        otherSeq.add(nullBool1);

        assertFalse(value.remove(nullBool1));
    }

    public void testNewSeqWithDatagram()
    {
        IonValue first = system().newInt(12);
        IonDatagram dg = loader().load("1 2 3");

        ArrayList<IonValue> children = new ArrayList<IonValue>();
        children.add(first);
        children.add(dg);

        try {
            newSequence(children);
            fail("Expected IllegalArgumentException: adding Datagram to sequence");
        }
        catch (IllegalArgumentException e) { }

        // FIXME:  first now has a container... but its garbage!

        first = system().newInt(13);
        try {
            newSequence(first, dg);
            fail("Expected IllegalArgumentException: adding Datagram to sequence");
        }
        catch (IllegalArgumentException e) { }
    }


    public void testClearRemovesChildsContainer()
    {
        IonValue val = system().newString("test");
        IonSequence seq = newSequence(val);
        seq.clear();
        assertNull("Removed value should have null container",
                   val.getContainer());
    }

    public void testMakeNullRemovesChildsContainer()
    {
        IonValue val = system().newString("test");
        IonSequence seq = newSequence(val);
        seq.makeNull();
        assertNull("Removed value should have null container",
                   val.getContainer());
    }

    public void testAddAll()
    {
        IonSequence seq = makeNull();
        List<IonValue> l = null;
        try
        {
            seq.addAll(l);
            fail("expected exception");
        }
        catch (NullPointerException e) { }

        seq = makeEmpty();
        try
        {
            seq.addAll(l);
            fail("expected exception");
        }
        catch (NullPointerException e) { }

        l = new ArrayList<IonValue>();

        boolean changed = seq.addAll(l);
        assertFalse(changed);

        l.add(system().newNull());
        changed = seq.addAll(l);
        assertTrue(changed);
        assertSame(l.get(0), seq.get(0));

        try
        {
            seq.addAll(l);
            fail("expected exception");
        }
        catch (ContainedValueException e) { }
    }

    public void testContains()
    {
        IonNull nullValue1 = system().newNull();
        IonNull nullValue2 = system().newNull();

        IonSequence seq = makeNull();
        assertFalse(seq.contains(nullValue1));
        try
        {
            seq.contains(null);
            fail("expected exception");
        }
        catch (NullPointerException e) { }

        seq = makeEmpty();
        assertFalse(seq.contains(nullValue1));
        try
        {
            seq.contains(null);
            fail("expected exception");
        }
        catch (NullPointerException e) { }

        seq.add(nullValue2);
        assertTrue(seq.contains(nullValue1));
        assertTrue(seq.contains(nullValue2));
    }

    public void testContainsAll()
    {
        IonNull nullValue1 = system().newNull();
        IonNull nullValue2 = system().newNull();
        IonInt  intValue1  = system().newInt(1);

        List<Object> empty = new ArrayList<Object>();
        List<Object> hasNull = Arrays.asList((Object)nullValue1);
        List<Object> hasNullAndInt = Arrays.asList((Object)nullValue1,
                                                   (Object)intValue1);

        IonSequence seq = makeNull();
        assertTrue(seq.containsAll(empty));
        assertFalse(seq.containsAll(hasNull));
        assertFalse(seq.containsAll(hasNullAndInt));
        try
        {
            seq.containsAll(null);
            fail("expected exception");
        }
        catch (NullPointerException e) { }

        seq = makeEmpty();
        assertTrue(seq.containsAll(empty));
        assertFalse(seq.containsAll(hasNull));
        assertFalse(seq.containsAll(hasNullAndInt));
        try
        {
            seq.containsAll(null);
            fail("expected exception");
        }
        catch (NullPointerException e) { }

        seq.add(nullValue2);
        assertTrue(seq.containsAll(empty));
        assertTrue(seq.containsAll(hasNull));
        assertFalse(seq.containsAll(hasNullAndInt));

        seq.add(intValue1);
        assertTrue(seq.containsAll(empty));
        assertTrue(seq.containsAll(hasNull));
        assertTrue(seq.containsAll(hasNullAndInt));
    }


    public void testToArray()
    {
        IonSequence seq = makeNull();
        assertEquals(0, seq.toArray().length);

        seq = makeEmpty();
        assertEquals(0, seq.toArray().length);

        seq.add().newNull();

        IonValue[] array = seq.toArray();
        checkArray(seq, array);

        seq.add().newInt(2);
        IonValue[] array2 = seq.toArray();
        checkArray(seq, array2);

        Object[] objArray = new Object[2];
        assertSame(objArray, seq.toArray(objArray));
        checkArray(seq, objArray);

        seq.remove(seq.get(1));  // TODO remove(1)
        assertSame(objArray, seq.toArray(objArray));
        assertSame(seq.get(0), objArray[0]);
        assertEquals(null, objArray[1]);

        try
        {
            seq.toArray(null);
            fail("expected exception");
        }
        catch (NullPointerException e) { }

        Integer[] intArray = new Integer[0];
        try
        {
            seq.toArray(intArray);
            fail("expected exception");
        }
        catch (ArrayStoreException e) { }
    }

    public void checkArray(IonSequence expected, Object[] actual)
    {
        assertEquals(expected.size(), actual.length);
        for (int i = 0; i < actual.length; i++)
        {
            assertSame(expected.get(i), actual[i]);
        }
    }

    public void testAddOfClone()
    {
        IonSequence s = newSequence();

        IonList v1 = system().newEmptyList();
        IonList v2 = system().clone(v1);
        s.add(v2);

        v1 = system().newList(system().newInt(12));
        v1.addTypeAnnotation("foo");
        v2 = system().clone(v1);
        s.add(v2);
        v2.addTypeAnnotation("foo");

        forceMaterialization(v1);

        IonDatagram dg = system().newDatagram(s);
        byte[] buf = dg.getBytes();
        assertNotNull("there should be a buffer", buf);
    }

    public void testRemoveViaIteratorThenDirect()
    {
        IonSequence s = newSequence();
        IonInt v0 = system().newInt(0);
        IonInt v1 = system().newInt(1);
        s.add(v0);
        s.add(v1);

        Iterator<IonValue> i = s.iterator();
        assertSame(v0, i.next());
        i.remove();

        assertSame(v1, i.next());
        s.remove(v1);
    }


    public void testAddFactory()
    {
        IonSequence s = makeEmpty();

        IonInt i = s.add().newInt(23);
        checkInt(23, i);
        assertSame(i, s.get(0));

        IonString str = s.add().newString("g");
        assertEquals(2, s.size());
        checkString("g", str);
        assertSame(str, s.get(1));
    }
}
