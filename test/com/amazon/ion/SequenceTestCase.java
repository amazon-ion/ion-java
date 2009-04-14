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
        IonSequence value = makeEmpty();
        testBadAdds(value);

        value.add().newInt(3);
        testBadAdds(value);
    }

    protected void testBadAdds(IonSequence s)
    {
        try {
            s.add(null);
            fail("Expected exception");
        }
        catch (NullPointerException e) { }

        // TODO cast to Collection and add Object

        IonSequence s2 = makeEmpty();
        IonNull n = s2.add().newNull();
        try {
            s.add(n);
            fail("Expected exception");
        }
        catch (ContainedValueException e) { }
    }


    public void testBadRemoves()
    {
        IonSequence value = makeEmpty();
        testBadRemoves(value);

        value.add().newNullBool();
        testBadRemoves(value);
    }

    protected void testBadRemoves(IonSequence value)
    {
        try {
            value.remove((Object)null);
            fail("Expected NullPointerException");
        }
        catch (NullPointerException e) { }

        try {
            value.remove((IonValue)null);
            fail("Expected NullPointerException");
        }
        catch (NullPointerException e) { }

        IonBool nullBool1 = system().newNullBool();
        assertFalse(value.remove(nullBool1));

        IonSequence otherSeq = makeEmpty();
        otherSeq.add(nullBool1);

        assertFalse(value.remove(nullBool1));

        try {
            value.remove(new Integer(1));
        }
        catch (ClassCastException e) { }
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

    public void testAddAll()
    {
        IonSequence seq = makeEmpty();
        testAddAll(seq);
    }

    protected void testAddAll(IonSequence seq)
    {
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

        IonSequence seq = makeEmpty();
        assertFalse(seq.contains(nullValue1));
        try
        {
            seq.contains(null);
            fail("expected exception");
        }
        catch (NullPointerException e) { }

        seq.add(nullValue2);
        assertFalse(seq.contains(nullValue1));
        assertTrue(seq.contains(nullValue2));
    }

    public void testContainsAll()
    {
        IonNull nullValue1 = system().newNull();
        IonNull nullValue2 = system().newNull();
        IonInt  intValue1  = system().newInt(1);

        List<Object> empty = new ArrayList<Object>();
        List<Object> hasJavaNull = Arrays.asList((Object)null);
        List<Integer> hasJavaInt = Arrays.asList(new Integer(0));
        List<Object> hasNull1 = Arrays.asList((Object)nullValue1);
        List<Object> hasNull2AndInt = Arrays.asList((Object)intValue1,
                                                    (Object)nullValue2);

        IonSequence seq = makeEmpty();
        assertTrue(seq.containsAll(empty));
        assertFalse(seq.containsAll(hasNull1));
        assertFalse(seq.containsAll(hasNull2AndInt));
        try
        {
            seq.containsAll(null);
            fail("expected exception");
        }
        catch (NullPointerException e) { }
        try
        {
            seq.containsAll(hasJavaNull);
            fail("expected exception");
        }
        catch (NullPointerException e) { }
        try
        {
            seq.containsAll(hasJavaInt);
            fail("expected exception");
        }
        catch (ClassCastException e) { }


        seq.add(nullValue2);
        assertTrue(seq.containsAll(empty));
        assertFalse(seq.containsAll(hasNull1));
        assertFalse(seq.containsAll(hasNull2AndInt));

        seq.add(intValue1);
        assertTrue(seq.containsAll(empty));
        assertFalse(seq.containsAll(hasNull1));
        assertTrue(seq.containsAll(hasNull2AndInt));

        seq.add(nullValue1);
        assertTrue(seq.containsAll(empty));
        assertTrue(seq.containsAll(hasNull1));
        assertTrue(seq.containsAll(hasNull2AndInt));
        try
        {
            seq.containsAll(hasJavaNull);
            fail("expected exception");
        }
        catch (NullPointerException e) { }
        try
        {
            seq.containsAll(hasJavaInt);
            fail("expected exception");
        }
        catch (ClassCastException e) { }
    }


    public void testRemoveAll()
    {
        IonNull nullValue1 = system().newNull();
        IonNull nullValue2 = system().newNull();
        IonInt  intValue1  = system().newInt(1);

        List<Object> empty = new ArrayList<Object>();
        List<Object> hasJavaNull = Arrays.asList((Object)null);
        List<Integer> hasJavaInt = Arrays.asList(new Integer(0));
        List<Object> hasNull1 = Arrays.asList((Object)nullValue1);
        List<Object> hasNull2AndInt = Arrays.asList((Object)intValue1,
                                                    (Object)nullValue2);

        IonSequence seq = makeEmpty();
        assertFalse(seq.removeAll(empty));
        assertFalse(seq.removeAll(hasNull1));
        assertFalse(seq.removeAll(hasNull2AndInt));
        try
        {
            seq.removeAll(null);
            fail("expected exception");
        }
        catch (NullPointerException e) { }
        try
        {
            seq.removeAll(hasJavaNull);
            fail("expected exception");
        }
        catch (NullPointerException e) { }
        try
        {
            seq.removeAll(hasJavaInt);
            fail("expected exception");
        }
        catch (ClassCastException e) { }


        seq.add(nullValue2);
        assertFalse(seq.removeAll(empty));
        assertFalse(seq.removeAll(hasNull1));
        assertTrue(seq.removeAll(hasNull2AndInt));
        assertEquals(0, seq.size());

        seq.add(nullValue2);
        seq.add(intValue1);
        assertFalse(seq.removeAll(empty));
        assertFalse(seq.removeAll(hasNull1));
        assertTrue(seq.removeAll(hasNull2AndInt));
        assertEquals(0, seq.size());

        seq.add(intValue1);
        seq.add(nullValue1);
        assertFalse(seq.removeAll(empty));
        assertTrue(seq.removeAll(hasNull1));
        assertNull(nullValue1.getContainer());
        seq.add(nullValue2);
        assertTrue(seq.removeAll(hasNull2AndInt));
        assertEquals(0, seq.size());

        seq.add(nullValue2);
        seq.add(intValue1);
        try
        {
            seq.removeAll(hasJavaNull);
            fail("expected exception");
        }
        catch (NullPointerException e) { }
        try
        {
            seq.removeAll(hasJavaInt);
            fail("expected exception");
        }
        catch (ClassCastException e) { }
        assertTrue(seq.containsAll(hasNull2AndInt));
    }

    public void testRetainAll()
    {
        IonNull nullValue1 = system().newNull();
        IonNull nullValue2 = system().newNull();
        IonInt  intValue1  = system().newInt(1);

        List<Object> empty = new ArrayList<Object>();
        List<Object> hasJavaNull = Arrays.asList((Object)null);
        List<Integer> hasJavaInt = Arrays.asList(new Integer(0));
        List<Object> hasNull1 = Arrays.asList((Object)nullValue1);
        List<Object> hasNull2AndInt = Arrays.asList((Object)intValue1,
                                                    (Object)nullValue2);

        IonSequence seq = makeEmpty();
        // FIXME implement IonDatagram.retainAll
        if (seq.getType() == IonType.DATAGRAM) return;

        assertFalse(seq.retainAll(empty));
        assertFalse(seq.retainAll(hasNull1));
        assertFalse(seq.retainAll(hasNull2AndInt));
//        try
//        {
//            seq.retainAll(null);
//            fail("expected exception");
//        }
//        catch (NullPointerException e) { }
//        try
//        {
//            seq.retainAll(hasJavaNull);
//            fail("expected exception");
//        }
//        catch (NullPointerException e) { }
//        try
//        {
//            seq.retainAll(hasJavaInt);
//            fail("expected exception");
//        }
//        catch (ClassCastException e) { }

        seq.add(nullValue2);
        try
        {
            seq.retainAll(null);
            fail("expected exception");
        }
        catch (NullPointerException e) { }
        try
        {
            seq.retainAll(hasJavaNull);
            fail("expected exception");
        }
        catch (NullPointerException e) { }
        try
        {
            seq.retainAll(hasJavaInt);
            fail("expected exception");
        }
        catch (ClassCastException e) { }

        assertFalse(seq.retainAll(hasNull2AndInt));
        assertEquals(1, seq.size());
        assertSame(seq, nullValue2.getContainer());

        assertTrue(seq.retainAll(hasNull1));
        assertSame(null, nullValue2.getContainer());

        seq.add(intValue1);
        assertFalse(seq.retainAll(hasNull2AndInt));
        assertSame(seq, intValue1.getContainer());

        seq.add(nullValue1);
        assertTrue(seq.retainAll(empty));
        assertEquals(0, seq.size());

        seq.add(intValue1);
        seq.add(nullValue1);
        assertTrue(seq.retainAll(hasNull1));
        assertEquals(1, seq.size());
        assertSame(null, intValue1.getContainer());
        assertFalse(seq.retainAll(hasNull1));
        assertSame(seq, nullValue1.getContainer());
    }

    public void testToArray()
    {
        IonSequence seq = makeEmpty();
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

        if (s.getType() != IonType.DATAGRAM)
        {
            IonDatagram dg = system().newDatagram(s);
            byte[] buf = dg.getBytes();
            assertNotNull("there should be a buffer", buf);
        }
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

    public void testIndexOf()
    {
        IonNull nullValue1 = system().newNull();
        IonNull nullValue2 = system().newNull();

        IonSequence s = makeEmpty();
        try {
            s.indexOf(null);
            fail("expected exception");
        }
        catch (NullPointerException e) { }
        try {
            s.indexOf(new Integer(0));
            fail("expected exception");
        }
        catch (ClassCastException e) { }

        assertEquals(-1, s.indexOf(nullValue1));

        s.add(nullValue1);
        assertEquals(0,  s.indexOf(nullValue1));
        assertEquals(-1, s.indexOf(nullValue2));
        try {
            s.indexOf(null);
            fail("expected exception");
        }
        catch (NullPointerException e) { }
        try {
            s.indexOf(new Integer(0));
            fail("expected exception");
        }
        catch (ClassCastException e) { }


        IonSequence s2 = makeEmpty();
        s2.add(nullValue2);
        assertEquals(0,  s2.indexOf(nullValue2));
        assertEquals(-1, s.indexOf(nullValue2));
        s2.remove(nullValue2);

        s.add().newBlob(null);
        s.add(nullValue2);
        assertEquals(0, s.indexOf(nullValue1));
        assertEquals(2, s.indexOf(nullValue2));

    }
}
