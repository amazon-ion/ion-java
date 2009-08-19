/* Copyright (c) 2007-2009 Amazon.com, Inc.  All rights reserved. */

package com.amazon.ion;

import java.util.Iterator;



public abstract class ContainerTestCase
    extends IonTestCase
{
    /**
     * Creates the null container relevant to subclass.
     */
    protected abstract IonContainer makeNull();

    protected abstract IonContainer makeEmpty();

    protected abstract void add(IonContainer container, IonValue child);

    /**
     * Creates a container, using given fragments of Ion text.
     */
    protected abstract IonContainer wrapAndParse(String... children);

    /**
     * Creates a container holding given values.
     */
    protected abstract IonContainer wrap(IonValue... children);


    public void checkNullContainer(IonContainer value)
    {
        assertTrue(value.isNullValue());
        assertFalse(value.iterator().hasNext());
        assertEquals(0, value.size());

        try
        {
            value.remove(null);
            fail("Expected NullPointerException");
        }
        catch (NullPointerException e) { }

        // Remove from null container is no-op.
        assertFalse(value.remove(system().newNull()));

        try
        {
            value.isEmpty();
            fail("Expected NullValueException");
        }
        catch (NullValueException e) { }
    }

    public void testNullMakeReadOnly()
    {
        IonContainer c = makeNull();
        if (c == null) return; // Hack for datagram

        c.makeReadOnly();
        assertTrue(c.isNullValue());
        assertTrue(c.isReadOnly());

        try
        {
            add(c, system().newNull());
            fail("expected exception");
        }
        catch (ReadOnlyValueException e) { }

        assertTrue(c.isNullValue());
        assertTrue(c.isReadOnly());
    }

    public void testEmptyMakeReadOnly()
    {
        IonContainer c = makeEmpty();
        c.makeReadOnly();
        assertTrue(c.isEmpty());
        assertTrue(c.isReadOnly());

        try
        {
            add(c, system().newNull());
            fail("expected exception");
        }
        catch (ReadOnlyValueException e) { }

        assertTrue(c.isEmpty());
        assertTrue(c.isReadOnly());
    }

    public void testNonEmptyMakeReadOnly()
    {
        IonSystem s = system();
        IonContainer c = wrap(s.newString("one"), s.newInt(2), s.newDecimal(3));
        c.makeReadOnly();
        assertEquals(3, c.size());
        assertTrue(c.isReadOnly());
        IonValue first = c.iterator().next();
        assertTrue(first.isReadOnly());

        try
        {
            add(c, system().newNull());
            fail("expected exception");
        }
        catch (ReadOnlyValueException e) { }

        try
        {
            c.remove(first);
            fail("expected exception");
        }
        catch (ReadOnlyValueException e) { }

        try
        {
            ((IonString)first).setValue("changed");
            fail("expected exception");
        }
        catch (ReadOnlyValueException e) { }

        assertEquals(3, c.size());
        assertTrue(c.isReadOnly());
    }


    /**
     * Clears a container and verifies that it is not a <code>null</code>
     * value, it is empty, its size is 0, and that it has no iterable
     * contained values.
     * @param container the container to test
     */
    public void testClearContainer(IonContainer container)
    {
        container.clear();
        assertFalse(container.isNullValue());
        assertTrue(container.isEmpty());
        assertEquals(container.size(), 0);
        assertFalse(container.iterator().hasNext());
    }

    /**
     * Using an iterator, remove a value in the container and verify
     * that the value no longer exists in the container.
     * @param container the container to test
     */
    public void testIteratorRemove(IonContainer container)
    {
        Iterator<IonValue> it = container.iterator();
        IonValue removedValue = it.next();
        int prevSize = container.size();
        it.remove();

        // make sure we removed something
        assertEquals(container.size(), prevSize - 1);
        assertNull(removedValue.getContainer());
        assertNull(removedValue.getFieldName());
        // TODO check that index is not set.

        // make sure the value we removed isn't there anymore
        it = container.iterator();
        while (it.hasNext())
        {
            IonValue value = it.next();
            assertNotSame(removedValue, value);
        }

        // Another call to remove should be no-op
        assertFalse(container.remove(removedValue));

        // TODO concurrency tests.  it.remove should throw after (eg) container.makeNull().
        // TODO test proper behavior of remove() when next() not called, or when remove() called twice.
        // (should throw IllegalStateException)
    }

    public void testRemoveFromNull()
    {
        IonValue n = system().newNull();

        IonContainer c = wrap(n);
        assertTrue(c.remove(n));
        assertFalse(c.remove(n));
        assertTrue(c.isEmpty());

        if (c instanceof IonDatagram) return;

        c.makeNull();
        assertFalse(c.remove(n));
    }


    public void testRemoveFromContainer()
    {
        IonValue n = system().newNull();
        assertFalse(n.removeFromContainer());

        IonValue m = system().newInt(2);
        IonContainer c = wrap(n, m);

        assertTrue(n.removeFromContainer());
        assertEquals(null, n.getContainer());
        assertEquals(null, n.getFieldName());

        assertEquals(1, c.size());
        assertSame(m, c.iterator().next());
        assertTrue(m.removeFromContainer());
        assertEquals(null, m.getContainer());
        assertEquals(null, m.getFieldName());
        assertEquals(0, c.size());
    }


    public void testDetachHasDifferentSymtab()
    {
        IonContainer list = wrapAndParse("sym1", "[sym2]", "{f:sym3}", "a::3");
        if (list instanceof IonDatagram) return;

        SymbolTable topSymtab = list.getSymbolTable();
        assertNotNull(topSymtab);

        {
            IonValue child = list.iterator().next();
            assertSame(topSymtab, child.getSymbolTable());

            list.remove(child);
            assertEquals("sym1", child.toString());
            assertNotSame(topSymtab, child.getSymbolTable());
        }
        {
            IonList child = (IonList) list.iterator().next();
            assertSame(topSymtab, child.getSymbolTable());

            list.remove(child);
            assertEquals("[sym2]", child.toString());
            assertNotSame(topSymtab, child.getSymbolTable());
            assertNotSame(topSymtab, child.get(0).getSymbolTable());
        }
        {
            IonStruct child = (IonStruct) list.iterator().next();
            assertSame(topSymtab, child.getSymbolTable());

            list.remove(child);
            assertEquals("{f:sym3}", child.toString());
            assertNotSame(topSymtab, child.getSymbolTable());
            assertNotSame(topSymtab, child.get("f").getSymbolTable());
        }
        {
            IonValue child = list.iterator().next();
            assertSame(topSymtab, child.getSymbolTable());

            list.remove(child);
            assertEquals("a::3", child.toString());
            assertNotSame(topSymtab, child.getSymbolTable());
        }
    }


    /**
     * Isolates issue ION-25.
     */
    public void testUnmaterializedInsert()
    {
        IonContainer c = wrapAndParse((String[])null);
        IonValue v = system().singleValue("1");
        add(c, v);
    }


    public void testAddingReadOnlyChild()
    {
        IonContainer c = makeEmpty();

        IonNull n = system().newNull();
        n.makeReadOnly();

        try
        {
            add(c, n);
            fail("expected exception");
        }
        catch (ReadOnlyValueException e) { }

        assertEquals(null, n.getContainer());
        assertTrue(c.isEmpty());
    }

    public void testRemovingReadOnlyChild()
    {
        IonContainer c = makeEmpty();

        IonNull n = system().newNull();
        add(c, n);

        n.makeReadOnly();

        try
        {
            c.remove(n);
            fail("expected exception");
        }
        catch (ReadOnlyValueException e) { }

        assertSame(c, n.getContainer());
        assertSame(n, c.iterator().next());
    }
}
