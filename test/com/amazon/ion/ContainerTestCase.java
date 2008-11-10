/* Copyright (c) 2007-2008 Amazon.com, Inc.  All rights reserved. */

package com.amazon.ion;

import java.util.Iterator;



public abstract class ContainerTestCase
    extends IonTestCase
{
    /**
     * Creates Ion text of a container, using given fragments of Ion text.
     */
    protected abstract String wrap(String... children);

    /**
     * Creates a container holding given values.
     */
    protected abstract IonContainer wrap(IonValue... children);


    public void checkNullContainer(IonContainer value)
    {
        assertTrue(value.isNullValue());
        assertFalse(value.iterator().hasNext());

        try
        {
            value.size();
            fail("Expected NullValueException");
        }
        catch (NullValueException e) { }

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

        c.makeNull();
        assertFalse(c.remove(n));
    }


    public void testDetachHasDifferentSymtab()
    {
        IonContainer list = (IonContainer)
            oneValue(wrap("sym1", "[sym2]", "{f:sym3}", "a::3"));
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
}
