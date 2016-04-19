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

import java.util.Iterator;
import org.junit.Ignore;
import org.junit.Test;
import software.amazon.ion.IonContainer;
import software.amazon.ion.IonDatagram;
import software.amazon.ion.IonException;
import software.amazon.ion.IonList;
import software.amazon.ion.IonNull;
import software.amazon.ion.IonString;
import software.amazon.ion.IonStruct;
import software.amazon.ion.IonSymbol;
import software.amazon.ion.IonSystem;
import software.amazon.ion.IonType;
import software.amazon.ion.IonValue;
import software.amazon.ion.NullValueException;
import software.amazon.ion.ReadOnlyValueException;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.system.SimpleCatalog;



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
     * Creates Ion text, using given fragments of Ion text.
     */
    protected abstract String wrap(String... children);

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


    @Test
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


    @Test
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


    @Test
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

    @Test
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


    @Test
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


    /** TODO amznlabs/ion-java#8 datagram is lazy creating local symtabs */
    @Test @Ignore
    public void testDetachHasDifferentSymtab()
    {
        String data = wrap("sym1", "[sym2]", "{f:sym3}", "a::3");

        IonDatagram dg = loader().load(data);

        // Don't test this for datagram, which has different symtab behavior.
        if (dg.get(0).getType() == IonType.SYMBOL) return;

// FIXME: this forces the local symbol tables to be created
Iterator<IonValue> it = dg.systemIterator();
it.next();

        IonContainer list = (IonContainer) dg.get(0);
        if (list instanceof IonDatagram) return;

        SymbolTable topSymtab = list.getSymbolTable();
        checkLocalTable(topSymtab);
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


    @Test
    public void testUnmaterializedInsert()
    {
        IonContainer c = wrapAndParse((String[])null);
        IonValue v = system().singleValue("1");
        add(c, v);
    }


    @Test
    public void testAddingReadOnlyChild()
    {
        IonContainer c = makeEmpty();

        IonNull n = system().newNull();
        n.makeReadOnly();
        assertEquals(null, n.getContainer());

        try
        {
            add(c, n);
            fail("expected exception");
        }
        catch (ReadOnlyValueException e) { }

        assertEquals(null, n.getContainer());
        assertTrue(c.isEmpty());
    }


    @Test
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

    /** TODO amznlabs/ion-java#6 */
    @Test @Ignore
    public void testSelfContainment()
    {
        IonContainer c = makeEmpty();
        try {
            add(c, c);
            fail("expected exception");
        }
        catch (IonException e) { }
    }

    @Test
    public void testCloneOfReadOnlyContainer()
    {
        IonContainer c = makeEmpty();
        IonSymbol child = system().newSymbol("s"); // Symbol hits more code
        add(c, child);
        c.makeReadOnly();

        checkClones(c, child);
    }

    @Test
    public void testCloneOfReadOnlyChild()
    {
        IonContainer c = makeEmpty();
        IonSymbol child = system().newSymbol("s"); // Symbol hits more code
        add(c, child);
        child.makeReadOnly();

        checkClones(c, child);
    }

    private void checkClones(IonContainer c, IonSymbol child)
    {
        IonContainer clone = c.clone();
        checkClone(c, child, clone);

        clone = system().clone(c); // clone w/ same system
        checkClone(c, child, clone);

        clone = newSystem(new SimpleCatalog()).clone(c); // clone w/ other system
        checkClone(c, child, clone);
    }

    private void checkClone(IonContainer c, IonSymbol child, IonContainer clone)
    {
        assertEquals(c, clone);
        assertFalse("clone should not be read-only", clone.isReadOnly());

        IonValue childClone = clone.iterator().next();
        assertEquals(child, childClone);
        assertFalse("clone should not be read-only", childClone.isReadOnly());
    }
}
