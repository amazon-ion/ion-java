// Copyright (c) 2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonInt;
import com.amazon.ion.IonList;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonTestCase;
import com.amazon.ion.IonType;

/**
 *
 */
public class TreeReaderTest
    extends IonTestCase
{
    public void testInitialStateForScalar()
    {
        IonInt value = system().newInt(23);
        IonReader r = system().newReader(value);

        assertTrue(r.hasNext());
        assertEquals(IonType.INT, r.next());
    }

    public void testInitialStateForList()
    {
        IonList list = system().newEmptyList();
        IonReader r = system().newReader(list);

        assertTrue(r.hasNext());
        assertEquals(IonType.LIST, r.next());
        assertEquals(0, r.getDepth());

        r.stepIn();

        assertEquals(1, r.getDepth());
        assertFalse(r.hasNext());

        r.stepOut();

        assertFalse(r.hasNext());
    }

    public void testInitialStateForStruct()
    {
        IonStruct value = system().newEmptyStruct();
        IonReader r = system().newReader(value);

        assertFalse(r.isInStruct());

        assertTrue(r.hasNext());
        assertFalse(r.isInStruct());

        assertEquals(IonType.STRUCT, r.next());
        assertEquals(0, r.getDepth());
        assertFalse(r.isInStruct());

        r.stepIn();
        assertTrue(r.isInStruct());
        assertEquals(1, r.getDepth());

        assertFalse(r.hasNext());
        assertTrue(r.isInStruct());

        r.stepOut();
        assertFalse(r.isInStruct());
        assertFalse(r.hasNext());
    }
}
