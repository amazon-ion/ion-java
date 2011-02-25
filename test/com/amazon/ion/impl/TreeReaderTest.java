// Copyright (c) 2009-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonInt;
import com.amazon.ion.IonList;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonTestCase;
import com.amazon.ion.IonType;
import org.junit.Test;

/**
 *
 */
public class TreeReaderTest
    extends IonTestCase
{
    @Test
    public void testInitialStateForScalar()
    {
        IonInt value = system().newInt(23);
        IonReader r = system().newReader(value);

        assertTrue(r.hasNext());
        assertEquals(IonType.INT, r.next());
    }

    @Test
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
}
