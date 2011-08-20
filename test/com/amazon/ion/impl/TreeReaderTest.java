// Copyright (c) 2009-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.junit.IonAssert.assertTopLevel;

import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonInt;
import com.amazon.ion.IonList;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonTestCase;
import com.amazon.ion.IonType;
import com.amazon.ion.TestUtils;
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

    @Test
    public void testReadingReadOnly()
    {
        IonDatagram dg = loader().load("{hello:hello}");

        // Make just part of the datagram read-only
        IonStruct s = (IonStruct) dg.get(0);
        s.makeReadOnly();

        IonReader r = system().newReader(s);
        TestUtils.deepRead(r);

        r = system().newReader(dg);
        TestUtils.deepRead(r);

        // Now the whole thing
        dg.makeReadOnly();

        r = system().newReader(dg);
        TestUtils.deepRead(r);
    }

    @Test
    public void testHoistedReader()
    {
        IonStruct s = struct("{f:null}");
        IonReader r = system().newReader(s.get("f"));
        assertSame(IonType.NULL, r.next());
        assertTopLevel(r);
    }
}
