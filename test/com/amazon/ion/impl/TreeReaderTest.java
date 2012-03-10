// Copyright (c) 2009-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.junit.IonAssert.assertTopLevel;

import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonInt;
import com.amazon.ion.IonList;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonType;
import com.amazon.ion.TestUtils;
import com.amazon.ion.streaming.ReaderTestCase;
import org.junit.Test;

/**
 *
 */
public class TreeReaderTest
    extends ReaderTestCase
{
    @Test
    public void testInitialStateForScalar()
    {
        IonInt value = system().newInt(23);
        in = system().newReader(value);

        check().next().isInt(23);
    }

    @Test
    public void testInitialStateForList()
    {
        IonList list = system().newEmptyList();
        in = system().newReader(list);

        check().next().type(IonType.LIST);
        assertEquals(0, in.getDepth());

        in.stepIn();
        {
            assertEquals(1, in.getDepth());
            expectEof();
        }
        in.stepOut();
        expectTopEof();
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
    public void testReadingStructFields()
    {
        IonStruct s = struct("{f:null}");
        in = system().newReader(s.get("f"));

        assertTopLevel(in, /* inStruct */ true);
        expectNoCurrentValue();

        check().next().fieldName("f").type(IonType.NULL);
        assertTopLevel(in, /* inStruct */ true);
    }
}
