// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl;

import static com.amazon.ion.junit.IonAssert.assertTopLevel;

import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonInt;
import com.amazon.ion.IonList;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.TestUtils;
import com.amazon.ion.streaming.ReaderTestCase;
import org.junit.Test;


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

        assertTopLevel(in, /* inStruct */ false);
        expectNoCurrentValue();

        check().next().fieldName((String)null).type(IonType.NULL);
        assertTopLevel(in, /* inStruct */ false);
    }

    @Test
    public void testModificationDuringRead()
    {
        IonList list = system().newEmptyList();
        list.add().newString("abc");
        list.add().newString("def");
        in = system().newReader(list);
        assertEquals(IonType.LIST, in.next());
        in.stepIn();
        assertEquals(IonType.STRING, in.next());
        // Violate the contract by modifying the value.
        list.remove(0);
        // Since the value was modified while it was being written, the result of the following is undefined. But
        // one thing is for sure: it should not cause an infinite loop.
        in.next();
    }

    @Test
    public void testSystemReaderGetBytesProgressResetsOnNext() throws Exception
    {
        IonValue list = oneValue("[{{\"abcdef\"}}, {{\"ghi\"}}]");
        in = system().newSystemReader(list);
        assertEquals(IonReaderTreeSystem.class, in.getClass());
        byte[] buffer = new byte[5];

        assertEquals(IonType.LIST, in.next());
        in.stepIn();
        assertEquals(IonType.CLOB, in.next());
        assertEquals(2, in.getBytes(buffer, 1, 2));
        assertEquals("ab", new String(buffer, 1, 2, "UTF-8"));
        assertEquals(2, in.getBytes(buffer, 2, 2));
        assertEquals("cd", new String(buffer, 2, 2, "UTF-8"));

        assertEquals(IonType.CLOB, in.next());
        assertEquals(3, in.getBytes(buffer, 2, 3));
        assertEquals("ghi", new String(buffer, 2, 3, "UTF-8"));
        assertEquals(0, in.getBytes(buffer, 2, 3));
    }

    @Test
    public void testGetBytesRejectsOutOfRangeArguments() throws Exception
    {
        in = system().newReader(oneValue("{{\"abcdef\"}}"));
        byte[] buffer = new byte[4];
        assertEquals(IonType.CLOB, in.next());

        assertThrows(IndexOutOfBoundsException.class, () -> in.getBytes(buffer, 0, -1));
        assertThrows(IndexOutOfBoundsException.class, () -> in.getBytes(buffer, -1, 1));
        assertThrows(IndexOutOfBoundsException.class, () -> in.getBytes(buffer, 2, 3));

        assertEquals(4, in.getBytes(buffer, 0, 4));
        assertEquals("abcd", new String(buffer, 0, 4, "UTF-8"));
        assertEquals(2, in.getBytes(buffer, 0, 4));
        assertEquals("ef", new String(buffer, 0, 2, "UTF-8"));
        assertThrows(IndexOutOfBoundsException.class, () -> in.getBytes(buffer, 5, 0));
    }

    @Test
    public void testGetBytesAcceptsLenBeyondBufferWhenRemainingBytesFit() throws Exception
    {
        in = system().newReader(oneValue("{{\"abcdef\"}}"));
        byte[] buffer = new byte[6];
        assertEquals(IonType.CLOB, in.next());

        assertEquals(6, in.getBytes(buffer, 0, 1000));
        assertEquals("abcdef", new String(buffer, 0, 6, "UTF-8"));
        assertEquals(0, in.getBytes(buffer, 0, 1000));
    }
}
