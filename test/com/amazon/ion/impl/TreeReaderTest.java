/*
 * Copyright 2007-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

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
}
