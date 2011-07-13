// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.streaming;


import com.amazon.ion.impl.IonReaderPosition;

import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonTestCase;
import com.amazon.ion.IonType;
import com.amazon.ion.impl.IonReaderBinaryWithPosition;
import com.amazon.ion.junit.IonAssert;
import org.junit.Test;

/**
 *
 */
public class ReaderPositioningTest
    extends IonTestCase
{
    private IonReaderBinaryWithPosition read(String text)
    {
        byte[] binary = encode(text);

        IonReaderBinaryWithPosition in =
            new IonReaderBinaryWithPosition(system(), catalog(),
                                                 binary, 0, binary.length);
        return in;
    }


    @Test
    public void testWalkingBackwards()
    {
        String text =
            "null true 3 4e0 5.0 6666-06-06T '7' \"8\" {{\"\"}} {{}} [] () {}";

        IonDatagram dg = loader().load(text);
        byte[] binary = dg.getBytes();

        IonReaderPosition[] positions = new IonReaderPosition[dg.size()];

        IonReaderBinaryWithPosition in =
            new IonReaderBinaryWithPosition(system(), catalog(),
                                                 binary, 0, binary.length);
        for (int i = 0; i < dg.size(); i++)
        {
            assertEquals(dg.get(i).getType(), in.next());
            positions[i] = in.getCurrentPosition();
            // TODO test reading the value before calling getPos
        }
        assertEquals(null, in.next());

        for (int i = dg.size() - 1; i >= 0; i--)
        {
            in.seek(positions[i]);
            assertEquals(dg.get(i).getType(), in.next());
            IonAssert.assertIonEquals(dg.get(i), system().newValue(in));
        }
    }


    @Test
    public void testSeekingIntoContainers()
    {
        IonReaderBinaryWithPosition in = read("{f:v,g:[c]} s");

        in.next();
        in.stepIn();
            in.next();
            IonReaderPosition fPos = in.getCurrentPosition();
            assertEquals("v", in.stringValue());
            in.next();
            IonReaderPosition gPos = in.getCurrentPosition();
            in.stepIn();
                in.next();
                assertEquals("c", in.stringValue());
                IonReaderPosition cPos = in.getCurrentPosition();
                assertEquals(null, in.next());
            in.stepOut();
            assertEquals(null, in.next());
        in.stepOut();
        in.next();
        IonReaderPosition sPos = in.getCurrentPosition();
        assertEquals(null, in.next());


        in.seek(fPos);
        assertEquals(IonType.SYMBOL, in.next());
        assertEquals(null, in.getFieldName());
        assertEquals("v", in.stringValue());
        assertEquals(null, in.next());

        in.seek(cPos);
        in.next();
        assertEquals("c", in.stringValue());
        assertEquals(null, in.getFieldName());
        assertEquals(null, in.next());

        in.seek(gPos);
        assertEquals(IonType.LIST, in.next());
        assertEquals(null, in.getFieldName());
        in.stepIn();
            in.next();
            assertEquals("c", in.stringValue());
            assertEquals(null, in.next());
        in.stepOut();
        assertEquals(null, in.next());

        in.seek(fPos);
        assertEquals(null, in.getFieldName());
        assertEquals(IonType.SYMBOL, in.next());
        assertEquals("v", in.stringValue());
        assertEquals(null, in.next());

        in.seek(sPos);
        assertEquals(IonType.SYMBOL, in.next());
        assertEquals("s", in.stringValue());
        assertEquals(null, in.next());
    }

    // TODO test annotations

    @Test(expected=IllegalStateException.class)
    public void testGetPosBeforeFirstTopLevel()
    {
        IonReaderBinaryWithPosition in = read("foo");
        IonReaderPosition pos = in.getCurrentPosition();
    }

    @Test(expected=IllegalStateException.class)  // TODO similar for list/sexp
    public void testGetPosBeforeFirstStructChild()
    {
        IonReaderBinaryWithPosition in = read("{f:v}");
        in.next();
        in.stepIn();
        in.getCurrentPosition();
    }

    @Test(expected=IllegalStateException.class)  // TODO similar for list/sexp
    public void testGetPosAfterLastStructChild()
    {
        IonReaderBinaryWithPosition in = read("{f:v}");
        in.next();
        in.stepIn();
        in.next();
        in.next();
        in.getCurrentPosition();
    }


    @Test(expected=IllegalStateException.class)
    public void testGetPosAtEndOfStream()
    {
        IonReaderBinaryWithPosition in = read("foo");
        in.next();
        assertEquals(null, in.next());
        IonReaderPosition pos = in.getCurrentPosition();
        // TODO what does this mean?
    }
}
