// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.streaming;


import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonTestCase;
import com.amazon.ion.impl.IonReaderBinaryWithPosition_test;
import com.amazon.ion.impl.IonReaderBinaryWithPosition_test.IonReaderPosition;
import com.amazon.ion.junit.IonAssert;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 */
public class ReaderPositioningTest
    extends IonTestCase
{
    private IonReaderBinaryWithPosition_test read(String text)
    {
        byte[] binary = encode(text);

        IonReaderBinaryWithPosition_test in =
            new IonReaderBinaryWithPosition_test(system(), catalog(),
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

        IonReaderBinaryWithPosition_test in =
            new IonReaderBinaryWithPosition_test(system(), catalog(),
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

    @Test @Ignore // TODO this throws assertion failure
    public void testGetPosBeforeFirstTopLevel()
    {
        IonReaderBinaryWithPosition_test in = read("foo");
        IonReaderPosition pos = in.getCurrentPosition();
    }

    @Test  // TODO similar for list/sexp
    public void testGetPosBeforeFirstStructChild()
    {
        IonReaderBinaryWithPosition_test in = read("{f:v}");
        in.next();
        in.stepIn();
        IonReaderPosition pos = in.getCurrentPosition();
        in.stepOut();
        in.seek(pos);
        in.next();
    }


    @Test
    public void testGetPosAtEndOfStream()
    {
        IonReaderBinaryWithPosition_test in = read("foo");
        in.next();
        assertEquals(null, in.next());
        IonReaderPosition pos = in.getCurrentPosition();
        // TODO what does this mean?
    }
}
