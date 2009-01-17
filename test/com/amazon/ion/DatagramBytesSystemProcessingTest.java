/*
 * Copyright (c) 2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;

import java.util.Iterator;

/**
 * FIXME this replicates {@link LoadBinaryBytesSystemProcessingTest}
 * Except for how we get the bytes.
 */
public class DatagramBytesSystemProcessingTest
    extends IteratorSystemProcessingTest
{
    private byte[] myBytes;


    @Override
    protected boolean processingBinary()
    {
        return true;
    }

    @Override
    protected void prepare(String text)
        throws Exception
    {
        IonLoader loader = loader();
        IonDatagram datagram = loader.load(text);
        myBytes = datagram.toBytes();
    }

    @Override
    protected Iterator<IonValue> iterate()
    {
        IonLoader loader = loader();
        IonDatagram datagram = loader.load(myBytes);
        return datagram.iterator();
    }

    @Override
    protected Iterator<IonValue> systemIterate()
    {
        IonLoader loader = loader();
        IonDatagram datagram = loader.load(myBytes);
        return datagram.systemIterator();
    }

    @Override
    protected void checkMissingSymbol(String expected, int expectedSid)
        throws Exception
    {
        checkSymbol("$" + expectedSid, expectedSid);
    }
}
