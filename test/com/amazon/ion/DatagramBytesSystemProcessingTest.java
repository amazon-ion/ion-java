/*
 * Copyright (c) 2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;

import java.util.Iterator;

/**
 *
 */
public class DatagramBytesSystemProcessingTest
    extends IteratorSystemProcessingTest
{
    @Override
    protected Iterator<IonValue> iterate(String text)
    {
        IonLoader loader = loader();
        IonDatagram datagram = loader.load(text);

        IonDatagram datagramFromBytes = loader.load(datagram.toBytes());
        return datagramFromBytes.iterator();
    }

    @Override
    protected Iterator<IonValue> systemIterate(String text)
    {
        IonLoader loader = loader();
        IonDatagram datagram = loader.load(text);

        IonDatagram datagramFromBytes = loader.load(datagram.toBytes());
        return datagramFromBytes.systemIterator();
    }
}
