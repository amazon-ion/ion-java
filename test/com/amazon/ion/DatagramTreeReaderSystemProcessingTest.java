/*
 * Copyright (c) 2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;


/**
 *
 */
public class DatagramTreeReaderSystemProcessingTest
    extends ReaderSystemProcessingTestCase
{
    @Override
    protected IonReader read(String text) throws Exception
    {
        IonLoader loader = loader();
        IonDatagram datagram = loader.load(text);
        return system().newReader(datagram);
    }

    @Override
    protected IonReader systemRead(String text) throws Exception
    {
        IonLoader loader = loader();
        IonDatagram datagram = loader.load(text);
        return system().newSystemReader(datagram);
    }
}
