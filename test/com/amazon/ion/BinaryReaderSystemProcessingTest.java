/*
 * Copyright (c) 2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;


/**
 *
 */
public class BinaryReaderSystemProcessingTest
    extends ReaderSystemProcessingTestCase
{
    @Override
    protected IonReader read(String text) throws Exception
    {
        IonLoader loader = loader();
        IonDatagram datagram = loader.load(text);
        byte[] bytes = datagram.toBytes();
        return system().newReader(bytes);
    }

    @Override
    protected IonReader systemRead(String text) throws Exception
    {
        IonLoader loader = loader();
        IonDatagram datagram = loader.load(text);
        byte[] bytes = datagram.toBytes();
        return system().newSystemReader(bytes);
    }
}
