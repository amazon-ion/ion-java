/*
 * Copyright (c) 2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;


/**
 *
 */
public class LoadTextBytesSystemProcessingTest
    extends DatagramIteratorSystemProcessingTest
{
    private byte[] myBytes;

    @Override
    protected void prepare(String text)
        throws Exception
    {
//        myBytes = text.getBytes("UTF-8");
        myBytes = convertUtf16UnitsToUtf8(text);
    }

    @Override
    protected IonDatagram load()
        throws Exception
    {
        IonLoader loader = loader();
        IonDatagram datagram = loader.load(myBytes);
        return datagram;
    }
}
