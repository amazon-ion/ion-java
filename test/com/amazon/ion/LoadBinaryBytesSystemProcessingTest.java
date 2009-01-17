/*
 * Copyright (c) 2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;


/**
 *
 */
public class LoadBinaryBytesSystemProcessingTest
    extends DatagramIteratorSystemProcessingTest
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
        myBytes = encode(text);
    }

    @Override
    protected IonDatagram load()
        throws Exception
    {
        IonLoader loader = loader();
        IonDatagram datagram = loader.load(myBytes);
        return datagram;
    }

    @Override
    protected void checkMissingSymbol(String expected, int expectedSid)
        throws Exception
    {
        checkSymbol("$" + expectedSid, expectedSid);
    }
}
