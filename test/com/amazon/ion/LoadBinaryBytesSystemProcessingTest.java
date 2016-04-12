// Copyright (c) 2008-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;



public class LoadBinaryBytesSystemProcessingTest
    extends DatagramIteratorSystemProcessingTest
{
    private byte[] myBytes;


    @Override
    protected void prepare(String text)
        throws Exception
    {
        myMissingSymbolTokensHaveText = false;
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
}
