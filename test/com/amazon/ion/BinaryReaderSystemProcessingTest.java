// Copyright (c) 2008-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;



/**
 *
 */
public class BinaryReaderSystemProcessingTest
    extends ReaderSystemProcessingTestCase
{
    protected byte[] myBytes;


    @Override
    protected void prepare(String text)
        throws Exception
    {
        myMissingSymbolTokensHaveText = false;

        IonLoader loader = loader();
        IonDatagram datagram = loader.load(text);
        myBytes = datagram.getBytes();
    }

    @Override
    public IonReader read() throws Exception
    {
        return system().newReader(myBytes);
    }

    @Override
    public IonReader systemRead() throws Exception
    {
        return system().newSystemReader(myBytes);
    }
}
