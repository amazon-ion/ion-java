/*
 * Copyright (c) 2008-2009 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;


/**
 *
 */
public class BinaryReaderSystemProcessingTest
    extends ReaderSystemProcessingTestCase
{
    protected byte[] myBytes;

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
    protected IonReader read() throws Exception
    {
        return system().newReader(myBytes);
    }

    @Override
    protected IonReader systemRead() throws Exception
    {
        return system().newSystemReader(myBytes);
    }

    @Override
    protected void checkMissingSymbol(String expected, int expectedSid)
        throws Exception
    {
        checkSymbol("$" + expectedSid, expectedSid);
    }
}
