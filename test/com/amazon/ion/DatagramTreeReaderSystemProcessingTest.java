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
    private String myText;

    @Override
    protected boolean processingBinary()
    {
        return false;
    }

    @Override
    protected void prepare(String text)
    {
        myText = text;
    }

    @Override
    protected IonReader read() throws Exception
    {
        IonLoader loader = loader();
        IonDatagram datagram = loader.load(myText);
        return system().newReader(datagram);
    }

    @Override
    protected IonReader systemRead() throws Exception
    {
        IonLoader loader = loader();
        IonDatagram datagram = loader.load(myText);
        return system().newSystemReader(datagram);
    }

    @Override
    protected void checkMissingSymbol(String expected, int expectedSid)
        throws Exception
    {
        checkSymbol(expected, expectedSid);
    }
}
