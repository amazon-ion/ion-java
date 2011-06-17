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
    protected void prepare(String text)
    {
        myText = text;
    }

    @Override
    public IonReader read() throws Exception
    {
        IonLoader loader = loader();
        IonDatagram datagram = loader.load(myText);
        return system().newReader(datagram);
    }

    @Override
    public IonReader systemRead() throws Exception
    {
        IonLoader loader = loader();
        IonDatagram datagram = loader.load(myText);
        return system().newSystemReader(datagram);
    }

    @Override
    protected boolean checkMissingSymbol(String expected, int expectedSymbolTableSid, int expectedLocalSid)
        throws Exception
    {
        // the symbol is missing from the shared symbol table
        // so the only valid sid is the local id that got assigned
        // to fill in the gap
        checkSymbol(expected, expectedLocalSid);

        // when missing from a shared table the symbol
        // will have been added to the local symbols
        return true;
    }

}
