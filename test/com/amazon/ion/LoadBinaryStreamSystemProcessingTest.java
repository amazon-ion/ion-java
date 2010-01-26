/*
 * Copyright (c) 2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

/**
 *
 */
public class LoadBinaryStreamSystemProcessingTest
    extends DatagramIteratorSystemProcessingTest
{
    private byte[] myBytes;

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
        InputStream in = new ByteArrayInputStream(myBytes);
        IonLoader loader = loader();
        IonDatagram datagram = loader.load(in);
        return datagram;
    }

    @Override
    protected boolean checkMissingSymbol(String expected, int expectedSymbolTableSid, int expectedLocalSid)
        throws Exception
    {
        checkSymbol("$" + expectedSymbolTableSid, expectedSymbolTableSid);

        // when missing from a shared table the symbol
        // will not have been added to the local symbols
        return false;
    }
}
