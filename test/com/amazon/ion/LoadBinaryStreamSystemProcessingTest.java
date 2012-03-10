// Copyright (c) 2008-2011 Amazon.com, Inc.  All rights reserved.

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
        myMissingSymbolTokensHaveText = false;
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
}
