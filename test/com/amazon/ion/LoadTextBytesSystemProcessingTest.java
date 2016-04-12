// Copyright (c) 2008-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import com.amazon.ion.impl._Private_Utils;


public class LoadTextBytesSystemProcessingTest
    extends DatagramIteratorSystemProcessingTest
{
    private byte[] myBytes;

    @Override
    protected void prepare(String text)
        throws Exception
    {
        myBytes = _Private_Utils.convertUtf16UnitsToUtf8(text);
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
