/*
 * Copyright (c) 2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

/**
 *
 */
public class LoadTextStreamSystemProcessingTest
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
        InputStream in = new ByteArrayInputStream(myBytes);
        IonLoader loader = loader();
        IonDatagram datagram = loader.load(in);

        // Force symtab preparation  FIXME should not be necessary
        datagram.byteSize();

        return datagram;
    }
}
