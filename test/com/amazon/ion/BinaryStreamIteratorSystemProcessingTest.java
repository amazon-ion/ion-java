// Copyright (c) 2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Iterator;


public class BinaryStreamIteratorSystemProcessingTest
    extends IteratorSystemProcessingTest
{
    private byte[] myBytes;
    private InputStream myStream;

    @Override
    protected void prepare(String text)
        throws Exception
    {
        myBytes = encode(text);
        myStream = new ByteArrayInputStream(myBytes);
    }

    @Override
    protected Iterator<IonValue> iterate()
    {
        return system().iterate(myStream);
    }

    @Override
    protected Iterator<IonValue> systemIterate()
    {
        return system().systemIterate(myStream);
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
