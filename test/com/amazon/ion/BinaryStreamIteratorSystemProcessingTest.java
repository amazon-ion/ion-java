// Copyright (c) 2009-2011 Amazon.com, Inc.  All rights reserved.

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
        myMissingSymbolTokensHaveText = false;
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
}
