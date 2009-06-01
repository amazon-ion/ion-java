// Copyright (c) 2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Iterator;


public class TextStreamIteratorSystemProcessingTest
    extends IteratorSystemProcessingTest
{
    private byte[] myBytes;
    private InputStream myStream;


    @Override
    protected void prepare(String text)
        throws Exception
    {
        myBytes = text.getBytes("UTF-8");
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
