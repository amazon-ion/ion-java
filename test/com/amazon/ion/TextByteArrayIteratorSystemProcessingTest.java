// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import java.util.Iterator;


public class TextByteArrayIteratorSystemProcessingTest
    extends IteratorSystemProcessingTest
{
    private byte[] myBytes;

    @Override
    protected void prepare(String text)
        throws Exception
    {
        myBytes = convertUtf16UnitsToUtf8(text);
    }

    @Override
    protected Iterator<IonValue> iterate()
    {
        return system().iterate(myBytes);
    }

    @Override
    protected Iterator<IonValue> systemIterate()
    {
        return system().systemIterate(myBytes);
    }
}
