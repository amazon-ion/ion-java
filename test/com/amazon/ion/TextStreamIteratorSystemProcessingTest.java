// Copyright (c) 2009-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import com.amazon.ion.impl._Private_Utils;
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
        myBytes = _Private_Utils.convertUtf16UnitsToUtf8(text);

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
