// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import java.io.Reader;
import java.io.StringReader;
import java.util.Iterator;


public class JavaReaderIteratorSystemProcessingTest
    extends IteratorSystemProcessingTest
{
    private Reader myStream;

    @Override
    protected void prepare(String text)
        throws Exception
    {
        myStream = new StringReader(text);
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
