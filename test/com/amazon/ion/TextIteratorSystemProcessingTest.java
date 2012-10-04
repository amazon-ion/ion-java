// Copyright (c) 2008-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import com.amazon.ion.impl._Private_IonSystem;
import java.util.Iterator;


public class TextIteratorSystemProcessingTest
    extends IteratorSystemProcessingTestCase
{
    private String myText;


    @Override
    protected Iterator<IonValue> iterate()
        throws Exception
    {
        return system().iterate(myText);
    }

    @Override
    protected Iterator<IonValue> systemIterate()
        throws Exception
    {
        _Private_IonSystem sys = system();
        Iterator<IonValue> it = sys.systemIterate(myText);
        return it;
    }


    @Override
    protected void prepare(String text)
        throws Exception
    {
        myText = text;
    }
}
