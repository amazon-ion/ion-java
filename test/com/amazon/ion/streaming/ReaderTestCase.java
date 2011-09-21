// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.streaming;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonTestCase;
import com.amazon.ion.ReaderMaker;
import com.amazon.ion.junit.IonAssert;
import org.junit.After;


/**
 *
 */
public abstract class ReaderTestCase
    extends IonTestCase
{
    protected ReaderMaker myReaderMaker;
    protected IonReader in;

    public void setReaderMaker(ReaderMaker maker)
    {
        myReaderMaker = maker;
    }

    @After @Override
    public void tearDown() throws Exception
    {
        in = null;
        super.tearDown();
    }

    //========================================================================

    protected void read(byte[] ionData)
    {
        in = myReaderMaker.newReader(system(), ionData);
    }

    protected void read(String ionText)
    {
        in = myReaderMaker.newReader(system(), ionText);
    }

    //========================================================================

    protected void expectNoCurrentValue()
    {
        IonAssert.assertNoCurrentValue(in);
    }

    protected void expectTopLevel()
    {
        IonAssert.assertTopLevel(in);
    }

    protected void expectEof()
    {
        IonAssert.assertEof(in);
    }

    protected void expectTopEof()
    {
        IonAssert.assertTopEof(in);
    }
}
