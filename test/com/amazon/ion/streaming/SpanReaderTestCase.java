// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.streaming;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonTestCase;
import com.amazon.ion.ReaderMaker;
import com.amazon.ion.Span;
import com.amazon.ion.SpanReader;
import com.amazon.ion.junit.IonAssert;

/**
 *
 */
public abstract class SpanReaderTestCase
    extends IonTestCase
{
    protected ReaderMaker myReaderMaker;

    public void setReaderMaker(ReaderMaker maker)
    {
        myReaderMaker = maker;
    }


    protected IonReader in;
    protected SpanReader sr;



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


    protected void hoist(Span s)
    {
        sr.hoist(s);
        expectTopLevel();
        expectNoCurrentValue();
    }
}
