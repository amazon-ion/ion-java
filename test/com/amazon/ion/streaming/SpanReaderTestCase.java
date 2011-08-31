// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.streaming;

import com.amazon.ion.Facets;
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
    protected final boolean mySpanReaderRequired;

    public SpanReaderTestCase(boolean spanReaderRequired)
    {
        mySpanReaderRequired = spanReaderRequired;
    }

    protected void initSpanReader()
    {
        if (mySpanReaderRequired)
        {
            sr = Facets.assumeFacet(SpanReader.class, in);
        }
        else
        {
            sr = in.asFacet(SpanReader.class);
        }
    }

    protected final void read(byte[] ionData)
    {
        in = myReaderMaker.newReader(system(), ionData);
        initSpanReader();
    }

    protected final void read(String ionText)
    {
        in = myReaderMaker.newReader(system(), ionText);
        initSpanReader();
    }



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
