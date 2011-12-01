// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.streaming;

import com.amazon.ion.InternedSymbol;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonTestCase;
import com.amazon.ion.IonType;
import com.amazon.ion.NullValueException;
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

    protected void expectField(String name)
    {
        IonAssert.expectField(in, name);
    }

    protected void expectNextField(String name)
    {
        IonAssert.expectNextField(in, name);
    }

    protected void expectString(String text)
    {
        assertEquals("getType", IonType.STRING, in.getType());
        assertEquals("isNullValue", text == null, in.isNullValue());
        assertEquals("stringValue", text, in.stringValue());
    }

    /**
     * @param text null means to expect null.symbol
     */
    protected void expectSymbol(String text)
    {
        assertEquals("getType", IonType.SYMBOL, in.getType());
        assertEquals("isNullValue", text == null, in.isNullValue());
        assertEquals("stringValue", text, in.stringValue());

        InternedSymbol is = in.symbolValue();
        if (text == null)
        {
            assertEquals("symbolValue", null, is);
            try {
                in.getSymbolId();
                fail("expected exception on " + in.getType());
            }
            catch (NullValueException e) { }
        }
        else
        {
            assertEquals("symbolValue.text", text, is.getText());
            in.getSymbolId(); // Shouldn't throw, at least
        }
    }
}
