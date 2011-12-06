// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.util.IonTextUtils.printString;

import com.amazon.ion.InternedSymbol;
import com.amazon.ion.UnknownSymbolException;


final class InternedSymbolImpl
    implements InternedSymbol
{
    private final String myText;
    private final int mySid;

    InternedSymbolImpl(String text, int sid)
    {
        assert text != null || sid > 0 : "Neither text nor sid is defined";

        myText = text;
        mySid = sid;
    }

    InternedSymbolImpl(int sid)
    {
        assert sid > 0 : "sid is undefined";

        myText = null;
        mySid = sid;
    }


    public String getText()
    {
        return myText;
    }

    public String assumeText()
    {
        if (myText == null) throw new UnknownSymbolException(mySid);
        return myText;
    }

    public int getId()
    {
        return mySid;
    }

    @Override
    public String toString()
    {
        String text = (myText == null ? null : printString(myText));
        return "InternedSymbol::{text:" + text + ",id:" + mySid + "}";
    }
}
