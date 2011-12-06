// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.InternedSymbol;
import com.amazon.ion.UnknownSymbolException;
import com.amazon.ion.util.IonTextUtils;


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
        return "InternedSymbol::{text:" + IonTextUtils.printString(myText)
            + ",id:" + mySid + "}";
    }
}
