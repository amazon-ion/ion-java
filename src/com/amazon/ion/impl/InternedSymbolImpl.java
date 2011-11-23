// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.InternedSymbol;


final class InternedSymbolImpl
    implements InternedSymbol
{
    private final String myText;
    private final int mySid;

    InternedSymbolImpl(String text, int sid)
    {
        myText = text;
        mySid = sid;
    }


    public String stringValue()
    {
        return myText;
    }

    public int getSymbolId()
    {
        return mySid;
    }
}