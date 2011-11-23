// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.InternedSymbol;
import com.amazon.ion.UnknownSymbolException;


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
}