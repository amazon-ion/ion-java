// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

/**
 * NOT SUITABLE FOR PUBLIC USE since it doesn't enforce correctness.
 */
class FakeInternedSymbol
    implements InternedSymbol
{
    private final String myText;
    private final int mySid;

    FakeInternedSymbol(String text, int sid)
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