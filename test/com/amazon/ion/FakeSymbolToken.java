// Copyright (c) 2011-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

/**
 * NOT SUITABLE FOR PUBLIC USE since it doesn't enforce correctness.
 */
public class FakeSymbolToken
    implements SymbolToken
{
    private final String myText;
    private final int mySid;

    public FakeSymbolToken(String text, int sid)
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

    public int getSid()
    {
        return mySid;
    }
}
