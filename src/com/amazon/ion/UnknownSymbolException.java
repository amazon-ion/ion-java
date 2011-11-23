// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

/**
 * Indicates that a symbol ID could not be translated into text.
 */
public class UnknownSymbolException
    extends IonException
{
    private static final long serialVersionUID = 1L;

    private final int mySid;

    public UnknownSymbolException(int sid)
    {
        mySid = sid;
    }

    public int getSid()
    {
        return mySid;
    }

    @Override
    public String getMessage()
    {
        return "Unknown symbol text for $" + mySid;
    }
}
