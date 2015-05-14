// Copyright (c) 2015 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.bin;

import java.io.Closeable;


/** Simple abstraction for a block of memory. */
/*package*/ abstract class Block implements Closeable
{
    public final byte[] data;
    public int limit;

    /*package*/ Block(final byte[] data)
    {
        this.data = data;
        this.limit = 0;
    }

    public final void reset()
    {
        limit = 0;
    }

    public final int remaining()
    {
        return data.length - limit;
    }

    public final int capacity()
    {
        return data.length;
    }

    public abstract void close();
}
