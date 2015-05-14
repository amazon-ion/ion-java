// Copyright (c) 2015 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.bin;

import java.io.Closeable;

/**
 * Creates blocks of, usually, fixed size.
 * <p>
 * Implementations are generally <b>not</b> thread-safe.
 */
/*package*/ abstract class BlockAllocator implements Closeable
{
    /*package*/ BlockAllocator() {}

    public abstract Block allocateBlock();

    public abstract int getBlockSize();

    public abstract void close();
}
