// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.bin;

import java.io.Closeable;


/**
 * An abstraction for a block of managed memory.  A {@link Block} is acquired by a caller via
 * {@link BlockAllocator#allocateBlock()} and released by {@link #close()}.
 * <p>
 * This class and its implementations are <b>not</b> thread-safe.
 */
public abstract class Block implements Closeable
{
    /** The data backing this block. */
    public final byte[] data;
    /** The first index for which data has not been written to or read from. */
    public int limit;

    /*package*/ Block(final byte[] data)
    {
        this.data = data;
        this.limit = 0;
    }

    /** Resets the limit to zero. */
    public final void reset()
    {
        limit = 0;
    }

    /** Returns the unused amount of bytes from the limit to the capacity of the data array. */
    public final int remaining()
    {
        return data.length - limit;
    }

    /** Returns the underlying data array's capacity. */
    public final int capacity()
    {
        return data.length;
    }

    /**
     * Releases the block back to the {@link BlockAllocator} from whence it was allocated from.
     * This method <b>must</b> be called when the block is no longer needed.
     * This is used to allow underlying resources to be reclaimed and/or released properly.
     */
    public abstract void close();
}
