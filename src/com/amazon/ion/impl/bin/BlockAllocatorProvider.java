// Copyright (c) 2015 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.bin;

/**
 * A polymorphic source of {@link BlockAllocator}
 * <p>
 * Implementations must be thread-safe.
 */
/*package*/ abstract class BlockAllocatorProvider
{
    /*package*/ BlockAllocatorProvider() {}

    /** Vends a potentially non-thread safe {@link BlockAllocator}. */
    public abstract BlockAllocator vendAllocator(int blockSize);
}
