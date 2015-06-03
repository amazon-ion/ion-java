// Copyright (c) 2015 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.bin;

/**
 * Constructs {@link BlockAllocator} instances.  Such instances returned by {@link #vendAllocator(int)}
 * should not be shared across threads and must be closed when no longer needed to re-use or clean up
 * any underlying resources.
 * <p>
 * Implementations must be thread-safe.
 */
/*package*/ abstract class BlockAllocatorProvider
{
    /*package*/ BlockAllocatorProvider() {}

    /**
     * Returns a {@link BlockAllocator} that vends blocks of the given size.
     * This method can be used across threads, but the returned {@link BlockAllocator} instances are not
     * thread-safe and must have their {@link BlockAllocator#close()} method called to allow its underlying resources
     * to be released or reused properly.
     */
    public abstract BlockAllocator vendAllocator(int blockSize);
}
