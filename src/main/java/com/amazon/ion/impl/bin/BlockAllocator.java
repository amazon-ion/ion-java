// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.bin;

import java.io.Closeable;

/**
 * Creates {@link Block} instances of fixed size.  Instances are constructed by a {@link BlockAllocatorProvider}
 * And are usable to allocate {@link Block} instances by the caller until {@link #close()} is called.
 * <p>
 * A user must call {@link Block#close()} on all blocks allocated from this allocator, before invoking {@link #close()}.
 * <p>
 * Implementations are <b>not</b> thread-safe.
 */
public abstract class BlockAllocator implements Closeable
{
    /*package*/ BlockAllocator() {}

    /**
     * Allocates a {@link Block} instance to the caller.  The caller owns the block until {@link Block#close()} is
     * called--once {@link Block#close()} is called, an implementation may re-allocate it to another caller.
     */
    public abstract Block allocateBlock();

    /** Returns the fixed block size of the allocator. */
    public abstract int getBlockSize();

    /**
     * Releases the allocator back to the {@link BlockAllocatorProvider} from whence it came.
     * This is used to allow underlying resources to be reclaimed and/or released properly.
     */
    public abstract void close();
}
