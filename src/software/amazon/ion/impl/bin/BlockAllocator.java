/*
 * Copyright 2015-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package software.amazon.ion.impl.bin;

import java.io.Closeable;

/**
 * Creates {@link Block} instances of fixed size.  Instances are constructed by a {@link BlockAllocatorProvider}
 * And are usable to allocate {@link Block} instances by the caller until {@link #close()} is called.
 * <p>
 * A user must call {@link Block#close()} on all blocks allocated from this allocator, before invoking {@link #close()}.
 * <p>
 * Implementations are <b>not</b> thread-safe.
 */
/*package*/ abstract class BlockAllocator implements Closeable
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
