/*
 * Copyright 2007-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.ion.impl.bin;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A singleton implementation of {@link BlockAllocatorProvider} offering a thread-safe free block list
 * for each block size.
 *
 * <p>
 * This implementation is thread-safe.
 */
/*package*/ final class PooledBlockAllocatorProvider extends BlockAllocatorProvider
{
    /**
     * A {@link BlockAllocator} of for a particular size that has a single thread-safe free list.
     * <p>
     * This implementation is thread-safe.
     */
    private static final class PooledBlockAllocator extends BlockAllocator
    {
        private final int blockSize, blockLimit;
        private final ConcurrentLinkedQueue<Block> freeBlocks;
        private final AtomicInteger size = new AtomicInteger(0);
        static final int FREE_CAPACITY = 1024 * 1024 * 64; // 64MB

        public PooledBlockAllocator(final int blockSize)
        {
            this.blockSize = blockSize;
            this.freeBlocks = new ConcurrentLinkedQueue<Block>();
            this.blockLimit = FREE_CAPACITY / blockSize;
        }

        @Override
        public Block allocateBlock()
        {
            Block block = freeBlocks.poll();
            if (block == null)
            {
                block = new Block(new byte[blockSize])
                {
                    @Override
                    public void close()
                    {
                        // In the common case, the pool is not full. Optimistically increment the size.
                        if (size.getAndIncrement() < blockLimit)
                        {
                            reset();
                            freeBlocks.add(this);
                        }
                        else
                        {
                            // The pool was full. Since the size was optimistically incremented, decrement it now.
                            // Note: there is a race condition here that is deliberately allowed as an optimization.
                            // Under high contention, multiple threads could end up here before the first one
                            // decrements the size, causing blocks to be dropped wastefully. This is not harmful
                            // because blocks will be re-allocated when necessary; the pool is kept as close as
                            // possible to capacity on a best-effort basis. This race condition should not be "fixed"
                            // without a thorough study of the performance implications.
                            size.decrementAndGet();
                        }
                    }
                };
            }
            else
            {
                // A block was retrieved from the pool; decrement the pool size.
                size.decrementAndGet();
            }
            return block;
        }

        @Override
        public int getBlockSize()
        {
            return blockSize;
        }

        @Override
        public void close() {}
    }

    // A globally shared instance of the PooledBlockAllocatorProvider.
    // This instance allows BlockAllocators to be re-used across instantiations of classes like
    // the binary Ion writer, thereby avoiding costly array initializations.
    private static final PooledBlockAllocatorProvider INSTANCE = new PooledBlockAllocatorProvider();
    private final ConcurrentMap<Integer, BlockAllocator> allocators;

    private PooledBlockAllocatorProvider()
    {
        allocators = new ConcurrentHashMap<Integer, BlockAllocator>();
    }

    public static PooledBlockAllocatorProvider getInstance() {
        return INSTANCE;
    }

    @Override
    public BlockAllocator vendAllocator(final int blockSize)
    {
        if (blockSize <= 0)
        {
            throw new IllegalArgumentException("Invalid block size: " + blockSize);
        }

        BlockAllocator allocator = allocators.get(blockSize);
        if (allocator == null)
        {
            allocator = new PooledBlockAllocator(blockSize);
            final BlockAllocator existingAllocator = allocators.putIfAbsent(blockSize, allocator);
            if (existingAllocator != null)
            {
                allocator = existingAllocator;
            }
        }
        return allocator;
    }
}
