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

/**
 * A simple pooling implementation of {@link BlockAllocatorProvider} with a global thread-safe free block list
 * for each block size.
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
    private final class PooledBlockAllocator extends BlockAllocator
    {
        private final int blockSize, blockLimit;
        private final ConcurrentLinkedQueue<Block> freeBlocks;
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
                        if (freeBlocks.size() < blockLimit) {
                            reset();
                            freeBlocks.add(this);
                        }
                    }
                };
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

    private final ConcurrentMap<Integer, BlockAllocator> allocators;

    public PooledBlockAllocatorProvider()
    {
        allocators = new ConcurrentHashMap<Integer, BlockAllocator>();
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
