// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.bin;

/**
 * Utility implementations of {@link BlockAllocatorProvider}.
 */
public final class BlockAllocatorProviders
{
    private BlockAllocatorProviders() {}

    private static final class BasicBlockAllocatorProvider extends BlockAllocatorProvider
    {
        @Override
        public BlockAllocator vendAllocator(final int blockSize)
        {
            return new BlockAllocator() {
                @Override
                public Block allocateBlock()
                {
                    return new Block(new byte[blockSize])
                    {
                        @Override
                        public void close() {}
                    };
                }

                @Override
                public int getBlockSize()
                {
                    return blockSize;
                }

                @Override
                public void close() {}
            };
        }
    }

    private static final BlockAllocatorProvider BASIC_PROVIDER = new BasicBlockAllocatorProvider();

    /**
     * A {@link BlockAllocatorProvider} that does no caching of {@link BlockAllocator} or {@link Block} instances  whatsoever.
     */
    public static BlockAllocatorProvider basicProvider() {
        return BASIC_PROVIDER;
    }
}
