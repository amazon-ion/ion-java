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

/**
 * Utility implementations of {@link BlockAllocatorProvider}.
 */
/*package*/ final class BlockAllocatorProviders
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
