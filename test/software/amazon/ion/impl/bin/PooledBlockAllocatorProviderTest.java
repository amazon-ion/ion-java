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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import software.amazon.ion.impl.bin.Block;
import software.amazon.ion.impl.bin.BlockAllocator;
import software.amazon.ion.impl.bin.PooledBlockAllocatorProvider;

public class PooledBlockAllocatorProviderTest
{
    private PooledBlockAllocatorProvider provider;

    @Before
    public void setup()
    {
        provider = new PooledBlockAllocatorProvider();
    }

    @After
    public void teardown()
    {
        // make sure the provider is not retained
        provider = null;
    }

    @Test
    public void testReuseBlockAfterClose()
    {
        final BlockAllocator allocator = provider.vendAllocator(8);
        final Block block1 = allocator.allocateBlock();
        final Block block2 = allocator.allocateBlock();
        assertNotSame(block1.data, block2.data);
        assertEquals(0, block1.limit);
        assertEquals(8, block1.data.length);
        block1.limit = 7;
        block1.close();
        final Block block1Again = allocator.allocateBlock();
        assertSame(block1.data, block1Again.data);
        assertEquals(0, block1Again.limit);
    }

    @Test
    public void testReuseAllocatorBlocksAcrossAllocators()
    {
        final BlockAllocator allocator1 = provider.vendAllocator(8);
        final Block block1 = allocator1.allocateBlock();
        final Block block2 = allocator1.allocateBlock();
        block1.limit = 4;
        block1.close();
        block2.limit = 5;
        block2.close();

        final BlockAllocator allocator2 = provider.vendAllocator(8);
        {
            final Block block = allocator2.allocateBlock();
            assertSame(block, block1);
            assertEquals(0, block.limit);
            block.close();
        }

        allocator1.close();

        final BlockAllocator allocator3 = provider.vendAllocator(8);
        final Block block1Again = allocator3.allocateBlock();
        final Block block2Again = allocator3.allocateBlock();
        assertSame(block2, block1Again);
        assertEquals(0, block1Again.limit);
        assertSame(block1, block2Again);
        assertEquals(0, block2Again.limit);

        block1Again.close();
        block2Again.close();
    }
}
