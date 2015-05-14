// Copyright (c) 2015 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.bin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import com.amazon.ion.impl.bin.Block;
import com.amazon.ion.impl.bin.BlockAllocator;
import com.amazon.ion.impl.bin.PooledBlockAllocatorProvider;

import org.junit.Before;
import org.junit.Test;

public class PooledBlockAllocatorProviderTest
{
    private PooledBlockAllocatorProvider provider;

    @Before
    public void setup()
    {
        provider = new PooledBlockAllocatorProvider();
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
    public void testReuseAllocatorBlocksAfterClose()
    {
        final BlockAllocator allocator1 = provider.vendAllocator(8);
        final Block block1 = allocator1.allocateBlock();
        final Block block2 = allocator1.allocateBlock();
        block1.close();
        block2.close();

        final BlockAllocator allocator2 = provider.vendAllocator(8);
        {
            final Block block = allocator2.allocateBlock();
            assertNotSame(block, block1);
            block.close();
        }

        allocator1.close();

        final BlockAllocator allocator3 = provider.vendAllocator(8);
        final Block block1Again = allocator3.allocateBlock();
        final Block block2Again = allocator3.allocateBlock();
        assertSame(block1.data, block1Again.data);
        assertSame(block2.data, block2Again.data);

        block1Again.close();
        block2Again.close();
    }

    @Test(expected = IllegalStateException.class)
    public void testCloseBlockAfterClosingAllocator()
    {
        final BlockAllocator allocator = provider.vendAllocator(8);
        final Block block = allocator.allocateBlock();
        allocator.close();
        block.close();
    }
}
