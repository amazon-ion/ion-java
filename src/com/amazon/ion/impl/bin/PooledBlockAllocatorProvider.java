// Copyright (c) 2015 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.bin;

import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

/** A simple pooling implementation of {@link BlockAllocatorProvider}. */
/*package*/ class PooledBlockAllocatorProvider extends BlockAllocatorProvider
{
    private final ConcurrentMap<Integer, ConcurrentLinkedQueue<Queue<byte[]>>> freeBlockLists;

    public PooledBlockAllocatorProvider()
    {
        freeBlockLists = new ConcurrentHashMap<Integer, ConcurrentLinkedQueue<Queue<byte[]>>>();
    }

    private ConcurrentLinkedQueue<Queue<byte[]>> allocatorFreeList(final int blockSize) {
        ConcurrentLinkedQueue<Queue<byte[]>> free = freeBlockLists.get(blockSize);
        if (free == null)
        {
            free = new ConcurrentLinkedQueue<Queue<byte[]>>();
            ConcurrentLinkedQueue<Queue<byte[]>> prev = freeBlockLists.putIfAbsent(blockSize, free);
            if (prev != null)
            {
                free = prev;
            }
        }
        return free;
    }

    private static Queue<byte[]> CLOSED_QUEUE = new AbstractQueue<byte[]>()
    {
        public boolean offer(byte[] e)
        {
            throw new IllegalStateException("Allocator is closed");
        }

        public byte[] poll()
        {
            throw new IllegalStateException("Allocator is closed");
        }

        public byte[] peek()
        {
            throw new IllegalStateException("Allocator is closed");
        }

        @Override
        public Iterator<byte[]> iterator()
        {
            throw new IllegalStateException("Allocator is closed");
        }

        @Override
        public int size()
        {
            throw new IllegalStateException("Allocator is closed");
        }


    };

    @Override
    public BlockAllocator vendAllocator(final int blockSize)
    {
        final ConcurrentLinkedQueue<Queue<byte[]>> freeBlockList = allocatorFreeList(blockSize);
        final Queue<byte[]> blockList = freeBlockList.poll();

        return new BlockAllocator()
        {
            // XXX this does not have to be thread-safe
            private Queue<byte[]> freeBlocks = blockList != null ? blockList : new LinkedList<byte[]>();

            @Override
            public Block allocateBlock()
            {
                final byte[] data;
                if (!freeBlocks.isEmpty())
                {
                    data = freeBlocks.poll();
                }
                else
                {
                    data = new byte[blockSize];
                }


                return new Block(data)
                {
                    @Override
                    public void close()
                    {
                        // return ourselves to the free list
                        freeBlocks.add(this.data);
                    }
                };
            }

            @Override
            public int getBlockSize()
            {
                return blockSize;
            }

            @Override
            public void close()
            {
                // return ourselves to the free list
                freeBlockList.add(freeBlocks);

                // make sure other blocks allocated don't get put back into this returned list
                freeBlocks = CLOSED_QUEUE;
            }
        };
    }
}
