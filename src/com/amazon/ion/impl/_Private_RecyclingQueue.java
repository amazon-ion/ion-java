package com.amazon.ion.impl;

import com.amazon.ion.impl.bin.utf8.Pool;
import com.amazon.ion.impl.bin.utf8.Poolable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A queue whose elements are recycled. This queue will be extended and iterated frequently.
 * @param <T> the PatchPoint stored.
 */
public class _Private_RecyclingQueue<T> extends Poolable<_Private_RecyclingQueue<T>> {

    /**
     * Iterator for the queue.
     */
    private class ElementIterator implements Iterator<PatchPoint> {
        int i = 0;
        @Override
        public boolean hasNext() {
            return i <= currentIndex;
        }

        @Override
        public PatchPoint next() {
            return elements.get(i++);
        }
    }

    private final ElementIterator iterator;
    private final List<PatchPoint> elements;
    private int currentIndex;
    // Initial capacity of the recycling queue.
    private final int INITIAL_CAPACITY = 512;
    private PatchPoint top;

    public _Private_RecyclingQueue(Pool<_Private_RecyclingQueue<T>> pool) {
        super(pool);
        elements = new ArrayList<PatchPoint>(INITIAL_CAPACITY);
        currentIndex = -1;
        iterator = new ElementIterator();
    }

    public void truncate(int index) {
        currentIndex = index;
    }

    public PatchPoint get(int index) {
        return elements.get(index);
    }

    /**
     * Pushes an element onto the top of the queue, instantiating a new element only if the queue has not
     * previously grown to the new depth.
     * @return the element at the top of the queue after the push. This element must be initialized by the caller.
     */
    public PatchPoint push() {
        currentIndex++;
        if (currentIndex >= elements.size()) {
            top = new PatchPoint();
            elements.add(top);
        }  else {
            top = elements.get(currentIndex);
        }
        return top;
    }

    /**
     * Reclaim the current element.
     */
    public void remove() {
        currentIndex = Math.max(-1, currentIndex - 1);
    }

    public Iterator<PatchPoint> iterate() {
        iterator.i = 0;
        return iterator;
    }

    /**
     * @return true if the queue is empty; otherwise, false.
     */
    public boolean isEmpty() {
        return currentIndex < 0;
    }

    /**
     * Reset the index and make the queue reusable.
     */
    public void clear() {
        currentIndex = -1;
    }

    /**
     * @return the number of elements within the queue.
     */
    public int size() {
        return currentIndex + 1;
    }
}