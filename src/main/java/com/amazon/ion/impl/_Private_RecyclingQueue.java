package com.amazon.ion.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
/**
 * A queue whose elements are recycled. This queue will be extended and iterated frequently.
 * @param <T> the type of elements stored.
 */
public class _Private_RecyclingQueue<T> {

    /**
     * Factory for new queue elements.
     * @param <T> the type of element.
     */
    public interface ElementFactory<T> {
        /**
         * @return a new instance.
         */
        T newElement();
    }

    @FunctionalInterface
    public interface Recycler<T> {
        /**
         * Re-initialize an element
         */
        void recycle(T t);
    }

    /**
     * Iterator for the queue.
     */
    private class ElementIterator implements Iterator<T> {
        int i = 0;
        @Override
        public boolean hasNext() {
            return i <= currentIndex;
        }

        @Override
        public T next() {
            return elements.get(i++);
        }
    }

    private final ElementIterator iterator;
    private final List<T> elements;
    private final ElementFactory<T> elementFactory;
    private int currentIndex;
    private T top;

    /**
     * @param initialCapacity the initial capacity of the underlying collection.
     * @param elementFactory the factory used to create a new element on {@link #push()} when the queue has
     *                       not previously grown to the new depth.
     */
    public _Private_RecyclingQueue(int initialCapacity, ElementFactory<T> elementFactory) {
        elements = new ArrayList<T>(initialCapacity);
        this.elementFactory = elementFactory;
        currentIndex = -1;
        iterator = new ElementIterator();
    }

    public void truncate(int index) {
        currentIndex = index;
    }

    public T get(int index) {
        return elements.get(index);
    }

    /**
     * Pushes an element onto the top of the queue, instantiating a new element only if the queue has not
     * previously grown to the new depth.
     * @return the element at the top of the queue after the push. This element must be initialized by the caller.
     */
    public int push(Recycler<T> recycler) {
        currentIndex++;
        if (currentIndex >= elements.size()) {
            top = elementFactory.newElement();
            elements.add(top);
        }  else {
            top = elements.get(currentIndex);
        }
        recycler.recycle(top);
        return currentIndex;
    }

    /**
     * Reclaim the current element.
     */
    public void remove() {
        currentIndex = Math.max(-1, currentIndex - 1);
    }

    public Iterator<T> iterate() {
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