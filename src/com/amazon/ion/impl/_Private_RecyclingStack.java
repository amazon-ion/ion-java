package com.amazon.ion.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

/**
 * A stack whose elements are recycled. This can be useful when the stack needs to grow and shrink
 * frequently and has a predictable maximum depth.
 * @param <T> the type of elements stored.
 */
public final class _Private_RecyclingStack<T> implements Iterable<T> {
    private $Iterator stackIterator;
    @Override
    public ListIterator<T> iterator() {
        if (stackIterator != null) {
            stackIterator.cursor = _Private_RecyclingStack.this.currentIndex;
        } else {
            stackIterator = new $Iterator();
        }
        return stackIterator;
    }

    /**
     * Factory for new stack elements.
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

    private final List<T> elements;
    private final ElementFactory<T> elementFactory;
    private int currentIndex;
    private T top;

    /**
     * @param initialCapacity the initial capacity of the underlying collection.
     * @param elementFactory the factory used to create a new element on {@link #push(Recycler)} when the stack has
     *                       not previously grown to the new depth.
     */
    public _Private_RecyclingStack(int initialCapacity, ElementFactory<T> elementFactory) {
        elements = new ArrayList<>(initialCapacity);
        this.elementFactory = elementFactory;
        currentIndex = -1;
        top = null;
    }

    /**
     * Pushes an element onto the top of the stack, instantiating a new element only if the stack has not
     * previously grown to the new depth.
     * @return the element at the top of the stack after the push. This element must be initialized by the caller.
     */
    public T push(Recycler<T> recycler) {
        currentIndex++;
        if (currentIndex >= elements.size()) {
            top = elementFactory.newElement();
            elements.add(top);
        }  else {
            top = elements.get(currentIndex);
        }
        recycler.recycle(top);
        return top;
    }

    /**
     * @return the element at the top of the stack, or null if the stack is empty.
     */
    public T peek() {
        return top;
    }

    /**
     * Pops an element from the stack, retaining a reference to the element so that it can be reused the
     * next time the stack grows to the element's depth.
     * @return the element that was at the top of the stack before the pop, or null if the stack was empty.
     */
    public T pop() {
        T popped = top;
        currentIndex--;
        if (currentIndex >= 0) {
            top = elements.get(currentIndex);
        } else {
            top = null;
            currentIndex = -1;
        }
        return popped;
    }

    /**
     * @return true if the stack is empty; otherwise, false.
     */
    public boolean isEmpty() {
        return top == null;
    }

    /**
     * @return the number of elements on the stack.
     */
    public int size() {
        return currentIndex + 1;
    }

    private class $Iterator implements ListIterator<T> {
        private int cursor;

        @Override
        public boolean hasNext() {
            return cursor >= 0;
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            // post-decrement because "next" is where the cursor is
            return _Private_RecyclingStack.this.elements.get(cursor--);
        }

        @Override
        public boolean hasPrevious() {
            return cursor + 1 <= _Private_RecyclingStack.this.currentIndex;
        }

        @Override
        public T previous() {
            if (!hasPrevious()) {
                throw new NoSuchElementException();
            }
            // pre-increment: "next" is where the cursor is, so "previous" is upward in stack
            return _Private_RecyclingStack.this.elements.get(++cursor);
        }

        @Override
        public int nextIndex() {
            return cursor;
        }

        @Override
        public int previousIndex() {
            return cursor + 1;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void set(T t) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void add(T t) {
            throw new UnsupportedOperationException();
        }
    }

}
