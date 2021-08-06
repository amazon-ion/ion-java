package com.amazon.ion.impl.bin.utf8;

import java.util.concurrent.ArrayBlockingQueue;

abstract class Pool<T extends Poolable<?>> {

    /**
     * Allocates objects to be pooled.
     * @param <T> the type of object.
     */
    interface Allocator<T extends Poolable<?>> {

        /**
         * Allocate a new object and link it to the given pool.
         * @param pool the pool to which the new object will be linked.
         * @return a new instance.
         */
        T newInstance(Pool<T> pool);
    }

    // The maximum number of objects that can be waiting in the queue before new ones will be discarded.
    private static final int MAX_QUEUE_SIZE = 128;

    // A queue of previously initialized objects that can be loaned out.
    private final ArrayBlockingQueue<T> bufferQueue;

    // Allocator of objects to be pooled.
    private final Allocator<T> allocator;

    Pool(Allocator<T> allocator) {
        this.allocator = allocator;
        bufferQueue = new ArrayBlockingQueue<T>(MAX_QUEUE_SIZE);
    }

    /**
     * If the pool is not empty, removes an object from the pool and returns it;
     * otherwise, constructs a new object.
     *
     * @return An object.
     */
    public T getOrCreate() {
        // The `poll` method does not block. If the queue is empty it returns `null` immediately.
        T object = bufferQueue.poll();
        if (object == null) {
            // No buffers were available in the pool. Create a new one.
            object = allocator.newInstance(this);
        }
        return object;
    }

    /**
     * Adds the provided instance to the pool. If the pool is full, the instance will
     * be discarded.
     *
     * Callers MUST NOT use an object after returning it to the pool.
     *
     * @param object   An object to add to the pool.
     */
    public void returnToPool(T object) {
        // The `offer` method does not block. If the queue is full, it returns `false` immediately.
        // If the provided instance cannot be added to the pool, we discard it silently.
        bufferQueue.offer(object);
    }
}
