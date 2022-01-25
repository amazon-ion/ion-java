package com.amazon.ion.impl.bin.utf8;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

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
    private final Queue<T> objectQueue;

    // The current size of the queue. Note: some implementations of Queue.size() (including ConcurrentLinkedQueue's)
    // are not constant-time operations. Tracking the size externally is a performance optimization.
    private final AtomicInteger size;

    // Allocator of objects to be pooled.
    private final Allocator<T> allocator;

    Pool(Allocator<T> allocator) {
        this.allocator = allocator;
        objectQueue = new ConcurrentLinkedQueue<T>();
        size = new AtomicInteger(0);
    }

    /**
     * If the pool is not empty, removes an object from the pool and returns it;
     * otherwise, constructs a new object.
     *
     * @return An object.
     */
    public T getOrCreate() {
        // The `poll` method does not block. If the queue is empty it returns `null` immediately.
        T object = objectQueue.poll();
        if (object == null) {
            // No objects were available in the pool. Create a new one.
            object = allocator.newInstance(this);
        } else {
            // An object was retrieved from the pool; decrement the pool size.
            size.decrementAndGet();
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
        if (size.getAndIncrement() < MAX_QUEUE_SIZE) {
            objectQueue.offer(object);
        } else {
            // The pool was full. Since the size was optimistically incremented, decrement it now.
            // Note: there is a race condition here that is deliberately allowed as an optimization.
            // Under high contention, multiple threads could end up here before the first one
            // decrements the size, causing objects to be dropped wastefully. This is not harmful
            // because objects will be re-allocated when necessary; the pool is kept as close as
            // possible to capacity on a best-effort basis. This race condition should not be "fixed"
            // without a thorough study of the performance implications.
            size.decrementAndGet();
        }
    }
}
