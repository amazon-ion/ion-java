package com.amazon.ion.impl.bin.utf8;

import java.util.concurrent.ArrayBlockingQueue;

/**
 * A thread-safe shared pool of {@link Utf8StringEncoder}s that can be used for UTF8 encoding and decoding.
 */
public class Utf8StringEncoderPool {
    // The maximum number of Utf8Encoders that can be waiting in the queue before new ones will be discarded.
    private static final int MAX_QUEUE_SIZE = 32;

    // A singleton instance.
    private static final Utf8StringEncoderPool INSTANCE = new Utf8StringEncoderPool();

    // A queue of previously initialized encoders that can be loaned out.
    ArrayBlockingQueue<Utf8StringEncoder> bufferQueue;

    // Do not allow instantiation; all classes should share the singleton instance.
    private Utf8StringEncoderPool() {
        bufferQueue = new ArrayBlockingQueue<Utf8StringEncoder>(MAX_QUEUE_SIZE);
    }

    /**
     * @return a threadsafe shared instance of {@link Utf8StringEncoderPool}.
     */
    public static Utf8StringEncoderPool getInstance() {
        return INSTANCE;
    }

    /**
     * If the pool is not empty, removes an instance of {@link Utf8StringEncoder} from the pool and returns it;
     * otherwise, constructs a new instance.
     *
     * @return An instance of {@link Utf8StringEncoder}.
     */
    public Utf8StringEncoder getOrCreateUtf8Encoder() {
        // The `poll` method does not block. If the queue is empty it returns `null` immediately.
        Utf8StringEncoder encoder = bufferQueue.poll();
        if (encoder == null) {
            // No buffers were available in the pool. Create a new one.
            encoder = new Utf8StringEncoder();
        }
        return encoder;
    }

    /**
     * Adds the provided instance of {@link Utf8StringEncoder} to the pool. If the pool is full, the instance will
     * be discarded.
     *
     * Callers MUST NOT use an encoder after returning it to the pool.
     *
     * @param encoder   A {@link Utf8StringEncoder} to add to the pool.
     */
    public void returnEncoderToPool(Utf8StringEncoder encoder) {
        // The `offer` method does not block. If the queue is full, it returns `false` immediately.
        // If the provided instance cannot be added to the pool, we discard it silently.
        bufferQueue.offer(encoder);
    }

}
