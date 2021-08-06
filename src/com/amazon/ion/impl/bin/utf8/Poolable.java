package com.amazon.ion.impl.bin.utf8;

import java.io.Closeable;

/**
 * Base class for types that may be pooled.
 * @param <T> the concrete type.
 */
abstract class Poolable<T extends Poolable<T>> implements Closeable {

    // The pool to which this object is linked.
    private final Pool<T> pool;

    /**
     * @param pool the pool to which the object will be returned upon {@link #close()}.
     */
    Poolable(Pool<T> pool) {
        this.pool = pool;
    }

    /**
     * Attempts to return this instance to the pool with which it is associated, if any.
     *
     * Do not continue to use this instance after calling this method.
     */
    @Override
    public void close() {
        pool.returnToPool((T) this);
    }
}
