package com.amazon.ion.impl.bin.utf8;

import java.nio.ByteBuffer;

/**
 * Holds a reusable {@link ByteBuffer}. Instances of this class are reusable but are NOT threadsafe.
 *
 * Instances are vended by {@link ByteBufferPool#getOrCreate()}.
 *
 * Users are expected to call {@link #close()} when the decoder is no longer needed.
 */
public class PoolableByteBuffer extends Poolable<PoolableByteBuffer> {

    static final int BUFFER_SIZE_IN_BYTES = 4 * 1024;

    // The reusable buffer.
    private final ByteBuffer buffer;

    /**
     * @param pool the pool to which the object will be returned upon {@link #close()}.
     */
    PoolableByteBuffer(Pool<PoolableByteBuffer> pool) {
        super(pool);
        buffer = ByteBuffer.allocate(BUFFER_SIZE_IN_BYTES);
    }

    /**
     * @return the buffer.
     */
    public ByteBuffer getBuffer() {
        return buffer;
    }
}
