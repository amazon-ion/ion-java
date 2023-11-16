package com.amazon.ion.impl.bin.utf8;

/**
 * A thread-safe shared pool of {@link PoolableByteBuffer}s.
 */
public class ByteBufferPool extends Pool<PoolableByteBuffer> {

    private static final ByteBufferPool INSTANCE = new ByteBufferPool();

    // Do not allow instantiation; all classes should share the singleton instance.
    private ByteBufferPool() {
        super(new Allocator<PoolableByteBuffer>() {
            @Override
            public PoolableByteBuffer newInstance(Pool<PoolableByteBuffer> pool) {
                return new PoolableByteBuffer(pool);
            }
        });
    }

    /**
     * @return a threadsafe shared instance of {@link ByteBufferPool}.
     */
    public static ByteBufferPool getInstance() {
        return INSTANCE;
    }
}
