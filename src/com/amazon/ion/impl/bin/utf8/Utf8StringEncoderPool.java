package com.amazon.ion.impl.bin.utf8;

/**
 * A thread-safe shared pool of {@link Utf8StringEncoder}s that can be used for UTF8 encoding.
 */
public class Utf8StringEncoderPool extends Pool<Utf8StringEncoder> {

    private static final Utf8StringEncoderPool INSTANCE = new Utf8StringEncoderPool();

    // Do not allow instantiation; all classes should share the singleton instance.
    private Utf8StringEncoderPool() {
        super(new Allocator<Utf8StringEncoder>() {
            @Override
            public Utf8StringEncoder newInstance(Pool<Utf8StringEncoder> pool) {
                return new Utf8StringEncoder(pool);
            }
        });
    }

    /**
     * @return a threadsafe shared instance of {@link Utf8StringEncoderPool}.
     */
    public static Utf8StringEncoderPool getInstance() {
        return INSTANCE;
    }

}
