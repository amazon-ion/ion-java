package com.amazon.ion.impl.bin.utf8;

/**
 * A thread-safe shared pool of {@link Utf8StringDecoder}s that can be used for UTF8 decoding.
 */
public class Utf8StringDecoderPool extends Pool<Utf8StringDecoder> {

    private static final Utf8StringDecoderPool INSTANCE = new Utf8StringDecoderPool();

    // Do not allow instantiation; all classes should share the singleton instance.
    private Utf8StringDecoderPool() {
        super(new Allocator<Utf8StringDecoder>() {
            @Override
            public Utf8StringDecoder newInstance(Pool<Utf8StringDecoder> pool) {
                return new Utf8StringDecoder(pool);
            }
        });
    }

    /**
     * @return a threadsafe shared instance of {@link Utf8StringDecoderPool}.
     */
    public static Utf8StringDecoderPool getInstance() {
        return INSTANCE;
    }
}
