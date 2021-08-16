package com.amazon.ion;

import com.amazon.ion.impl.ReaderLookaheadBuffer;

/**
 * Provides logic common to all BufferConfiguration implementations.
 * @param <Configuration> the type of the concrete subclass of this BufferConfiguration that is applicable to the
 *                        ReaderLookaheadBufferBase subclass.
 */
public abstract class BufferConfiguration<Configuration extends BufferConfiguration<Configuration>> {

    /**
     * Functional interface for handling oversized values.
     */
    public interface OversizedValueHandler {
        /**
         * Invoked each time a value (and any symbol tables that immediately precede it) exceed the buffer size limit
         * specified by the LookaheadReaderWrapper instance, but the symbol tables by themselves do not exceed the
         * limit. This is recoverable. If the implementation wishes to recover, it should simply return normally from
         * this method. The oversized value will be flushed from the input pipe; normal processing will resume with the
         * next value. If the implementation wishes to abort processing immediately, it may throw an exception from this
         * method. Such an exception will propagate upward and will be thrown from
         * {@link ReaderLookaheadBuffer#fillInput()}.
         * @throws Exception if handler logic fails.
         */
        void onOversizedValue() throws Exception;
    }

    /**
     * Functional interface for reporting processed data.
     */
    public interface DataHandler {
        /**
         * Invoked whenever the bytes from a value are processed, regardless of whether the bytes are buffered or
         * skipped due to the value being oversized.
         * @param numberOfBytes the number of bytes processed.
         * @throws Exception if handler logic fails.
         */
        void onData(int numberOfBytes) throws Exception;
    }

    /**
     * Provides logic common to all BufferConfiguration Builder implementations.
     * @param <Configuration> the type of BufferConfiguration.
     * @param <BuilderType> the type of Builder that builds BufferConfiguration subclasses of type `Configuration`.
     */
    public static abstract class Builder<
        Configuration extends BufferConfiguration<Configuration>,
        BuilderType extends BufferConfiguration.Builder<Configuration, BuilderType>
    > {

        /**
         * Large enough that most streams will never need to grow the buffer. NOTE: this only needs to be large
         * enough to exceed the length of the longest top-level value plus any system values that precede it.
         */
        static final int DEFAULT_INITIAL_BUFFER_SIZE = 32 * 1024; // bytes

        /**
         * The initial size of the lookahead buffer, in bytes.
         */
        private int initialBufferSize = DEFAULT_INITIAL_BUFFER_SIZE;

        /**
         * The maximum number of bytes that will be buffered.
         */
        private int maximumBufferSize = Integer.MAX_VALUE;

        /**
         * The handler that will be notified when oversized values are encountered.
         */
        private OversizedValueHandler oversizedValueHandler = null;

        /**
         * The handler that will be notified when data is processed.
         */
        private DataHandler dataHandler = null;

        /**
         * Sets the initial size of the buffer that will be used to hold the data between top-level values. Default:
         * 32KB.
         *
         * @param initialBufferSizeInBytes the value.
         * @return this Builder.
         */
        public final BuilderType withInitialBufferSize(final int initialBufferSizeInBytes) {
            initialBufferSize = initialBufferSizeInBytes;
            return (BuilderType) this;
        }

        /**
         * @return the initial size of the lookahead buffer, in bytes.
         */
        public final int getInitialBufferSize() {
            return initialBufferSize;
        }

        /**
         * Sets the handler that will be notified when oversized values are encountered. If the maximum buffer size is
         * finite (see {@link #withMaximumBufferSize(int)}, this handler is required to be non-null.
         *
         * @param handler the handler.
         * @return this builder.
         */
        public final BuilderType onOversizedValue(final OversizedValueHandler handler) {
            oversizedValueHandler = handler;
            return (BuilderType) this;
        }

        /**
         * Sets the handler that will be notified when data is processed. The handler may be null, in which case the
         * number of bytes processed will not be reported.
         *
         * @param handler the handler.
         * @return this builder.
         */
        public final BuilderType onData(final DataHandler handler) {
            dataHandler = handler;
            return (BuilderType) this;
        }

        /**
         * @return the handler that will be notified when oversized values are encountered.
         */
        public final OversizedValueHandler getOversizedValueHandler() {
            return oversizedValueHandler;
        }

        /**
         * @return the handler that will be notified when data is processed.
         */
        public final DataHandler getDataHandler() {
            return dataHandler;
        }

        /**
         * Set the maximum number of bytes between top-level values. This can be used to limit growth of the internal
         * buffer. For binary Ion, the minimum value is 5 because all valid binary Ion data begins with a 4-byte Ion
         * version marker and the smallest value is 1 byte. For delimited text Ion, the minimum value is 2 because the
         * smallest text Ion value is 1 byte and the smallest delimiter is 1 byte. Default: Integer.MAX_VALUE.
         *
         * @param maximumBufferSizeInBytes the value.
         * @return this builder.
         */
        public final BuilderType withMaximumBufferSize(final int maximumBufferSizeInBytes) {
            maximumBufferSize = maximumBufferSizeInBytes;
            return (BuilderType) this;
        }

        /**
         * @return the maximum number of bytes that will be buffered.
         */
        public int getMaximumBufferSize() {
            return maximumBufferSize;
        }

        /**
         * Gets the minimum allowed maximum buffer size.
         * @return the value.
         */
        public abstract int getMinimumMaximumBufferSize();

        /**
         * @return the no-op {@link OversizedValueHandler} for the type of BufferConfiguration that this Builder builds.
         */
        public abstract OversizedValueHandler getNoOpOversizedValueHandler();

        /**
         * @return the no-op {@link DataHandler} for the type of BufferConfiguration that this Builder builds.
         */
        public abstract DataHandler getNoOpDataHandler();

        /**
         * Creates a new BufferConfiguration from the Builder's current settings.
         * @return a new instance.
         */
        public abstract Configuration build();
    }

    /**
     * The initial size of the lookahead buffer, in bytes.
     */
    private final int initialBufferSize;

    /**
     * The maximum number of bytes that will be buffered.
     */
    private final int maximumBufferSize;

    /**
     * The handler that will be notified when oversized values are encountered.
     */
    private final OversizedValueHandler oversizedValueHandler;

    /**
     * The handler that will be notified when data is processed.
     */
    private final DataHandler dataHandler;

    /**
     * Constructs an instance from the given Builder.
     * @param builder the builder containing the settings to apply to the new configuration.
     */
    protected BufferConfiguration(Builder<Configuration, ?> builder) {
        initialBufferSize = builder.getInitialBufferSize();
        maximumBufferSize = builder.getMaximumBufferSize();
        if (initialBufferSize > maximumBufferSize) {
            throw new IllegalArgumentException("Initial buffer size may not exceed the maximum buffer size.");
        }
        if (maximumBufferSize < builder.getMinimumMaximumBufferSize()) {
            throw new IllegalArgumentException(String.format(
                "Maximum buffer size must be at least %d bytes.", builder.getMinimumMaximumBufferSize()
            ));
        }
        if (builder.getOversizedValueHandler() == null) {
            requireUnlimitedBufferSize();
            oversizedValueHandler = builder.getNoOpOversizedValueHandler();
        } else {
            oversizedValueHandler = builder.getOversizedValueHandler();
        }
        if (builder.getDataHandler() == null) {
            dataHandler = builder.getNoOpDataHandler();
        } else {
            dataHandler = builder.getDataHandler();
        }
    }

    /**
     * Requires that the maximum buffer size not be limited.
     */
    protected void requireUnlimitedBufferSize() {
        if (maximumBufferSize < Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                "Must specify an OversizedValueHandler when a maximum buffer size is specified."
            );
        }
    }

    /**
     * @return the initial size of the lookahead buffer, in bytes.
     */
    public final int getInitialBufferSize() {
        return initialBufferSize;
    }

    /**
     * @return the maximum number of bytes that will be buffered.
     */
    public final int getMaximumBufferSize() {
        return maximumBufferSize;
    }

    /**
     * @return the handler that will be notified when oversized values are encountered.
     */
    public final OversizedValueHandler getOversizedValueHandler() {
        return oversizedValueHandler;
    }

    /**
     * @return the handler that will be notified when data is processed.
     */
    public final DataHandler getDataHandler() {
        return dataHandler;
    }
}
