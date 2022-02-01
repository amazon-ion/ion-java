package com.amazon.ion;

import com.amazon.ion.impl.IonReaderLookaheadBuffer;

/**
 * Configures Ion lookahead buffers.
 */
public final class IonBufferConfiguration extends BufferConfiguration<IonBufferConfiguration> {

    /**
     * Functional interface for handling oversized symbol tables.
     */
    public interface OversizedSymbolTableHandler {
        /**
         * Invoked when the user specifies a finite maximum buffer size and that size is exceeded by symbol table(s)
         * alone. Because symbol tables cannot be truncated without corrupting values that follow in the stream,
         * this condition is not recoverable. After this method is called,
         * {@link IonReaderLookaheadBuffer#fillInput()} has no effect.
         * @throws Exception if handler logic fails.
         */
        void onOversizedSymbolTable() throws Exception;
    }

    /**
     * Builds IonBufferConfiguration instances.
     */
    public static final class Builder extends BufferConfiguration.Builder<IonBufferConfiguration, Builder> {

        /**
         * 4-byte IVM + 1 byte user value.
         */
        private static final int MINIMUM_MAX_VALUE_SIZE = 5;

        /**
         * An OversizedValueHandler that does nothing.
         */
        private static final OversizedValueHandler NO_OP_OVERSIZED_VALUE_HANDLER = new OversizedValueHandler() {

            @Override
            public void onOversizedValue() {
                // If no maximum buffer size is configured, values cannot be considered oversized and this
                // implementation will never be called.
                // If a maximum buffer size is configured, a handler must also be configured. In that case,
                // this implementation will only be called if the user provides it to the builder manually.
            }
        };

        /**
         * A DataHandler that does nothing.
         */
        private static final DataHandler NO_OP_DATA_HANDLER = new DataHandler() {

            @Override
            public void onData(int bytes) {
                // Do nothing.
            }
        };

        /**
         * An OversizedSymbolTableHandler that does nothing.
         */
        private static final OversizedSymbolTableHandler NO_OP_OVERSIZED_SYMBOL_TABLE_HANDLER
            = new OversizedSymbolTableHandler() {

            @Override
            public void onOversizedSymbolTable() {
                // If no maximum buffer size is configured, symbol tables cannot be considered oversized and this
                // implementation will never be called.
                // If a maximum buffer size is configured, a handler must also be configured. In that case,
                // this implementation will only be called if the user provides it to the builder manually.
            }
        };

        /**
         * The handler that will be notified when oversized symbol tables are encountered.
         */
        private OversizedSymbolTableHandler oversizedSymbolTableHandler = null;

        private Builder() {
            // Must be publicly instantiated via the factory method.
        }

        /**
         * Provides the standard builder with the default initial buffer size, an unbounded maximum buffer size, and
         * an event handler that does nothing.
         * @return a standard Builder.
         */
        public static Builder standard() {
            return new Builder();
        }

        /**
         * Provides a new builder that would build IonBufferConfiguration instances with configuration identical to
         * the given configuration.
         * @param existingConfiguration an existing configuration.
         * @return a new mutable builder.
         */
        public static Builder from(IonBufferConfiguration existingConfiguration) {
            return IonBufferConfiguration.Builder.standard()
                .onData(existingConfiguration.getDataHandler())
                .onOversizedValue(existingConfiguration.getOversizedValueHandler())
                .onOversizedSymbolTable(existingConfiguration.getOversizedSymbolTableHandler())
                .withInitialBufferSize(existingConfiguration.getInitialBufferSize())
                .withMaximumBufferSize(existingConfiguration.getMaximumBufferSize());
        }

        /**
         * Sets the handler that will be notified when oversized symbol tables are encountered. If the maximum buffer
         * size is finite (see {@link #withMaximumBufferSize(int)}, this handler is required to be non-null.
         *
         * @param handler the handler.
         * @return this builder.
         */
        public Builder onOversizedSymbolTable(OversizedSymbolTableHandler handler) {
            oversizedSymbolTableHandler = handler;
            return this;
        }

        /**
         * @return the handler that will be notified when oversized symbol tables are encountered.
         */
        public OversizedSymbolTableHandler getOversizedSymbolTableHandler() {
            return oversizedSymbolTableHandler;
        }

        @Override
        public int getMinimumMaximumBufferSize() {
            return MINIMUM_MAX_VALUE_SIZE;
        }

        @Override
        public OversizedValueHandler getNoOpOversizedValueHandler() {
            return NO_OP_OVERSIZED_VALUE_HANDLER;
        }

        @Override
        public DataHandler getNoOpDataHandler() {
            return NO_OP_DATA_HANDLER;
        }

        /**
         * @return an OversizedSymbolTableHandler that does nothing.
         */
        public OversizedSymbolTableHandler getNoOpOversizedSymbolTableHandler() {
            return NO_OP_OVERSIZED_SYMBOL_TABLE_HANDLER;
        }

        @Override
        public IonBufferConfiguration build() {
            return new IonBufferConfiguration(this);
        }
    }

    /**
     * The handler that will be notified when oversized symbol tables are encountered.
     */
    private final OversizedSymbolTableHandler oversizedSymbolTableHandler;

    /**
     * Constructs an instance from the given Builder.
     * @param builder the builder containing the settings to apply to the new configuration.
     */
    private IonBufferConfiguration(Builder builder) {
        super(builder);
        if (builder.getOversizedSymbolTableHandler() == null) {
            requireUnlimitedBufferSize();
            oversizedSymbolTableHandler = builder.getNoOpOversizedSymbolTableHandler();
        } else {
            oversizedSymbolTableHandler = builder.getOversizedSymbolTableHandler();
        }
    }

    /**
     * @return the handler that will be notified when oversized symbol tables are encountered.
     */
    public OversizedSymbolTableHandler getOversizedSymbolTableHandler() {
        return oversizedSymbolTableHandler;
    }
}
