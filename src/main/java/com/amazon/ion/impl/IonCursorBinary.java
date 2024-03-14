// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ion.impl;

import com.amazon.ion.BufferConfiguration;
import com.amazon.ion.IonBufferConfiguration;
import com.amazon.ion.IonException;
import com.amazon.ion.IonCursor;
import com.amazon.ion.IonType;
import com.amazon.ion.IvmNotificationConsumer;
import com.amazon.ion.SystemSymbols;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static com.amazon.ion.util.IonStreamUtils.throwAsIonException;

/**
 * An IonCursor over binary Ion data, capable of buffering or skipping Ion values at any depth. Records byte
 * indices of the value currently buffered, enabling parsers direct access to the bytes in the value's representation.
 */
class IonCursorBinary implements IonCursor {

    private static final int LOWER_SEVEN_BITS_BITMASK = 0x7F;
    private static final int HIGHEST_BIT_BITMASK = 0x80;
    private static final int VALUE_BITS_PER_VARUINT_BYTE = 7;
    // Note: because long is a signed type, Long.MAX_VALUE is represented in Long.SIZE - 1 bits.
    private static final int MAXIMUM_SUPPORTED_VAR_UINT_BYTES = (Long.SIZE - 1) / VALUE_BITS_PER_VARUINT_BYTE;
    private static final int IVM_START_BYTE = 0xE0;
    private static final int IVM_FINAL_BYTE = 0xEA;
    private static final int IVM_REMAINING_LENGTH = 3; // Length of the IVM after the first byte.
    private static final int SINGLE_BYTE_MASK = 0xFF;
    private static final int LIST_TYPE_ORDINAL = IonType.LIST.ordinal();
    private static final IvmNotificationConsumer NO_OP_IVM_NOTIFICATION_CONSUMER = (x, y) -> {};

    // Initial capacity of the stack used to hold ContainerInfo. Each additional level of nesting in the data requires
    // a new ContainerInfo. Depths greater than 8 are assumed to be rare.
    private static final int CONTAINER_STACK_INITIAL_CAPACITY = 8;

    // When set as an 'endIndex', indicates that the value is delimited.
    private static final int DELIMITED_MARKER = -1;

    /**
     * The kind of location at which `checkpoint` points.
     */
    private enum CheckpointLocation {
        BEFORE_UNANNOTATED_TYPE_ID,
        BEFORE_ANNOTATED_TYPE_ID,
        AFTER_SCALAR_HEADER,
        AFTER_CONTAINER_HEADER
    }

    /**
     * The state representing where the cursor left off after the previous operation.
     */
    private enum State {
        FILL,
        FILL_DELIMITED,
        SEEK,
        SEEK_DELIMITED,
        READY,
        TERMINATED
    }

    /**
     * State only used when the cursor's data source is refillable and the cursor is in slow mode.
     */
    private static class RefillableState {

        /**
         * At this and all greater depths, the buffer is known to hold all values in their entirety. This means slow mode
         * can be disabled until stepping out of this depth.
         */
        int fillDepth = -1;

        /**
         * The current size of the internal buffer.
         */
        long capacity;

        /**
         * The total number of bytes that have been discarded (shifted out of the buffer or skipped directly from
         * the input stream).
         */
        long totalDiscardedBytes = 0;

        /**
         * The state of the reader when in slow mode (when slow mode is disabled, the reader is always implicitly in the
         * READY state). This enables the reader to complete the previous IonCursor API invocation if it returned a
         * NEEDS_DATA event.
         */
        State state = State.READY;

        /**
         * The number of bytes that still need to be consumed from the input during a fill or seek operation.
         */
        long bytesRequested = 0;

        /**
         * The maximum size of the buffer. If the user attempts to buffer more bytes than this, an exception will be raised.
         */
        final int maximumBufferSize;

        /**
         * The source of data, for refillable streams.
         */
        final InputStream inputStream;

        /**
         * Handler invoked when a single value would exceed `maximumBufferSize`.
         */
        BufferConfiguration.OversizedValueHandler oversizedValueHandler;

        /**
         * Indicates whether the current value is being skipped due to being oversized.
         */
        boolean isSkippingCurrentValue = false;

        /**
         * The number of bytes of an oversized value skipped during single-byte read operations.
         */
        int individualBytesSkippedWithoutBuffering = 0;

        RefillableState(InputStream inputStream, int capacity, int maximumBufferSize) {
            this.inputStream = inputStream;
            this.capacity = capacity;
            this.maximumBufferSize = maximumBufferSize;
        }

    }

    /**
     * Stack to hold container info. Stepping into a container results in a push; stepping out results in a pop.
     */
    Marker[] containerStack = new Marker[CONTAINER_STACK_INITIAL_CAPACITY];

    /**
     * The index of the current container in `containerStack`.
     */
    int containerIndex = -1;

    /**
     * The Marker representing the parent container of the current value.
     */
    Marker parent = null;

    /**
     * The start offset into the user-provided byte array, or 0 if the user provided an InputStream.
     */
    private final long startOffset;

    /**
     * The index of the next byte in the buffer that is available to be read. Always less than or equal to `limit`.
     */
    private long offset;

    /**
     * The index at which the next byte received will be written. Always greater than or equal to `offset`.
     */
    long limit;

    /**
     * A slice of the current buffer. May be used to create ByteBuffer views over value representation bytes for
     * quicker parsing.
     */
    ByteBuffer byteBuffer;

    /**
     * The handler that will be notified when data is processed.
     */
    private final BufferConfiguration.DataHandler dataHandler;

    /**
     * Marker for the sequence of annotation symbol IDs on the current value. If there are no annotations on
     * the current value, the startIndex will be negative.
     */
    final Marker annotationSequenceMarker = new Marker(-1, 0);

    /**
     * Indicates whether the current value is annotated.
     */
    boolean hasAnnotations = false;

    /**
     * Marker representing the current value.
     */
    final Marker valueMarker = new Marker(-1, 0);

    /**
     * The index of the first byte in the header of the value at which the reader is currently positioned.
     */
    long valuePreHeaderIndex = 0;

    /**
     * Type ID for the current value.
     */
    IonTypeID valueTid = null;

    /**
     * The consumer to be notified when Ion version markers are encountered.
     */
    private IvmNotificationConsumer ivmConsumer = NO_OP_IVM_NOTIFICATION_CONSUMER;

    /**
     * The event that occurred as a result of the last call to any of the cursor's IonCursor methods.
     */
    IonCursor.Event event = IonCursor.Event.NEEDS_DATA;

    /**
     * The buffer in which the cursor stores slices of the Ion stream.
     */
    byte[] buffer;

    /**
     * The major version of the Ion encoding currently being read.
     */
    private int majorVersion = -1;

    /**
     * The minor version of the Ion encoding currently being read.
     */
    int minorVersion = 0;

    /**
     * The field SID of the current value, if any.
     */
    int fieldSid = -1;

    /**
     * The index of the first byte in the buffer that has not yet been successfully processed. The checkpoint is
     * only advanced when sufficient progress has been made, e.g. when a complete value header has been processed, or
     * a complete value has been skipped.
     */
    private long checkpoint;

    /**
     * The index of the next byte to be read from the buffer.
     */
    private long peekIndex;

    /**
     * The set of type IDs to use for Ion version currently active in the stream.
     */
    private IonTypeID[] typeIds = IonTypeID.TYPE_IDS_NO_IVM;

    /**
     * Holds information necessary for reading from refillable input. Null if the cursor is byte-backed.
     */
    private final RefillableState refillableState;

    /**
     * Describes the byte at the `checkpoint` index.
     */
    private CheckpointLocation checkpointLocation = CheckpointLocation.BEFORE_UNANNOTATED_TYPE_ID;

    /**
     * Indicates whether the cursor is in slow mode. Slow mode must be used when the input source is refillable (i.e.
     * a stream) and the cursor has not buffered the current value's bytes. When slow mode is disabled, the cursor can
     * consume bytes directly from its buffer without range checks or refilling. When a value has been buffered (see:
     * `fillValue()`), its entire representation (including child values if applicable) can be read with slow mode
     * disabled, resulting in better performance.
     */
    boolean isSlowMode;

    /**
     * Indicates whether the current value extends beyond the end of the buffer.
     */
    boolean isValueIncomplete = false;

    /**
     * The total number of bytes that had been consumed from the stream as of the last time progress was reported to
     * the data handler.
     */
    private long lastReportedByteTotal = 0;


    /**
     * @return the given configuration's DataHandler, or null if that DataHandler is a no-op.
     */
    private static BufferConfiguration.DataHandler getDataHandler(IonBufferConfiguration configuration) {
        // Using null instead of a no-op handler enables a quick null check to skip calculating the amount of data
        // processed, improving performance.
        BufferConfiguration.DataHandler dataHandler = configuration.getDataHandler();
        return dataHandler == IonBufferConfiguration.DEFAULT.getDataHandler() ? null : dataHandler;
    }

    /**
     * Constructs a new fixed (non-refillable) cursor from the given byte array.
     * @param configuration the configuration to use. The buffer size and oversized value configuration are unused, as
     *                      the given buffer is used directly.
     * @param buffer the byte array containing the bytes to read.
     * @param offset the offset into the byte array at which the first byte of Ion data begins.
     * @param length the number of bytes to be read from the byte array.
     */
    IonCursorBinary(
        final IonBufferConfiguration configuration,
        byte[] buffer,
        int offset,
        int length
    ) {
        this.dataHandler = getDataHandler(configuration);
        peekIndex = offset;
        valuePreHeaderIndex = offset;
        checkpoint = peekIndex;

        for (int i = 0; i < CONTAINER_STACK_INITIAL_CAPACITY; i++) {
            containerStack[i] = new Marker(-1, -1);
        }

        this.buffer = buffer;
        this.startOffset = offset;
        this.offset = offset;
        this.limit = offset + length;
        byteBuffer = ByteBuffer.wrap(buffer, offset, length);
        isSlowMode = false;
        refillableState = null;
    }

    /**
     * @param value a non-negative number.
     * @return the exponent of the next power of two greater than or equal to the given number.
     */
    private static int logBase2(int value) {
        return 32 - Integer.numberOfLeadingZeros(value == 0 ? 0 : value - 1);
    }

    /**
     * @param value a non-negative number
     * @return the next power of two greater than or equal to the given number.
     */
    static int nextPowerOfTwo(int value) {
        long highBit = Integer.toUnsignedLong(Integer.highestOneBit(value));
        // Paraphrased from JLS 5.1.3. Narrowing Primitive Conversion
        // For a narrowing conversion of a floating-point number to an integral type T
        // If the input is not NaN and is not representable as the target type int, then the value must be too large
        // (a positive value of large magnitude or positive infinity), and the result is the largest representable
        // value of type int.
        return (int)(double) ((highBit == value) ? value : highBit << 1);
    }

    /**
     * Cache of configurations for fixed-sized streams. FIXED_SIZE_CONFIGURATIONS[i] returns a configuration with
     * buffer size max(8, 2^i). Retrieve a configuration large enough for a given size using
     * FIXED_SIZE_CONFIGURATIONS(logBase2(size)). Only supports sizes less than or equal to
     * STANDARD_BUFFER_CONFIGURATION.getInitialBufferSize(). This limits the number of fixed-size configurations,
     * keeping the footprint small and avoiding the need to create fixed-size configurations that require allocating
     * really large initial buffers. Say the user provides a ByteArrayInputStream backed by a 2 GB byte array -- in
     * that case, even though the data is fixed, the cursor should not allocate another 2 GB buffer to copy into. In
     * that case, unless the user provides a custom buffer configuration, the cursor will just use the standard one
     * that starts at 32K and copy into it incrementally as needed.
     */
    private static final IonBufferConfiguration[] FIXED_SIZE_CONFIGURATIONS;

    static {
        int maxBufferSizeExponent = logBase2(IonBufferConfiguration.DEFAULT.getInitialBufferSize());
        FIXED_SIZE_CONFIGURATIONS = new IonBufferConfiguration[maxBufferSizeExponent + 1];
        for (int i = 0; i <= maxBufferSizeExponent; i++) {
            // Create a buffer configuration for buffers of size 2^i. The minimum size is 8: the smallest power of two
            // larger than the minimum buffer size allowed.
            int size = Math.max(8, (int) Math.pow(2, i));
            FIXED_SIZE_CONFIGURATIONS[i] = IonBufferConfiguration.Builder.from(IonBufferConfiguration.DEFAULT)
                .withInitialBufferSize(size)
                .withMaximumBufferSize(size)
                .build();
        }
    }

    /**
     * Validates the given configuration.
     * @param configuration the configuration to validate.
     */
    private static void validate(IonBufferConfiguration configuration) {
        if (configuration == null) {
            throw new IllegalArgumentException("Buffer configuration must not be null.");
        }
        if (configuration.getInitialBufferSize() < 1) {
            throw new IllegalArgumentException("Initial buffer size must be at least 1.");
        }
        if (configuration.getMaximumBufferSize() < configuration.getInitialBufferSize()) {
            throw new IllegalArgumentException("Maximum buffer size cannot be less than the initial buffer size.");
        }
    }

    /**
     * Provides a fixed-size buffer configuration suitable for the given ByteArrayInputStream.
     * @param inputStream the stream.
     * @param alreadyReadLen the number of bytes already read from the stream. The configuration provided will allow
     *                       enough space for these bytes.
     * @return a fixed IonBufferConfiguration.
     */
    private static IonBufferConfiguration getFixedSizeConfigurationFor(
        ByteArrayInputStream inputStream,
        int alreadyReadLen
    ) {
        // Note: ByteArrayInputStream.available() can return a negative number because its constructor does
        // not validate that the offset and length provided are actually within range of the provided byte array.
        // Setting the result to 0 in this case avoids an error when looking up the fixed sized configuration.
        int fixedBufferSize = Math.max(0, inputStream.available());
        if (alreadyReadLen > 0) {
            fixedBufferSize += alreadyReadLen;
        }
        if (IonBufferConfiguration.DEFAULT.getInitialBufferSize() > fixedBufferSize) {
            return FIXED_SIZE_CONFIGURATIONS[logBase2(fixedBufferSize)];
        }
        return IonBufferConfiguration.DEFAULT;
    }

    /**
     * Constructs a refillable cursor from the given input stream.
     * @param configuration the configuration to use.
     * @param alreadyRead the byte array containing the bytes already read (often the IVM).
     * @param alreadyReadOff the offset into `alreadyRead` at which the first byte that was already read exists.
     * @param alreadyReadLen the number of bytes already read from `alreadyRead`.
     */
    IonCursorBinary(
        IonBufferConfiguration configuration,
        InputStream inputStream,
        byte[] alreadyRead,
        int alreadyReadOff,
        int alreadyReadLen
    ) {
        if (configuration == IonBufferConfiguration.DEFAULT) {
            dataHandler = null;
            if (inputStream instanceof ByteArrayInputStream) {
                // ByteArrayInputStreams are fixed-size streams. Clamp the reader's internal buffer size at the size of
                // the stream to avoid wastefully allocating extra space that will never be needed. It is still
                // preferable for the user to manually specify the buffer size if it's less than the default, as doing
                // so allows this branch to be skipped.
                configuration = getFixedSizeConfigurationFor((ByteArrayInputStream) inputStream, alreadyReadLen);
            }
        } else {
            validate(configuration);
            dataHandler = getDataHandler(configuration);
        }
        peekIndex = 0;
        checkpoint = 0;

        for (int i = 0; i < CONTAINER_STACK_INITIAL_CAPACITY; i++) {
            containerStack[i] = new Marker(-1, -1);
        }

        this.buffer = new byte[configuration.getInitialBufferSize()];
        this.startOffset = 0;
        this.offset = 0;
        this.limit = 0;
        if (alreadyReadLen > 0) {
            System.arraycopy(alreadyRead, alreadyReadOff, buffer, 0, alreadyReadLen);
            limit = alreadyReadLen;
        }
        byteBuffer = ByteBuffer.wrap(buffer, 0, configuration.getInitialBufferSize());
        isSlowMode = true;
        refillableState = new RefillableState(
            inputStream,
            configuration.getInitialBufferSize(),
            configuration.getMaximumBufferSize()
        );
        registerOversizedValueHandler(configuration.getOversizedValueHandler());
    }

    /*
     * This class contains methods with the prefix 'slow', which usually have
     * counterparts with the prefix 'unchecked'. The 'unchecked' variants are
     * faster because they are always called from contexts where the buffer is
     * already known to hold all bytes that will be required or available to
     * complete the call, and therefore no filling or bounds checking is required.
     * Sometimes a 'slow' method does not have an 'unchecked' variant. This is
     * typically because the 'unchecked' variant is simple enough to be written
     * inline at the call site. Public API methods will delegate to either the
     * 'slow' or 'unchecked' variant depending on whether 'isSlowMode' is true.
     * In general, 'isSlowMode' may be disabled whenever the input source is a
     * byte array (and therefore cannot grow and does not need to be filled),
     * or whenever the current value's parent container has already been filled.
     * Where a choice exists, 'slow' methods must call other 'slow' methods, and
     * 'unchecked' methods must call other 'unchecked' methods.
     */

    /* ---- Begin: internal buffer manipulation methods ---- */

    /**
     * @param index a byte index in the buffer.
     * @return the number of bytes available in the buffer after the given index.
     */
    private long availableAt(long index) {
        return limit - index;
    }

    /**
     * Ensures that there is space for at least 'minimumNumberOfBytesRequired' additional bytes in the buffer,
     * growing the buffer if necessary. May consolidate buffered bytes to the beginning of the buffer, shifting indices
     * accordingly.
     * @param minimumNumberOfBytesRequired the minimum number of additional bytes to buffer.
     * @return true if the buffer has sufficient capacity; otherwise, false.
     */
    private boolean ensureCapacity(long minimumNumberOfBytesRequired) {
        if (freeSpaceAt(offset) >= minimumNumberOfBytesRequired) {
            // No need to shift any bytes or grow the buffer.
            return true;
        }
        int maximumFreeSpace = refillableState.maximumBufferSize;
        int startOffset = (int) offset;
        if (minimumNumberOfBytesRequired > maximumFreeSpace) {
            refillableState.isSkippingCurrentValue = true;
            return false;
        }
        long shortfall = minimumNumberOfBytesRequired - refillableState.capacity;
        if (shortfall > 0) {
            int newSize = (int) Math.min(Math.max(refillableState.capacity * 2, nextPowerOfTwo((int) (refillableState.capacity + shortfall))), maximumFreeSpace);
            byte[] newBuffer = new byte[newSize];
            moveBytesToStartOfBuffer(newBuffer, startOffset);
            refillableState.capacity = newSize;
            buffer = newBuffer;
            byteBuffer = ByteBuffer.wrap(buffer, (int) offset, (int) refillableState.capacity);
        } else {
            // The current capacity can accommodate the requested size; move the existing bytes to the beginning
            // to make room for the remaining requested bytes to be filled at the end.
            moveBytesToStartOfBuffer(buffer, startOffset);
        }
        return true;
    }

    /**
     * Attempts to fill the buffer so that it contains at least `numberOfBytes` after `index`.
     * @param index the index after which to fill.
     * @param numberOfBytes the number of bytes after `index` that need to be present.
     * @return false if not enough bytes were available in the stream to satisfy the request; otherwise, true.
     */
    private boolean fillAt(long index, long numberOfBytes) {
        long shortfall = numberOfBytes - availableAt(index);
        if (shortfall > 0) {
            refillableState.bytesRequested = numberOfBytes + (index - offset);
            if (ensureCapacity(refillableState.bytesRequested)) {
                // Fill all the free space, not just the shortfall; this reduces I/O.
                shortfall = refill(refillableState.bytesRequested);
            } else {
                // The request cannot be satisfied, but not because data was unavailable. Return normally; it is the
                // caller's responsibility to recover.
                shortfall = 0;
            }
        }
        if (shortfall <= 0) {
            refillableState.bytesRequested = 0;
            refillableState.state = State.READY;
            return true;
        }
        refillableState.state = State.FILL;
        return false;
    }

    /**
     * Moves all buffered (but not yet read) bytes from 'buffer' to the destination buffer.
     * @param destinationBuffer the destination buffer, which may be 'buffer' itself or a new buffer.
     */
    private void moveBytesToStartOfBuffer(byte[] destinationBuffer, int fromIndex) {
        long size = availableAt(fromIndex);
        if (size > 0) {
            System.arraycopy(buffer, fromIndex, destinationBuffer, 0, (int) size);
        }
        if (fromIndex > 0) {
            shiftIndicesLeft(fromIndex);
        }
        offset = 0;
        limit = size;
    }

    /**
     * @return the number of bytes that can be written at the end of the buffer.
     */
    private long freeSpaceAt(long index) {
        return refillableState.capacity - index;
    }

    /**
     * Reads a single byte without adding it to the buffer. Used when skipping an oversized value, in cases where
     * the byte values are important (e.g. within the header of the oversized value, in order to determine
     * the number of bytes to skip).
     * @return the next byte, or -1 if the stream is at its end.
     */
    private int readByteWithoutBuffering() {
        int b = -1;
        try {
            b = refillableState.inputStream.read();
        } catch (EOFException e) {
            // Certain InputStream implementations (e.g. GZIPInputStream) throw EOFException if more bytes are requested
            // to read than are currently available (e.g. if a header or trailer is incomplete).
        } catch (IOException e) {
            throwAsIonException(e);
        }
        if (b >= 0) {
            refillableState.individualBytesSkippedWithoutBuffering += 1;
        }
        return b;
    }

    /**
     * Peek at the next byte from the stream, assuming it will be buffered unless the current value is being skipped.
     * @return the byte, or -1 if the end of the stream has been reached.
     */
    private int slowPeekByte() {
        if (refillableState.isSkippingCurrentValue) {
            return readByteWithoutBuffering();
        }
        return buffer[(int)(peekIndex++)] & SINGLE_BYTE_MASK;
    }

    /**
     * Read the next byte from the stream, ensuring the byte is buffered.
     * @return the byte, or -1 if the end of the stream has been reached.
     */
    private int slowReadByte() {
        if (refillableState.isSkippingCurrentValue) {
            // If the value is being skipped, the byte will not have been buffered.
            return readByteWithoutBuffering();
        }
        if (!fillAt(peekIndex, 1)) {
            return -1;
        }
        return slowPeekByte();
    }

    /**
     * Shift all active container end indices left by the given amount. This is used when bytes have been shifted
     * to the start of the buffer in order to make room at the end.
     * @param shiftAmount the amount to shift left.
     */
    private void shiftContainerEnds(long shiftAmount) {
        for (int i = containerIndex; i >= 0; i--) {
            if (containerStack[i].endIndex > 0) {
                containerStack[i].endIndex -= shiftAmount;
            }
        }
    }

    /**
     * Shift all indices left by the given amount. This is used when data is moved in the underlying
     * buffer either due to buffer growth or NOP padding being reclaimed to make room for a value that would otherwise
     * exceed the buffer's maximum size.
     * @param shiftAmount the amount to shift left.
     */
    private void shiftIndicesLeft(int shiftAmount) {
        peekIndex = Math.max(peekIndex - shiftAmount, 0);
        valuePreHeaderIndex -= shiftAmount;
        valueMarker.startIndex -= shiftAmount;
        valueMarker.endIndex -= shiftAmount;
        checkpoint -= shiftAmount;
        if (annotationSequenceMarker.startIndex > -1) {
            annotationSequenceMarker.startIndex -= shiftAmount;
            annotationSequenceMarker.endIndex -= shiftAmount;
        }
        shiftContainerEnds(shiftAmount);
        refillableState.totalDiscardedBytes += shiftAmount;
    }

    /**
     * Attempts to fill the buffer with up to the requested number of additional bytes. It is the caller's
     * responsibility to ensure that there is space in the buffer.
     * @param minimumNumberOfBytesRequired the minimum number of bytes requested to fill the current value.
     * @return the shortfall between the number of bytes that were filled and the minimum number requested. If less than
     *  1, then at least `minimumNumberOfBytesRequired` were filled.
     */
    private long refill(long minimumNumberOfBytesRequired) {
        int numberOfBytesFilled = -1;
        long shortfall;
        // Sometimes an InputStream implementation will return fewer than the number of bytes requested even
        // if the stream is not at EOF. If this happens and there is still a shortfall, keep requesting bytes
        // until either the shortfall is filled or EOF is reached.
        do {
            try {
                numberOfBytesFilled = refillableState.inputStream.read(buffer, (int) limit, (int) freeSpaceAt(limit));
            } catch (EOFException e) {
                // Certain InputStream implementations (e.g. GZIPInputStream) throw EOFException if more bytes are requested
                // to read than are currently available (e.g. if a header or trailer is incomplete).
                numberOfBytesFilled = -1;
            } catch (IOException e) {
                throwAsIonException(e);
            }
            if (numberOfBytesFilled > 0) {
                limit += numberOfBytesFilled;
            }
            shortfall = minimumNumberOfBytesRequired - availableAt(offset);
        } while (shortfall > 0 && numberOfBytesFilled >= 0);
        return shortfall;
    }

    /**
     * Seeks forward in the stream up to the requested number of bytes, from `offset`.
     * @param numberOfBytes the number of bytes to seek from `offset`.
     * @return true if not enough data was available in the stream; otherwise, false.
     */
    private boolean slowSeek(long numberOfBytes) {
        long size = availableAt(offset);
        long unbufferedBytesToSkip = numberOfBytes - size;
        if (unbufferedBytesToSkip <= 0) {
            offset += numberOfBytes;
            refillableState.bytesRequested = 0;
            refillableState.state = State.READY;
            return false;
        }
        offset = limit;
        long shortfall;
        long skipped = 0;
        do {
            try {
                skipped = refillableState.inputStream.skip(unbufferedBytesToSkip);
            } catch (EOFException e) {
                // Certain InputStream implementations (e.g. GZIPInputStream) throw EOFException if more bytes are requested
                // to skip than are currently available (e.g. if a header or trailer is incomplete).
                skipped = 0;
            } catch (IOException e) {
                throwAsIonException(e);
            }
            refillableState.totalDiscardedBytes += skipped;
            shiftContainerEnds(skipped);
            shortfall = unbufferedBytesToSkip - skipped;
            unbufferedBytesToSkip = shortfall;
        } while (shortfall > 0 && skipped > 0);
        if (shortfall <= 0) {
            refillableState.bytesRequested = 0;
            refillableState.state = State.READY;
            return false;
        }
        refillableState.bytesRequested = shortfall;
        refillableState.state = State.SEEK;
        return true;
    }

    /* ---- End: internal buffer manipulation methods ---- */

    /* ---- Begin: version-dependent parsing methods ---- */
    /* ---- Ion 1.0 ---- */

    /**
     * Reads a 2+ byte VarUInt, given the first byte. NOTE: the VarUInt must fit in a `long`. This must only be
     * called when it is known that the buffer already contains all the bytes in the VarUInt.
     * @return the value.
     */
    private long uncheckedReadVarUInt_1_0(byte currentByte) {
        long result = currentByte & LOWER_SEVEN_BITS_BITMASK;
        do {
            if (peekIndex >= limit) {
                throw new IonException("Malformed data: declared length exceeds the number of bytes remaining in the stream.");
            }
            currentByte = buffer[(int) (peekIndex++)];
            result = (result << VALUE_BITS_PER_VARUINT_BYTE) | (currentByte & LOWER_SEVEN_BITS_BITMASK);
        } while (currentByte >= 0);
        if (result < 0) {
            throw new IonException("Found a VarUInt that was too large to fit in a `long`");
        }
        return result;
    }

    /**
     * Reads a VarUInt, ensuring enough data is available in the buffer. NOTE: the VarUInt must fit in a `long`.
     * @return the value.
     */
    private long slowReadVarUInt_1_0() {
        int currentByte;
        int numberOfBytesRead = 0;
        long value = 0;
        while (numberOfBytesRead < MAXIMUM_SUPPORTED_VAR_UINT_BYTES) {
            currentByte = slowReadByte();
            if (currentByte < 0) {
                return -1;
            }
            numberOfBytesRead++;
            value = (value << VALUE_BITS_PER_VARUINT_BYTE) | (currentByte & LOWER_SEVEN_BITS_BITMASK);
            if ((currentByte & HIGHEST_BIT_BITMASK) != 0) {
                return value;
            }
        }
        throw new IonException("Found a VarUInt that was too large to fit in a `long`");
    }

    /**
     * Reads the header of an annotation wrapper. This must only be called when it is known that the buffer already
     * contains all the bytes in the header. Sets `valueMarker` with the start and end indices of the wrapped value.
     * Sets `annotationSequenceMarker` with the start and end indices of the sequence of annotation SIDs. After
     * successful return, `peekIndex` will point at the type ID byte of the wrapped value.
     * @param valueTid the type ID of the annotation wrapper.
     * @return true if the length of the wrapped value extends beyond the bytes currently buffered; otherwise, false.
     */
    private boolean uncheckedReadAnnotationWrapperHeader_1_0(IonTypeID valueTid) {
        long endIndex;
        if (valueTid.variableLength) {
            if (peekIndex >= limit) {
                throw new IonException("Malformed data: declared length exceeds the number of bytes remaining in the stream.");
            }
            byte b = buffer[(int) peekIndex++];
            if (b < 0) {
                endIndex = (b & LOWER_SEVEN_BITS_BITMASK);
            } else {
                endIndex = uncheckedReadVarUInt_1_0(b);
            }
        } else {
            endIndex = valueTid.length;
        }
        endIndex += peekIndex;
        setMarker(endIndex, valueMarker);
        if (endIndex > limit || endIndex < 0) {
            throw new IonException("Malformed data: declared length exceeds the number of bytes remaining in the stream.");
        }
        byte b = buffer[(int) peekIndex++];
        int annotationsLength;
        if (b < 0) {
            annotationsLength = (b & LOWER_SEVEN_BITS_BITMASK);
        } else {
            annotationsLength = (int) uncheckedReadVarUInt_1_0(b);
        }
        annotationSequenceMarker.startIndex = peekIndex;
        annotationSequenceMarker.endIndex = annotationSequenceMarker.startIndex + annotationsLength;
        peekIndex = annotationSequenceMarker.endIndex;
        if (peekIndex >= endIndex) {
            throw new IonException("Annotation wrapper must wrap a value.");
        }
        return false;
    }

    /**
     * Reads the header of an annotation wrapper, ensuring enough data is available in the buffer. Sets `valueMarker`
     * with the start and end indices of the wrapped value. Sets `annotationSequenceMarker` with the start and end
     * indices of the sequence of annotation SIDs. After successful return, `peekIndex` will point at the type ID byte
     * of the wrapped value.
     * @param valueTid the type ID of the annotation wrapper.
     * @return true if there are not enough bytes in the stream to complete the value; otherwise, false.
     */
    private boolean slowReadAnnotationWrapperHeader_1_0(IonTypeID valueTid) {
        long valueLength;
        if (valueTid.variableLength) {
            // At this point the value must be at least 4 more bytes: 1 for the smallest-possible wrapper length, 1
            // for the smallest-possible annotations length, one for the smallest-possible annotation, and 1 for the
            // smallest-possible value representation.
            if (!fillAt(peekIndex, 4)) {
                return true;
            }
            valueLength = slowReadVarUInt_1_0();
            if (valueLength < 0) {
                return true;
            }
        } else {
            // At this point the value must be at least 3 more bytes: 1 for the smallest-possible annotations
            // length, 1 for the smallest-possible annotation, and 1 for the smallest-possible value representation.
            if (!fillAt(peekIndex, 3)) {
                return true;
            }
            valueLength = valueTid.length;
        }
        // Record the post-length index in a value that will be shifted in the event the buffer needs to refill.
        setMarker(peekIndex + valueLength, valueMarker);
        int annotationsLength = (int) slowReadVarUInt_1_0();
        if (annotationsLength < 0) {
            return true;
        }
        if (!fillAt(peekIndex, annotationsLength)) {
            return true;
        }
        if (refillableState.isSkippingCurrentValue) {
            // The value is already oversized, so the annotations sequence cannot be buffered.
            return true;
        }
        annotationSequenceMarker.typeId = valueTid;
        annotationSequenceMarker.startIndex = peekIndex;
        annotationSequenceMarker.endIndex = annotationSequenceMarker.startIndex + annotationsLength;
        peekIndex = annotationSequenceMarker.endIndex;
        if (peekIndex >= valueMarker.endIndex) {
            throw new IonException("Annotation wrapper must wrap a value.");
        }
        return false;
    }


    /**
     * Throws if the given marker represents an empty ordered struct, which is prohibited by the Ion 1.0 specification.
     * @param marker the marker for the value to check.
     */
    private void prohibitEmptyOrderedStruct_1_0(Marker marker) {
        if (marker.typeId.type == IonType.STRUCT &&
            marker.typeId.lowerNibble == IonTypeID.ORDERED_STRUCT_NIBBLE &&
            marker.endIndex == peekIndex
        ) {
            throw new IonException("Ordered struct must not be empty.");
        }
    }

    /**
     * Calculates the end index for the given type ID and sets `event` based on the type of value encountered, if any.
     * At the time of invocation, `peekIndex` must point to the first byte after the value's type ID byte. After return,
     * `peekIndex` will point to the first byte in the value's representation, or, in the case of a NOP pad, the first
     * byte that follows the pad.
     * @param valueTid the type ID of the value.
     * @param isAnnotated true if the value is annotated.
     * @return the end index of the value or NOP pad.
     */
    private long calculateEndIndex_1_0(IonTypeID valueTid, boolean isAnnotated) {
        long endIndex;
        if (valueTid.variableLength) {
            if (peekIndex >= limit) {
                throw new IonException("Malformed data: declared length exceeds the number of bytes remaining in the stream.");
            }
            byte b = buffer[(int) peekIndex++];
            if (b < 0) {
                endIndex = (b & LOWER_SEVEN_BITS_BITMASK) + peekIndex;
            } else {
                endIndex = uncheckedReadVarUInt_1_0(b) + peekIndex;
                if (endIndex < 0) {
                    throw new IonException("Unsupported value: declared length is too long.");
                }
            }
        } else {
            endIndex = valueTid.length + peekIndex;
        }
        if (valueTid.type != null && valueTid.type.ordinal() >= LIST_TYPE_ORDINAL) {
            event = Event.START_CONTAINER;
        } else if (valueTid.isNopPad) {
            uncheckedSeekPastNopPad(endIndex, isAnnotated);
        } else {
            event = Event.START_SCALAR;
        }
        return endIndex;
    }

    /**
     * Reads the field SID from the VarUInt starting at `peekIndex`, ensuring enough data is available in the buffer.
     * @return true if there are not enough bytes in the stream to complete the field SID; otherwise, false.
     */
    private boolean slowReadFieldName_1_0() {
        // The value must have at least 2 more bytes: 1 for the smallest-possible field SID and 1 for
        // the smallest-possible representation.
        if (!fillAt(peekIndex, 2)) {
            return true;
        }
        fieldSid = (int) slowReadVarUInt_1_0();
        return fieldSid < 0;
    }

    /* ---- Ion 1.1 ---- */

    private long uncheckedReadVarUInt_1_1() {
        throw new UnsupportedOperationException();
    }

    private long slowReadVarUInt_1_1() {
        throw new UnsupportedOperationException();
    }

    private boolean uncheckedReadAnnotationWrapperHeader_1_1(IonTypeID valueTid) {
        throw new UnsupportedOperationException();
    }

    private boolean slowReadAnnotationWrapperHeader_1_1(IonTypeID valueTid) {
        throw new UnsupportedOperationException();
    }

    private long calculateEndIndex_1_1(IonTypeID valueTid, boolean isAnnotated) {
        throw new UnsupportedOperationException();
    }

    private void uncheckedReadFieldName_1_1() {
        throw new UnsupportedOperationException();
    }

    private boolean slowReadFieldName_1_1() {
        throw new UnsupportedOperationException();
    }

    private boolean uncheckedIsDelimitedEnd_1_1() {
        throw new UnsupportedOperationException();
    }

    private boolean slowIsDelimitedEnd_1_1() {
        throw new UnsupportedOperationException();
    }

    boolean skipRemainingDelimitedContainerElements_1_1() {
        throw new UnsupportedOperationException();
    }

    private void seekPastDelimitedContainer_1_1() {
        throw new UnsupportedOperationException();
    }

    private boolean slowFindDelimitedEnd_1_1() {
        throw new UnsupportedOperationException();
    }

    private boolean slowSeekToDelimitedEnd_1_1() {
        throw new UnsupportedOperationException();
    }

    private boolean slowFillDelimitedContainer_1_1() {
        throw new UnsupportedOperationException();
    }

    private boolean slowSkipRemainingDelimitedContainerElements_1_1() {
        throw new UnsupportedOperationException();
    }

    /* ---- End: version-dependent parsing methods ---- */

    /* ---- Begin: version-agnostic parsing, utility, and public API methods ---- */

    /**
     * Attempts to make the cursor READY by finishing the operation that was in progress last time the end of the stream
     * was reached. This should not be called when the cursor state is already READY.
     * @return true if the cursor is ready; otherwise, false.
     */
    private boolean slowMakeBufferReady() {
        boolean isReady;
        switch (refillableState.state) {
            case SEEK:
                isReady = !slowSeek(refillableState.bytesRequested);
                break;
            case FILL:
                isReady = fillAt(offset, refillableState.bytesRequested);
                break;
            case FILL_DELIMITED:
                refillableState.state = State.READY;
                isReady = slowFindDelimitedEnd_1_1();
                break;
            case SEEK_DELIMITED:
                isReady = slowSeekToDelimitedEnd_1_1();
                break;
            case TERMINATED:
                isReady = false;
                break;
            default:
                throw new IllegalStateException();
        }
        if (!isReady) {
            event = Event.NEEDS_DATA;
        }
        return isReady;
    }

    /**
     * Sets `checkpoint` to the current `peekIndex`, which is at the given type of location.
     * @param location the type of checkpoint location. Must not be BEFORE_UNANNOTATED_TYPE_ID.
     */
    private void setCheckpoint(CheckpointLocation location) {
        checkpointLocation = location;
        checkpoint = peekIndex;
    }

    /**
     * Sets `checkpoint` to the current `peekIndex`, which must be before an unannotated type ID, and seeks the
     * buffer to that point.
     */
    private void setCheckpointBeforeUnannotatedTypeId() {
        reset();
        offset = peekIndex;
        checkpointLocation = CheckpointLocation.BEFORE_UNANNOTATED_TYPE_ID;
        checkpoint = peekIndex;
    }

    /**
     * Validates and sets the given marker, which must fit within its parent container (if applicable). The resulting
     * `startIndex` will be set to `peekIndex` (the next byte to be consumed from the cursor's buffer), and its
     * `endIndex` will be set to the given value.
     * @param endIndex the value's end index.
     * @param markerToSet the marker to set.
     */
    private void setMarker(long endIndex, Marker markerToSet) {
        if (parent != null && endIndex > parent.endIndex && parent.endIndex > DELIMITED_MARKER) {
            throw new IonException("Value exceeds the length of its parent container.");
        }
        markerToSet.startIndex = peekIndex;
        markerToSet.endIndex = endIndex;
    }

    /**
     * Determines whether the cursor has reached the end of the current container. If true, `event` will be set to
     * END_CONTAINER and information about the current value will be reset.
     * @return true if the end of the current container has been reached; otherwise, false.
     */
    private boolean checkContainerEnd() {
        if (parent.endIndex > peekIndex) {
            return false;
        }
        if (parent.endIndex == DELIMITED_MARKER) {
            return isSlowMode ? slowIsDelimitedEnd_1_1() : uncheckedIsDelimitedEnd_1_1();
        }
        if (parent.endIndex == peekIndex) {
            event = Event.END_CONTAINER;
            valueTid = null;
            fieldSid = -1;
            return true;
        }
        throw new IonException("Contained values overflowed the parent container length.");
    }

    /**
     * Resets state specific to the current value.
     */
    private void reset() {
        valueMarker.startIndex = -1;
        valueMarker.endIndex = -1;
        fieldSid = -1;
        hasAnnotations = false;
    }

    /**
     * Reads the final three bytes of an IVM. `peekIndex` must point to the first byte after the opening `0xE0` byte.
     * After return, `majorVersion`, `minorVersion`, and `typeIds` will be updated accordingly, and `peekIndex` will
     * point to the first byte after the IVM.
     */
    private void readIvm() {
        majorVersion = buffer[(int) (peekIndex++)];
        minorVersion = buffer[(int) (peekIndex++)];
        if ((buffer[(int) (peekIndex++)] & SINGLE_BYTE_MASK) != IVM_FINAL_BYTE) {
            throw new IonException("Invalid Ion version marker.");
        }
        if (majorVersion != 1) {
            throw new IonException(String.format("Unsupported Ion version: %d.%d", majorVersion, minorVersion));
        }
        if (minorVersion == 0) {
            typeIds = IonTypeID.TYPE_IDS_1_0;
        } else {
            throw new IonException(String.format("Unsupported Ion version: %d.%d", majorVersion, minorVersion));
        }
        ivmConsumer.ivmEncountered(majorVersion, minorVersion);
    }

    /**
     * Validates and skips a NOP pad. After return, `peekIndex` will point to the first byte after the NOP pad.
     * @param endIndex the endIndex of the NOP pad.
     * @param isAnnotated true if the NOP pad occurs within an annotation wrapper (which is illegal); otherwise, false.
     */
    private void uncheckedSeekPastNopPad(long endIndex, boolean isAnnotated) {
        if (isAnnotated) {
            throw new IonException(
                "Invalid annotation wrapper: NOP pad may not occur inside an annotation wrapper."
            );
        }
        if (endIndex > limit) {
            throw new IonException("Invalid NOP pad.");
        }
        peekIndex = endIndex;
        if (parent != null) {
            checkContainerEnd();
        }
    }

    /**
     * Validates and skips a NOP pad. After return, `peekIndex` will point to the first byte after the NOP pad.
     * @param valueLength the length of the NOP pad.
     * @param isAnnotated true if the NOP pad occurs within an annotation wrapper (which is illegal); otherwise, false.
     * @return true if not enough data was available to seek past the NOP pad; otherwise, false.
     */
    private boolean slowSeekPastNopPad(long valueLength, boolean isAnnotated) {
        if (isAnnotated) {
            throw new IonException(
                "Invalid annotation wrapper: NOP pad may not occur inside an annotation wrapper."
            );
        }
        if (slowSeek(peekIndex + valueLength - offset)) {
            event = Event.NEEDS_DATA;
            return true;
        }
        peekIndex = offset;
        setCheckpointBeforeUnannotatedTypeId();
        if (parent != null) {
            checkContainerEnd();
        }
        return false;
    }

    /**
     * Validates that an annotated value's endIndex matches the annotation wrapper's endIndex (which is contained in
     * `valueMarker.endIndex`).
     * @param endIndex the annotated value's endIndex.
     */
    private void validateAnnotationWrapperEndIndex(long endIndex) {
        if (valueMarker.endIndex >= 0 && endIndex != valueMarker.endIndex) {
            // valueMarker.endIndex refers to the end of the annotation wrapper.
            throw new IonException("Annotation wrapper length does not match the length of the wrapped value.");
        }
    }

    /**
     * Reads a value header, consuming the value's annotation wrapper header, if any. Upon invocation,
     * `peekIndex` must be positioned on the first byte that follows the given type ID byte. After return, `peekIndex`
     * will be positioned after the type ID byte of the value, and `markerToSet.typeId` will be set with the IonTypeID
     * representing the value.
     * @param  typeIdByte the type ID byte. This may be an annotation wrapper's type ID.
     * @param isAnnotated true if this type ID is on a value within an annotation wrapper; false if it is not.
     * @param markerToSet the Marker to set with information parsed from the type ID and/or annotation wrapper header.
     * @return false if the header belonged to NOP pad; otherwise, true. When false, the caller should call the method
     *  again to read the header for the value that follows.
     */
    private boolean uncheckedReadHeader(final int typeIdByte, final boolean isAnnotated, final Marker markerToSet) {
        IonTypeID valueTid = typeIds[typeIdByte];
        if (!valueTid.isValid) {
            throw new IonException("Invalid type ID.");
        } else if (valueTid.type == IonTypeID.ION_TYPE_ANNOTATION_WRAPPER) {
            if (isAnnotated) {
                throw new IonException("Nested annotation wrappers are invalid.");
            }
            if (minorVersion == 0 ? uncheckedReadAnnotationWrapperHeader_1_0(valueTid) : uncheckedReadAnnotationWrapperHeader_1_1(valueTid)) {
                return true;
            }
            hasAnnotations = true;
            return uncheckedReadHeader(buffer[(int)(peekIndex++)] & SINGLE_BYTE_MASK, true, valueMarker);
        } else {
            long endIndex = minorVersion == 0
                ? calculateEndIndex_1_0(valueTid, isAnnotated)
                : calculateEndIndex_1_1(valueTid, isAnnotated);
            if (isAnnotated) {
                validateAnnotationWrapperEndIndex(endIndex);
            }
            setMarker(endIndex, markerToSet);
            if (endIndex > limit) {
                isValueIncomplete = true;
            }
        }
        markerToSet.typeId = valueTid;
        if (event == Event.START_CONTAINER) {
            if (minorVersion == 0) {
                prohibitEmptyOrderedStruct_1_0(markerToSet);
            }
            return true;
        }
        return event == Event.START_SCALAR;
    }

    /**
     * Reads a value or annotation wrapper header, ensuring enough bytes are buffered. Upon invocation, `peekIndex` must
     * be positioned on the first byte that follows the given type ID byte. After return, `peekIndex`
     * will be positioned after the type ID byte of the value, and `markerToSet.typeId` will be set with the IonTypeID
     * representing the value.
     * @param  typeIdByte the type ID byte. This may be an annotation wrapper's type ID.
     * @param isAnnotated true if this type ID is on a value within an annotation wrapper; false if it is not.
     * @param markerToSet the Marker to set with information parsed from the type ID and/or annotation wrapper header.
     * @return false if enough bytes were buffered and the header belonged to an annotation wrapper or NOP pad;
     *  otherwise, true. When false, the caller should call the method again to read the header for the value that
     *  follows.
     */
    private boolean slowReadHeader(final int typeIdByte, final boolean isAnnotated, final Marker markerToSet) {
        IonTypeID valueTid = typeIds[typeIdByte];
        if (!valueTid.isValid) {
            throw new IonException("Invalid type ID.");
        } else if (valueTid.type == IonTypeID.ION_TYPE_ANNOTATION_WRAPPER) {
            if (isAnnotated) {
                throw new IonException("Nested annotation wrappers are invalid.");
            }
            hasAnnotations = true;
            if (minorVersion == 0 ? slowReadAnnotationWrapperHeader_1_0(valueTid) : slowReadAnnotationWrapperHeader_1_1(valueTid)) {
                return true;
            }
            setCheckpoint(CheckpointLocation.BEFORE_ANNOTATED_TYPE_ID);
        } else if (slowReadValueHeader(valueTid, isAnnotated, markerToSet)) {
            if (refillableState.isSkippingCurrentValue) {
                // If the value will be skipped, its type ID must be set so that the core reader can determine
                // whether it represents a symbol table.
                markerToSet.typeId = valueTid;
            }
            return true;
        }
        markerToSet.typeId = valueTid;
        if (checkpointLocation == CheckpointLocation.AFTER_SCALAR_HEADER) {
            return true;
        }
        if (checkpointLocation == CheckpointLocation.AFTER_CONTAINER_HEADER) {
            if (minorVersion == 0) {
                prohibitEmptyOrderedStruct_1_0(markerToSet);
            }
            return true;
        }
        return false;
    }

    /**
     * Reads a value header, ensuring enough bytes are buffered. Upon invocation, `peekIndex` must
     * be positioned on the first byte that follows the given type ID byte. After return, `peekIndex`
     * will be positioned after the type ID byte of the value, and `markerToSet.typeId` will be set with the IonTypeID
     * representing the value.
     * @param  valueTid the type ID of the value. Must not be an annotation wrapper.
     * @param isAnnotated true if this type ID is on a value within an annotation wrapper; false if it is not.
     * @param markerToSet the Marker to set with information parsed from the header.
     * @return true if not enough data was available in the stream to complete the header; otherwise, false.
     */
    private boolean slowReadValueHeader(IonTypeID valueTid, boolean isAnnotated, Marker markerToSet) {
        long valueLength = 0;
        long endIndex = 0;
        if (valueTid.isDelimited) {
            endIndex = DELIMITED_MARKER;
        } else if (valueTid.variableLength) {
            // At this point the value must be at least 2 more bytes: 1 for the smallest-possible value length
            // and 1 for the smallest-possible value representation.
            if (!fillAt(peekIndex, 2)) {
                return true;
            }
            valueLength = minorVersion == 0 ? slowReadVarUInt_1_0() : slowReadVarUInt_1_1();
            if (valueLength < 0) {
                return true;
            }
        } else {
            valueLength = valueTid.length;
        }
        if (valueTid.type != null && valueTid.type.ordinal() >= LIST_TYPE_ORDINAL) {
            setCheckpoint(CheckpointLocation.AFTER_CONTAINER_HEADER);
            event = Event.START_CONTAINER;
        } else if (valueTid.isNopPad) {
            if (slowSeekPastNopPad(valueLength, isAnnotated)) {
                return true;
            }
            valueLength = 0;
        } else {
            setCheckpoint(CheckpointLocation.AFTER_SCALAR_HEADER);
            event = Event.START_SCALAR;
        }
        if (endIndex != DELIMITED_MARKER) {
            if (refillableState.isSkippingCurrentValue) {
                // Any bytes that were skipped directly from the input must still be included in the logical endIndex so
                // that the rest of the oversized value's bytes may be skipped.
                endIndex = peekIndex + valueLength + refillableState.individualBytesSkippedWithoutBuffering;
            } else {
                endIndex = peekIndex + valueLength;
            }
            if (endIndex < 0) {
                throw new IonException("Unsupported value: declared length is too long.");
            }
        }

        if (isAnnotated) {
            validateAnnotationWrapperEndIndex(endIndex);
        }
        setMarker(endIndex, markerToSet);
        return false;
    }

    /**
     * Doubles the size of the cursor's container stack.
     */
    private void growContainerStack() {
        Marker[] newStack = new Marker[containerStack.length * 2];
        System.arraycopy(containerStack, 0, newStack, 0, containerStack.length);
        for (int i = containerStack.length; i < newStack.length; i++) {
            newStack[i] = new Marker(-1, -1);
        }
        containerStack = newStack;
    }

    /**
     * Push a Marker representing the current container onto the stack.
     */
    private void pushContainer() {
        if (++containerIndex >= containerStack.length) {
            growContainerStack();
        }
        parent = containerStack[containerIndex];
    }

    /**
     * Step into the current container.
     */
    private void uncheckedStepIntoContainer() {
        if (valueTid == null || valueTid.type.ordinal() < LIST_TYPE_ORDINAL) {
            // Note: this is IllegalStateException for consistency with the legacy binary IonReader implementation.
            // Ideally it would be IonException and IllegalStateException would be reserved for indicating bugs in
            // within the library.
            throw new IllegalStateException("Must be positioned on a container to step in.");
        }
        // Push the remaining length onto the stack, seek past the container's header, and increase the depth.
        pushContainer();
        parent.typeId = valueTid;
        parent.endIndex = valueTid.isDelimited ? DELIMITED_MARKER : valueMarker.endIndex;
        valueTid = null;
        event = Event.NEEDS_INSTRUCTION;
        reset();
    }

    /**
     * Steps into the container on which the cursor is currently positioned, ensuring that the buffer is ready.
     * @return `event`, which conveys the result.
     */
    private Event slowStepIntoContainer() {
        if (refillableState.state != State.READY && !slowMakeBufferReady()) {
            return event;
        }
        // Must be positioned on a container.
        if (checkpointLocation != CheckpointLocation.AFTER_CONTAINER_HEADER) {
            // Note: this is IllegalStateException for consistency with the legacy binary IonReader implementation.
            // Ideally it would be IonException and IllegalStateException would be reserved for indicating bugs in
            // within the library.
            throw new IllegalStateException("Must be positioned on a container to step in.");
        }
        // Push the remaining length onto the stack, seek past the container's header, and increase the depth.
        pushContainer();
        if (containerIndex == refillableState.fillDepth) {
            isSlowMode = false;
        }
        parent.typeId = valueMarker.typeId;
        parent.endIndex = valueTid.isDelimited ? DELIMITED_MARKER : valueMarker.endIndex;
        setCheckpointBeforeUnannotatedTypeId();
        valueTid = null;
        hasAnnotations = false;
        event = Event.NEEDS_INSTRUCTION;
        return event;
    }

    @Override
    public Event stepIntoContainer() {
        if (isSlowMode) {
            if (containerIndex != refillableState.fillDepth - 1) {
                if (valueMarker.endIndex > DELIMITED_MARKER && valueMarker.endIndex <= limit) {
                    refillableState.fillDepth = containerIndex + 1;
                } else {
                    return slowStepIntoContainer();
                }
            }
            isSlowMode = false;
        }
        uncheckedStepIntoContainer();
        return event;
    }

    /**
     * Puts the cursor back in slow mode. Must not be called when the cursor is byte-backed.
     */
    private void resumeSlowMode() {
        refillableState.fillDepth = -1;
        isSlowMode = true;
    }

    @Override
    public Event stepOutOfContainer() {
        if (isSlowMode) {
            return slowStepOutOfContainer();
        }
        if (parent == null) {
            // Note: this is IllegalStateException for consistency with the legacy binary IonReader implementation.
            // Ideally it would be IonException and IllegalStateException would be reserved for indicating bugs in
            // within the library.
            throw new IllegalStateException("Cannot step out at top level.");
        }
        // Seek past the remaining bytes at this depth and pop from the stack.
        if (parent.endIndex == DELIMITED_MARKER) {
            if (skipRemainingDelimitedContainerElements_1_1()) {
                return event;
            }
        } else {
            peekIndex = parent.endIndex;
        }
        if (!isSlowMode) {
            setCheckpointBeforeUnannotatedTypeId();
        }
        if (--containerIndex >= 0) {
            parent = containerStack[containerIndex];
            if (refillableState != null && containerIndex < refillableState.fillDepth) {
                resumeSlowMode();
            }
        } else {
            parent = null;
            containerIndex = -1;
            if (refillableState != null) {
                resumeSlowMode();
            }
        }

        valueTid = null;
        event = Event.NEEDS_INSTRUCTION;
        return event;
    }

    /**
     * Steps out of the container on which the cursor is currently positioned, ensuring that the buffer is ready and
     * that enough bytes are available in the stream.
     * @return `event`, which conveys the result.
     */
    private Event slowStepOutOfContainer() {
        if (parent == null) {
            // Note: this is IllegalStateException for consistency with the legacy binary IonReader implementation.
            // Ideally it would be IonException and IllegalStateException would be reserved for indicating bugs in
            // within the library.
            throw new IllegalStateException("Cannot step out at top level.");
        }
        if (refillableState.state != State.READY && !slowMakeBufferReady()) {
            return event;
        }
        event = Event.NEEDS_DATA;
        // Seek past any remaining bytes from the previous value.
        if (parent.endIndex == DELIMITED_MARKER) {
            if (slowSkipRemainingDelimitedContainerElements_1_1()) {
                return event;
            }
        } else {
            if (slowSeek(parent.endIndex - offset)) {
                return event;
            }
            peekIndex = offset;
        }
        setCheckpointBeforeUnannotatedTypeId();
        if (--containerIndex >= 0) {
            parent = containerStack[containerIndex];
        } else {
            parent = null;
            containerIndex = -1;
        }
        event = Event.NEEDS_INSTRUCTION;
        valueTid = null;
        hasAnnotations = false;
        return event;
    }

    /**
     * Advances to the next value within a container, checking for container end and consuming the field name, if any.
     * @return true if the end of the container has been reached; otherwise, false.
     */
    private boolean uncheckedNextContainedToken() {
        if (parent.endIndex == DELIMITED_MARKER) {
            return uncheckedIsDelimitedEnd_1_1();
        } else if (parent.endIndex == peekIndex) {
            event = Event.END_CONTAINER;
            return true;
        } else if (parent.endIndex < peekIndex) {
            throw new IonException("Contained values overflowed the parent container length.");
        } else if (parent.typeId.type == IonType.STRUCT) {
            if (minorVersion == 0) {
                byte b = buffer[(int) peekIndex++];
                if (b < 0) {
                    fieldSid = (b & LOWER_SEVEN_BITS_BITMASK);
                } else {
                    fieldSid = (int) uncheckedReadVarUInt_1_0(b);
                }
            } else {
                uncheckedReadFieldName_1_1();
            }
        }
        valuePreHeaderIndex = peekIndex;
        return false;
    }

    /**
     * Reports the total number of bytes consumed from the stream since the last report, up to the current `peekIndex`.
     */
    private void reportConsumedData() {
        long totalNumberOfBytesRead = getTotalOffset() + (peekIndex - valuePreHeaderIndex);
        dataHandler.onData((int) (totalNumberOfBytesRead - lastReportedByteTotal));
        lastReportedByteTotal = totalNumberOfBytesRead;
    }

    /**
     * Advances to the next token, seeking past the previous value if necessary. After return `event` will convey
     * the result (e.g. START_SCALAR, END_CONTAINER)
     * @return true if the next token was an Ion version marker or NOP pad; otherwise, false. If true, this method
     *  should be called again to advance to the following value.
     */
    private boolean uncheckedNextToken() {
        if (peekIndex < valueMarker.endIndex) {
            peekIndex = valueMarker.endIndex;
        } else if (valueTid != null && valueTid.isDelimited) {
            seekPastDelimitedContainer_1_1();
        }
        valueTid = null;
        if (dataHandler != null) {
            reportConsumedData();
        }
        if (peekIndex >= limit) {
            setCheckpointBeforeUnannotatedTypeId();
            if (parent != null && parent.endIndex == peekIndex) {
                event = Event.END_CONTAINER;
            }
            return false;
        }
        reset();
        int b;
        if (parent == null) { // Depth 0
            valuePreHeaderIndex = peekIndex;
            b = buffer[(int)(peekIndex++)] & SINGLE_BYTE_MASK;
            if (b == IVM_START_BYTE) {
                readIvm();
                return true;
            }
        } else {
            if (uncheckedNextContainedToken()) {
                return false;
            }
            if (peekIndex >= limit) {
                throw new IonException("Malformed data: declared length exceeds the number of bytes remaining in the container.");
            }
            b = buffer[(int)(peekIndex++)] & SINGLE_BYTE_MASK;
        }
        if (uncheckedReadHeader(b, false, valueMarker)) {
            valueTid = valueMarker.typeId;
            return false;
        }
        valueTid = valueMarker.typeId;
        return true;
    }

    /**
     * Advances to the next token from the current checkpoint, seeking past the previous value if necessary, and
     * ensuring that enough bytes are available in the stream. After return `event` will convey the result (e.g.
     * START_SCALAR, END_CONTAINER, NEEDS_DATA).
     */
    private void slowNextToken() {
        peekIndex = checkpoint;
        event = Event.NEEDS_DATA;
        while (true) {
            if ((refillableState.state != State.READY && !slowMakeBufferReady()) || (parent != null && checkContainerEnd())) {
                return;
            }
            int b;
            switch (checkpointLocation) {
                case BEFORE_UNANNOTATED_TYPE_ID:
                    if (dataHandler != null) {
                        reportConsumedData();
                    }
                    valueTid = null;
                    hasAnnotations = false;
                    if (parent != null && parent.typeId.type == IonType.STRUCT && (minorVersion == 0 ? slowReadFieldName_1_0() : slowReadFieldName_1_1())) {
                        return;
                    }
                    valuePreHeaderIndex = peekIndex;
                    b = slowReadByte();
                    if (b < 0) {
                        return;
                    }
                    if (b == IVM_START_BYTE && parent == null) {
                        if (!fillAt(peekIndex, IVM_REMAINING_LENGTH)) {
                            return;
                        }
                        readIvm();
                        setCheckpointBeforeUnannotatedTypeId();
                        continue;
                    }
                    if (slowReadHeader(b, false, valueMarker)) {
                        valueTid = valueMarker.typeId;
                        return;
                    }
                    valueTid = valueMarker.typeId;
                    // Either a NOP has been skipped, or an annotation wrapper has been consumed.
                    continue;
                case BEFORE_ANNOTATED_TYPE_ID:
                    valueTid = null;
                    b = slowReadByte();
                    if (b < 0) {
                        return;
                    }
                    slowReadHeader(b, true, valueMarker);
                    valueTid = valueMarker.typeId;
                    // If already within an annotation wrapper, neither an IVM nor a NOP is possible, so the cursor
                    // must be positioned after the header for the wrapped value.
                    return;
                case AFTER_SCALAR_HEADER:
                case AFTER_CONTAINER_HEADER:
                    if (slowSkipRemainingValueBytes()) {
                        return;
                    }
                    // The previous value's bytes have now been skipped; continue.
            }
        }
    }

    /**
     * Skips past the remaining bytes in the current value, ensuring enough bytes are available in the stream. If the
     * skip was successful, sets the checkpoint at the resulting index.
     * @return true if not enough data was available in the stream; otherwise, false.
     */
    private boolean slowSkipRemainingValueBytes() {
        if (valueMarker.endIndex == DELIMITED_MARKER && valueTid != null && valueTid.isDelimited) {
            seekPastDelimitedContainer_1_1();
            if (event == Event.NEEDS_DATA) {
                return true;
            }
        } else if (limit >= valueMarker.endIndex) {
            offset = valueMarker.endIndex;
        } else if (slowSeek(valueMarker.endIndex - offset)) {
            return true;
        }
        peekIndex = offset;
        valuePreHeaderIndex = peekIndex;
        if (refillableState.fillDepth > containerIndex) {
            // This value was filled, but was skipped. Reset the fillDepth so that the reader does not think the
            // next value was filled immediately upon encountering it.
            refillableState.fillDepth = -1;
        }
        setCheckpointBeforeUnannotatedTypeId();
        return false;
    }

    /**
     * Advances to the next token, ensuring enough bytes are available in the stream and checking against size limits.
     * If an oversized value is encountered, attempts to skip past it.
     * @return the result of the operation (e.g. START_SCALAR, END_CONTAINER).
     */
    private Event slowOverflowableNextToken() {
        while (true) {
            slowNextToken();
            if (refillableState.isSkippingCurrentValue) {
                seekPastOversizedValue();
                continue;
            }
            return event;
        }
    }

    /**
     * Seeks past an oversized value.
     */
    private void seekPastOversizedValue() {
        refillableState.oversizedValueHandler.onOversizedValue();
        if (refillableState.state != State.TERMINATED) {
            slowSeek(valueMarker.endIndex - offset - refillableState.individualBytesSkippedWithoutBuffering);
            refillableState.totalDiscardedBytes += refillableState.individualBytesSkippedWithoutBuffering;
            peekIndex = offset;
            // peekIndex now points at the first byte after the value. If any bytes were skipped directly from
            // the input stream before the 'slowSeek', peekIndex will be less than the value's pre-calculated endIndex.
            // This requires the end indices for all parent containers to be shifted left by the number of bytes that
            // were skipped without buffering.
            shiftContainerEnds(refillableState.individualBytesSkippedWithoutBuffering);
            setCheckpointBeforeUnannotatedTypeId();
        }
        refillableState.isSkippingCurrentValue = false;
        refillableState.individualBytesSkippedWithoutBuffering = 0;
    }

    @Override
    public Event nextValue() {
        if (isSlowMode) {
            return slowNextValue();
        }
        event = Event.NEEDS_DATA;
        while (uncheckedNextToken());
        return event;
    }

    /**
     * Advances to the next value, seeking past the previous if necessary, and ensuring enough bytes are available in
     * the stream.
     * @return the result of the operation (e.g. START_SCALAR, END_CONTAINER).
     */
    private Event slowNextValue() {
        if (refillableState.fillDepth > containerIndex) {
            // This value was filled, but was skipped. Reset the fillDepth so that the reader does not think the
            // next value was filled immediately upon encountering it.
            refillableState.fillDepth = -1;
            peekIndex = valueMarker.endIndex;
            setCheckpointBeforeUnannotatedTypeId();
            slowNextToken();
            return event;
        }
        return slowOverflowableNextToken();
    }

    @Override
    public Event fillValue() {
        event = Event.VALUE_READY;
        if (isSlowMode && refillableState.fillDepth <= containerIndex) {
            slowFillValue();
            if (refillableState.isSkippingCurrentValue) {
                seekPastOversizedValue();
            }
        }
        return event;
    }

    /**
     * Fills the buffer with the contents of the value on which the cursor is currently positioned, ensuring that
     * enough bytes are available in the stream.
     * @return `event`, which conveys the result.
     */
    private Event slowFillValue() {
        if (refillableState.state != State.READY && !slowMakeBufferReady()) {
            return event;
        }
        // Must be positioned after a header.
        if (checkpointLocation != CheckpointLocation.AFTER_SCALAR_HEADER && checkpointLocation != CheckpointLocation.AFTER_CONTAINER_HEADER) {
            throw new IllegalStateException();
        }
        event = Event.NEEDS_DATA;

        if (valueMarker.endIndex == DELIMITED_MARKER) {
            if (slowFillDelimitedContainer_1_1()) {
                return event;
            }
        }
        if (limit >= valueMarker.endIndex || fillAt(peekIndex, valueMarker.endIndex - valueMarker.startIndex)) {
            if (refillableState.isSkippingCurrentValue) {
                event = Event.NEEDS_INSTRUCTION;
            } else {
                if (checkpointLocation == CheckpointLocation.AFTER_CONTAINER_HEADER) {
                    // This container is buffered in its entirety. There is no need to fill the buffer again until stepping
                    // out of the fill depth.
                    refillableState.fillDepth = containerIndex + 1;
                }
                event = Event.VALUE_READY;
            }
        }
        return event;
    }

    @Override
    public Event getCurrentEvent() {
        return event;
    }

    public int getIonMajorVersion() {
        return majorVersion;
    }

    public int getIonMinorVersion() {
        return minorVersion;
    }

    public boolean hasAnnotations() {
        return hasAnnotations;
    }

    Marker getValueMarker() {
        return valueMarker;
    }

    /**
     * Slices the buffer using the given offset and limit. Slices are treated as if they were at the top level. This
     * can be used to seek the reader to a "span" of bytes that represent a value in the stream.
     * @param offset the offset at which the slice will begin.
     * @param limit the slice's limit.
     * @param ionVersionId the Ion version ID for the slice, e.g. $ion_1_0 for Ion 1.0.
     */
    void slice(long offset, long limit, String ionVersionId) {
        peekIndex = offset;
        this.limit = limit;
        setCheckpointBeforeUnannotatedTypeId();
        valueMarker.endIndex = -1;
        event = Event.NEEDS_DATA;
        valueTid = null;
        // Slices are treated as if they were at the top level.
        parent = null;
        containerIndex = -1;
        if (SystemSymbols.ION_1_0.equals(ionVersionId)) {
            typeIds = IonTypeID.TYPE_IDS_1_0;
            majorVersion = 1;
            minorVersion = 0;
        } else {
            // TODO changes are needed here to support Ion 1.1.
            throw new IonException(String.format("Attempted to seek using an unsupported Ion version %s.", ionVersionId));
        }
    }

    /**
     * @return the total number of bytes read since the stream began.
     */
    long getTotalOffset() {
        return valuePreHeaderIndex + (refillableState == null ? -startOffset : refillableState.totalDiscardedBytes);
    }

    boolean isByteBacked() {
        return refillableState == null;
    }

    public void registerIvmNotificationConsumer(IvmNotificationConsumer ivmConsumer) {
        this.ivmConsumer = ivmConsumer;
    }

    void registerOversizedValueHandler(BufferConfiguration.OversizedValueHandler oversizedValueHandler) {
        // Non-refillable streams cannot overflow.
        if (refillableState != null) {
            refillableState.oversizedValueHandler = oversizedValueHandler;
        }
    }

    @Override
    public Event endStream() {
        if (isValueIncomplete || isAwaitingMoreData()) {
            throw new IonException("Unexpected EOF.");
        }
        return Event.NEEDS_DATA;
    }

    /**
     * @return true if the cursor is expecting more data in order to complete a token; otherwise, false.
     */
    private boolean isAwaitingMoreData() {
        if (isSlowMode) {
            return slowIsAwaitingMoreData();
        }
        return valueMarker.endIndex > limit;
    }

    /**
     * @return true if the cursor is expecting more data in order to complete a token; otherwise, false.
     */
    private boolean slowIsAwaitingMoreData() {
        // Note: this does not detect early end-of-stream in all cases, as doing so would require reading from
        // the input stream, which is not worth the expense.
        return refillableState.state != State.TERMINATED
            && (refillableState.state == State.SEEK
            || refillableState.state == State.SEEK_DELIMITED
            || refillableState.bytesRequested > 1
            || peekIndex > checkpoint);
    }

    /**
     * Terminates the cursor. Called when a non-recoverable event occurs, like encountering a symbol table that
     * exceeds the maximum buffer size.
     */
    void terminate() {
        refillableState.state = State.TERMINATED;
    }

    @Override
    public void close() {
        if (refillableState != null) {
            try {
                refillableState.inputStream.close();
            } catch (IOException e) {
                throwAsIonException(e);
            }
        }
        buffer = null;
        containerStack = null;
        byteBuffer = null;
    }

    /* ---- End: version-agnostic parsing, utility, and public API methods ---- */
}
