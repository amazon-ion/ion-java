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
import com.amazon.ion.impl.bin.FlexInt;
import com.amazon.ion.impl.bin.OpCodes;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static com.amazon.ion.impl.IonTypeID.DELIMITED_END_ID;
import static com.amazon.ion.impl.IonTypeID.ONE_ANNOTATION_FLEX_SYM_LOWER_NIBBLE_1_1;
import static com.amazon.ion.impl.IonTypeID.ONE_ANNOTATION_SID_LOWER_NIBBLE_1_1;
import static com.amazon.ion.impl.IonTypeID.SYSTEM_MACRO_INVOCATION_ID;
import static com.amazon.ion.impl.IonTypeID.SYSTEM_SYMBOL_VALUE;
import static com.amazon.ion.impl.IonTypeID.TWO_ANNOTATION_FLEX_SYMS_LOWER_NIBBLE_1_1;
import static com.amazon.ion.impl.IonTypeID.TWO_ANNOTATION_SIDS_LOWER_NIBBLE_1_1;
import static com.amazon.ion.impl.IonTypeID.TYPE_IDS_1_1;
import static com.amazon.ion.impl.bin.Ion_1_1_Constants.FLEX_SYM_MAX_SYSTEM_SYMBOL;
import static com.amazon.ion.impl.bin.Ion_1_1_Constants.FLEX_SYM_SYSTEM_SYMBOL_OFFSET;
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
        State state;

        /**
         * The number of bytes that still need to be consumed from the input during a fill or seek operation.
         */
        long bytesRequested = 0;

        /**
         * The maximum size of the buffer. If the user attempts to buffer more bytes than this, an exception will be raised.
         */
        final int maximumBufferSize;

        /**
         * The number of bytes shifted left in the buffer during the current operation to make room for more bytes. This
         * is needed when rewinding to a previous location, as any saved indices at that location will need to be
         * shifted by this amount.
         */
        long pendingShift = 0;

        /**
         * The source of data, for refillable streams.
         */
        final InputStream inputStream;

        /**
         * Index of the first "pinned" byte in the buffer. Pinned bytes must be preserved in the buffer until un-pinned.
         */
        long pinOffset = -1;

        /**
         * The target depth to which the reader should seek. This is used when a container is determined to be oversize
         * while buffering one of its children.
         */
        int targetSeekDepth = -1;

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

        /**
         * The last byte that was read without being buffered (due to the buffer exceeding the maximum size). This
         * allows for one byte to be un-read even if an oversize value is being skipped. Un-reading is necessary
         * when the cursor probes for, but does not find, an end delimiter.
         */
        int lastUnbufferedByte = -1;

        /**
         * Whether to skip over annotation sequences rather than recording them for consumption by the user. This is
         * used when probing forward in the stream for the end of a delimited container while remaining logically
         * positioned on the current value. This is only needed in 'slow' mode because in quick mode the entire
         * container is assumed to be buffered in its entirety and no probing occurs.
         */
        private boolean skipAnnotations = false;

        RefillableState(InputStream inputStream, int capacity, int maximumBufferSize, State initialState) {
            this.inputStream = inputStream;
            this.capacity = capacity;
            this.maximumBufferSize = maximumBufferSize;
            this.state = initialState;
        }

    }

    /**
     * Marks an argument group.
     */
    private static class ArgumentGroupMarker {

        /**
         * Marks the start index of the current page in the argument group.
         */
        long pageStartIndex = -1;

        /**
         * Marks the end index of the current page in the argument group. If -1, this indicates that the argument
         * group is delimited and the end of the page has not yet been found.
         */
        long pageEndIndex = -1;

        /**
         * For tagless groups, the primitive type of the tagless values in the group; otherwise, null. When null,
         * there is always a single page of values in the group, and the end is reached either when an end delimiter
         * is found (for delimited groups), or when the cursor's `peekIndex` reaches `pageEndIndex`. When non-null,
         * there may be multiple pages of tagless values in the group; whenever the cursor reaches `pageEndIndex`, it
         * must read a FlexUInt at that position to calculate the end index of the next page.
         */
        TaglessEncoding taglessEncoding = null;
    }

    /**
     * Dummy state that indicates the cursor has been terminated and that additional API calls will have no effect.
     */
    private static final RefillableState TERMINATED_STATE = new RefillableState(null, -1, -1, State.TERMINATED);

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

    ArgumentGroupMarker[] argumentGroupStack = new ArgumentGroupMarker[CONTAINER_STACK_INITIAL_CAPACITY];
    int argumentGroupIndex = -1;

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
    final Marker annotationSequenceMarker = new Marker(-1, -1);

    /**
     * Holds both inline text markers and symbol IDs. If representing a symbol ID, the symbol ID value will
     * be contained in the endIndex field, and the startIndex field will be -1.
     */
    final MarkerList annotationTokenMarkers = new MarkerList(8);

    /**
     * Indicates whether the current value is annotated.
     */
    boolean hasAnnotations = false;

    /**
     * Marker representing the current value.
     */
    final Marker valueMarker = new Marker(-1, -1);

    /**
     * The index of the first byte in the header of the value at which the reader is currently positioned.
     */
    long valuePreHeaderIndex = 0;

    /**
     * Type ID for the current value.
     */
    IonTypeID valueTid = null;

    /**
     * Marker for the current inlineable field name.
     */
    final Marker fieldTextMarker = new Marker(-1, -1);

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
    private int majorVersion = 1;

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
    private RefillableState refillableState;

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
     * The ID of the current macro invocation. When `isSystemInvocation` is true, a positive value indicates a system
     * macro address, while a negative value indicates a system symbol ID. When `isSystemInvocation` is false, a
     * positive value indicates a user macro address, while a negative value indicates that the cursor's current token
     * is not a macro invocation.
     */
    private long macroInvocationId = -1;

    /**
     * True if the given token represents a system invocation (either a system macro invocation or a system symbol
     * value). When true, `macroInvocationId` is used to retrieve the ID of the system token.
     */
    private boolean isSystemInvocation = false;

    /**
     * The type of the current value, if tagless. Otherwise, null.
     */
    TaglessEncoding taglessType = null;

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

        for (int i = 0; i < CONTAINER_STACK_INITIAL_CAPACITY; i++) {
            argumentGroupStack[i] = new ArgumentGroupMarker();
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

        for (int i = 0; i < CONTAINER_STACK_INITIAL_CAPACITY; i++) {
            argumentGroupStack[i] = new ArgumentGroupMarker();
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
            configuration.getMaximumBufferSize(),
            State.READY
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
     * @param numberOfBytes the number of bytes starting at `index` that need to be present.
     * @param index the index after which to fill.
     * @return true if the buffer has sufficient capacity; otherwise, false.
     */
    private boolean ensureCapacity(long numberOfBytes, long index) {
        int maximumFreeSpace = refillableState.maximumBufferSize;
        int startOffset = (int) offset;
        if (refillableState.pinOffset > -1) {
            maximumFreeSpace -=  (int) (offset - refillableState.pinOffset);
            startOffset = (int) refillableState.pinOffset;
        }
        long minimumNumberOfBytesRequired = numberOfBytes + (index - startOffset);
        if (minimumNumberOfBytesRequired < 0) {
            throw new IonException("The number of bytes required cannot be represented in a Java long.");
        }
        refillableState.bytesRequested = minimumNumberOfBytesRequired;
        if (freeSpaceAt(startOffset) >= minimumNumberOfBytesRequired) {
            // No need to shift any bytes or grow the buffer.
            return true;
        }
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
            ByteOrder byteOrder = byteBuffer.order();
            byteBuffer = ByteBuffer.wrap(buffer, (int) offset, (int) refillableState.capacity);
            byteBuffer.order(byteOrder);
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
     * @param numberOfBytes the number of bytes starting at `index` that need to be present.
     * @return false if not enough bytes were available in the stream to satisfy the request; otherwise, true.
     */
    private boolean fillAt(long index, long numberOfBytes) {
        long shortfall = numberOfBytes - availableAt(index);
        if (shortfall > 0) {
            if (ensureCapacity(numberOfBytes, index)) {
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
        if (refillableState.pinOffset > 0) {
            refillableState.pinOffset = 0;
        }
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
        if (refillableState.lastUnbufferedByte > -1) {
            b = refillableState.lastUnbufferedByte;
            refillableState.lastUnbufferedByte = -1;
            return b;
        }
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
        if (fieldTextMarker.startIndex > -1) {
            fieldTextMarker.startIndex -= shiftAmount;
            fieldTextMarker.endIndex -= shiftAmount;
        }
        if (annotationSequenceMarker.startIndex > -1) {
            annotationSequenceMarker.startIndex -= shiftAmount;
            annotationSequenceMarker.endIndex -= shiftAmount;
        }
        // Note: even provisional annotation token markers must be shifted because a shift may occur between
        // provisional creation and commit.
        for (int i = 0; i < annotationTokenMarkers.provisionalSize(); i++) {
            Marker marker = annotationTokenMarkers.provisionalGet(i);
            if (marker.startIndex > -1) {
                marker.startIndex -= shiftAmount;
                marker.endIndex -= shiftAmount;
            }
        }
        shiftContainerEnds(shiftAmount);
        refillableState.pendingShift = shiftAmount;
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
            shortfall = minimumNumberOfBytesRequired - availableAt(refillableState.pinOffset > -1 ? refillableState.pinOffset : offset);
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
            // The value has been entirely skipped, so its endIndex is now the buffer's limit.
            valueMarker.endIndex = limit;
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
            if (endIndex == 0) {
                throw new IonException("Annotation wrapper must wrap a value.");
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
        long annotationsLength;
        if (b < 0) {
            annotationsLength = (b & LOWER_SEVEN_BITS_BITMASK);
        } else {
            annotationsLength = uncheckedReadVarUInt_1_0(b);
        }
        annotationSequenceMarker.startIndex = peekIndex;
        annotationSequenceMarker.endIndex = annotationSequenceMarker.startIndex + annotationsLength;
        peekIndex = annotationSequenceMarker.endIndex;
        if (peekIndex >= endIndex) {
            throw new IonException("Annotation wrapper must wrap a value.");
        }
        if (peekIndex < 0) {
            throw new IonException("Malformed data: declared length exceeds the number of bytes remaining in the stream.");
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
            if (valueLength == 0) {
                throw new IonException("Annotation wrapper must wrap a value.");
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
        long annotationsLength = slowReadVarUInt_1_0();
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
        valueMarker.typeId = valueTid;
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

    /**
     * Reads a 3+ byte FlexUInt into a long. After this method returns, `peekIndex` points to the first byte after the
     * end of the FlexUInt.
     * @param firstByte the first byte of the FlexUInt.
     * @return the value.
     */
    private long uncheckedReadLargeFlexUInt_1_1(int firstByte) {
        if (firstByte == 0) {
            // Note: this is conservative, as 9-byte flex subfields (with a continuation bit in the second byte) can fit
            // in a long. However, the flex subfields parsed by the methods in this class are used only in cases that
            // require an int anyway (symbol IDs, decimal scale), so the added complexity is not warranted.
            throw new IonException("Flex subfield exceeds the length of a long.");
        }
        byte length = (byte) (Integer.numberOfTrailingZeros(firstByte) + 1);
        long result = firstByte >>> length;
        for (byte i = 1; i < length; i++) {
            result |= ((long) (buffer[(int) (peekIndex++)] & SINGLE_BYTE_MASK) << (8 * i - length));
        }
        return result;
    }

    /**
     * Reads a FlexUInt. NOTE: the FlexUInt must fit in a `long`. This must only be called when it is known that the
     * buffer already contains all the bytes in the FlexUInt.
     * @return the value.
     */
    private long uncheckedReadFlexUInt_1_1() {
        // Up-cast to int, ensuring the most significant bit in the byte is not treated as the sign.
        int currentByte = buffer[(int)(peekIndex++)] & SINGLE_BYTE_MASK;
        if ((currentByte & 1) == 1) { // TODO perf: analyze whether these special case checks are a net positive
            // Single byte; shift out the continuation bit.
            return currentByte >>> 1;
        }
        if ((currentByte & 2) != 0) {
            // Two bytes; upcast the second byte to int, ensuring the most significant bit is not treated as the sign.
            // Make room for the six value bits in the first byte. Or with those six value bits after shifting out the
            // two continuation bits.
            return ((buffer[(int) peekIndex++] & SINGLE_BYTE_MASK) << 6) | (currentByte >>> 2);
        }
        return uncheckedReadLargeFlexUInt_1_1(currentByte);
    }

    /**
     * Reads the length of a FlexUInt (or FlexInt) at the given position.
     * Does not alter the state of the peekIndex or anything else.
     * @return the number of bytes used to encode the FlexUInt (or FlexInt) that starts a "position"
     */
    private long uncheckedReadLengthOfFlexUInt_1_1(long position) {
        int length = 1;
        while (true) {
            int numZeros = Integer.numberOfTrailingZeros(buffer[(int) position]);
            if (numZeros < 8) {
                length += numZeros;
                return length;
            } else {
                // We don't actually know the length without looking at even more bytes,
                // so look at another.
                length += 8;
                position++;
            }
        }
    }

    /**
     * Reads a multi-byte FlexUInt into a long, ensuring enough data is available in the buffer. After this method
     * returns, `peekIndex` points to the first byte after the end of the FlexUInt.
     * @param firstByte the first byte of the FlexUInt.
     * @return the value.
     */
    private long slowReadLargeFlexUInt_1_1(int firstByte) {
        if (firstByte == 0) {
            // Note: this is conservative, as 9-byte FlexUInts (with a continuation bit in the second byte) can fit
            // in a long. However, the FlexUInt parsing methods in this class are only used to calculate value length,
            // and the added complexity is not warranted to increase the maximum value size above 2^56 - 1 (72 PB).
            throw new IonException("Found a FlexUInt that was too large to fit in a `long`");
        }
        byte length = (byte) (Integer.numberOfTrailingZeros(firstByte) + 1);
        if (!fillAt(peekIndex, length - 1)) {
            return -1;
        }
        long result = firstByte >>> length;
        for (byte i = 1; i < length; i++) {
            result |= ((long) (buffer[(int) (peekIndex++)] & SINGLE_BYTE_MASK) << (8 * i - length));
        }
        return result;
    }

    /**
     * Reads a FlexUInt, ensuring enough data is available in the buffer. NOTE: the FlexUInt must fit in a `long`.
     * @return the value.
     */
    private long slowReadFlexUInt_1_1() {
        int currentByte = slowReadByte();
        if (currentByte < 0) {
            return -1;
        }
        if ((currentByte & 1) == 1) {
            return currentByte >>> 1;
        }
        return slowReadLargeFlexUInt_1_1(currentByte);
    }

    /**
     * Reads the length of a FlexUInt (or FlexInt) at the given position.
     * Does not alter the state of the peekIndex. May fill data, if needed.
     * @return the number of bytes used to encode the FlexUInt (or FlexInt) that starts a "position"
     *         or -1 if the end of the stream has been reached
     */
    private long slowReadLengthOfFlexUInt_1_1(long position) {
        int length = 1;
        while (true) {
            if (!fillAt(position, 1)) {
                return -1;
            }
            int numZeros = Integer.numberOfTrailingZeros(buffer[(int) position]);
            if (numZeros < 8) {
                length += numZeros;
                return length;
            } else {
                // We don't actually know the length without looking at even more bytes,
                // so add 8 to length, and then look at the next byte.
                length += 8;
                position++;
            }
        }
    }

    /**
     * Reads the header of an Ion 1.1 annotation wrapper. This must only be called when it is known that the buffer
     * already contains all the bytes in the header. Sets `valueMarker` with the start and end indices of the wrapped
     * value. Sets `annotationSequenceMarker` with the start and end indices of the sequence of annotation SIDs, if
     * applicable, or fills `annotationTokenMarkers` if the annotation wrapper contains FlexSyms. After
     * successful return, `peekIndex` will point at the type ID byte of the wrapped value.
     * @param valueTid the type ID of the annotation wrapper.
     * @return true if the length of the wrapped value extends beyond the bytes currently buffered; otherwise, false.
     */
    private boolean uncheckedReadAnnotationWrapperHeader_1_1(IonTypeID valueTid) {
        annotationTokenMarkers.clear();
        if (valueTid.variableLength) {
            // Opcodes 0xE6 (variable-length annotation SIDs) and 0xE9 (variable-length annotation FlexSyms)
            int annotationsLength = (int) uncheckedReadFlexUInt_1_1();
            annotationSequenceMarker.typeId = valueTid;
            annotationSequenceMarker.startIndex = peekIndex;
            annotationSequenceMarker.endIndex = annotationSequenceMarker.startIndex + annotationsLength;
            peekIndex = annotationSequenceMarker.endIndex;
        } else {
            if (valueTid.isInlineable) {
                // Opcodes 0xE7 (one annotation FlexSym) and 0xE8 (two annotation FlexSyms)
                Marker provisionalMarker = annotationTokenMarkers.provisionalElement();
                uncheckedReadFlexSym_1_1(provisionalMarker);
                if (provisionalMarker.endIndex < 0) {
                    return true;
                }
                if (valueTid.lowerNibble == TWO_ANNOTATION_FLEX_SYMS_LOWER_NIBBLE_1_1) {
                    // Opcode 0xE8 (two annotation FlexSyms)
                    provisionalMarker = annotationTokenMarkers.provisionalElement();
                    uncheckedReadFlexSym_1_1(provisionalMarker);
                    if (provisionalMarker.endIndex < 0) {
                        return true;
                    }
                    annotationTokenMarkers.commit();
                }
                annotationTokenMarkers.commit();
            } else {
                // Opcodes 0xE4 (one annotation SID) and 0xE5 (two annotation SIDs)
                int annotationSid = (int) uncheckedReadFlexUInt_1_1();
                annotationTokenMarkers.provisionalElement().endIndex = annotationSid;
                if (valueTid.lowerNibble == TWO_ANNOTATION_SIDS_LOWER_NIBBLE_1_1) {
                    // Opcode 0xE5 (two annotation SIDs)
                    annotationSid = (int) uncheckedReadFlexUInt_1_1();
                    annotationTokenMarkers.provisionalElement().endIndex = annotationSid;
                    annotationTokenMarkers.commit();
                }
                annotationTokenMarkers.commit();
            }
        }
        return false;
    }

    /**
     * Skips a non-length-prefixed annotation sequence (opcodes E4, E5, E7, or E8), ensuring enough space is available
     * in the buffer. After this method returns, `peekIndex` points to the first byte after the end of the annotation
     * sequence.
     * @param valueTid the type ID of the annotation sequence to skip.
     * @return true if there are not enough bytes in the stream to complete the annotation sequence; otherwise, false.
     */
    private boolean slowSkipNonPrefixedAnnotations_1_1(IonTypeID valueTid) {
        if (valueTid.isInlineable) {
            // Opcodes 0xE7 (one annotation FlexSym) and 0xE8 (two annotation FlexSyms)
            if (slowSkipFlexSym_1_1(null) == FlexSymType.INCOMPLETE) {
                return true;
            }
            if (valueTid.lowerNibble == TWO_ANNOTATION_FLEX_SYMS_LOWER_NIBBLE_1_1) {
                // Opcode 0xE8 (two annotation FlexSyms)
                return slowSkipFlexSym_1_1(null) == FlexSymType.INCOMPLETE;
            }
        } else {
            // Opcodes 0xE4 (one annotation SID) and 0xE5 (two annotation SIDs)
            int annotationSid = (int) slowReadFlexUInt_1_1();
            if (annotationSid < 0) {
                return true;
            }
            if (valueTid.lowerNibble == TWO_ANNOTATION_SIDS_LOWER_NIBBLE_1_1) {
                // Opcode 0xE5 (two annotation SIDs)
                annotationSid = (int) slowReadFlexUInt_1_1();
                return annotationSid < 0;
            }
        }
        return false;
    }

    /**
     * Reads the header of an Ion 1.1 annotation wrapper, ensuring enough data is available in the buffer. Sets
     * `valueMarker` with the start and end indices of the wrapped value. Sets `annotationSequenceMarker` with the start
     * and end indices of the sequence of annotation SIDs, if applicable, or fills `annotationTokenMarkers` if the
     * annotation wrapper contains FlexSyms. After successful return, `peekIndex` will point at the type ID byte of the
     * wrapped value.
     * @param valueTid the type ID of the annotation wrapper.
     * @return true if there are not enough bytes in the stream to complete the value; otherwise, false.
     */
    private boolean slowReadAnnotationWrapperHeader_1_1(IonTypeID valueTid) {
        if (!refillableState.skipAnnotations) {
            annotationTokenMarkers.clear();
        }
        if (valueTid.variableLength) {
            // Opcodes 0xE6 (variable-length annotation SIDs) and 0xE9 (variable-length annotation FlexSyms)
            // At this point the value must be at least 3 more bytes: one for the smallest-possible annotations
            // length, one for the smallest-possible annotation, and 1 for the smallest-possible value
            // representation.
            if (!fillAt(peekIndex, 3)) {
                return true;
            }
            int annotationsLength = (int) slowReadFlexUInt_1_1();
            if (annotationsLength < 0) {
                return true;
            }
            if (!fillAt(peekIndex, annotationsLength)) {
                return true;
            }
            long annotationsEnd = peekIndex + annotationsLength;
            if (!refillableState.skipAnnotations) {
                annotationSequenceMarker.typeId = valueTid;
                annotationSequenceMarker.startIndex = peekIndex;
                annotationSequenceMarker.endIndex = annotationsEnd;
            }
            peekIndex = annotationsEnd;
        } else {
            // At this point the value must have at least one more byte for each annotation FlexSym (one for lower
            // nibble 7, two for lower nibble 8), plus one for the smallest-possible value representation.
            if (!fillAt(peekIndex, (valueTid.lowerNibble == ONE_ANNOTATION_FLEX_SYM_LOWER_NIBBLE_1_1 || valueTid.lowerNibble == ONE_ANNOTATION_SID_LOWER_NIBBLE_1_1) ? 2 : 3)) {
                return true;
            }
            if (refillableState.skipAnnotations) {
                return slowSkipNonPrefixedAnnotations_1_1(valueTid);
            }
            if (valueTid.isInlineable) {
                // Opcodes 0xE7 (one annotation FlexSym) and 0xE8 (two annotation FlexSyms)
                Marker provisionalMarker = annotationTokenMarkers.provisionalElement();
                if (slowReadFlexSym_1_1(provisionalMarker)) {
                    return true;
                }
                if (valueTid.lowerNibble == TWO_ANNOTATION_FLEX_SYMS_LOWER_NIBBLE_1_1) {
                    // Opcode 0xE8 (two annotation FlexSyms)
                    provisionalMarker = annotationTokenMarkers.provisionalElement();
                    if (slowReadFlexSym_1_1(provisionalMarker)) {
                        return true;
                    }
                    annotationTokenMarkers.commit();
                }
                annotationTokenMarkers.commit();
            } else {
                // Opcodes 0xE4 (one annotation SID) and 0xE5 (two annotation SIDs)
                int annotationSid = (int) slowReadFlexUInt_1_1();
                if (annotationSid < 0) {
                    return true;
                }
                annotationTokenMarkers.provisionalElement().endIndex = annotationSid;
                if (valueTid.lowerNibble == TWO_ANNOTATION_SIDS_LOWER_NIBBLE_1_1) {
                    // Opcode 0xE5 (two annotation SIDs)
                    annotationSid = (int) slowReadFlexUInt_1_1();
                    if (annotationSid < 0) {
                        return true;
                    }
                    annotationTokenMarkers.provisionalElement().endIndex = annotationSid;
                    annotationTokenMarkers.commit();
                }
                annotationTokenMarkers.commit();
            }

        }
        valueMarker.typeId = valueTid;
        return false;
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
    private long calculateEndIndex_1_1(IonTypeID valueTid, boolean isAnnotated) {
        if (valueTid.isDelimited) {
            event = Event.START_CONTAINER;
            return DELIMITED_MARKER;
        }
        long length = valueTid.length;
        if (valueTid.variableLength) {
            length = uncheckedReadFlexUInt_1_1();
        } else if (length < 0) {
            // The value is a FlexInt or FlexUInt, so read the continuation bits to determine the length.
            length = uncheckedReadLengthOfFlexUInt_1_1(peekIndex);
        }
        long endIndex = peekIndex + length;
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
     * Reads the field name at `peekIndex`. After this method returns `peekIndex` points to the first byte of the
     * value that follows the field name. If the field name contained a symbol ID, `fieldSid` is set to that symbol ID.
     * If it contained inline text, `fieldSid` is set to -1, and the start and end indexes of the inline text are
     * described by `fieldTextMarker`.
     */
    private void uncheckedReadFieldName_1_1() {
        if (parent.typeId.isInlineable) {
            fieldSid = (int) uncheckedReadFlexSym_1_1(fieldTextMarker);
        } else {
            // 0 in field name position of a SID struct indicates that all field names that follow are represented as
            // using FlexSyms.
            if (buffer[(int) peekIndex] == FlexInt.ZERO) {
                peekIndex++;
                parent.typeId = IonTypeID.STRUCT_WITH_FLEX_SYMS_ID;
                fieldSid = (int) uncheckedReadFlexSym_1_1(fieldTextMarker);
            } else {
                fieldSid = (int) uncheckedReadFlexUInt_1_1();
                fieldTextMarker.startIndex = -1;
                fieldTextMarker.endIndex = fieldSid;
            }
        }
    }

    /**
     * Reads a 3+ byte FlexInt into a long. After this method returns, `peekIndex` points to the first byte after the
     * end of the FlexUInt.
     * @param firstByte the first byte of the FlexInt.
     * @return the value.
     */
    private long uncheckedReadLargeFlexInt_1_1(int firstByte) {
        firstByte &= SINGLE_BYTE_MASK;
        // FlexInts are essentially just FlexUInts that interpret the most significant bit as a sign that needs to be
        // extended.
        long result = uncheckedReadLargeFlexUInt_1_1(firstByte);
        if (buffer[(int) peekIndex - 1] < 0) {
            // Sign extension.
            result |= ~(-1 >>> Long.numberOfLeadingZeros(result));
        }
        return result;
    }

    /**
     * Reads a FlexInt into a long. After this method returns, `peekIndex` points to the first byte after the
     * end of the FlexUInt.
     * @return the value.
     */
    private long uncheckedReadFlexInt_1_1() {
        // The following up-cast to int performs sign extension, if applicable.
        int currentByte = buffer[(int)(peekIndex++)];
        if ((currentByte & 1) == 1) {
            // Single byte; shift out the continuation bit while preserving the sign.
            return currentByte >> 1;
        }
        if ((currentByte & 2) != 0) {
            // Two bytes; up-cast the second byte to int, thereby performing sign extension. Make room for the six
            // value bits in the first byte. Or with those six value bits after shifting out the two continuation bits.
            return buffer[(int) peekIndex++] << 6 | ((currentByte & SINGLE_BYTE_MASK) >>> 2);
        }
        return uncheckedReadLargeFlexInt_1_1(currentByte);
    }

    /**
     * Reads a FlexSym. After this method returns, `peekIndex` points to the first byte after the end of the FlexSym.
     * When the FlexSym contains inline text, the given Marker's start and end indices are populated with the start and
     * end of the UTF-8 byte sequence, and this method returns -1. When the FlexSym contains a symbol ID, the given
     * Marker's endIndex is set to the symbol ID value and its startIndex is set to -1. When this FlexSym wraps a
     * delimited end marker, neither the Marker's startIndex nor its endIndex is set.
     * @param markerToSet the marker to populate.
     * @return the user-space symbol ID value if one was present, otherwise -1.
     */
    private long uncheckedReadFlexSym_1_1(Marker markerToSet) {
        long result = uncheckedReadFlexInt_1_1();
        if (result == 0) {
            int nextByte = buffer[(int)(peekIndex++)];
            if (isFlexSymSystemSymbolOrSid0(nextByte & SINGLE_BYTE_MASK)) {
                setSystemSymbolMarker(markerToSet, (byte)(nextByte - FLEX_SYM_SYSTEM_SYMBOL_OFFSET));
                return -1;
            } else if (nextByte != OpCodes.DELIMITED_END_MARKER) {
                throw new IonException("FlexSym 0 may only precede symbol zero, system symbol, or delimited end.");
            }
            markerToSet.typeId = IonTypeID.DELIMITED_END_ID;
            return -1;
        } else if (result < 0) {
            markerToSet.startIndex = peekIndex;
            markerToSet.endIndex = peekIndex - result;
            peekIndex = markerToSet.endIndex;
            return -1;
        } else {
            markerToSet.startIndex = -1;
            markerToSet.endIndex = result;
        }
        return result;
    }

    /*
     * Determines whether a byte (specifically, the byte following a FlexSym escape byte) represents a system symbol.
     *
     * @param byteAfterEscapeCode The unsigned value of the byte after the FlexSym escape byte
     * @return true if the byte is in the reserved range for system symbols or $0.
     */
    private static boolean isFlexSymSystemSymbolOrSid0(int byteAfterEscapeCode) {
        return byteAfterEscapeCode >= FLEX_SYM_SYSTEM_SYMBOL_OFFSET && byteAfterEscapeCode <= FLEX_SYM_MAX_SYSTEM_SYMBOL;
    }

    /**
     * Reads a FlexInt into a long, ensuring enough space is available in the buffer. After this method returns false,
     * `peekIndex` points to the first byte after the end of the FlexInt and `markerToSet.endIndex` contains the
     * FlexInt value.
     * @param firstByte the first (least-significant) byte of the FlexInt.
     * @param markerToSet the marker to populate.
     * @return true if there are not enough bytes to complete the FlexSym; otherwise, false.
     */
    private boolean slowReadLargeFlexInt_1_1(int firstByte, Marker markerToSet) {
        firstByte &= SINGLE_BYTE_MASK;
        // FlexInts are essentially just FlexUInts that interpret the most significant bit as a sign that needs to be
        // extended.
        long result = slowReadLargeFlexUInt_1_1(firstByte);
        if (result < 0) {
            return true;
        }
        if (buffer[(int) peekIndex - 1] < 0) {
            // Sign extension.
            result |= ~(-1 >>> Long.numberOfLeadingZeros(result));
        }
        markerToSet.endIndex = result;
        return false;
    }

    /**
     * Reads a FlexInt into a long, ensuring enough space is available in the buffer. After this method returns false,
     * `peekIndex` points to the first byte after the end of the FlexInt and `markerToSet.endIndex` contains the
     * FlexInt value.
     * @param markerToSet the marker to populate.
     * @return true if there are not enough bytes to complete the FlexSym; otherwise, false.
     */
    private boolean slowReadFlexInt_1_1(Marker markerToSet) {
        int currentByte = slowReadByte();
        if (currentByte < 0) {
            return true;
        }
        if ((currentByte & 1) == 1) {
            // Single byte; shift out the continuation bit while preserving the sign. The downcast to byte and implicit
            // upcast back to int results in sign extension.
            markerToSet.endIndex = ((byte) currentByte) >> 1;
            return false;
        }
        return slowReadLargeFlexInt_1_1(currentByte, markerToSet);
    }

    /**
     * Reads a FlexSym, ensuring enough space is available in the buffer. After this method returns, `peekIndex`
     * points to the first byte after the end of the FlexSym. When the FlexSym contains inline text, the given Marker's
     * start and end indices are populated with the start and end of the UTF-8 byte sequence, and this method returns
     * -1. When the FlexSym contains a symbol ID, the given Marker's endIndex is set to the symbol ID value and its
     * startIndex is set to -1. When this FlexSym wraps a delimited end marker, neither the Marker's startIndex nor its
     * endIndex is set.
     * @param markerToSet the marker to populate.
     * @return true if there are not enough bytes to complete the FlexSym; otherwise, false.
     */
    private boolean slowReadFlexSym_1_1(Marker markerToSet) {
        if (slowReadFlexInt_1_1(markerToSet)) {
            return true;
        }
        long result = markerToSet.endIndex;
        markerToSet.endIndex = -1;
        if (result == 0) {
            int nextByte = slowReadByte();
            if (nextByte < 0) {
                return true;
            }
            if (isFlexSymSystemSymbolOrSid0(nextByte)) {
                setSystemSymbolMarker(markerToSet, nextByte - FLEX_SYM_SYSTEM_SYMBOL_OFFSET);
                return false;
            } else if ((byte) nextByte != OpCodes.DELIMITED_END_MARKER) {
                throw new IonException("FlexSyms may only wrap symbol zero, empty string, or delimited end.");
            }
            markerToSet.typeId = DELIMITED_END_ID;
        } else if (result < 0) {
            markerToSet.startIndex = peekIndex;
            markerToSet.endIndex = peekIndex - result;
            peekIndex = markerToSet.endIndex;
        } else {
            markerToSet.startIndex = -1;
            markerToSet.endIndex = result;
        }
        return false;
    }

    /**
     * FlexSym encoding types.
     */
    private enum FlexSymType {
        INCOMPLETE {
            @Override
            IonTypeID typeIdFor(int length) {
                throw new IllegalStateException("The FlexSym is incomplete.");
            }
        },
        INLINE_TEXT {
            @Override
            IonTypeID typeIdFor(int length) {
                if (length <= 0xF) {
                    return TYPE_IDS_1_1[0xA0 | length];
                }
                return TYPE_IDS_1_1[OpCodes.VARIABLE_LENGTH_INLINE_SYMBOL & SINGLE_BYTE_MASK];
            }
        },
        SYMBOL_ID {
            @Override
            IonTypeID typeIdFor(int length) {
                if (length == 0) {
                    return TYPE_IDS_1_1[OpCodes.SYMBOL_ADDRESS_1_BYTE & SINGLE_BYTE_MASK];
                }
                if (length < 3) {
                    return TYPE_IDS_1_1[0xE0 | length];
                }
                return TYPE_IDS_1_1[OpCodes.SYMBOL_ADDRESS_MANY_BYTES & SINGLE_BYTE_MASK];
            }
        },
        SYSTEM_SYMBOL_ID {
            @Override
            IonTypeID typeIdFor(int length) {
                return SYSTEM_SYMBOL_VALUE;
            }
        },
        STRUCT_END {
            @Override
            IonTypeID typeIdFor(int length) {
                throw new IllegalStateException("The special struct end FlexSym is not associated with a type ID.");
            }
        };

        /**
         * Classifies a special FlexSym (beginning with FlexInt zero) based on the byte that follows.
         * @param specialByte the byte that followed FlexInt zero.
         * @return the FlexSymType that corresponds to the given special byte.
         */
        static FlexSymType classifySpecialFlexSym(int specialByte) {
            if (specialByte < 0) {
                return FlexSymType.INCOMPLETE;
            }
            if (isFlexSymSystemSymbolOrSid0(specialByte)) {
                return FlexSymType.SYSTEM_SYMBOL_ID;
            }
            if ((byte) specialByte == OpCodes.DELIMITED_END_MARKER) {
                return FlexSymType.STRUCT_END;
            }
            throw new IonException("FlexSyms may only wrap symbol zero, empty string, or delimited end.");
        }

        /**
         * Gets the most appropriate IonTypeID for a FlexSym of this type and the given length.
         * @param length the length of the FlexSym.
         * @return an Ion 1.1 IonTypeID with appropriate values for 'length' and 'isInlineable'.
         */
        abstract IonTypeID typeIdFor(int length);
    }

    /**
     * Skips a FlexSym. After this method returns, `peekIndex` points to the first byte after the end of the FlexSym.
     * @param markerToSet the method returns `INLINE_TEXT, will have `startIndex` and `endIndex` set to the bounds of
     *                    the inline UTF-8 byte sequence.
     * @return the type of FlexSym that was skipped.
     */
    private FlexSymType uncheckedSkipFlexSym_1_1(Marker markerToSet) {
        long result = uncheckedReadFlexInt_1_1();
        if (result == 0) {
            markerToSet.startIndex = peekIndex + 1;
            markerToSet.endIndex = markerToSet.startIndex;
            int specialByte = buffer[(int) peekIndex++] & SINGLE_BYTE_MASK;
            FlexSymType type = FlexSymType.classifySpecialFlexSym(specialByte);
            if (type == FlexSymType.SYSTEM_SYMBOL_ID) {
                setSystemSymbolMarker(markerToSet, (byte)(specialByte - FLEX_SYM_SYSTEM_SYMBOL_OFFSET));
            }
            return type;
        } else if (result < 0) {
            markerToSet.startIndex = peekIndex;
            markerToSet.endIndex = peekIndex - result;
            peekIndex = markerToSet.endIndex;
            return FlexSymType.INLINE_TEXT;
        }
        return FlexSymType.SYMBOL_ID;
    }

    /**
     * Skips a FlexSym, ensuring enough space is available in the buffer. After this method returns, `peekIndex` points
     * to the first byte after the end of the FlexSym.
     * @param markerToSet if non-null and the method returns `INLINE_TEXT`, will have `startIndex` and `endIndex` set
     *                    to the bounds of the inline UTF-8 byte sequence.
     * @return INCOMPLETE if there are not enough bytes in the stream to complete the FlexSym; otherwise, the type
     *  of FlexSym that was skipped.
     */
    private FlexSymType slowSkipFlexSym_1_1(Marker markerToSet) {
        long result = slowReadFlexUInt_1_1();
        if (result < 0) {
            return FlexSymType.INCOMPLETE;
        }
        if (buffer[(int) peekIndex - 1] < 0) {
            // Sign extension.
            result |= ~(-1 >>> Long.numberOfLeadingZeros(result));
        }
        if (result == 0) {
            int specialByte = slowReadByte();
            FlexSymType flexSymType = FlexSymType.classifySpecialFlexSym(specialByte);
            if (markerToSet != null && flexSymType != FlexSymType.INCOMPLETE) {
                markerToSet.startIndex = peekIndex;
                markerToSet.endIndex = peekIndex;
            }
            if (markerToSet != null && flexSymType == FlexSymType.SYSTEM_SYMBOL_ID) {
                // FIXME: See if we can set the SID in the endIndex here without causing the slow reader to get confused
                //   about where the end of the value is for tagless symbols.
                //   I.e. use setSystemSymbolMarker(markerToSet, (byte)(specialByte - FLEX_SYM_SYSTEM_SYMBOL_OFFSET));
                markerToSet.typeId = SYSTEM_SYMBOL_VALUE;
                markerToSet.startIndex = peekIndex - 1;
            }
            return flexSymType;
        } else if (result < 0) {
            if (markerToSet != null) {
                markerToSet.startIndex = peekIndex;
                markerToSet.endIndex = peekIndex - result;
            }
            peekIndex -= result;
            return FlexSymType.INLINE_TEXT;
        }
        return FlexSymType.SYMBOL_ID;
    }

    /**
     * Reads the field name FlexSym at `peekIndex`, ensuring enough bytes are available in the buffer. After this method
     * returns `peekIndex` points to the first byte of the value that follows the field name. If the field name
     * contained a symbol ID, `fieldSid` is set to that symbol ID. If it contained inline text, `fieldSid` is set to -1,
     * and the start and end indexes of the inline text are described by `fieldTextMarker`.
     * @return true if there are not enough bytes in the stream to complete the field name; otherwise, false.
     */
    private boolean slowReadFieldNameFlexSym_1_1() {
        if (slowReadFlexSym_1_1(fieldTextMarker)) {
            return true;
        }
        if (fieldTextMarker.startIndex < 0) {
            fieldSid = (int) fieldTextMarker.endIndex;
        }
        return false;
    }

    /**
     * Reads the field name FlexSym or FlexUInt at `peekIndex`, ensuring enough bytes are available in the buffer. After
     * this method returns `peekIndex` points to the first byte of the value that follows the field name. If the field
     * name contained a symbol ID, `fieldSid` is set to that symbol ID. If it contained inline text, `fieldSid` is set
     * to -1, and the start and end indexes of the inline text are described by `fieldTextMarker`.
     * @return true if there are not enough bytes in the stream to complete the field name; otherwise, false.
     */
    private boolean slowReadFieldName_1_1() {
        // The value must have at least 2 more bytes: 1 for the smallest-possible field SID and 1 for
        // the smallest-possible representation.
        if (!fillAt(peekIndex, 2)) {
            return true;
        }
        if (parent.typeId.isInlineable) {
            return slowReadFieldNameFlexSym_1_1();
        } else {
            // 0 in field name position of a SID struct indicates that all field names that follow are represented as
            // using FlexSyms.
            if (buffer[(int) peekIndex] == FlexInt.ZERO) {
                peekIndex++;
                setCheckpoint(CheckpointLocation.BEFORE_UNANNOTATED_TYPE_ID);
                parent.typeId = IonTypeID.STRUCT_WITH_FLEX_SYMS_ID;
                return slowReadFieldNameFlexSym_1_1();
            } else {
                fieldSid = (int) slowReadFlexUInt_1_1();
                fieldTextMarker.startIndex = -1;
                fieldTextMarker.endIndex = fieldSid;
                return fieldSid < 0;
            }
        }
    }

    /**
     * Determines whether the current delimited container has reached its end.
     * @return true if the container is at its end; otherwise, false.
     */
    private boolean uncheckedIsDelimitedEnd_1_1() {
        if (parent.typeId.type == IonType.STRUCT) {
            uncheckedReadFieldName_1_1();
            if (fieldSid < 0 && fieldTextMarker.typeId != null && fieldTextMarker.typeId.lowerNibble == OpCodes.DELIMITED_END_MARKER) {
                parent.endIndex = peekIndex;
                event = Event.END_CONTAINER;
                return true;
            }
        } else if (buffer[(int) peekIndex] == OpCodes.DELIMITED_END_MARKER) {
            peekIndex++;
            parent.endIndex = peekIndex;
            event = Event.END_CONTAINER;
            return true;
        }
        return false;
    }

    /**
     * Un-reads one byte. It is up to the caller to ensure the provided byte is actually the last byte read.
     * @param b the byte to un-read.
     */
    private void unreadByte(int b) {
        if (refillableState.isSkippingCurrentValue) {
            refillableState.lastUnbufferedByte = b;
        } else {
            peekIndex--;
        }
    }

    /**
     * Determines whether the cursor is at the end of a delimited struct.
     * @param currentByte the byte on which the cursor is currently positioned.
     * @return true if the struct is at its end or if not enough data is available; otherwise, false.
     */
    private boolean slowIsDelimitedStructEnd_1_1(int currentByte) {
        if (currentByte == FlexInt.ZERO) {
            // This is a special FlexSym in field position. Determine whether the next byte is DELIMITED_END_MARKER.
            currentByte = slowReadByte();
            if (currentByte < 0) {
                return true;
            }
            if (currentByte == (OpCodes.DELIMITED_END_MARKER & SINGLE_BYTE_MASK)) {
                event = Event.END_CONTAINER;
                valueTid = null;
                fieldSid = -1;
                return true;
            }
            // Note: slowReadByte() increments the peekIndex, but if the delimiter end is not found, the byte
            // needs to remain available.
            unreadByte(currentByte);
        }
        return false;
    }

    /**
     * Determines whether the current delimited container has reached its end, ensuring enough bytes are available
     * in the stream.
     * @return true if the container is at its end or if not enough data is available; otherwise, false.
     */
    private boolean slowIsDelimitedEnd_1_1() {
        int b = slowReadByte();
        if (b < 0) {
            return true;
        }
        if (parent.typeId.type == IonType.STRUCT && slowIsDelimitedStructEnd_1_1(b)) {
            parent.endIndex = peekIndex;
            return true;
        } else if (b == (OpCodes.DELIMITED_END_MARKER & SINGLE_BYTE_MASK)) {
            parent.endIndex = peekIndex;
            event = Event.END_CONTAINER;
            valueTid = null;
            fieldSid = -1;
            return true;
        }
        // Note: slowReadByte() increments the peekIndex, but if the delimiter end is not found, the byte
        // needs to remain available.
        unreadByte(b);
        return false;
    }

    /**
     * Skips past the remaining elements of the current delimited container.
     * @return true if the end of the stream was reached before skipping past all remaining elements; otherwise, false.
     */
    boolean uncheckedSkipRemainingDelimitedContainerElements_1_1() {
        // TODO this needs to be updated to handle the case where the container contains non-prefixed macro invocations,
        //  as the length of these invocations is unknown to the cursor. Input from the macro evaluator is needed.
        while (event != Event.END_CONTAINER) {
            event = Event.NEEDS_DATA;
            while (uncheckedNextToken());
            if (event == Event.NEEDS_DATA) {
                return true;
            }
        }
        return false;
    }

    /**
     * Skips past the remaining elements of the current delimited container, ensuring enough bytes are available in
     * the stream.
     * @return true if the end of the stream was reached before skipping past all remaining elements; otherwise, false.
     */
    private boolean slowSkipRemainingDelimitedContainerElements_1_1() {
        // TODO this needs to be updated ot handle the case where the container contains non-prefixed macro invocations,
        //  as the length of these invocations is unknown to the cursor. Input from the macro evaluator is needed.
        while (event != Event.END_CONTAINER) {
            slowNextToken();
            if (event == Event.START_CONTAINER && valueMarker.endIndex == DELIMITED_MARKER) {
                seekPastDelimitedContainer_1_1();
            }
            if (event == Event.NEEDS_DATA) {
                return true;
            }
        }
        return false;
    }

    /**
     * Seek past a delimited container that was never stepped into.
     */
    private void seekPastDelimitedContainer_1_1() {
        stepIntoContainer();
        stepOutOfContainer();
    }

    /**
     * Locates the end of the delimited container on which the reader is currently positioned.
     * @return true if the end of the container was found; otherwise, false.
     */
    private boolean slowFindDelimitedEnd_1_1() {
        // Pin the current buffer offset so that all bytes encountered while finding the end of the delimited container
        // are buffered. If the pin is already set, do not overwrite; this indicates a retry after previously
        // running out of data.
        if (refillableState.pinOffset < 0) {
            refillableState.pinOffset = offset;
        }
        if (parent == null) {
            // At depth zero, there can not be any more upward recursive calls to which the shift needs to be
            // conveyed.
            refillableState.pendingShift = 0;
        }
        // Save the cursor's current state so that it can return to this position after finding the delimited end.
        long savedPeekIndex = peekIndex;
        long savedStartIndex = valueMarker.startIndex;
        long savedEndIndex = valueMarker.endIndex;
        int savedFieldSid = fieldSid;
        IonTypeID savedFieldTid = fieldTextMarker.typeId;
        long savedFieldTextStartIndex = fieldTextMarker.startIndex;
        long savedFieldTextEndIndex = fieldTextMarker.endIndex;
        IonTypeID savedValueTid = valueMarker.typeId;
        IonTypeID savedAnnotationTid = annotationSequenceMarker.typeId;
        long savedAnnotationStartIndex = annotationSequenceMarker.startIndex;
        long savedAnnotationsEndIndex = annotationSequenceMarker.endIndex;
        CheckpointLocation savedCheckpointLocation = checkpointLocation;
        long savedCheckpoint = checkpoint;
        int savedContainerIndex = containerIndex;
        Marker savedParent = parent;
        boolean savedHasAnnotations = hasAnnotations;
        // The cursor remains logically positioned at the current value despite probing forward for the end of the
        // delimited value. Accordingly, do not overwrite the existing annotations with any annotations found during
        // the probe.
        refillableState.skipAnnotations = true;
        // ------------

        // TODO performance: the following line causes the end indexes of any child delimited containers that are not
        //  contained within a length-prefixed container to be calculated. Currently these are thrown away, but storing
        //  them in case those containers are later accessed could make them faster to skip. This would require some
        //  additional complexity.
        seekPastDelimitedContainer_1_1();

        refillableState.skipAnnotations = false;
        boolean isReady = event != Event.NEEDS_DATA;
        if (refillableState.isSkippingCurrentValue) {
            // This delimited container is oversized. The cursor must seek past it.
            refillableState.state = State.SEEK_DELIMITED;
            refillableState.targetSeekDepth = savedContainerIndex;
            refillableState.pendingShift = 0;
            return isReady;
        }

        // Restore the state of the cursor at the start of the delimited container.
        long pendingShift = refillableState.pendingShift;
        valueMarker.startIndex = savedStartIndex - pendingShift;
        valueMarker.endIndex = (savedEndIndex == DELIMITED_MARKER) ? DELIMITED_MARKER : (savedEndIndex - pendingShift);
        fieldSid = savedFieldSid;
        valueMarker.typeId = savedValueTid;
        valueTid = savedValueTid;
        annotationSequenceMarker.typeId = savedAnnotationTid;
        annotationSequenceMarker.startIndex = savedAnnotationStartIndex - pendingShift;
        annotationSequenceMarker.endIndex = savedAnnotationsEndIndex - pendingShift;
        fieldTextMarker.typeId = savedFieldTid;
        fieldTextMarker.startIndex = savedFieldTextStartIndex - pendingShift;
        fieldTextMarker.endIndex = savedFieldTextEndIndex - pendingShift;
        checkpointLocation = savedCheckpointLocation;
        checkpoint = savedCheckpoint - pendingShift;
        containerIndex = savedContainerIndex;
        hasAnnotations = savedHasAnnotations;

        savedPeekIndex -= pendingShift;
        parent = savedParent;
        if (isReady) {
            // Record the endIndex so that it does not need to be calculated repetitively.
            valueMarker.endIndex = peekIndex;
            event = Event.START_CONTAINER;
            refillableState.state = State.READY;
            refillableState.pinOffset = -1;
        } else {
            // The fill is not complete, but there is currently no more data. The cursor will have to resume the fill
            // before processing the next request.
            refillableState.state = State.FILL_DELIMITED;
        }

        peekIndex = savedPeekIndex;
        return isReady;
    }

    /**
     * Seeks to the end of the delimited container at `refillableState.targetSeekDepth`.
     * @return true if the end of the container was reached; otherwise, false.
     */
    private boolean slowSeekToDelimitedEnd_1_1() {
        refillableState.state = State.READY;
        refillableState.isSkippingCurrentValue = true;
        while (containerIndex > refillableState.targetSeekDepth) {
            stepOutOfContainer();
            if (event == Event.NEEDS_DATA) {
                refillableState.state = State.SEEK_DELIMITED;
                refillableState.isSkippingCurrentValue = false;
                return false;
            }
        }
        // The end of the container has been reached. Report the number of bytes skipped and exit seek mode.
        if (dataHandler != null) {
            reportSkippedData();
        }
        refillableState.totalDiscardedBytes += refillableState.individualBytesSkippedWithoutBuffering;
        refillableState.individualBytesSkippedWithoutBuffering = 0;
        refillableState.isSkippingCurrentValue = false;
        event = Event.NEEDS_INSTRUCTION;
        return true;
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
            throw new IonException(String.format("Value [%s:%s] exceeds the length of its parent container [%s:%s].", peekIndex, endIndex, parent.startIndex, parent.endIndex));
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
        if (peekIndex < valueMarker.endIndex ||  parent.endIndex > peekIndex) {
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
        valueTid = null;
        valueMarker.typeId = null;
        valueMarker.startIndex = -1;
        valueMarker.endIndex = -1;
        fieldSid = -1;
        fieldTextMarker.typeId = null;
        fieldTextMarker.startIndex = -1;
        fieldTextMarker.endIndex = -1;
        hasAnnotations = false;
        annotationSequenceMarker.typeId = null;
        annotationSequenceMarker.startIndex = -1;
        annotationSequenceMarker.endIndex = -1;
        macroInvocationId = -1;
        isSystemInvocation = false;
        taglessType = null;
    }

    /**
     * Reads the final three bytes of an IVM. `peekIndex` must point to the first byte after the opening `0xE0` byte.
     * After return, `majorVersion`, `minorVersion`, and `typeIds` will be updated accordingly, and `peekIndex` will
     * point to the first byte after the IVM.
     */
    private void readIvm() {
        if (limit < peekIndex + IVM_REMAINING_LENGTH) {
            throw new IonException("Incomplete Ion version marker.");
        }
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
            byteBuffer.order(ByteOrder.BIG_ENDIAN);
        } else if (minorVersion == 1) {
            typeIds = IonTypeID.TYPE_IDS_1_1;
            byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
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

    /*
     * The given Marker's endIndex is set to the system symbol ID value and its startIndex is set to -1
     * @param markerToSet the marker to set.
     */
    private void setSystemSymbolMarker(Marker markerToSet, int systemSid) {
        event = Event.START_SCALAR;
        markerToSet.typeId = SYSTEM_SYMBOL_VALUE;
        markerToSet.startIndex = -1;
        markerToSet.endIndex = systemSid;
    }

    /**
     * Sets the given marker to represent the current system macro invocation.
     * Before calling this method, `macroInvocationId` must be set from the one-byte FixedUInt that represents the ID.
     * @param markerToSet the marker to set.
     */
    private void setSystemMacroInvocationMarker(Marker markerToSet) {
        isSystemInvocation = true;
        event = Event.NEEDS_INSTRUCTION;
        markerToSet.typeId = SYSTEM_MACRO_INVOCATION_ID;
        markerToSet.startIndex = peekIndex;
        markerToSet.endIndex = -1;
    }

    /**
     * Sets the given marker to represent the current user macro invocation.
     * @param valueTid the type ID of the macro invocation.
     * @param markerToSet the Marker to set with information parsed from the macro invocation. After return, the
     *                    marker's type ID will be set, startIndex will point to the first byte of the invocation's
     *                    body, and endIndex will either be -1 (when not a system symbol or prefixed invocation), or
     *                    will be set to the end of the invocation.
     * @param length the declared length of the invocation. Ignored unless this is a length-prefixed invocation
     *               (denoted by `valueTid.variableLength == true`).
     */
    private void setUserMacroInvocationMarker(IonTypeID valueTid, Marker markerToSet, long length) {
        // It's not yet known whether the invocation represents a scalar or container, or even if it is complete.
        // A higher-level reader must provide additional instructions to evaluate the invocation.
        event = Event.NEEDS_INSTRUCTION;
        markerToSet.typeId = valueTid;
        markerToSet.startIndex = peekIndex;
        // Unless this is a length-prefixed invocation, the end index of the macro invocation cannot be known until
        // evaluation.
        markerToSet.endIndex = valueTid.variableLength ? peekIndex + length : -1;
    }

    /**
     * Reads a macro invocation header, ensuring enough bytes are buffered. `peekIndex` must be positioned on the
     * first byte that follows the opcode. After return, `peekIndex` will be positioned after any macro address
     * byte(s), and `macroInvocationId` will be set to the address of the macro being invoked.
     * @param valueTid the type ID of the macro invocation.
     * @param markerToSet the Marker to set with information parsed from the macro invocation. After return, the
     *                    marker's type ID will be set, startIndex will point to the first byte of the invocation's
     *                    body, and endIndex will either be -1 (when not a system symbol or prefixed invocation), or
     *                    will be set to the end of the invocation.
     */
    private void uncheckedReadMacroInvocationHeader(IonTypeID valueTid, Marker markerToSet) {
        if (valueTid.macroId < 0) {
            if (valueTid.lowerNibble == 0x4) {
                // Opcode 0xF4: Read the macro ID as a FlexUInt.
                macroInvocationId = uncheckedReadFlexUInt_1_1();
            } else if (valueTid.variableLength) {
                // Opcode 0xF5: Read the macro ID as a FlexUInt, then read the length as a FlexUInt.
                macroInvocationId = uncheckedReadFlexUInt_1_1();
                setUserMacroInvocationMarker(valueTid, markerToSet, uncheckedReadFlexUInt_1_1());
                return;
            } else {
                // Opcode 0xEF: system macro invocation
                macroInvocationId = buffer[(int) peekIndex++];
                setSystemMacroInvocationMarker(markerToSet);
                return;
            }
        } else if (valueTid.length > 0) {
            // Opcodes 0x4_: the rest of the macro ID follows in a 1-byte FixedUInt.
            // Opcodes 0x5_: the rest of the macro ID follows in a 2-byte FixedUInt.
            int remainingId = buffer[(int) peekIndex++] & SINGLE_BYTE_MASK;
            if (valueTid.length > 1) {
                remainingId |= ((buffer[(int) peekIndex++] & SINGLE_BYTE_MASK) << 8);
            }
            macroInvocationId = valueTid.macroId + remainingId;
        } else {
            // Opcodes 0x00 - 0x3F -- the opcode is the macro ID.
            macroInvocationId = valueTid.macroId;
        }
        setUserMacroInvocationMarker(valueTid, markerToSet, -1);
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
            throw new IonException("Invalid type ID: " + valueTid.theByte);
        } else if (valueTid.type == IonTypeID.ION_TYPE_ANNOTATION_WRAPPER) {
            if (isAnnotated) {
                throw new IonException("Nested annotation wrappers are invalid.");
            }
            if (minorVersion == 0 ? uncheckedReadAnnotationWrapperHeader_1_0(valueTid) : uncheckedReadAnnotationWrapperHeader_1_1(valueTid)) {
                return true;
            }
            hasAnnotations = true;
            return uncheckedReadHeader(buffer[(int) (peekIndex++)] & SINGLE_BYTE_MASK, true, valueMarker);
        } else if (minorVersion == 1 && valueTid.isMacroInvocation) {
            uncheckedReadMacroInvocationHeader(valueTid, markerToSet);
            return true;
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
            if (minorVersion == 1 && valueTid.isNull && valueTid.length > 0) {
                valueTid = IonTypeID.NULL_TYPE_IDS_1_1[buffer[(int)(peekIndex++)] & SINGLE_BYTE_MASK];
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
        if (minorVersion == 1) {
            if (valueTid.isMacroInvocation) {
                setCheckpointAfterValueHeader();
                return true;
            }
            if (valueTid.isNull && valueTid.length > 0) {
                int nullTypeIndex = slowReadByte();
                if (nullTypeIndex < 0) {
                    return true;
                }
                markerToSet.typeId = IonTypeID.NULL_TYPE_IDS_1_1[nullTypeIndex];
            }
        }
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
     * Reads a macro invocation header, ensuring enough bytes are buffered. `peekIndex` must be positioned on the
     * first byte that follows the opcode. After return, `peekIndex` will be positioned after any macro address
     * byte(s), and `macroInvocationId` will be set to the address of the macro being invoked.
     * @param valueTid the type ID of the macro invocation.
     * @param markerToSet the Marker to set with information parsed from the macro invocation. After returning `false`,
     *                    the marker's type ID will be set, startIndex will point to the first byte of the invocation's
     *                    body, and endIndex will either be -1 (when not a system symbol or prefixed invocation), or
     *                    will be set to the end of the invocation.
     * @param macroId the ID of the invocation, if known. This is only the case for opcode 0xF5 (denoted by
     *                `valueTid.variableLength == true`), which has its macro ID encoded as a FlexUInt before its
     *                length.
     * @return true if not enough data was available in the stream to complete the header; otherwise, false.
     */
     private boolean slowReadMacroInvocationHeader(IonTypeID valueTid, Marker markerToSet, long macroId) {
         if (valueTid.macroId < 0) {
             if (valueTid.lowerNibble == 0x4) {
                 // Opcode 0xF4: Read the macro ID as a FlexUInt.
                 macroInvocationId = slowReadFlexUInt_1_1();
                 if (macroInvocationId < 0) {
                     return true;
                 }
             } else if (valueTid.variableLength) {
                 // Opcode 0xF5: The macro ID was already read as a FlexUInt. Now read the length as a FlexUInt.
                 macroInvocationId = macroId;
                 long length = slowReadFlexUInt_1_1();
                 if (length < 0) {
                     return true;
                 }
                 setUserMacroInvocationMarker(valueTid, markerToSet, length);
                 return false;
             } else {
                 // Opcode 0xEF: system macro invocation or system symbol value.
                 int truncatedId = slowReadByte();
                 if (truncatedId < 0) {
                     return true;
                 }
                 // The downcast to byte then upcast to long results in sign extension, treating the byte as a FixedInt.
                 macroInvocationId = (byte) truncatedId;
                 setSystemMacroInvocationMarker(markerToSet);
                 return false;
             }
         } else if (valueTid.length > 0) {
             // Opcode 0x4: the rest of the macro ID follows in a 1-byte FixedUInt.
             // Opcode 0x5: the rest of the macro ID follows in a 2-byte FixedUInt.
             if (!fillAt(peekIndex, valueTid.length)) {
                 return true;
             }
             int remainingId = slowPeekByte();
             if (valueTid.length > 1) {
                 remainingId |= ((byte) slowPeekByte() << 8);
             }
             macroInvocationId = valueTid.macroId + remainingId;
         } else {
             // Opcodes 0x00 - 0x3F -- the opcode is the macro ID.
             macroInvocationId = valueTid.macroId;
         }
         setUserMacroInvocationMarker(valueTid, markerToSet, -1);
         setCheckpoint(CheckpointLocation.BEFORE_UNANNOTATED_TYPE_ID);
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
            valueLength = minorVersion == 0 ? slowReadVarUInt_1_0() : slowReadFlexUInt_1_1();
            if (valueLength < 0) {
                return true;
            }
        } else if (valueTid.length < 0 && minorVersion > 0) {
            // The value is itself a FlexInt or FlexUInt, so read the continuation bits to determine the length.
            valueLength = slowReadLengthOfFlexUInt_1_1(peekIndex);
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
        } else if (minorVersion == 1 && valueTid.isMacroInvocation) {
            // Note: The 0xF5 opcode is variable-length, but unlike other variable-length opcodes, it encodes the
            // macro ID, rather than the length, as the first FlexUInt following the opcode. Therefore, for opcode
            // 0xF5, `valueLength` below refers to the ID of the invocation. For the other macro invocation opcodes,
            // this value is not used.
            return slowReadMacroInvocationHeader(valueTid, markerToSet, valueLength);
        } else {
            setCheckpoint(CheckpointLocation.AFTER_SCALAR_HEADER);
            event = Event.START_SCALAR;
        }
        if (endIndex != DELIMITED_MARKER) {
            if (refillableState.isSkippingCurrentValue && valueLength > 0) {
                // Any bytes that were skipped directly from the input must still be included in the logical endIndex so
                // that the rest of the oversized value's bytes may be skipped. However, if the value's length is 0,
                // then the type ID byte must have been skipped. In this case, the skipped type ID byte is already
                // accounted for in the peekIndex.
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
        markerToSet.typeId = valueTid;
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
     * Doubles the size of the cursor's argument group stack.
     */
    private void growArgumentGroupStack() {
        ArgumentGroupMarker[] newStack = new ArgumentGroupMarker[argumentGroupStack.length * 2];
        System.arraycopy(argumentGroupStack, 0, newStack, 0, argumentGroupStack.length);
        for (int i = argumentGroupStack.length; i < newStack.length; i++) {
            newStack[i] = new ArgumentGroupMarker();
        }
        argumentGroupStack = newStack;
    }

    /**
     * Push a Marker representing the current argument group onto the stack.
     * @return the marker at the new top of the stack.
     */
    private ArgumentGroupMarker pushArgumentGroup() {
        if (++argumentGroupIndex >= argumentGroupStack.length) {
            growArgumentGroupStack();
        }
        return argumentGroupStack[argumentGroupIndex];
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
     * Steps into an e-expression, treating it as a logical container.
     */
    void stepIntoEExpression() {
        if (valueTid == null || !valueTid.isMacroInvocation) {
            throw new IonException("Must be positioned on an e-expression.");
        }
        pushContainer();
        parent.typeId = valueTid;
        // TODO support length prefixed e-expressions.
        // TODO when the length is known to be within the buffer, exit slow mode.
        parent.endIndex = DELIMITED_MARKER;
        valueTid = null;
        event = Event.NEEDS_INSTRUCTION;
        reset();
    }

    /**
     * Steps out of an e-expression, restoring the context of the parent container (if any).
     */
    void stepOutOfEExpression() {
        if (parent == null) {
            throw new IonException("Cannot step out at the top level.");
        }
        if (!parent.typeId.isMacroInvocation) {
            throw new IonException("Not positioned within an e-expression.");
        }
        // TODO support early step-out when support for lazy parsing of e-expressions is added (including continuable
        //  reading).
        if (valueMarker.endIndex > peekIndex) {
            peekIndex = valueMarker.endIndex;
        }
        setCheckpointBeforeUnannotatedTypeId();
        if (--containerIndex >= 0) {
            parent = containerStack[containerIndex];
        } else {
            parent = null;
            containerIndex = -1;
        }
        valueTid = null;
        event = Event.NEEDS_INSTRUCTION;
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
            if (uncheckedSkipRemainingDelimitedContainerElements_1_1()) {
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
     * Reports the total number of bytes skipped without buffering since the last report.
     */
    private void reportSkippedData() {
        long totalNumberOfBytesRead = getTotalOffset() + refillableState.individualBytesSkippedWithoutBuffering;
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
            if (parent.typeId.isMacroInvocation) {
                // When traversing a macro invocation, the cursor must visit each parameter; after visiting each one,
                // peekIndex will point to the first byte in the next parameter or value.
                valuePreHeaderIndex = peekIndex;
            } else if (uncheckedNextContainedToken()) {
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
        // TODO this needs to be updated ot handle the case where the value is a non-prefixed macro invocation,
        //  as the length of these invocations is unknown to the cursor. Input from the macro evaluator is needed.
        if (valueMarker.endIndex == DELIMITED_MARKER && valueTid != null && valueTid.isDelimited) {
            seekPastDelimitedContainer_1_1();
            if (event == Event.NEEDS_DATA) {
                return true;
            }
        } else if (refillableState.pinOffset > -1) {
            // Bytes in the buffer are being pinned, so buffer the remaining bytes instead of seeking past them.
            if (!fillAt(refillableState.pinOffset, valueMarker.endIndex - refillableState.pinOffset)) {
                return true;
            }
            offset = valueMarker.endIndex;
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
        if (refillableState.state == State.SEEK_DELIMITED) {
            // Discard all buffered bytes.
            slowSeek(availableAt(offset));
            refillableState.pinOffset = -1;
            refillableState.totalDiscardedBytes += refillableState.individualBytesSkippedWithoutBuffering;
            refillableState.state = State.SEEK_DELIMITED;
            peekIndex = offset;
            shiftContainerEnds(refillableState.individualBytesSkippedWithoutBuffering);
        } else if (refillableState.state != State.TERMINATED) {
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

    /**
     * Skips any bytes remaining in the previous value, positioning the cursor on the next token.
     * @return true if not enough data was available in the stream to skip the previous value; otherwise, false.
     */
    private boolean slowSkipToNextToken() {
        if ((refillableState.state != State.READY && !slowMakeBufferReady())) {
            return true;
        }
        if (checkpointLocation == CheckpointLocation.AFTER_SCALAR_HEADER || checkpointLocation == CheckpointLocation.AFTER_CONTAINER_HEADER) {
            return slowSkipRemainingValueBytes();
        }
        return false;
    }

    /**
     * Reads the length and type of the FlexSym that starts at the given position, ensuring enough bytes are available
     * in the stream. After this method returns with a value greater than or equal to zero, `valueTid` and
     * `valueMarker.typeId` will be set to the IonTypeID that most closely corresponds to the length and type of the
     * FlexSym.
     * @return the length of the FlexSym, or -1 if not enough bytes are available in the stream to determine the length.
     */
    private long readFlexSymLengthAndType_1_1() {
        FlexSymType flexSymType;
        if (isSlowMode) {
            flexSymType = slowSkipFlexSym_1_1(valueMarker);
            if (flexSymType == FlexSymType.INCOMPLETE) {
                return -1;
            }
        } else {
            flexSymType = uncheckedSkipFlexSym_1_1(valueMarker);
        }
        int lengthOfFlexSym = (int) (peekIndex - valueMarker.startIndex);
        peekIndex = valueMarker.startIndex;
        valueTid = flexSymType.typeIdFor(lengthOfFlexSym);
        valueMarker.typeId = valueTid;
        return lengthOfFlexSym;
    }

    /**
     * Calculates the length and type of variable-length primitive value, ensuring enough bytes are available in the
     * stream.
     * @param taglessEncoding the variable-length primitive type of the tagless value that starts at `peekIndex`.
     * @return the length of the value, or -1 if not enough bytes are available in the stream to determine the length.
     */
    private long calculateTaglessLengthAndType(TaglessEncoding taglessEncoding) {
        // TODO length calculation for these types could be deferred until they are consumed to avoid duplicate
        //  work. This would trade some added complexity for a potential performance gain that would need to be
        //  quantified.
        long length;
        switch (taglessEncoding) {
            case FLEX_UINT:
            case FLEX_INT:
                length = isSlowMode ? slowReadLengthOfFlexUInt_1_1(peekIndex) : uncheckedReadLengthOfFlexUInt_1_1(peekIndex);
                break;
            case FLEX_SYM:
                length = readFlexSymLengthAndType_1_1();
                break;
            default:
                throw new IllegalStateException("Length is built into the primitive type's IonTypeID.");
        }
        if (valueTid == SYSTEM_SYMBOL_VALUE) {
            return 1;
        }
        if (length >= 0) {
            valueMarker.endIndex = peekIndex + length;
        }
        return length;
    }

    /**
     * Skips any bytes remaining in the current token, positioning the cursor on the first byte of the next token.
     * @return true if not enough data was available in the stream to skip the previous value; otherwise, false.
     */
    private boolean skipToNextToken() {
        event = Event.NEEDS_DATA;
        if (isSlowMode) {
            if (slowSkipToNextToken()) {
                return true;
            }
        } else {
            if (peekIndex < valueMarker.endIndex) {
                peekIndex = valueMarker.endIndex;
            } else if (valueTid != null && valueTid.isDelimited) {
                seekPastDelimitedContainer_1_1();
            }
        }
        if (dataHandler != null) {
            reportConsumedData();
        }
        reset();
        return false;
    }

    /**
     * Advances the cursor to the next value, assuming that it is tagless with the given type, skipping the current
     * value (if any). This method may return:
     * <ul>
     *     <li>NEEDS_DATA, if not enough data is available in the stream</li>
     *     <li>START_SCALAR, if the reader is now positioned on a scalar value</li>
     * </ul>
     * @param taglessEncoding the {@link TaglessEncoding} of the tagless value on which to position the cursor.
     * @return an Event conveying the result of the operation.
     */
    public Event nextTaglessValue(TaglessEncoding taglessEncoding) {
        event = Event.NEEDS_DATA;
        if (isSlowMode) {
            if (slowSkipToNextToken()) {
                return event;
            }
        } else {
            if (peekIndex < valueMarker.endIndex) {
                peekIndex = valueMarker.endIndex;
            } else if (valueTid != null && valueTid.isDelimited) {
                seekPastDelimitedContainer_1_1();
            }
        }
        if (dataHandler != null) {
            reportConsumedData();
        }
        reset();
        taglessType = taglessEncoding;
        valueTid = taglessEncoding.typeID;
        valueMarker.typeId = valueTid;
        valueMarker.startIndex = peekIndex;
        valuePreHeaderIndex = peekIndex;
        if (valueTid.variableLength) {
            if (calculateTaglessLengthAndType(taglessEncoding) < 0) {
                return event;
            }
        } else {
            valueMarker.endIndex = peekIndex + valueTid.length;
        }
        setCheckpoint(CheckpointLocation.AFTER_SCALAR_HEADER);
        event = Event.START_SCALAR;
        return event;
    }

    /**
     * Fills the argument encoding bitmap (AEB) of the given byte width that is expected to occur at
     * the cursor's current `peekIndex`. This method may return:
     * <ul>
     *     <li>NEEDS_DATA, if not enough data is available in the stream</li>
     *     <li>NEEDS_INSTRUCTION, if the AEB was filled and the cursor is now positioned on the first byte of the
     *     macro invocation.</li>
     * </ul>
     * After return, `valueMarker` is set with the start and end indices of the AEB.
     * @param numberOfBytes the byte width of the AEB.
     * @return an Event conveying the result of the operation.
     */
    public Event fillArgumentEncodingBitmap(int numberOfBytes) {
        event = Event.NEEDS_DATA;
        valueMarker.typeId = null;
        valueMarker.startIndex = peekIndex;
        valueMarker.endIndex = peekIndex + numberOfBytes;
        if (isSlowMode && !fillAt(peekIndex, numberOfBytes)) {
            return event;
        }
        peekIndex = valueMarker.endIndex;
        setCheckpoint(CheckpointLocation.BEFORE_UNANNOTATED_TYPE_ID);
        event = Event.NEEDS_INSTRUCTION;
        return event;
    }

    /**
     * Reads the group continuation FlexUInt on which the cursor is currently positioned.
     * @return the value of the continuation, or -1 if the end of the stream was reached.
     */
    private long readGroupContinuation() {
        long groupContinuation;
        if (isSlowMode) {
            groupContinuation = slowReadFlexUInt_1_1();
            if (groupContinuation < 0) {
                return -1;
            }
            setCheckpoint(CheckpointLocation.BEFORE_UNANNOTATED_TYPE_ID);
        } else {
            groupContinuation = uncheckedReadFlexUInt_1_1();
        }
        return groupContinuation;
    }

    /**
     * Positions the cursor after the previous token, then enters the tagged argument group that occurs at that
     * position. It is up to the caller to ensure that a group actually exists at that location. This method may return:
     * <ul>
     *     <li>NEEDS_DATA, if not enough data is available in the stream to complete the operation.</li>
     *     <li>NEEDS_INSTRUCTION, if the cursor successfully entered the argument group. Subsequently, the user must
     *     invoke {@link #nextGroupedValue()} to position it on the next value.</li>
     * </ul>
     * @return an Event conveying the result of the operation.
     */
    public Event enterTaggedArgumentGroup() {
        if (skipToNextToken()) {
            return event;
        }
        long groupContinuation = readGroupContinuation();
        if (groupContinuation < 0) {
            return event;
        }
        ArgumentGroupMarker group = pushArgumentGroup();
        group.pageStartIndex = peekIndex;
        if (groupContinuation == 0) {
            // Delimited argument group.
            group.pageEndIndex = -1;
        } else {
            group.pageEndIndex = peekIndex + groupContinuation;
        }
        group.taglessEncoding = null;
        valueMarker.endIndex = peekIndex;
        event = Event.NEEDS_INSTRUCTION;
        return event;
    }

    /**
     * Positions the cursor after the previous token, then enters the tagless argument group that occurs at that
     * position. It is up to the caller to ensure that a group actually exists at that location. This method may return:
     * <ul>
     *     <li>NEEDS_DATA, if not enough data is available in the stream to complete the operation.</li>
     *     <li>NEEDS_INSTRUCTION, if the cursor successfully entered the argument group. Subsequently, the user must
     *     invoke {@link #nextGroupedValue()} to position it on the next value.</li>
     * </ul>
     * @param taglessEncoding the primitive type of the values in the group.
     * @return an Event conveying the result of the operation.
     */
    public Event enterTaglessArgumentGroup(TaglessEncoding taglessEncoding) {
        if (skipToNextToken()) {
            return event;
        }
        long indexBeforeFirstContinuation = peekIndex;
        long groupContinuation = readGroupContinuation();
        if (groupContinuation < 0) {
            return event;
        }
        if (groupContinuation == 0) {
            // This is an empty group. Rather than storing extra state to track this rare special case, simply
            // rewind and cause the continuation to be read again during nextGroupedValue().
            peekIndex = indexBeforeFirstContinuation;
        }
        ArgumentGroupMarker group = pushArgumentGroup();;
        group.pageStartIndex = peekIndex;
        group.pageEndIndex = peekIndex + groupContinuation;
        group.taglessEncoding = taglessEncoding;
        valueMarker.endIndex = peekIndex;
        event = Event.NEEDS_INSTRUCTION;
        return event;
    }

    /**
     * Attempts to fill the current page of the current argument group. This should only be called when it has been
     * determined that the page is not already buffered in its entirety.
     * @param group the group containing the page to fill.
     * @return true if not enough data was available to fill the page; otherwise, false.
     * @throws IonException if the cursor is not in 'slow' mode, indicating unexpected EOF.
     */
    private boolean fillArgumentGroupPage(ArgumentGroupMarker group) {
        if (isSlowMode) {
            // Fill the entire page.
            if (!fillAt(group.pageStartIndex, group.pageEndIndex - group.pageStartIndex)) {
                event = Event.NEEDS_DATA;
                return true;
            }
            // TODO performance: exit slow mode until the page is finished.
        } else {
            throw new IonException("Unexpected EOF: argument group extended beyond the end of the buffer.");
        }
        return false;
    }

    /**
     * Sets the checkpoint based on whether a scalar or container header has just been read. It is up to the caller
     * to ensure that the cursor is positioned immediately after a value header.
     */
    private void setCheckpointAfterValueHeader() {
        switch (event) {
            case START_SCALAR:
                setCheckpoint(CheckpointLocation.AFTER_SCALAR_HEADER);
                break;
            case START_CONTAINER:
                setCheckpoint(CheckpointLocation.AFTER_CONTAINER_HEADER);
                break;
            case NEEDS_INSTRUCTION:
                // A macro invocation header has just been read.
                setCheckpoint(CheckpointLocation.BEFORE_UNANNOTATED_TYPE_ID);
                break;
            default:
                throw new IllegalStateException();
        }
    }

    /**
     * Positions the cursor on the next value in the tagged group. Upon return, the value will be filled and
     * `valueMarker` set to the value's start and end indices.
     * @param group the group to which the value belongs.
     * @return an Event conveying the result of the operation.
     */
    private Event nextGroupedTaggedValue(ArgumentGroupMarker group) {
        boolean isUserValue; // if false, the header represents no-op padding
        if (group.pageEndIndex < 0) {
            // Delimited.
            int b;
            if (isSlowMode) {
                b = slowReadByte();
                if (b < 0) {
                    event = Event.NEEDS_DATA;
                    return event;
                }
                if (b == (OpCodes.DELIMITED_END_MARKER & SINGLE_BYTE_MASK)) {
                    group.pageEndIndex = peekIndex;
                    setCheckpoint(CheckpointLocation.BEFORE_UNANNOTATED_TYPE_ID);
                    event = Event.NEEDS_INSTRUCTION;
                    return event;
                }
                isUserValue = slowReadHeader(b, false, valueMarker);
            } else {
                b = buffer[(int)(peekIndex++)] & SINGLE_BYTE_MASK;
                if (b == (OpCodes.DELIMITED_END_MARKER & SINGLE_BYTE_MASK)) {
                    group.pageEndIndex = peekIndex;
                    event = Event.NEEDS_INSTRUCTION;
                    return event;
                }
                isUserValue = uncheckedReadHeader(b, false, valueMarker);
                setCheckpointAfterValueHeader();
            }
        } else {
            if (peekIndex == group.pageEndIndex) {
                // End of the group
                event = Event.NEEDS_INSTRUCTION;
                return event;
            }
            if (group.pageEndIndex > limit && fillArgumentGroupPage(group)) {
                return event;
            }
            isUserValue = uncheckedReadHeader(buffer[(int)(peekIndex++)] & SINGLE_BYTE_MASK, false, valueMarker);
            setCheckpointAfterValueHeader();
        }
        valueTid = valueMarker.typeId;
        if (!isUserValue) {
            throw new IonException("No-op padding is not currently supported in argument groups.");
        }
        return event;
    }

    /**
     * Positions the cursor on the next value in the tagless group. Upon return, the value will be filled and
     * `valueMarker` set to the value's start and end indices.
     * @param group the group to which the value belongs.
     * @return an Event conveying the result of the operation.
     */
    private Event nextGroupedTaglessValue(ArgumentGroupMarker group) {
        if (peekIndex == group.pageEndIndex) {
            // End of the page.
            long continuation = readGroupContinuation();
            if (continuation == 0) {
                // End of the group
                event = Event.NEEDS_INSTRUCTION;
                return event;
            }
            group.pageEndIndex = peekIndex + continuation;
        }
        if (group.pageEndIndex > limit && fillArgumentGroupPage(group)) {
            return event;
        }
        // TODO performance: for fixed-width tagless types, the following could be skipped after the first value.
        nextTaglessValue(group.taglessEncoding);
        return event;
    }

    /**
     * Positions the cursor on the next value in the group. Upon return, the value will be filled and `valueMarker` set
     * to the value's start and end indices. This method may return:
     * <ul>
     *     <li>NEEDS_DATA, if not enough data is available in the stream</li>
     *     <li>START_SCALAR, if the reader is now positioned on a scalar value</li>
     *     <li>START_CONTAINER, if the reader is now positioned on a container value</li>
     *     <li>NEEDS_INSTRUCTION, if the cursor reached the end of the argument group. Subsequently, the caller must
     *     call {@link #exitArgumentGroup()}.</li>
     * </ul>
     * @return an Event conveying the result of the operation.
     */
    public Event nextGroupedValue() {
        ArgumentGroupMarker group = argumentGroupStack[argumentGroupIndex];
        if (peekIndex < valueMarker.endIndex) {
            peekIndex = valueMarker.endIndex;
        }
        if (group.taglessEncoding == null) {
            return nextGroupedTaggedValue(group);
        }
        return nextGroupedTaglessValue(group);
    }

    /**
     * Seeks the cursor to the end of the current page of the argument group.
     * @param group the group in which to seek.
     * @return true if there was not enough data to complete the seek; otherwise, false.
     */
    private boolean seekToEndOfArgumentGroupPage(ArgumentGroupMarker group) {
        if (isSlowMode) {
            if (slowSeek(group.pageEndIndex - offset)) {
                return true;
            }
            peekIndex = offset;
        } else {
            peekIndex = group.pageEndIndex;
        }
        return false;
    }

    // Dummy delimited container to be used when seeking forward to a delimited end marker of a synthetic container,
    // like an argument group.
    private static final IonTypeID DUMMY_DELIMITED_CONTAINER = TYPE_IDS_1_1[OpCodes.DELIMITED_SEXP & SINGLE_BYTE_MASK];

    /**
     * Seeks to the end of the current delimited argument group.
     * @return true if not enough data was available to complete the seek; otherwise, false.
     */
    private boolean seekToEndOfDelimitedArgumentGroup() {
        // Push a dummy delimited container onto the stack, preparing the cursor to seek forward to the delimited end
        // marker applicable at the current depth.
        pushContainer();
        parent.endIndex = -1;
        parent.typeId = DUMMY_DELIMITED_CONTAINER;
        boolean isEof;
        if (isSlowMode) {
            isEof = slowSkipRemainingDelimitedContainerElements_1_1();
        } else {
            isEof = uncheckedSkipRemainingDelimitedContainerElements_1_1();
        }
        // Pop the dummy delimited container from the stack.
        if (--containerIndex >= 0) {
            parent = containerStack[containerIndex];
        } else {
            parent = null;
            containerIndex = -1;
        }
        return isEof;
    }

    /**
     * Exits the cursor's current tagged argument group.
     * @param group the group to exit.
     * @return an Event conveying the result of the operation (either NEEDS_DATA or NEEDS_INSTRUCTION).
     */
    private Event exitTaggedArgumentGroup(ArgumentGroupMarker group) {
        if (group.pageEndIndex < 0) {
            if (seekToEndOfDelimitedArgumentGroup()) {
                return event;
            }
        } else if (seekToEndOfArgumentGroupPage(group)) {
            return event;
        }
        event = Event.NEEDS_INSTRUCTION;
        return event;
    }

    /**
     * Exits the cursor's current tagless argument group.
     * @param group the group to exit.
     * @return an Event conveying the result of the operation (either NEEDS_DATA or NEEDS_INSTRUCTION).
     */
    private Event exitTaglessArgumentGroup(ArgumentGroupMarker group) {
        long continuation = -1;
        while (continuation != 0) {
            if (seekToEndOfArgumentGroupPage(group)) {
                return event;
            }
            continuation = readGroupContinuation();
            if (continuation < 0) {
                return event;
            }
            group.pageEndIndex = peekIndex + continuation;
        }
        event = Event.NEEDS_INSTRUCTION;
        return event;
    }

    /**
     * Exits the cursor's current argument group. This method may return:
     * <ul>
     *     <li>NEEDS_DATA, if not enough data is available in the stream to exit the group.</li>
     *     <li>NEEDS_INSTRUCTION, if the cursor successfully exited the argument group. Subsequently, the user must
     *     invoke a method on the cursor to position it on the next value.</li>
     * </ul>
     * @return an Event conveying the result of the operation.
     */
    public Event exitArgumentGroup() {
        ArgumentGroupMarker group = argumentGroupStack[argumentGroupIndex];
        if (group.pageEndIndex >= 0 && peekIndex >= group.pageEndIndex) {
            event = Event.NEEDS_INSTRUCTION;
            return event;
        }
        event = Event.NEEDS_DATA;
        if (group.taglessEncoding == null) {
            return exitTaggedArgumentGroup(group);
        }
        return exitTaglessArgumentGroup(group);
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
            if (!slowFindDelimitedEnd_1_1()) {
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

    long getMacroInvocationId() {
        return macroInvocationId;
    }

    boolean isSystemInvocation() {
        return isSystemInvocation;
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
        refillableState = TERMINATED_STATE;
        // Use a unified code path for all cursors after termination. This path forces a termination check before
        // accessing the input stream or buffer.
        isSlowMode = true;
    }

    @Override
    public void close() {
        if (refillableState != null && refillableState.inputStream != null) {
            try {
                refillableState.inputStream.close();
            } catch (IOException e) {
                throwAsIonException(e);
            }
        }
        buffer = null;
        containerStack = null;
        argumentGroupStack = null;
        byteBuffer = null;
        terminate();
    }

    /* ---- End: version-agnostic parsing, utility, and public API methods ---- */
}
