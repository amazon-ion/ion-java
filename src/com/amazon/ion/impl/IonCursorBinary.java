// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ion.impl;

import com.amazon.ion.BufferConfiguration;
import com.amazon.ion.IonBufferConfiguration;
import com.amazon.ion.IonException;
import com.amazon.ion.IonCursor;
import com.amazon.ion.IonType;
import com.amazon.ion.IvmNotificationConsumer;

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
    private static final int VALUE_BITS_PER_VARUINT_BYTE = 7;
    private static final int IVM_START_BYTE = 0xE0;
    private static final int IVM_FINAL_BYTE = 0xEA;
    private static final int SINGLE_BYTE_MASK = 0xFF;
    private static final int LIST_TYPE_ORDINAL = IonType.LIST.ordinal();
    private static final IvmNotificationConsumer NO_OP_IVM_NOTIFICATION_CONSUMER = (x, y) -> {};

    // Initial capacity of the stack used to hold ContainerInfo. Each additional level of nesting in the data requires
    // a new ContainerInfo. Depths greater than 8 will be rare.
    private static final int CONTAINER_STACK_INITIAL_CAPACITY = 8;

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
         * The current size of the internal buffer.
         */
        private long capacity;

        /**
         * The total number of bytes that have been discarded (shifted out of the buffer or skipped directly from
         * the input stream).
         */
        protected long totalDiscardedBytes = 0;

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
        private final InputStream inputStream;

        /**
         * Handler invoked when a single value would exceed `maximumBufferSize`.
         */
        private BufferConfiguration.OversizedValueHandler oversizedValueHandler;

        /**
         * Indicates whether the current value is being skipped due to being oversized.
         */
        private boolean isSkippingCurrentValue = false;

        /**
         * The number of bytes of an oversized value skipped during single-byte read operations.
         */
        private int individualBytesSkippedWithoutBuffering = 0;

        RefillableState(InputStream inputStream, int capacity, int maximumBufferSize) {
            this.inputStream = inputStream;
            this.capacity = capacity;
            this.maximumBufferSize = maximumBufferSize;
        }

    }

    /**
     * Stack to hold container info. Stepping into a container results in a push; stepping out results in a pop.
     */
    protected Marker[] containerStack = new Marker[CONTAINER_STACK_INITIAL_CAPACITY];

    /**
     * The index of the current container in `containerStack`.
     */
    protected int containerIndex = -1;

    /**
     * The Marker representing the parent container of the current value.
     */
    protected Marker parent = null;

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
    private long limit;

    /**
     * A slice of the current buffer.
     */
    protected ByteBuffer byteBuffer;

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
    protected final Marker valueMarker = new Marker(-1, 0);

    /**
     * The index of the first byte in the header of the value at which the reader is currently positioned.
     */
    protected long valuePreHeaderIndex = 0;

    /**
     * Type ID for the current value.
     */
    protected IonTypeID valueTid = null;

    /**
     * The consumer to be notified when Ion version markers are encountered.
     */
    private IvmNotificationConsumer ivmConsumer = NO_OP_IVM_NOTIFICATION_CONSUMER;

    /**
     * The event that occurred as a result of the last call to any of the cursor's IonCursor methods.
     */
    protected IonCursor.Event event = IonCursor.Event.NEEDS_DATA;

    /**
     * The buffer in which the cursor stores slices of the Ion stream.
     */
    protected byte[] buffer;

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
    protected int fieldSid = -1;

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
     * The total number of bytes that had been consumed from the stream as of the last time progress was reported to
     * the data handler.
     */
    private long lastReportedByteTotal = 0;


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
        this.dataHandler = (configuration == null) ? null : configuration.getDataHandler();
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
        refillableState = null;
    }

    /**
     * The standard {@link IonBufferConfiguration}. This will be used unless the user chooses custom settings.
     */
    private static final IonBufferConfiguration STANDARD_BUFFER_CONFIGURATION =
        IonBufferConfiguration.Builder.standard().build();

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
        return (int) Math.pow(2, logBase2(value));
    }

    /**
     * Cache of configurations for fixed-sized streams. FIXED_SIZE_CONFIGURATIONS[i] returns a configuration with
     * buffer size max(8, 2^i). Retrieve a configuration large enough for a given size using
     * FIXED_SIZE_CONFIGURATIONS(logBase2(size)). Only supports sizes less than or equal to
     * STANDARD_BUFFER_CONFIGURATION.getInitialBufferSize().
     */
    private static final IonBufferConfiguration[] FIXED_SIZE_CONFIGURATIONS;

    static {
        int maxBufferSizeExponent = logBase2(STANDARD_BUFFER_CONFIGURATION.getInitialBufferSize());
        FIXED_SIZE_CONFIGURATIONS = new IonBufferConfiguration[maxBufferSizeExponent + 1];
        for (int i = 0; i <= maxBufferSizeExponent; i++) {
            // Create a buffer configuration for buffers of size 2^i. The minimum size is 8: the smallest power of two
            // larger than the minimum buffer size allowed.
            int size = Math.max(8, (int) Math.pow(2, i));
            FIXED_SIZE_CONFIGURATIONS[i] = IonBufferConfiguration.Builder.from(STANDARD_BUFFER_CONFIGURATION)
                .withInitialBufferSize(size)
                .withMaximumBufferSize(size)
                .build();
        }
    }

    /**
     * Validates the given configuration.
     * @param configuration the configuration to validate.
     * @return the validated configuration.
     */
    private static IonBufferConfiguration validate(IonBufferConfiguration configuration) {
        if (configuration.getInitialBufferSize() < 1) {
            throw new IllegalArgumentException("Initial buffer size must be at least 1.");
        }
        if (configuration.getMaximumBufferSize() < configuration.getInitialBufferSize()) {
            throw new IllegalArgumentException("Maximum buffer size cannot be less than the initial buffer size.");
        }
        return configuration;
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
        this.dataHandler = (configuration == null) ? null : configuration.getDataHandler();
        if (configuration == null) {
            if (inputStream instanceof ByteArrayInputStream) {
                // ByteArrayInputStreams are fixed-size streams. Clamp the reader's internal buffer size at the size of
                // the stream to avoid wastefully allocating extra space that will never be needed. It is still
                // preferable for the user to manually specify the buffer size if it's less than the default, as doing
                // so allows this branch to be skipped.
                int fixedBufferSize;
                try {
                    fixedBufferSize = inputStream.available();
                } catch (IOException e) {
                    // ByteArrayInputStream.available() does not throw.
                    throw new IllegalStateException(e);
                }
                if (alreadyReadLen > 0) {
                    fixedBufferSize += alreadyReadLen;
                }
                if (STANDARD_BUFFER_CONFIGURATION.getInitialBufferSize() > fixedBufferSize) {
                    configuration = FIXED_SIZE_CONFIGURATIONS[logBase2(fixedBufferSize)];
                } else {
                    configuration = STANDARD_BUFFER_CONFIGURATION;
                }
            } else {
                configuration = STANDARD_BUFFER_CONFIGURATION;
            }
        }
        validate(configuration);
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
        refillableState = new RefillableState(
            inputStream,
            configuration.getInitialBufferSize(),
            configuration.getMaximumBufferSize()
        );
    }

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
                refill(freeSpaceAt(limit));
                shortfall = refillableState.bytesRequested - availableAt(offset);
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
     * Fills the buffer with up to the requested number of additional bytes. It is the caller's responsibility to
     * ensure that there is space in the buffer.
     * @param numberOfBytesToFill the number of additional bytes to attempt to add to the buffer.
     */
    private void refill(long numberOfBytesToFill) {
        int numberOfBytesFilled = -1;
        try {
            numberOfBytesFilled = refillableState.inputStream.read(buffer, (int) limit, (int) numberOfBytesToFill);
        } catch (IOException e) {
            throwAsIonException(e);
        }
        if (numberOfBytesFilled < 0) {
            return;
        }
        limit += numberOfBytesFilled;
    }

    /**
     * Seeks forward in the stream up to the requested number of bytes, from `offset`.
     * @param numberOfBytes the number of bytes to seek from `offset`.
     * @return true if the seek is complete; otherwise, false.
     */
    private boolean slowSeek(long numberOfBytes) {
        long size = availableAt(offset);
        long unbufferedBytesToSkip = numberOfBytes - size;
        if (unbufferedBytesToSkip <= 0) {
            offset += numberOfBytes;
            refillableState.bytesRequested = 0;
            refillableState.state = State.READY;
            return true;
        }
        offset = limit;
        long skipped = 0;
        try {
            skipped = refillableState.inputStream.skip(unbufferedBytesToSkip);
        } catch (EOFException e) {
            // Certain InputStream implementations (e.g. GZIPInputStream) throw EOFException if more bytes are requested
            // to skip than are currently available (e.g. if a header or trailer is incomplete).
        } catch (IOException e) {
            throwAsIonException(e);
        }
        refillableState.totalDiscardedBytes += skipped;
        long shortfall = numberOfBytes - (skipped + size);
        if (shortfall <= 0) {
            refillableState.bytesRequested = 0;
            refillableState.state = State.READY;
            return true;
        }
        refillableState.bytesRequested = shortfall;
        refillableState.state = State.SEEK;
        return false;
    }

    /* ---- End: internal buffer manipulation methods ---- */

    /* ---- Begin: version-dependent parsing methods ---- */
    /* ---- Ion 1.0 ---- */

    /**
     * Reads a 2+ byte VarUInt, given the first byte. NOTE: the VarUInt must fit in a `long`. This must only be
     * called when it is known that the buffer already contains all the bytes in the VarUInt.
     * @return the value.
     */
    private long readVarUInt_1_0(byte currentByte) {
        long result = currentByte & LOWER_SEVEN_BITS_BITMASK;
        do {
            // Note: if the varUInt  is malformed such that it extends beyond the declared length of the value *and*
            // beyond the end of the buffer, this will result in IndexOutOfBoundsException because only the declared
            // value length has been filled. Preventing this is simple: if (peekIndex >= limit) throw
            // new IonException(); However, we choose not to perform that check here because it is not worth sacrificing
            // performance in this inner-loop code in order to throw one type of exception over another in case of
            // malformed data.
            currentByte = buffer[(int) (peekIndex++)];
            result = (result << VALUE_BITS_PER_VARUINT_BYTE) | (currentByte & LOWER_SEVEN_BITS_BITMASK);
        } while (currentByte >= 0);
        return result;
    }

    /**
     * Reads the header of an annotation wrapper. This must only be called when it is known that the buffer already
     * contains all the bytes in the header. Sets `valueMarker` with the start and end indices of the wrapped value.
     * Sets `annotationSequenceMarker` with the start and end indices of the sequence of annotation SIDs. After
     * successful return, `peekIndex` will point at the type ID byte of the wrapped value.
     * @param valueTid the type ID of the annotation wrapper.
     * @return true if the length of the wrapped value extends beyond the bytes currently buffered; otherwise, false.
     */
    private boolean readAnnotationWrapperHeader_1_0(IonTypeID valueTid) {
        long endIndex;
        if (valueTid.variableLength) {
            byte b = buffer[(int) peekIndex++];
            if (b < 0) {
                endIndex = (b & LOWER_SEVEN_BITS_BITMASK);
            } else {
                endIndex = readVarUInt_1_0(b);
            }
        } else {
            endIndex = valueTid.length;
        }
        endIndex += peekIndex;
        setMarker(endIndex, valueMarker);
        if (endIndex > limit) {
            return true;
        }
        byte b = buffer[(int) peekIndex++];
        int annotationsLength;
        if (b < 0) {
            annotationsLength = (b & LOWER_SEVEN_BITS_BITMASK);
        } else {
            annotationsLength = (int) readVarUInt_1_0(b);
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
            byte b = buffer[(int) peekIndex++];
            if (b < 0) {
                endIndex = (b & LOWER_SEVEN_BITS_BITMASK);
            } else {
                endIndex = readVarUInt_1_0(b);
            }
        } else {
            endIndex = valueTid.length;
        }
        endIndex += peekIndex;
        if (valueTid.type != null && valueTid.type.ordinal() >= LIST_TYPE_ORDINAL) {
            event = Event.START_CONTAINER;
        } else if (valueTid.isNopPad) {
            seekPastNopPad(endIndex, isAnnotated);
        } else {
            event = Event.START_SCALAR;
        }
        return endIndex;
    }

    /* ---- Ion 1.1 ---- */

    private long readVarUInt_1_1() {
        throw new UnsupportedOperationException();
    }

    private boolean readAnnotationWrapperHeader_1_1(IonTypeID valueTid) {
        throw new UnsupportedOperationException();
    }

    private long calculateEndIndex_1_1(IonTypeID valueTid, boolean isAnnotated) {
        throw new UnsupportedOperationException();
    }

    private void readFieldName_1_1() {
        throw new UnsupportedOperationException();
    }

    private boolean isDelimitedEnd_1_1() {
        throw new UnsupportedOperationException();
    }

    boolean skipRemainingDelimitedContainerElements_1_1() {
        throw new UnsupportedOperationException();
    }

    private void seekPastDelimitedContainer_1_1() {
        throw new UnsupportedOperationException();
    }

    /* ---- End: version-dependent parsing methods ---- */

    /* ---- Begin: version-agnostic parsing, utility, and public API methods ---- */

    /**
     * Sets `checkpoint` to the current `peekIndex`, which must be before an unannotated type ID, and seeks the
     * buffer to that point.
     */
    private void setCheckpointBeforeUnannotatedTypeId() {
        reset();
        offset = peekIndex;
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
        if (parent != null && endIndex > parent.endIndex && parent.endIndex > -1) {
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
        if (parent.endIndex == -1) {
            return isDelimitedEnd_1_1();
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
    private void seekPastNopPad(long endIndex, boolean isAnnotated) {
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
    private boolean readHeader(final int typeIdByte, final boolean isAnnotated, final Marker markerToSet) {
        IonTypeID valueTid = typeIds[typeIdByte];
        if (!valueTid.isValid) {
            throw new IonException("Invalid type ID.");
        } else if (valueTid.type == IonTypeID.ION_TYPE_ANNOTATION_WRAPPER) {
            if (isAnnotated) {
                throw new IonException("Nested annotation wrappers are invalid.");
            }
            if (minorVersion == 0 ? readAnnotationWrapperHeader_1_0(valueTid) : readAnnotationWrapperHeader_1_1(valueTid)) {
                return true;
            }
            hasAnnotations = true;
            return readHeader(buffer[(int)(peekIndex++)] & SINGLE_BYTE_MASK, true, valueMarker);
        } else {
            long endIndex = minorVersion == 0
                ? calculateEndIndex_1_0(valueTid, isAnnotated)
                : calculateEndIndex_1_1(valueTid, isAnnotated);
            if (isAnnotated) {
                validateAnnotationWrapperEndIndex(endIndex);
            }
            setMarker(endIndex, markerToSet);
            if (endIndex > limit) {
                event = Event.NEEDS_DATA;
                return true;
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
     * Doubles the size of the cursor's container stack.
     */
    private void growContainerStack() {
        Marker[] newStack = new Marker[containerStack.length * 2];
        System.arraycopy(containerStack, 0, newStack, 0, containerStack.length);
        for (int i = containerStack.length; i < newStack.length; i++) {
            newStack[i] = new Marker(-1 ,-1);
        }
        containerStack = newStack;
    }

    /**
     * Push a Marker representing the current container onto the stack.
     */
    private void pushContainer() {
        containerIndex++;
        if (containerIndex >= containerStack.length) {
            growContainerStack();
        }
        parent = containerStack[containerIndex];
    }

    @Override
    public Event stepIntoContainer() {
        if (valueTid == null || valueTid.type.ordinal() < LIST_TYPE_ORDINAL) {
            throw new IonException("Must be positioned on a container to step in.");
        }
        // Push the remaining length onto the stack, seek past the container's header, and increase the depth.
        pushContainer();
        parent.typeId = valueTid;
        parent.endIndex = valueTid.isDelimited ? -1 : valueMarker.endIndex;
        valueTid = null;
        event = Event.NEEDS_INSTRUCTION;
        reset();
        return event;
    }

    @Override
    public Event stepOutOfContainer() {
        if (parent == null) {
            // Note: this is IllegalStateException for consistency with the other binary IonReader implementation.
            throw new IllegalStateException("Cannot step out at top level.");
        }
        // Seek past the remaining bytes at this depth and pop from the stack.
        if (parent.endIndex == -1) {
            if (skipRemainingDelimitedContainerElements_1_1()) {
                return event;
            }
        } else {
            peekIndex = parent.endIndex;
        }
        setCheckpointBeforeUnannotatedTypeId();
        containerIndex--;
        if (containerIndex >= 0) {
            parent = containerStack[containerIndex];
        } else {
            parent = null;
            containerIndex = -1;
        }

        valueTid = null;
        event = Event.NEEDS_INSTRUCTION;
        return event;
    }

    /**
     * Advances to the next value within a container, checking for container end and consuming the field name, if any.
     * @return true if the end of the container has been reached; otherwise, false.
     */
    private boolean nextContainedToken() {
        if (parent.endIndex == -1) {
            return isDelimitedEnd_1_1();
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
                    fieldSid = (int) readVarUInt_1_0(b);
                }
            } else {
                readFieldName_1_1();
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
     * @return false if the next token was an Ion version marker or NOP pad; otherwise, true. If false, this method
     *  should be called again to advance to the following value.
     */
    private boolean nextToken() {
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
            return true;
        }
        reset();
        int b;
        if (parent == null) { // Depth 0
            valuePreHeaderIndex = peekIndex;
            b = buffer[(int)(peekIndex++)] & SINGLE_BYTE_MASK;
            if (b == IVM_START_BYTE) {
                readIvm();
                return false;
            }
        } else {
            if (nextContainedToken()) {
                return true;
            }
            b = buffer[(int)(peekIndex++)] & SINGLE_BYTE_MASK;
        }
        if (readHeader(b, false, valueMarker)) {
            valueTid = valueMarker.typeId;
            return true;
        }
        valueTid = valueMarker.typeId;
        return false;
    }

    @Override
    public Event nextValue() {
        event = Event.NEEDS_DATA;
        while (true) {
            if (nextToken()) {
                break;
            }
        }
        return event;
    }

    @Override
    public Event fillValue() {
        event = Event.VALUE_READY;
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
     * Slices the buffer using the given offset and limit. Slices are treated as if they were at the top level.
     * @param offset the offset at which the slice will begin.
     * @param limit the slice's limit.
     */
    protected void slice(long offset, long limit) {
        peekIndex = offset;
        this.limit = limit;
        setCheckpointBeforeUnannotatedTypeId();
        valueMarker.endIndex = -1;
        event = Event.NEEDS_DATA;
        valueTid = null;
        containerIndex = -1; // Slices are treated as if they were at the top level.
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
        if (isAwaitingMoreData()) {
            throw new IonException("Unexpected EOF.");
        }
        return Event.NEEDS_DATA;
    }

    /**
     * @return true if the cursor is expecting more data in order to complete a token; otherwise, false.
     */
    private boolean isAwaitingMoreData() {
        return valueMarker.endIndex > limit;
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
    }

    /* ---- End: version-agnostic parsing, utility, and public API methods ---- */
}
