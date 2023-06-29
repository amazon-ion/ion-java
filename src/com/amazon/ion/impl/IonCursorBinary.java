// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ion.impl;

import com.amazon.ion.BufferConfiguration;
import com.amazon.ion.IonBufferConfiguration;
import com.amazon.ion.IonException;
import com.amazon.ion.IonCursor;
import com.amazon.ion.IonType;
import com.amazon.ion.IvmNotificationConsumer;

import java.nio.ByteBuffer;

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
    }

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
        return valuePreHeaderIndex - startOffset;
    }

    boolean isByteBacked() {
        return true;
    }

    public void registerIvmNotificationConsumer(IvmNotificationConsumer ivmConsumer) {
        this.ivmConsumer = ivmConsumer;
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

    @Override
    public void close() {

    }

    /* ---- End: version-agnostic parsing, utility, and public API methods ---- */
}
