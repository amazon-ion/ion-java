/*
 * Copyright 2007-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.ion.impl.bin;

import static com.amazon.ion.Decimal.isNegativeZero;
import static com.amazon.ion.IonType.BLOB;
import static com.amazon.ion.IonType.BOOL;
import static com.amazon.ion.IonType.CLOB;
import static com.amazon.ion.IonType.DECIMAL;
import static com.amazon.ion.IonType.FLOAT;
import static com.amazon.ion.IonType.INT;
import static com.amazon.ion.IonType.LIST;
import static com.amazon.ion.IonType.NULL;
import static com.amazon.ion.IonType.SEXP;
import static com.amazon.ion.IonType.STRING;
import static com.amazon.ion.IonType.STRUCT;
import static com.amazon.ion.IonType.SYMBOL;
import static com.amazon.ion.IonType.TIMESTAMP;
import static com.amazon.ion.IonType.isContainer;
import static com.amazon.ion.SystemSymbols.ION_1_0_SID;
import static com.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE_SID;
import static com.amazon.ion.Timestamp.Precision.DAY;
import static com.amazon.ion.Timestamp.Precision.MINUTE;
import static com.amazon.ion.Timestamp.Precision.MONTH;
import static com.amazon.ion.Timestamp.Precision.SECOND;
import static java.lang.Double.doubleToRawLongBits;
import static java.lang.Float.floatToRawIntBits;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonException;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.Timestamp;
import com.amazon.ion.impl._Private_RecyclingQueue;
import com.amazon.ion.impl._Private_RecyclingStack;
import com.amazon.ion.impl.bin.utf8.Utf8StringEncoder;
import com.amazon.ion.impl.bin.utf8.Utf8StringEncoderPool;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.ListIterator;

/**
 * Low-level binary {@link IonWriter} that understands encoding concerns but doesn't operate with any sense of symbol table management.
 */
@SuppressWarnings("deprecation")
/*package*/ final class IonRawBinaryWriter extends AbstractIonWriter implements _Private_IonRawWriter
{
    /** short-hand for array of bytes--useful for static definitions. */
    private static byte[] bytes(int... vals) {
        final byte[] octets = new byte[vals.length];

        for (int i = 0; i < vals.length; i++) {
            octets[i] = (byte) vals[i];
        }

        return octets;
    }

    private static final byte[] IVM = bytes(0xE0, 0x01, 0x00, 0xEA);

    private static final byte[] NULLS;
    static {
        final IonType[] types = IonType.values();
        NULLS = new byte[types.length];

        NULLS[NULL.ordinal()]           = (byte) 0x0F;
        NULLS[BOOL.ordinal()]           = (byte) 0x1F;
        NULLS[INT.ordinal()]            = (byte) 0x2F;
        NULLS[FLOAT.ordinal()]          = (byte) 0x4F;
        NULLS[DECIMAL.ordinal()]        = (byte) 0x5F;
        NULLS[TIMESTAMP.ordinal()]      = (byte) 0x6F;
        NULLS[SYMBOL.ordinal()]         = (byte) 0x7F;
        NULLS[STRING.ordinal()]         = (byte) 0x8F;
        NULLS[CLOB.ordinal()]           = (byte) 0x9F;
        NULLS[BLOB.ordinal()]           = (byte) 0xAF;
        NULLS[LIST.ordinal()]           = (byte) 0xBF;
        NULLS[SEXP.ordinal()]           = (byte) 0xCF;
        NULLS[STRUCT.ordinal()]         = (byte) 0xDF;
    }
    private static final byte NULL_NULL = NULLS[NULL.ordinal()];

    private static final byte BOOL_FALSE        = (byte) 0x10;
    private static final byte BOOL_TRUE         = (byte) 0x11;

    private static final byte INT_ZERO          = (byte) 0x20;

    private static final byte POS_INT_TYPE      = (byte) 0x20;
    private static final byte NEG_INT_TYPE      = (byte) 0x30;
    private static final byte FLOAT_TYPE        = (byte) 0x40;

    private static final byte DECIMAL_TYPE      = (byte) 0x50;
    private static final byte TIMESTAMP_TYPE    = (byte) 0x60;
    private static final byte SYMBOL_TYPE       = (byte) 0x70;
    private static final byte STRING_TYPE       = (byte) 0x80;

    private static final byte CLOB_TYPE         = (byte) 0x90;
    private static final byte BLOB_TYPE         = (byte) 0xA0;

    private static final byte DECIMAL_POS_ZERO               = (byte) 0x50;
    private static final byte DECIMAL_NEGATIVE_ZERO_MANTISSA = (byte) 0x80;

    private static final BigInteger BIG_INT_LONG_MAX_VALUE = BigInteger.valueOf(Long.MAX_VALUE);
    private static final BigInteger BIG_INT_LONG_MIN_VALUE = BigInteger.valueOf(Long.MIN_VALUE);

    private static final byte VARINT_NEG_ZERO   = (byte) 0xC0;

    final Utf8StringEncoder utf8StringEncoder = Utf8StringEncoderPool
            .getInstance()
            .getOrCreate();

    private static final byte[] makeTypedPreallocatedBytes(final int typeDesc, final int length)
    {
        final byte[] bytes = new byte[length];
        bytes[0]          = (byte) typeDesc;
        if (length > 1)
        {
            bytes[length - 1] = (byte) 0x80;
        }
        return bytes;
    }

    private static byte[][] makeContainerTypedPreallocatedTable(final int length) {
        final IonType[] types = IonType.values();
        byte[][] extendedSizes = new byte[types.length][];


        extendedSizes[LIST.ordinal()]   = makeTypedPreallocatedBytes(0xBE, length);
        extendedSizes[SEXP.ordinal()]   = makeTypedPreallocatedBytes(0xCE, length);
        extendedSizes[STRUCT.ordinal()] = makeTypedPreallocatedBytes(0xDE, length);

        return extendedSizes;
    }

    /**
     * Determines how container/container-like values should be padded
     */
    /*package*/ enum PreallocationMode
    {
        /** Allocate no length.  (forces side patching) */
        PREALLOCATE_0(0x0000,   1)
        {
            @Override
            /*package*/ void patchLength(final WriteBuffer buffer, final long position, final long lengthValue)
            {
                throw new IllegalStateException("Cannot patch in PREALLOCATE 0 mode");
            }
        },

        /** Preallocate 1 byte of length. */
        PREALLOCATE_1(0x007F,   2)
        {
            @Override
            /*package*/ void patchLength(final WriteBuffer buffer, long position, long lengthValue)
            {
                buffer.writeVarUIntDirect1At(position, lengthValue);
            }
        },

        /** Preallocate 2 bytes of length. */
        PREALLOCATE_2(0x3FFF,   3)
        {
            @Override
            /*package*/ void patchLength(final WriteBuffer buffer, long position, long lengthValue)
            {
                buffer.writeVarUIntDirect2At(position, lengthValue);
            }
        }
        ;

        private final int       contentMaxLength;
        private final int       typedLength;
        private final byte[][]  containerTypedPreallocatedBytes;
        private final byte[]    annotationsTypedPreallocatedBytes;

        private PreallocationMode(final int contentMaxLength, final int typedLength)
        {
            this.contentMaxLength = contentMaxLength;
            this.typedLength = typedLength;
            this.containerTypedPreallocatedBytes   = makeContainerTypedPreallocatedTable(typedLength);
            this.annotationsTypedPreallocatedBytes = makeTypedPreallocatedBytes(0xEE, typedLength);
        }

        /*package*/ abstract void patchLength(final WriteBuffer buffer, final long position, final long length);

        /**
         * Returns the number of header bytes that this mode would preallocate to hold the VarUInt-encoded length of
         * the current value. This number is equal to the total header length (i.e. `typedLength`) minus one, as it does
         * not include the type descriptor byte. (Examples: PREALLOCATE_0 returns `0`, PREALLOCATE_1 returns `1`, etc.)
         */
        int numberOfLengthBytes() {
            return typedLength - 1;
        }

        /*package*/ static PreallocationMode withPadSize(final int pad)
        {
            switch (pad)
            {
                case 0:
                    return PreallocationMode.PREALLOCATE_0;
                case 1:
                    return PreallocationMode.PREALLOCATE_1;
                case 2:
                    return PreallocationMode.PREALLOCATE_2;
            }
            throw new IllegalArgumentException("No such preallocation mode for: " + pad);
        }
    }

    private static final byte STRING_TYPE_EXTENDED_LENGTH       = (byte) 0x8E;
    private static final byte[] STRING_TYPED_PREALLOCATED_2     = makeTypedPreallocatedBytes(0x8E, 2);
    private static final byte[] STRING_TYPED_PREALLOCATED_3     = makeTypedPreallocatedBytes(0x8E, 3);

    /** Max supported annotation length specifier size supported. */
    private static final int MAX_ANNOTATION_LENGTH = 0x7F;

    private enum ContainerType
    {
        SEQUENCE(true),
        STRUCT(true),
        VALUE(false),
        ANNOTATION(false);

        public final boolean allowedInStepOut;

        private ContainerType(final boolean allowedInStepOut)
        {
            this.allowedInStepOut = allowedInStepOut;
        }
    }

    private class ContainerInfo
    {
        /** Whether the container is a struct */
        public ContainerType type;
        /** The location of the pre-allocated size descriptor in the buffer. */
        public long position;
        /** The size of the current value. */
        public long length;
        /**
         * The index of the patch point if present, <tt>-1</tt> otherwise.
         */
        public int patchIndex;

        public ContainerInfo()
        {
            type = null;
            position = -1;
            length = -1;
            patchIndex = -1;
        }

        public void appendPatch(final long oldPosition, final int oldLength, final long length)
        {
            if (patchIndex == -1) {
                // We have no assigned patch point, we need to make our own
                patchIndex = patchPoints.push(p -> p.initialize(oldPosition, oldLength, length));
            } else {
                // We have an assigned patch point already, but we need to overwrite it with the correct data
                patchPoints.get(patchIndex).initialize(oldPosition, oldLength, length);
            }
        }

        public ContainerInfo initialize(final ContainerType type, final long offset) {
            this.type = type;
            this.position = offset;
            this.length = 0;
            this.patchIndex = -1;

            return this;
        }

        @Override
        public String toString()
        {
            return "(CI " + type + " pos:" + position + " len:" + length + " patch:"+patchIndex+")";
        }
    }

    private static class PatchPoint
    {
        /** position of the data being patched out. */
        public long oldPosition;
        /** length of the data being patched out.*/
        public int oldLength;
        /** size of the container data or annotations.*/
        public long length;
        public PatchPoint()
        {
            oldPosition = -1;
            oldLength = -1;
            length = -1;
        }

        @Override
        public String toString()
        {
            return "(PP old::(" + oldPosition + " " + oldLength + ") patch::(" + length + ")";
        }

        public PatchPoint initialize(final long oldPosition, final int oldLength, final long length) {
            this.oldPosition = oldPosition;
            this.oldLength = oldLength;
            this.length = length;
            return this;
        }

        public PatchPoint clear() {
            return initialize(-1, -1, -1);
        }
    }

    /*package*/ enum StreamCloseMode
    {
        NO_CLOSE,
        CLOSE
    }

    /*package*/ enum StreamFlushMode
    {
        NO_FLUSH,
        FLUSH
    }

    private static final int SID_UNASSIGNED = -1;

    private final BlockAllocator                allocator;
    private final OutputStream                  out;
    private final StreamCloseMode               streamCloseMode;
    private final StreamFlushMode               streamFlushMode;
    private final PreallocationMode             preallocationMode;
    private final boolean                       isFloatBinary32Enabled;
    private final WriteBuffer                   buffer;
    private final _Private_RecyclingQueue<PatchPoint> patchPoints;
    private final _Private_RecyclingStack<ContainerInfo> containers;
    private int                                 depth;
    private boolean                             hasWrittenValuesSinceFinished;
    private boolean                             hasWrittenValuesSinceConstructed;

    private int                     currentFieldSid;
    private final IntList     currentAnnotationSids;
    // XXX this is for managed detection of TLV that is a LST--this is easier to track here than at the managed level
    private boolean                     hasTopLevelSymbolTableAnnotation;

    private boolean                     closed;

    /*package*/ IonRawBinaryWriter(final BlockAllocatorProvider provider,
                                   final int blockSize,
                                   final OutputStream out,
                                   final WriteValueOptimization optimization,
                                   final StreamCloseMode streamCloseMode,
                                   final StreamFlushMode streamFlushMode,
                                   final PreallocationMode preallocationMode,
                                   final boolean isFloatBinary32Enabled)
                                   throws IOException
    {
        super(optimization);

        if (out == null) { throw new NullPointerException(); }

        this.allocator         = provider.vendAllocator(blockSize);
        this.out               = out;
        this.streamCloseMode   = streamCloseMode;
        this.streamFlushMode   = streamFlushMode;
        this.preallocationMode = preallocationMode;
        this.isFloatBinary32Enabled = isFloatBinary32Enabled;
        this.buffer            = new WriteBuffer(allocator);
        this.patchPoints       = new _Private_RecyclingQueue<>(512, PatchPoint::new);
        this.containers        = new _Private_RecyclingStack<ContainerInfo>(
            10,
            new _Private_RecyclingStack.ElementFactory<ContainerInfo>() {
                public ContainerInfo newElement() {
                    return new ContainerInfo();
                }
            }
        );
        this.depth                            = 0;
        this.hasWrittenValuesSinceFinished    = false;
        this.hasWrittenValuesSinceConstructed = false;

        this.currentFieldSid                  = SID_UNASSIGNED;
        this.currentAnnotationSids            = new IntList();
        this.hasTopLevelSymbolTableAnnotation = false;
        this.closed = false;
    }

    /** Always returns {@link Symbols#systemSymbolTable()}. */
    public SymbolTable getSymbolTable()
    {
        return Symbols.systemSymbolTable();
    }

    // Current Value Meta

    public void setFieldName(final String name)
    {
        throw new UnsupportedOperationException("Cannot set field name on a low-level binary writer via string");
    }

    public void setFieldNameSymbol(final SymbolToken name)
    {
        setFieldNameSymbol(name.getSid());
    }

    public void setFieldNameSymbol(int sid)
    {
        if (!isInStruct())
        {
            throw new IonException("Cannot set field name outside of struct context");
        }
        currentFieldSid = sid;
    }

    public void setTypeAnnotations(final String... annotations)
    {
        throw new UnsupportedOperationException("Cannot set annotations on a low-level binary writer via string");
    }

    private void clearAnnotations()
    {
        currentAnnotationSids.clear();
        hasTopLevelSymbolTableAnnotation = false;
    }

    public void setTypeAnnotationSymbols(final SymbolToken... annotations)
    {
        clearAnnotations();
        if (annotations != null)
        {
            for (final SymbolToken annotation : annotations)
            {
                addTypeAnnotationSymbol(annotation.getSid());
            }
        }
    }

    public void setTypeAnnotationSymbols(int... sids)
    {
        clearAnnotations();
        if (sids != null)
        {
            for (final int sid : sids)
            {
                addTypeAnnotationSymbol(sid);
            }
        }
    }

    public void addTypeAnnotation(final String annotation)
    {
        throw new UnsupportedOperationException("Cannot add annotations on a low-level binary writer via string");
    }

    // Additional Current State Meta

    /*package*/ void addTypeAnnotationSymbol(final SymbolToken annotation)
    {
        addTypeAnnotationSymbol(annotation.getSid());

    }

    public void addTypeAnnotationSymbol(int sid)
    {
        if (depth == 0 && sid == ION_SYMBOL_TABLE_SID)
        {
            hasTopLevelSymbolTableAnnotation = true;
        }
        currentAnnotationSids.add(sid);
    }

    /*package*/ boolean hasAnnotations()
    {
        return !currentAnnotationSids.isEmpty();
    }

    /** Returns true if a value has been written since construction or {@link #finish()}. */
    /*package*/ boolean hasWrittenValuesSinceFinished()
    {
        return hasWrittenValuesSinceFinished;
    }

    /** Returns true if a value has been written since the writer was constructed. */
    /*package*/ boolean hasWrittenValuesSinceConstructed()
    {
        return hasWrittenValuesSinceConstructed;
    }

    /*package*/ boolean hasTopLevelSymbolTableAnnotation()
    {
        return hasTopLevelSymbolTableAnnotation;
    }

    /*package*/ int getFieldId()
    {
        return currentFieldSid;
    }

    // Compatibility with Implementation Writer Interface

    public IonCatalog getCatalog()
    {
        throw new UnsupportedOperationException();
    }

    public boolean isFieldNameSet()
    {
        return currentFieldSid > SID_UNASSIGNED;
    }

    public void writeIonVersionMarker() throws IOException
    {
        buffer.writeBytes(IVM);
    }

    public int getDepth()
    {
        return depth;
    }

    // Low-Level Writing

    private void updateLength(long length)
    {
        if (containers.isEmpty())
        {
            return;
        }

        containers.peek().length += length;
    }

    private void pushContainer(final ContainerType type)
    {
        // XXX we push before writing the type of container
        containers.push(c -> c.initialize(type, buffer.position() + 1));
    }

    private void addPatchPoint(final ContainerInfo container, final long position, final int oldLength, final long value)
    {
        // If we're adding a patch point we first need to ensure that all of our ancestors (containing values) already
        // have a patch point. No container can be smaller than the contents, so all outer layers also require patches.
        // Instead of allocating iterator, we share one iterator instance within the scope of the container stack and reset the cursor every time we track back to the ancestors.
        ListIterator<ContainerInfo> stackIterator = containers.iterator();
        // Walk down the stack until we find an ancestor which already has a patch point
        while (stackIterator.hasNext() && stackIterator.next().patchIndex == -1);

        // The iterator cursor is now positioned on an ancestor container that has a patch point
        // Ascend back up the stack, fixing the ancestors which need a patch point assigned before us
        while (stackIterator.hasPrevious()) {
            ContainerInfo ancestor = stackIterator.previous();
            if (ancestor.patchIndex == -1) {
                ancestor.patchIndex = patchPoints.push(PatchPoint::clear);
            }
        }

        // record the size of the length data.
        final int patchLength = WriteBuffer.varUIntLength(value);
        container.appendPatch(position, oldLength, value);
        updateLength(patchLength - oldLength);
    }

    /**
     * This is used to reclaim the placeholder patch point after scan the current container.
     * @param placeholderPatchIndex represents the index of the placeholder patch point.
     */
    private void reclaimPlaceholderPatchPoint(int placeholderPatchIndex) {
        if (placeholderPatchIndex >= patchPoints.size() - 1) {
            patchPoints.remove();
        }
    }

    private ContainerInfo popContainer()
    {
        final ContainerInfo currentContainer = containers.pop();
        if (currentContainer == null)
        {
            throw new IllegalStateException("Tried to pop container state without said container");
        }

        // only patch for real containers and annotations -- we use VALUE for tracking only
        long length = currentContainer.length;
        if (currentContainer.type != ContainerType.VALUE)
        {
            // patch in the length
            final long positionOfFirstLengthByte = currentContainer.position;
            if (length <= 0xD) {
                // The body of this container/wrapper is small enough that its length can fit in the lower nibble of
                // the type descriptor byte; we don't need the extra length bytes that were preallocated (if any).
                // We'll shift the encoded body of the container/wrapper backwards in the buffer to overwrite them.

                // The number of bytes we need to shift by is determined by the writer's preallocation mode.
                final int numberOfBytesToShiftBy = preallocationMode.numberOfLengthBytes();

                // `length` is the encoded length of the container/wrapper we're stepping out of. It does not
                // include any header bytes. In this `if` branch, we've confirmed that `length` is <= 0xD,
                // so this downcast from `long` to `int` is safe.
                final int lengthOfSliceToShift = (int) length;

                // Shift the container/wrapper body backwards in the buffer. Because this only happens when
                // `lengthOfSliceToShift` is 13 or fewer bytes, this will usually be a very fast memcpy.
                // It's slightly more work if the slice we're shifting happens to straddle two memory blocks
                // inside the buffer.
                buffer.shiftBytesLeft(lengthOfSliceToShift, numberOfBytesToShiftBy);

                // Overwrite the lower nibble of the original type descriptor byte with the body's encoded length.
                final long typeDescriptorPosition = positionOfFirstLengthByte - 1;
                final long type = (buffer.getUInt8At(typeDescriptorPosition) & 0xF0) | length;
                buffer.writeUInt8At(typeDescriptorPosition, type);

                // We've reclaimed some number of bytes; adjust the container length as appropriate.
                length -= numberOfBytesToShiftBy;
            }
            else if (currentContainer.length <= preallocationMode.contentMaxLength)
            {
                // The container's encoded body is too long to fit the length in the type descriptor byte, but it will
                // fit in the preallocated length bytes that were added to the buffer when the container was started.
                // Update those bytes with the VarUInt encoding of the length value.
                preallocationMode.patchLength(buffer, positionOfFirstLengthByte, length);
            }
            else
            {
                // The container's encoded body is too long to fit in the length bytes that were preallocated.
                // Write the length in the patch point list, making a note to include the VarUInt encoding of the length
                // at the right point when we go to flush the primary buffer to the output stream.
                addPatchPoint(currentContainer, positionOfFirstLengthByte, preallocationMode.numberOfLengthBytes(), length);
            }
        }

        // make sure to record length upward
        updateLength(length);
        return currentContainer;
    }

    private void writeVarUInt(final long value)
    {
        if (value < 0)
        {
            throw new IonException("Cannot write negative value as unsigned");
        }
        final int len = buffer.writeVarUInt(value);
        updateLength(len);
    }

    private void writeVarInt(final long value)
    {
        final int len = buffer.writeVarInt(value);
        updateLength(len);
    }

    private static void checkSid(int sid)
    {
        if (sid < 0)
        {
            throw new IllegalArgumentException("Invalid symbol with SID: " + sid);
        }
    }

    /** prepare to write values with field name and annotations. */
    private void prepareValue()
    {
        if (isInStruct() && currentFieldSid <= SID_UNASSIGNED)
        {
            throw new IllegalStateException("IonWriter.setFieldName() must be called before writing a value into a struct.");
        }
        if (currentFieldSid > SID_UNASSIGNED)
        {
            checkSid(currentFieldSid);
            writeVarUInt(currentFieldSid);

            // clear out field name
            currentFieldSid = SID_UNASSIGNED;
        }
        if (!currentAnnotationSids.isEmpty())
        {
            // we have to push a container context for annotations
            updateLength(preallocationMode.typedLength);
            pushContainer(ContainerType.ANNOTATION);
            buffer.writeBytes(preallocationMode.annotationsTypedPreallocatedBytes);

            final long annotationsLengthPosition = buffer.position();
            buffer.writeVarUInt(0L);
            int annotationsLength = 0;
            // XXX: This is a very hot path. This code intentionally avoids creating iterators.
            for (int m = 0; m < currentAnnotationSids.size(); m++)
            {
                final int symbol = currentAnnotationSids.get(m);
                checkSid(symbol);
                final int symbolLength = buffer.writeVarUInt(symbol);
                annotationsLength += symbolLength;
            }
            if (annotationsLength > MAX_ANNOTATION_LENGTH)
            {
                // TODO deal with side patching if we want to support > 32 4-byte symbols annotations... seems excessive
                throw new IonException("Annotations too large: " + currentAnnotationSids);
            }

            // update the annotations size
            updateLength(/*length specifier*/ 1 + annotationsLength);
            // patch the annotations length
            buffer.writeVarUIntDirect1At(annotationsLengthPosition, annotationsLength);

            // clear out annotations
            currentAnnotationSids.clear();
            hasTopLevelSymbolTableAnnotation = false;
        }
    }

    /** Closes out annotations. */
    private void finishValue()
    {
        if (!containers.isEmpty() && containers.peek().type == ContainerType.ANNOTATION)
        {
            // close out and patch the length
            popContainer();
        }
        hasWrittenValuesSinceFinished = true;
        hasWrittenValuesSinceConstructed = true;
    }

    // Container Manipulation

    public void stepIn(final IonType containerType) throws IOException
    {
        if (!isContainer(containerType))
        {
            throw new IonException("Cannot step into " + containerType);
        }
        prepareValue();
        updateLength(preallocationMode.typedLength);
        pushContainer(containerType == STRUCT ? ContainerType.STRUCT : ContainerType.SEQUENCE);
        depth++;
        buffer.writeBytes(preallocationMode.containerTypedPreallocatedBytes[containerType.ordinal()]);
    }

    public void stepOut() throws IOException
    {
        if (currentFieldSid > SID_UNASSIGNED)
        {
            throw new IonException("Cannot step out with field name set");
        }
        if (!currentAnnotationSids.isEmpty())
        {
            throw new IonException("Cannot step out with field name set");
        }
        if (containers.isEmpty() || !containers.peek().type.allowedInStepOut)
        {
            throw new IonException("Cannot step out when not in container");
        }
        // close out the container
        popContainer();
        depth--;
        // close out the annotations if any
        finishValue();
    }

    public boolean isInStruct()
    {
        return !containers.isEmpty() && containers.peek().type == ContainerType.STRUCT;
    }

    // Write Value Methods

    public void writeNull() throws IOException
    {
        prepareValue();
        updateLength(1);
        buffer.writeByte(NULL_NULL);
        finishValue();
    }

    public void writeNull(final IonType type) throws IOException
    {
        byte data = NULL_NULL;
        if (type != null)
        {
            data = NULLS[type.ordinal()];
            if (data == 0)
            {
                throw new IllegalArgumentException("Cannot write a null for: " + type);
            }
        }

        prepareValue();
        updateLength(1);
        buffer.writeByte(data);
        finishValue();
    }

    public void writeBool(final boolean value) throws IOException
    {
        prepareValue();
        updateLength(1);
        if (value)
        {
            buffer.writeByte(BOOL_TRUE);
        }
        else
        {
            buffer.writeByte(BOOL_FALSE);
        }
        finishValue();
    }

    /**
     * Writes a type descriptor followed by unsigned integer value.
     * Does not check for sign.
     * Note that this does not do {@link #prepareValue()} or {@link #finishValue()}.
     */
    private void writeTypedUInt(final int type, final long value)
    {
        if (value <= 0xFFL)
        {
            updateLength(2);
            buffer.writeUInt8(type | 0x01);
            buffer.writeUInt8(value);
        }
        else if (value <= 0xFFFFL)
        {
            updateLength(3);
            buffer.writeUInt8(type | 0x02);
            buffer.writeUInt16(value);
        }
        else if (value <= 0xFFFFFFL)
        {
            updateLength(4);
            buffer.writeUInt8(type | 0x03);
            buffer.writeUInt24(value);
        }
        else if (value <= 0xFFFFFFFFL)
        {
            updateLength(5);
            buffer.writeUInt8(type | 0x04);
            buffer.writeUInt32(value);
        }
        else if (value <= 0xFFFFFFFFFFL)
        {
            updateLength(6);
            buffer.writeUInt8(type | 0x05);
            buffer.writeUInt40(value);
        }
        else if (value <= 0xFFFFFFFFFFFFL)
        {
            updateLength(7);
            buffer.writeUInt8(type | 0x06);
            buffer.writeUInt48(value);
        }
        else if (value <= 0xFFFFFFFFFFFFFFL)
        {
            updateLength(8);
            buffer.writeUInt8(type | 0x07);
            buffer.writeUInt56(value);
        }
        else
        {
            updateLength(9);
            buffer.writeUInt8(type | 0x08);
            buffer.writeUInt64(value);
        }
    }

    public void writeInt(long value) throws IOException
    {
        prepareValue();
        if (value == 0)
        {
            updateLength(1);
            buffer.writeByte(INT_ZERO);
        }
        else
        {
            int type = POS_INT_TYPE;
            if (value < 0)
            {
                type = NEG_INT_TYPE;
                if (value == Long.MIN_VALUE)
                {
                    // XXX special case for min_value which will not play nice with signed arithmetic and fit into the positive space
                    // XXX we keep 2's complement of Long.MIN_VALUE because it encodes to unsigned 2 ** 63 (0x8000000000000000L)
                    // XXX WriteBuffer.writeUInt64() never looks at sign
                    updateLength(9);
                    buffer.writeUInt8(NEG_INT_TYPE | 0x8);
                    buffer.writeUInt64(value);
                }
                else
                {
                    // get the magnitude, sign is already encoded
                    value = -value;
                    writeTypedUInt(type, value);
                }
            }
            else
            {
                writeTypedUInt(type, value);
            }
        }
        finishValue();
    }

    /** Write a raw byte array as some type. Note that this does not do {@link #prepareValue()}. */
    private void writeTypedBytes(final int type, final byte[] data, final int offset, final int length)
    {
        int totalLength = 1 + length;
        if (length < 14)
        {
            buffer.writeUInt8(type | length);
        }
        else
        {
            // need to specify length explicitly
            buffer.writeUInt8(type | 0xE);
            final int sizeLength = buffer.writeVarUInt(length);
            totalLength += sizeLength;
        }
        updateLength(totalLength);
        buffer.writeBytes(data, offset, length);
    }

    public void writeInt(BigInteger value) throws IOException
    {
        if (value == null)
        {
            writeNull(IonType.INT);
            return;
        }
        if (value.compareTo(BIG_INT_LONG_MIN_VALUE) >= 0 && value.compareTo(BIG_INT_LONG_MAX_VALUE) <= 0)
        {
            // for the small stuff, just write it as a signed int64
            writeInt(value.longValue());
            return;
        }

        prepareValue();

        int type = POS_INT_TYPE;
        if(value.signum() < 0)
        {
            type = NEG_INT_TYPE;
            value = value.negate();
        }

        // generate big-endian representation of the positive value
        final byte[] magnitude = value.toByteArray();
        writeTypedBytes(type, magnitude, 0, magnitude.length);

        finishValue();
    }

    public void writeFloat(final double value) throws IOException
    {
        prepareValue();

        if (isFloatBinary32Enabled && value == ((double) ((float) value))) {
            updateLength(5);
            buffer.writeUInt8(FLOAT_TYPE | 4);
            buffer.writeUInt32(floatToRawIntBits((float) value));
        } else {
            updateLength(9);
            buffer.writeUInt8(FLOAT_TYPE | 8);
            buffer.writeUInt64(doubleToRawLongBits(value));
        }

        finishValue();
    }

    /** Encodes a decimal, updating the current container length context (which is probably a Decimal/Timestamp). */
    private void writeDecimalValue(final BigDecimal value)
    {
        final boolean isNegZero = isNegativeZero(value);
        final int signum = value.signum();
        final int exponent = -value.scale();

        writeVarInt(exponent);

        final BigInteger mantissaBigInt = value.unscaledValue();
        if (mantissaBigInt.compareTo(BIG_INT_LONG_MIN_VALUE) >= 0 && mantissaBigInt.compareTo(BIG_INT_LONG_MAX_VALUE) <= 0)
        {
            // we can fit into the long space
            final long mantissa = mantissaBigInt.longValue();
            if (signum == 0 && !isNegZero)
            {
                // positive zero does not need to be encoded
            }
            else if (isNegZero)
            {
                // XXX special case for negative zero, we have to encode as a signed zero in the Int format
                updateLength(1);
                buffer.writeByte(DECIMAL_NEGATIVE_ZERO_MANTISSA);
            }
            else if (mantissa == Long.MIN_VALUE)
            {
                // XXX special case for min value -- we need 64-bits to store the magnitude and we need a bit for sign
                updateLength(9);
                buffer.writeUInt8(0x80);
                buffer.writeUInt64(mantissa);
            }
            else if (mantissa >= 0xFFFFFFFFFFFFFF81L && mantissa <= 0x000000000000007FL)
            {
                updateLength(1);
                buffer.writeInt8(mantissa);
            }
            else if (mantissa >= 0xFFFFFFFFFFFF8001L && mantissa <= 0x0000000000007FFFL)
            {
                updateLength(2);
                buffer.writeInt16(mantissa);
            }
            else if (mantissa >= 0xFFFFFFFFFF800001L && mantissa <= 0x00000000007FFFFFL)
            {
                updateLength(3);
                buffer.writeInt24(mantissa);
            }
            else if (mantissa >= 0xFFFFFFFF80000001L && mantissa <= 0x000000007FFFFFFFL)
            {
                updateLength(4);
                buffer.writeInt32(mantissa);
            }
            else if (mantissa >= 0xFFFFFF8000000001L && mantissa <= 0x0000007FFFFFFFFFL)
            {
                updateLength(5);
                buffer.writeInt40(mantissa);
            }
            else if (mantissa >= 0xFFFF800000000001L && mantissa <= 0x00007FFFFFFFFFFFL)
            {
                updateLength(6);
                buffer.writeInt48(mantissa);
            }
            else if (mantissa >= 0xFF80000000000001L && mantissa <= 0x007FFFFFFFFFFFFFL)
            {
                updateLength(7);
                buffer.writeInt56(mantissa);
            }
            else
            {
                // TODO consider being more space efficient for integers that can be written with 6/7 bytes.
                updateLength(8);
                buffer.writeInt64(mantissa);
            }
        }
        else
        {
            final BigInteger magnitude = signum > 0 ? mantissaBigInt : mantissaBigInt.negate();
            final byte[] bits = magnitude.toByteArray();
            if (signum < 0)
            {
                if ((bits[0] & 0x80) == 0)
                {
                    bits[0] |= 0x80;
                }
                else
                {
                    // not enough space in the bits to store the negative sign
                    updateLength(1);
                    buffer.writeUInt8(0x80);
                }
            }
            updateLength(bits.length);
            buffer.writeBytes(bits);
        }
    }

    private void patchSingleByteTypedOptimisticValue(final byte type, final ContainerInfo info)
    {
        if (info.length <= 0xD)
        {
            // we fit -- overwrite the type byte
            buffer.writeUInt8At(info.position - 1, type | info.length);
        }
        else
        {
            // side patch
            buffer.writeUInt8At(info.position - 1, type | 0xE);
            addPatchPoint(info, info.position, 0, info.length);
        }
    }

    public void writeDecimal(final BigDecimal value) throws IOException
    {
        if (value == null)
        {
            writeNull(IonType.DECIMAL);
            return;
        }

        prepareValue();

        if (value.signum() == 0 && value.scale() == 0 && !isNegativeZero(value))
        {
            // 0d0 can be written in one byte
            updateLength(1);
            buffer.writeUInt8(DECIMAL_POS_ZERO);
        }
        else
        {
            // optimistically try to fit decimal length in low nibble (most should)
            updateLength(1);
            pushContainer(ContainerType.VALUE);
            buffer.writeByte(DECIMAL_TYPE);
            writeDecimalValue(value);
            final ContainerInfo info = popContainer();
            patchSingleByteTypedOptimisticValue(DECIMAL_TYPE, info);
        }

        finishValue();
    }

    public void writeTimestamp(final Timestamp value) throws IOException
    {
        if (value == null)
        {
            writeNull(IonType.TIMESTAMP);
            return;
        }
        prepareValue();

        // optimistically try to fit a timestamp length in low nibble (most should)
        updateLength(1);
        pushContainer(ContainerType.VALUE);
        buffer.writeByte(TIMESTAMP_TYPE);

        // OFFSET
        final Integer offset = value.getLocalOffset();
        if (offset == null)
        {
            // special case for unknown -00:00
            updateLength(1);
            buffer.writeByte(VARINT_NEG_ZERO);
        }
        else
        {
            writeVarInt(offset.intValue());
        }

        // YEAR
        final int year = value.getZYear();
        writeVarUInt(year);

        // XXX it is really convenient to rely on the ordinal
        final int precision = value.getPrecision().ordinal();

        if (precision >= MONTH.ordinal())
        {
            final int month = value.getZMonth();
            writeVarUInt(month);
        }
        if (precision >= DAY.ordinal())
        {
            final int day = value.getZDay();
            writeVarUInt(day);
        }
        if (precision >= MINUTE.ordinal())
        {
            final int hour = value.getZHour();
            writeVarUInt(hour);
            final int minute = value.getZMinute();
            writeVarUInt(minute);
        }
        if (precision >= SECOND.ordinal())
        {
            final int second = value.getZSecond();
            writeVarUInt(second);
            final BigDecimal fraction = value.getZFractionalSecond();
            if (fraction != null) {
                final BigInteger mantissaBigInt = fraction.unscaledValue();
                final int exponent = -fraction.scale();
                if (!(mantissaBigInt.equals(BigInteger.ZERO) && exponent > -1)) {
                    writeDecimalValue(fraction);
                }
            }
        }

        final ContainerInfo info = popContainer();
        patchSingleByteTypedOptimisticValue(TIMESTAMP_TYPE, info);

        finishValue();
    }

    public void writeSymbol(String content) throws IOException
    {
        throw new UnsupportedOperationException("Symbol writing via string is not supported in low-level binary writer");
    }

    public void writeSymbolToken(final SymbolToken content) throws IOException
    {
        if (content == null)
        {
            writeNull(IonType.SYMBOL);
            return;
        }
        writeSymbolToken(content.getSid());
    }

    boolean isIVM(int sid)
    {
        // When SID 2 occurs at the top level with no annotations, it has the
        // special properties of an IVM. Otherwise, it's considered a normal
        // symbol value.
        // TODO amazon-ion/ion-java/issues/88 requires this behavior to be changed,
        // such that top-level SID 2 is treated as a symbol value, not an IVM.
        return depth == 0 && sid == ION_1_0_SID && !hasAnnotations();
    }

    public void writeSymbolToken(int sid) throws IOException
    {
        if (isIVM(sid))
        {
            throw new IonException("Direct writing of IVM is not supported in low-level binary writer");
        }
        checkSid(sid);
        prepareValue();
        writeTypedUInt(SYMBOL_TYPE, sid);
        finishValue();
    }

    public void writeString(final String value) throws IOException
    {
        if (value == null)
        {
            writeNull(IonType.STRING);
            return;
        }
        prepareValue();

        // UTF-8 encode the String
        Utf8StringEncoder.Result encoderResult = utf8StringEncoder.encode(value);
        int utf8Length = encoderResult.getEncodedLength();
        byte[] utf8Buffer = encoderResult.getBuffer();

        // Write the type and length codes to the output stream.
        long previousPosition = buffer.position();
        if (utf8Length <= 0xD) {
            buffer.writeUInt8(STRING_TYPE | utf8Length);
        } else {
            buffer.writeUInt8(STRING_TYPE | 0xE);
            buffer.writeVarUInt(utf8Length);
        }

        // Write the encoded UTF-8 bytes to the output stream
        buffer.writeBytes(utf8Buffer, 0, utf8Length);

        long bytesWritten = buffer.position() - previousPosition;
        updateLength(bytesWritten);

        finishValue();
    }

    public void writeClob(byte[] data) throws IOException
    {
        if (data == null)
        {
            writeNull(IonType.CLOB);
            return;
        }
        writeClob(data, 0, data.length);
    }

    public void writeClob(final byte[] data, final int offset, final int length) throws IOException
    {
        if (data == null)
        {
            writeNull(IonType.CLOB);
            return;
        }
        prepareValue();
        writeTypedBytes(CLOB_TYPE, data, offset, length);
        finishValue();
    }

    public void writeBlob(byte[] data) throws IOException
    {
        if (data == null)
        {
            writeNull(IonType.BLOB);
            return;
        }
        writeBlob(data, 0, data.length);
    }

    public void writeBlob(final byte[] data, final int offset, final int length) throws IOException
    {
        if (data == null)
        {
            writeNull(IonType.BLOB);
            return;
        }
        prepareValue();
        writeTypedBytes(BLOB_TYPE, data, offset, length);
        finishValue();
    }

    @Override
    public void writeString(byte[] data, int offset, int length) throws IOException
    {
        if (data == null)
        {
            writeNull(IonType.STRING);
            return;
        }
        prepareValue();
        writeTypedBytes(STRING_TYPE, data, offset, length);
        finishValue();
    }

    /**
     * Writes a raw value into the buffer, updating lengths appropriately.
     * <p>
     * The implication here is that the caller is dumping some valid Ion payload with the correct context.
     */
    public void writeBytes(byte[] data, int offset, int length) throws IOException
    {
        prepareValue();
        updateLength(length);
        buffer.writeBytes(data, offset, length);
        finishValue();
    }

    // Stream Manipulation/Terminators

    /*package*/ long position()
    {
        return buffer.position();
    }

    /*package*/ void truncate(long position)
    {
        buffer.truncate(position);
        // TODO decide if it is worth making this faster than O(N)
        Iterator<PatchPoint> patchIterator = patchPoints.iterate();
        int i = 0;
        while (patchIterator.hasNext()) {
            PatchPoint patchPoint = patchIterator.next();
            if (patchPoint.length > -1) {
                if (patchPoint.oldPosition >= position) {
                    patchPoints.truncate(i - 1);
                    break;
                }
            }
            i++;
        }
    }

    public void flush() throws IOException {}

    public void finish() throws IOException
    {
        if (closed)
        {
            return;
        }
        if (!containers.isEmpty() || depth > 0)
        {
            throw new IllegalStateException("Cannot finish within container: " + containers);
        }

        if (patchPoints.isEmpty())
        {
            // nothing to patch--write 'em out!
            buffer.writeTo(out);
        }
        else
        {
            long bufferPosition = 0;
            Iterator<PatchPoint> iterator = patchPoints.iterate();
            while (iterator.hasNext())
            {
                PatchPoint patch = iterator.next();
                if (patch.length < 0) {
                    continue;
                }
                // write up to the thing to be patched
                final long bufferLength = patch.oldPosition - bufferPosition;
                buffer.writeTo(out, bufferPosition, bufferLength);

                // write out the patch
                WriteBuffer.writeVarUIntTo(out, patch.length);

                // skip over the preallocated varuint field
                bufferPosition = patch.oldPosition;
                bufferPosition += patch.oldLength;
            }
            buffer.writeTo(out, bufferPosition, buffer.position() - bufferPosition);
        }
        patchPoints.clear();
        buffer.reset();

        if (streamFlushMode == StreamFlushMode.FLUSH)
        {
            out.flush();
        }

        hasWrittenValuesSinceFinished = false;
    }

    public void close() throws IOException
    {
        if (closed)
        {
            return;
        }
        try
        {
            try
            {
                finish();
            }
            catch (final IllegalStateException e)
            {
                // callers don't expect this...
            }

            // release all of our blocks -- these should never throw
            buffer.close();
            allocator.close();
            utf8StringEncoder.close();
        }
        finally
        {
            closed = true;
            if (streamCloseMode == StreamCloseMode.CLOSE)
            {
                // release the stream
                out.close();
            }
        }
    }

}
