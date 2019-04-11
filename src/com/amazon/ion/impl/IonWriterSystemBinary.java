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

package com.amazon.ion.impl;

import static com.amazon.ion.impl._Private_IonConstants.tidDATAGRAM;
import static com.amazon.ion.impl._Private_IonConstants.tidList;
import static com.amazon.ion.impl._Private_IonConstants.tidSexp;
import static com.amazon.ion.impl._Private_IonConstants.tidStruct;
import static com.amazon.ion.impl.lite._Private_LiteDomTrampoline.reverseEncode;

import com.amazon.ion.IonException;
import com.amazon.ion.IonType;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.Timestamp;
import com.amazon.ion.impl.BlockedBuffer.BlockedByteInputStream;
import com.amazon.ion.impl.IonBinary.BufferManager;
import com.amazon.ion.system.IonWriterBuilder.InitialIvmHandling;
import com.amazon.ion.system.IonWriterBuilder.IvmMinimizing;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.LinkedList;
import java.util.Queue;


final class IonWriterSystemBinary
    extends IonWriterSystem
    implements _Private_ListWriter
{

    /**
     * Implements patching of the internal OutputStream with actual value
     * lengths and types. Every instance of this class represents a container
     * (including the datagram). Information about the children of the container
     * is stored in _types/_positions/_lengths arrays.
     *
     * The patching mechanism works as follows. For every primitive value, we store
     * its type, position in internal OutputStream and the serialized lengths
     * in this class. With every value insertion, its length is then summed to the
     * length of the @_parent. Upon seeing a container, a new PatchedValue is created
     * and inserted into @_children queue, then all subsequent values are written
     * as described above. As the container is finished, its @_parent becomes active
     * and values are continued to be written. In the end, a general tree is created
     * with information about all the values in the datagram. The values themselves
     * are serialized into internal OutputStream
     *
     * To finalize the internal OutputStream into a user stream, the arrays in
     * PatchedValues are traversed from the beginning. For every value in
     * in @_types[i], it's type descriptor is written into user stream. If the type
     * is primitive, content of the internal OutputStream is copied starting
     * from @_positions[i] with length @_lengths[i]. If @_types[i] is a container,
     * a child is popped from the @_children queue and recursively written as
     * described above. Whenever TID_SYMBOL_TABLE_PATCH is seen in @_types, the
     * current symbol table gets reset with the one on the top of @_symtabs queue.
     */
    static class PatchedValues {

        private final static int DEFAULT_PATCH_COUNT    = 10;

        /** first free position in the _types/_positions/_lengths arrays */
        int _freePos;
        /** types of the container */
        int[] _types;
        /** positions where values start in buffer */
        int[] _positions;
        /**
         * lengths of the values. The high 32bits of this value store the length
         * of the field name, the low 32bits store the actual value length. We need the
         * field name length to properly adjust the total @_parent length in @endPatch
         */
        long[] _lengths;
        // the parent
        PatchedValues _parent;
        Queue<PatchedValues> _children;
        Queue<SymbolTable> _symtabs;

        PatchedValues() {
            _freePos = -1;
            _types = new int[DEFAULT_PATCH_COUNT];
            _positions = new int[DEFAULT_PATCH_COUNT];
            _lengths = new long[DEFAULT_PATCH_COUNT];
        }

        void reset() {
            _freePos = -1;
            _children = null;
            _symtabs = null;
        }

        /**
         * Add a new PatchedValues instance to _children and return it
         */
        PatchedValues addChild() {
            PatchedValues pv = new PatchedValues();
            pv._parent = this;
            if (_children == null) {
                _children = new LinkedList<PatchedValues>();
            }
            _children.add(pv);
            return pv;
        }

        /**
         * Inject a symbol table at the specified position in internal OutputStream. Most of the
         * times, the injection will happen upon seeing the first non-system symbol. At that point
         * the information about the symbol is already stored here by @startPatch call, so we need
         * to be able to inject a symtab before this value (injectBeforeCurrent)
         *
         * @param st
         * @param injectBeforeCurrent Flags if symbol table must be injected before the current value
         *                            which essentially shifts the array by 1
         * @throws IllegalStateException if the PatchedValues is not a top-level one
         */
        void injectSymbolTable(SymbolTable st, boolean injectBeforeCurrent) {
            if (_parent != null) {
                // we're not on top-level
                throw new IllegalStateException("Cannot inject a symbol table when not on top-level");
            }
            if (_symtabs == null) {
                _symtabs = new LinkedList<SymbolTable>();
            }
            ++_freePos;
            if (_freePos == _positions.length) {
                grow();
            }
            if (injectBeforeCurrent) {
                // move current value to the right
                _types[_freePos] = _types[_freePos - 1];
                _lengths[_freePos] = _lengths[_freePos - 1];
                // set previous value to symbol table type
                _types[_freePos - 1] = TID_SYMBOL_TABLE_PATCH;
                _lengths[_freePos - 1] = 0;
            } else {
                // add symbol table to the next free position as a usual value
                _types[_freePos] = TID_SYMBOL_TABLE_PATCH;
                _lengths[_freePos] = 0;
            }
            // inject the symbol table to the current position on the top
            _symtabs.add(st);
        }

        int getType() {
            return _types[_freePos];
        }

        PatchedValues getParent() {
            return _parent;
        }

        /** Start the patch for given @type at given @pos */
        void startPatch(int type, int pos) {
            ++_freePos;
            if (_freePos == _positions.length) {
                grow();
            }
            _types[_freePos] = type;
            _lengths[_freePos] = 0;
            _positions[_freePos] = pos;
        }

        /** Set the field name length as high 32bits of @_lengths item */
        void patchFieldName(int fieldNameLength) {
            _lengths[_freePos] = ((long)fieldNameLength) << 32;
        }

        /**
         * Set the value length as low 32bits of @_lengths item. The low 32bits will be
         * summed with the given value (say, it's a list that is in struct)
         * @param len
         */
        void patchValue(int len) {
            long memLen = (_lengths[_freePos] & 0xFFFFFFFF00000000L);
            long curLen = (_lengths[_freePos] & 0x00000000FFFFFFFFL);
            _lengths[_freePos] = memLen | (curLen + len);
        }

        /**
         * End this patch. If there's a @_parent available, it's @_length will be summed
         * with the length of current patched value
         */
        void endPatch() {
            if (_parent != null) {
                int memberLen = (int)(_lengths[_freePos] >> 32);
                int valueLen = (int)(_lengths[_freePos] & 0xFFFFFFFF);
                int totalLen = memberLen + valueLen;
                switch (_types[_freePos]) {
                case TID_SYMBOL_TABLE_PATCH:
                case TID_RAW:
                    break;
                case TID_ANNOTATION_PATCH:
                    totalLen += IonBinary.lenVarUInt(valueLen);
                    break;
                default:
                    // add the type if it's specified
                    ++totalLen;
                    // add actual length of @totalLen :)
                    if (valueLen >= _Private_IonConstants.lnIsVarLen) {
                        totalLen += IonBinary.lenVarUInt(valueLen);
                    }
                }
                _parent.patchValue(totalLen);
            }
        }

        // grow all related arrays
        private void grow() {
            int newSize = _positions.length * 2;
            _types = growOne(_types, newSize);
            _positions = growOne(_positions, newSize);
            _lengths = growOne(_lengths, newSize);
        }
        // grow single array and return the result
        static int[] growOne(int[] source, int newSize) {
            int[] dest = new int[newSize];
            System.arraycopy(source, 0, dest, 0, source.length);
            return dest;
        }
        // grow single array and return the result
        static long[] growOne(long[] source, int newSize) {
            long[] dest = new long[newSize];
            System.arraycopy(source, 0, dest, 0, source.length);
            return dest;
        }
    }

    // top-level patch
    PatchedValues _patch = new PatchedValues();

    private final static int TID_ANNOTATION_PATCH = _Private_IonConstants.tidDATAGRAM + 1;
    private final static int TID_SYMBOL_TABLE_PATCH = _Private_IonConstants.tidDATAGRAM + 2;
    private final static int TID_RAW = _Private_IonConstants.tidDATAGRAM + 3;

    // private static final boolean _verbose_debug = false;

    static final int UNKNOWN_LENGTH = -1;

    BufferManager     _manager;
    IonBinary.Writer  _writer;

    /** Not null */
    private final OutputStream _user_output_stream;

    /**
     * Do we {@link #flush()} after each top-level value?
     * @see #closeValue()
     */
    private final boolean _auto_flush;

    boolean           _in_struct;

    /** Ensure we don't use a closed {@link #output} stream. */
    private boolean _closed;

    private final static int TID_FOR_SYMBOL_TABLE_PATCH = _Private_IonConstants.tidDATAGRAM + 1;
    private final static int DEFAULT_PATCH_COUNT        = 10;
    private final static int DEFAULT_PATCH_DEPTH        = 10;
    private final static int NOT_A_SYMBOL_TABLE_IDX     = -1;

    // Patch:
    //          offset in data stream
    //          accumulated length    -- combine w offset in long? offset:len (allows len+=more)
    //          type of data (should this be in the data stream?
    //          patched value's parent is struct flag (low nibble in data stream?)
    //
    // the patches are the accumulated list of patch points and are
    // in position order (which is conveniently the order they are
    // encountered and created).
    int        _patch_count     = 0;
    int []     _patch_lengths   = new int[DEFAULT_PATCH_COUNT];     // TODO: should these be merged? (since array access is expensive)
    int []     _patch_offsets   = new int[DEFAULT_PATCH_COUNT];     // should patch lengths and patch offsets be longs?
    int []     _patch_table_idx = new int[DEFAULT_PATCH_COUNT];
    int []     _patch_types     = new int[DEFAULT_PATCH_COUNT];
    boolean [] _patch_in_struct = new boolean[DEFAULT_PATCH_COUNT];

    // this is only loaded by the User writer, but it is read
    // by the "get byte" operations and must be coordinated
    // with the patch list that the system writer maintains here.
    int           _patch_symbol_table_count = 0;
    SymbolTable[] _patch_symbol_tables      = new SymbolTable[DEFAULT_PATCH_COUNT];

    // the patch stack is the list of patch points that currently
    // need updating.  The value is the index into the patch arrays.
    // As a value requiring patches is closed its patch idx is removed
    // from the stack.
    int    _top;
    int [] _patch_stack = new int[DEFAULT_PATCH_DEPTH];

    /**
     * This is the depth as seen by the user.  Since there are cases where we
     * don't push onto the patch stack and cases where we push non-user
     * containers onto the patch stack we compute this separately during
     * stepIn and stepOut.
     */
    private int _user_depth;


    /**
     * @param out OutputStream the users output byte stream, if specified
     * @param autoFlush when true the writer flushes to the output stream
     *  between top level values
     * @param ensureInitialIvm when true, an initial IVM will be emitted even
     *  when the user doesn't explicitly write one. When false, an initial IVM
     *  won't be emitted unless the user does it. That can result in an invalid
     *  Ion stream if not used carefully.
     * @throws NullPointerException if any parameter is null.
     */
    IonWriterSystemBinary(SymbolTable defaultSystemSymtab,
                          OutputStream out,
                          boolean autoFlush,
                          boolean ensureInitialIvm)
    {
        super(defaultSystemSymtab,
              (ensureInitialIvm ? InitialIvmHandling.ENSURE : null),
              IvmMinimizing.ADJACENT);

        out.getClass(); // Efficient null check
        _user_output_stream = out;

        // the buffer manager and writer
        // are used to hold the buffered
        // binary values pending flush().
        _manager = new BufferManager();
        _writer = _manager.openWriter();
        _auto_flush = autoFlush;
    }

    /**
     * Empty our buffers, assuming it is safe to do so.
     * This is called by {@link #flush()} and {@link #finish()}.
     */
    private void writeAllBufferedData()
        throws IOException
    {
        writeBytes(_user_output_stream);

        clearFieldName();
        clearAnnotations();

        _in_struct = false;
        _patch_count = 0;
        _patch_symbol_table_count = 0;
        _top = 0;
        try {
            _writer.setPosition(0);
            _writer.truncate();
        }
        catch (IOException e) {
            throw new IonException(e);
        }
    }

    @Override
    public void finish() throws IOException
    {
        if (getDepth() != 0) {
            throw new IllegalStateException(ERROR_FINISH_NOT_AT_TOP_LEVEL);
        }

        writeAllBufferedData();
        super.finish();
    }

    final OutputStream getOutputStream()
    {
        return _user_output_stream;
    }

    public final boolean isInStruct()
    {
        return _in_struct;
    }

    private final boolean topInStruct() {
        if (_top == 0) return false;
        boolean in_struct = _patch_in_struct[_patch_stack[_top - 1]];
        return in_struct;
    }
    protected final boolean atDatagramLevel()
    {
        return (topType() == _Private_IonConstants.tidDATAGRAM);
//        return is_datagram;
    }

    @Override
    public final int getDepth()
    {
        return _user_depth;
    }

    protected final IonType getContainer()
    {
        IonType type;
        int tid = parentType();
        switch (tid) {
        case tidList:
            type = IonType.LIST;
            break;
        case tidSexp:
            type = IonType.SEXP;
            break;
        case tidStruct:
            type = IonType.STRUCT;
            break;
        case tidDATAGRAM:
            type = IonType.DATAGRAM;
            break;
        default:
            throw new IonException("unexpected parent type "+tid+" does not represent a container");
        }
        return type;
    }

    @Override
    final SymbolTable inject_local_symbol_table() throws IOException
    {
        SymbolTable symbols = super.inject_local_symbol_table();
        PatchedValues top;
        // find the parent
        for (top = _patch; top.getParent() != null; top = top.getParent()) {}
        // inject the symbol table; if @_patch is not the top element, then inject
        // the symtab before this top-level value begins
        super.startValue();
        top.injectSymbolTable(symbols, _patch.getParent() != null);
        super.endValue();
        return symbols;
    }

    private final int topLength() {
        return _patch_lengths[_patch_stack[_top - 1]];
    }

    private final int topType() {
        if (_top == 0) return _Private_IonConstants.tidDATAGRAM;
        return _patch_types[_patch_stack[_top - 1]];
    }

    private final int parentType() {
        int ii = _top - 2;
        while (ii >= 0) {
            int type = _patch_types[_patch_stack[ii]];
            if (type != _Private_IonConstants.tidTypedecl) return type;
            ii--;
        }
        return _Private_IonConstants.tidDATAGRAM;
    }

    private final void startValue(int value_type)
        throws IOException
    {
        super.startValue();

        int[] sids = null;
        int sid_count = annotationCount();
        if (sid_count > 0) {
            // prepare the SIDs of annotations before doing the patch as this may
            // fail and leave the Writer in undefined state
            sids = super.internAnnotationsAndGetSids();
            _patch.startPatch(_Private_IonConstants.tidTypedecl, _writer.position());
        } else {
            _patch.startPatch(value_type, _writer.position());
        }
        // write field name
        if (_in_struct) {
            if (!isFieldNameSet()) {
                throw new IllegalStateException(ERROR_MISSING_FIELD_NAME);
            }
            int sid = super.getFieldId();
            if (sid < 0) {
                throw new UnsupportedOperationException("symbol resolution must be handled by the user writer");
            }
            int fieldNameLength = _writer.writeVarUIntValue(sid, true);
            _patch.patchFieldName(fieldNameLength);
            clearFieldName();
        }

        // write annotations
        if (sid_count > 0) {
            _patch = _patch.addChild();
            // add all annotations as if it's a value with type = -1
            _patch.startPatch(TID_ANNOTATION_PATCH, _writer.position());
            int len = 0;
            for (int ii=0; ii<sid_count; ii++) {
                len += _writer.writeVarUIntValue(sids[ii], true);
            }
            _patch.patchValue(len);
            _patch.endPatch();
            clearAnnotations();
            // add the actual value now
            _patch.startPatch(value_type, _writer.position());
        }
    }

    private final void closeValue()
        throws IOException
    {
        super.endValue();
        _patch.endPatch();
        if (_patch.getParent() != null
            && _patch.getParent().getType() == _Private_IonConstants.tidTypedecl)
        {
            // this is an annotated value, gotta get out
            _patch = _patch.getParent();
            _patch.endPatch();
            assert _patch != null;
        }
    }


    /**
     * {@inheritDoc}
     * <p>
     * The {@link OutputStream} spec is mum regarding the behavior of flush on
     * a closed stream, so we shouldn't assume that our stream can handle that.
     */
    public final void flush() throws IOException
    {
        if (! _closed)
        {
            if (atDatagramLevel() && ! hasAnnotations())
            {
                SymbolTable symtab = getSymbolTable();

                if (symtab != null &&
                    symtab.isReadOnly() &&
                    symtab.isLocalTable())
                {
                    // It's no longer possible to add more symbols to the local
                    // symtab, so we can safely write everything out.
                    writeAllBufferedData();
                }
            }

            _user_output_stream.flush();
        }
    }

    public final void close() throws IOException
    {
        if (! _closed) {
            try
            {
                if (getDepth() == 0) {
                    finish();
                }
            }
            finally
            {
                // Do this first so we are closed even if the call below throws.
                _closed = true;

                _user_output_stream.close();
            }
        }
    }



    @Override
    void writeIonVersionMarkerAsIs(SymbolTable systemSymtab)
        throws IOException
    {
        if (_user_depth != 0) {
            throw new IllegalStateException("IVM not on top-level");
        }
        super.startValue();
        _patch.startPatch(TID_RAW, _writer.position());
        _patch.patchValue(4);
        _writer.write(_Private_IonConstants.BINARY_VERSION_MARKER_1_0);
        _patch.endPatch();
        super.endValue();
    }

    @Override
    void writeLocalSymtab(SymbolTable symbols)
        throws IOException
    {
        // this method *should* be called when @_patch is a top-level value, but
        // we cannot be sure, so try to find the top-level anyway
        PatchedValues top;
        // find the parent
        for (top = _patch; top.getParent() != null; top = top.getParent()) {}
        super.startValue();
        top.injectSymbolTable(symbols, _patch.getParent() != null);
        super.endValue();
        super.writeLocalSymtab(symbols);
    }

    public final void stepIn(IonType containerType) throws IOException
    {
        int tid;
        switch (containerType)
        {
            case LIST:   tid = tidList;   break;
            case SEXP:   tid = tidSexp;   break;
            case STRUCT: tid = tidStruct; break;
            default:
                throw new IllegalArgumentException();
        }
        startValue(tid);
        _patch = _patch.addChild();
        _in_struct = (tid == tidStruct);
        ++_user_depth;
    }

    public final void stepOut() throws IOException
    {
        if (_patch.getParent() == null) {
            throw new IllegalStateException(IonMessages.CANNOT_STEP_OUT);
        }
        // container is over, getting out
        _patch = _patch.getParent();
        // now close current value
        closeValue();
        if (_patch.getParent() == null) {
            _in_struct = false;
            // we're on top-level
            if (_auto_flush) {
                flush();
            }
        } else {
            _in_struct = (_patch.getParent().getType() == _Private_IonConstants.tidStruct);
        }
        --_user_depth;
    }


    public void writeNull(IonType type) throws IOException
    {
        int tid;
        switch (type) {
        case NULL:      tid = _Private_IonConstants.tidNull;      break;
        case BOOL:      tid = _Private_IonConstants.tidBoolean;   break;
        case INT:       tid = _Private_IonConstants.tidPosInt;    break;
        case FLOAT:     tid = _Private_IonConstants.tidFloat;     break;
        case DECIMAL:   tid = _Private_IonConstants.tidDecimal;   break;
        case TIMESTAMP: tid = _Private_IonConstants.tidTimestamp; break;
        case SYMBOL:    tid = _Private_IonConstants.tidSymbol;    break;
        case STRING:    tid = _Private_IonConstants.tidString;    break;
        case BLOB:      tid = _Private_IonConstants.tidBlob;      break;
        case CLOB:      tid = _Private_IonConstants.tidClob;      break;
        case SEXP:      tid = _Private_IonConstants.tidSexp;      break;
        case LIST:      tid = _Private_IonConstants.tidList;      break;
        case STRUCT:    tid = _Private_IonConstants.tidStruct;    break;
        default:
            throw new IllegalArgumentException("Invalid type: " + type);
        }
        startValue(TID_RAW);
        _writer.write((tid << 4) | _Private_IonConstants.lnIsNull);
        _patch.patchValue(1);
        closeValue();
    }

    public void writeBool(boolean value) throws IOException
    {
        int ln = value ? _Private_IonConstants.lnBooleanTrue : _Private_IonConstants.lnBooleanFalse;
        startValue(TID_RAW);
        _writer.write((_Private_IonConstants.tidBoolean << 4) | ln);
        _patch.patchValue(1);
        closeValue();
    }
    public void writeInt(long value) throws IOException
    {
        int len;
        if (value < 0) {
            startValue(_Private_IonConstants.tidNegInt);
            len = _writer.writeUIntValue(-value);
        } else {
            startValue(_Private_IonConstants.tidPosInt);
            len = _writer.writeUIntValue(value);
        }
        _patch.patchValue(len);
        closeValue();
    }

    public void writeInt(BigInteger value) throws IOException
    {
        if (value == null) {
            writeNull(IonType.INT);
            return;
        }

        boolean     is_negative = (value.signum() < 0);
        BigInteger  positive = value;

        if (is_negative) {
            positive = value.negate();
        }

        int len = IonBinary.lenIonInt(positive);
        startValue(is_negative ? _Private_IonConstants.tidNegInt : _Private_IonConstants.tidPosInt);
        _writer.writeUIntValue(positive, len);
        _patch.patchValue(len);

        closeValue();
    }

    public void writeFloat(double value) throws IOException
    {
        int len = IonBinary.lenIonFloat(value);
        startValue(_Private_IonConstants.tidFloat); // int's are always less than varlen long
        len = _writer.writeFloatValue(value);
        _patch.patchValue(len);
        closeValue();
    }

    @Override
    public void writeDecimal(BigDecimal value) throws IOException
    {
        if (value == null) {
            writeNull(IonType.DECIMAL);
            return;
        }
        startValue(_Private_IonConstants.tidDecimal);
        int len = _writer.writeDecimalContent(value);
        _patch.patchValue(len);
        closeValue();
    }

    public void writeTimestamp(Timestamp value) throws IOException
    {
        if (value == null) {
            writeNull(IonType.TIMESTAMP);
            return;
        }
        startValue(_Private_IonConstants.tidTimestamp);
        int len = _writer.writeTimestamp(value);
        _patch.patchValue(len);
        closeValue();
    }

    public void writeString(String value) throws IOException
    {
        if (value == null) {
            writeNull(IonType.STRING);
            return;
        }
        startValue(_Private_IonConstants.tidString);
        int len = _writer.writeStringData(value);
        _patch.patchValue(len);
        closeValue();
    }

    @Override
    void writeSymbolAsIs(int symbolId) throws IOException
    {
        startValue(_Private_IonConstants.tidSymbol);
        int len = _writer.writeUIntValue(symbolId);
        _patch.patchValue(len);
        closeValue();
    }

    @Override
    public void writeSymbolAsIs(String value) throws IOException
    {
        if (value == null) {
            writeNull(IonType.SYMBOL);
            return;
        }
        int sid = add_symbol(value);
        writeSymbolAsIs(sid);
    }

    public void writeClob(byte[] value, int start, int len) throws IOException
    {
        if (value == null) {
            writeNull(IonType.CLOB);
            return;
        }
        if (start < 0 || len < 0 || start+len > value.length) {
            throw new IllegalArgumentException("the start and len must be contained in the byte array");
        }
        startValue(_Private_IonConstants.tidClob);
        _writer.write(value, start, len);
        _patch.patchValue(len);
        closeValue();
    }

    public void writeBlob(byte[] value, int start, int len) throws IOException
    {
        if (value == null) {
            writeNull(IonType.BLOB);
            return;
        }
        if (start < 0 || len < 0 || start+len > value.length) {
            throw new IllegalArgumentException("the start and len must be contained in the byte array");
        }
        startValue(_Private_IonConstants.tidBlob);
        _writer.write(value, start, len);
        _patch.patchValue(len);
        closeValue();
    }

    // just transfer the bytes into the current patch as 'proper' ion binary serialization
    public void writeRaw(byte[] value, int start, int len) throws IOException
    {
        startValue(TID_RAW);
        _writer.write(value, start, len);
        _patch.patchValue(len);
        closeValue();
    }

    public void writeBoolList(boolean[] values) throws IOException
    {
        stepIn(IonType.LIST);
        for (boolean b : values) {
            writeBool(b);
        }
        stepOut();
    }

    public void writeIntList(byte[] values) throws IOException
    {
        stepIn(IonType.LIST);
        for (byte b : values) {
            writeInt(b);
        }
        stepOut();
    }

    public void writeIntList(short[] values) throws IOException
    {
        stepIn(IonType.LIST);
        for (short s : values) {
            writeInt(s);
        }
        stepOut();
    }

    public void writeIntList(int[] values) throws IOException
    {
        stepIn(IonType.LIST);
        for (int i : values) {
            writeInt(i);
        }
        stepOut();
    }

    public void writeIntList(long[] values) throws IOException
    {
        stepIn(IonType.LIST);
        for (long l : values) {
            writeInt(l);
        }
        stepOut();
    }

    public void writeFloatList(float[] values) throws IOException
    {
        stepIn(IonType.LIST);
        for (float f : values) {
            writeFloat(f);
        }
        stepOut();
    }

    public void writeFloatList(double[] values) throws IOException
    {
        stepIn(IonType.LIST);
        for (double d : values) {
            writeFloat(d);
        }
        stepOut();
    }

    public void writeStringList(String[] values) throws IOException
    {
        stepIn(IonType.LIST);
        for (String s : values) {
            writeString(s);
        }
        stepOut();
    }

    // TODO make private after IonBinaryWriter is removed
    /**
     * Writes everything we've got into the output stream, performing all
     * necessary patches along the way.
     *
     * This implements {@link com.amazon.ion.IonBinaryWriter#writeBytes(OutputStream)}
     * via our subclass {@link IonWriterBinaryCompatibility.System}.
     */
    int writeBytes(OutputStream userstream) throws IOException {
        if (_patch.getParent() != null) {
            throw new IllegalStateException("Tried to flush while not on top-level");
        }

        try {
            BlockedByteInputStream datastream =
                new BlockedByteInputStream(_manager.buffer());
            int size = writeRecursive(datastream, userstream, _patch);
            return size;
        } finally {
            _patch.reset();
        }
    }

    int writeRecursive(BlockedByteInputStream datastream, OutputStream userstream, PatchedValues p) throws IOException {
        int totalSize = 0;
        for (int i = 0; i <= p._freePos; ++i) {
            int type = p._types[i];
            int pos = p._positions[i];
            int fnlen = (int)(p._lengths[i] >> 32);
            int vallen = (int)(p._lengths[i] & 0xFFFFFFFF);
            if (p.getParent() == null) {
                if (pos > totalSize) {
                    // write whatever data that we have in the datastream (eg external data)
                    datastream.writeTo(userstream, pos - totalSize);
                    totalSize = pos;
                }
                totalSize += fnlen + vallen;
            }
            // write member name
            if (fnlen > 0) {
                datastream.writeTo(userstream, fnlen);
            }
            switch (type) {
            case TID_ANNOTATION_PATCH:
                IonBinary.writeVarUInt(userstream, vallen);
                datastream.writeTo(userstream, vallen);
                break;

            case TID_SYMBOL_TABLE_PATCH:
                SymbolTable symtab = p._symtabs.remove();
                if (!symtab.isSystemTable()) {
                    byte[] symtabBytes = reverseEncode(1024, symtab);
                    userstream.write(symtabBytes);
                    totalSize += symtabBytes.length;
                }
                break;

            case TID_RAW:
                datastream.writeTo(userstream, vallen);
                break;

            default:
                // write type
                if (vallen >= _Private_IonConstants.lnIsVarLen) {
                    int typeByte = (type << 4) | _Private_IonConstants.lnIsVarLen;
                    userstream.write(typeByte);
                    IonBinary.writeVarUInt(userstream, vallen);
                } else {
                    int typeByte = (type << 4) | vallen;
                    userstream.write(typeByte);
                }
                switch (type) {
                case _Private_IonConstants.tidList:
                case _Private_IonConstants.tidSexp:
                case _Private_IonConstants.tidStruct:
                case _Private_IonConstants.tidTypedecl:
                    // write the container
                    assert p._children != null;
                    writeRecursive(datastream, userstream, p._children.remove());
                    break;

                default:
                    datastream.writeTo(userstream, vallen);
                }
            }
        }
        return totalSize;
    }

    static class CountingStream extends OutputStream
    {
        private final OutputStream _wrapped;
        private       int          _written;

        CountingStream(OutputStream userstream) {
            _wrapped = userstream;
        }

        public int getBytesWritten() {
            return _written;
        }

        @Override
        public void write(int b) throws IOException
        {
            _wrapped.write(b);
            _written++;
        }
        @Override
        public void write(byte[] bytes) throws IOException
        {
            _wrapped.write(bytes);
            _written += bytes.length;
        }
        @Override
        public void write(byte[] bytes, int off, int len) throws IOException
        {
            _wrapped.write(bytes, off, len);
            _written += len;
        }

    }

    protected int write_symbol_table(OutputStream userstream,
                                     SymbolTable symtab) throws IOException
    {
        CountingStream cs = new CountingStream(userstream);
        // TODO this is assuming the symtab needed here, broken for open content.
        IonWriterSystemBinary writer =
            new IonWriterSystemBinary(_default_system_symbol_table,
                                      cs,
                                      false /* autoflush */ ,
                                      false /* ensureInitialIvm */);
        symtab.writeTo(writer);
        writer.finish();
        int symtab_len = cs.getBytesWritten();
        return symtab_len;
    }

    protected int XXX_get_pending_length_with_no_symbol_tables()
    {
        int buffer_length = _manager.buffer().size();
        int patch_amount = 0;

        for (int patch_idx = 0; patch_idx < _patch_count; patch_idx ++) {
            // int vlen = _patch_list[patch_idx + IonBinaryWriter.POSITION_OFFSET];
            int vlen = _patch_lengths[patch_idx];
            if (vlen >= _Private_IonConstants.lnIsVarLen) {
                int ln = IonBinary.lenVarUInt(vlen);
                patch_amount += ln;
            }
        }

        int symbol_table_length = 0;

        int total_length = 0;
        total_length += buffer_length
                     +  patch_amount
                     +  symbol_table_length;

        return total_length;
    }
}
