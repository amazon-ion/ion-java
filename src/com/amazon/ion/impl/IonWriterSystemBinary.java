// Copyright (c) 2010-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.SystemSymbols.ION_1_0_SID;
import static com.amazon.ion.impl.IonConstants.tidDATAGRAM;
import static com.amazon.ion.impl.IonConstants.tidList;
import static com.amazon.ion.impl.IonConstants.tidSexp;
import static com.amazon.ion.impl.IonConstants.tidStruct;

import com.amazon.ion.IonException;
import com.amazon.ion.IonType;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SystemSymbols;
import com.amazon.ion.Timestamp;
import com.amazon.ion.impl.BlockedBuffer.BlockedByteInputStream;
import com.amazon.ion.impl.IonBinary.BufferManager;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;

/**
 *
 */
public final class IonWriterSystemBinary  // TODO protect, must fix IonStreamUtils
    extends IonWriterSystem
{
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

    /** Forces IVM in the event the caller forgets to write an IVM. */
    private boolean _ensure_initial_ivm;

    boolean           _in_struct;

    /** Ensure we don't use a closed {@link #output} stream. */
    private boolean _closed;

    private final static int TID_FOR_SYMBOL_TABLE_PATCH = IonConstants.tidDATAGRAM + 1;
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
        super(defaultSystemSymtab);

        out.getClass(); // Efficient null check
        _user_output_stream = out;

        // the buffer manager and writer
        // are used to hold the buffered
        // binary values pending flush().
        _manager = new BufferManager();
        _writer = _manager.openWriter();
        _auto_flush = autoFlush;
        _ensure_initial_ivm = ensureInitialIvm;
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
        _ensure_initial_ivm = true;
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
        return (topType() == IonConstants.tidDATAGRAM);
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


    private final void push(int typeid) {
        int pos = _writer.position();
        if (_top >= _patch_stack.length) {
            growStack();
        }
        if (_patch_count >= _patch_lengths.length) {// _patch_list.length) {
            growList();
        }
        _patch_stack[_top++] = _patch_count;

        _patch_lengths[_patch_count]   = 0;
        _patch_offsets[_patch_count]   = pos;
        _patch_table_idx[_patch_count] = NOT_A_SYMBOL_TABLE_IDX;
        _patch_types[_patch_count]     = typeid;
        _patch_in_struct[_patch_count] = _in_struct;
        _patch_count++;
    }

    private final void growStack() {
        int oldlen = _patch_stack.length;
        int newlen = oldlen * 2;
        int[] temp = new int[newlen];
        System.arraycopy(_patch_stack, 0, temp, 0, oldlen);
        _patch_stack = temp;
    }

    private final void growList() {
        int oldlen = _patch_lengths.length ;
        int newlen = oldlen * 2; // _patch_list.length * 2;
        int[] temp1 = new int[newlen];
        int[] temp2 = new int[newlen];
        int[] temp3 = new int[newlen];
        int[] temp4 = new int[newlen];
        boolean[] temp5 = new boolean[newlen];

        System.arraycopy(_patch_lengths,   0, temp1, 0, oldlen);
        System.arraycopy(_patch_offsets,   0, temp2, 0, oldlen);
        System.arraycopy(_patch_table_idx, 0, temp3, 0, oldlen);
        System.arraycopy(_patch_types,     0, temp4, 0, oldlen);
        System.arraycopy(_patch_in_struct, 0, temp5, 0, oldlen);

        _patch_lengths   = temp1;
        _patch_offsets   = temp2;
        _patch_table_idx = temp3;
        _patch_types     = temp4;
        _patch_in_struct = temp5;
    }

    private final void growSymbolPatchList() {
        int oldlen = _patch_symbol_tables.length;
        int newlen = oldlen * 2;

        SymbolTable[] temp1 = new SymbolTable[newlen];

        System.arraycopy(_patch_symbol_tables, 0, temp1, 0, oldlen);

        _patch_symbol_tables = temp1;
    }

    final void patch(int addedLength) {
        if (addedLength > 0 && _top > 0) {
            int patch_id = _patch_stack[_top - 1];
            _patch_lengths[patch_id] += addedLength;
        }
//        for (int ii = 0; ii < _top; ii++) {
//            int patch_id = _patch_stack[ii];
//            _patch_lengths[patch_id] += addedLength;
//        }
    }


    private final void set_symbol_table_prepend_new_local_table(SymbolTable symbols) throws IOException
    {

        // patch this symbol table in "here" at the end
        // which will grow the arrays as necessary
        patchInSymbolTable(symbols);

        if (_top == 0) {
            // if we're at the datagram level already
            // we don't need to back-track to our parent
            // so we're done
            return;
        }

        // grab the patch values we'll need to set
        int target_idx       = _patch_stack[0];
        int pos              = _patch_offsets[target_idx]; // the offset of our top most patch point
        int symbol_patch_idx = _patch_symbol_table_count - 1;
        int patch_idx        = _patch_count - 1;

        // the top level open container is at _patch_stack[0]
        // so we move everything from that point down one
        // and write the values saved above over them
        for (int ii=patch_idx; ii>target_idx; ii--) {
            int jj = ii - 1;
            _patch_lengths[ii]   = _patch_lengths[jj];
            _patch_offsets[ii]   = _patch_offsets[jj];
            _patch_table_idx[ii] = _patch_table_idx[jj];
            _patch_types[ii]     = _patch_types[jj];
            _patch_in_struct[ii] = _patch_in_struct[jj];
        }
        _patch_lengths[target_idx]   = 0;
        _patch_offsets[target_idx]   = pos;
        _patch_table_idx[target_idx] = symbol_patch_idx;
        _patch_types[target_idx]     = TID_FOR_SYMBOL_TABLE_PATCH;
        _patch_in_struct[target_idx] = false;

        // fix up the patch stack appropriately (patch)
        //_patch_stack[_top++] = _patch_count; - add 1 to each since the index is updated
        for (int ii = 0; ii < _top; ii++) {
            _patch_stack[ii]++;
        }
    }

    @Override
    final UnifiedSymbolTable inject_local_symbol_table() throws IOException
    {
        UnifiedSymbolTable symbols = super.inject_local_symbol_table();
        set_symbol_table_prepend_new_local_table(symbols);
        return symbols;
    }

    final void patchInSymbolTable(SymbolTable symbols) throws IOException
    {
        if (_ensure_initial_ivm) {
            // we have to check for this here since we
            // may be patching a symbol table in before
            // the version marker has been written
            writeIonVersionMarker();
        }

        int pos = _writer.position();

        if (_patch_count >= _patch_lengths.length) {
            growList();
        }
        if (_patch_symbol_table_count > _patch_symbol_tables.length) {
            growSymbolPatchList();
        }
        int table_idx = _patch_symbol_table_count++;
        _patch_symbol_tables[table_idx]         = symbols;
        _patch_lengths[_patch_count]            = 0;
        _patch_table_idx[_patch_count]          = table_idx;
        _patch_offsets[_patch_count]            = pos;
        _patch_types[_patch_count]              = TID_FOR_SYMBOL_TABLE_PATCH;
        _patch_in_struct[_patch_count]          = false;
        _patch_count++;
    }

    private final void pop() {
        // first grab the length since this container will now be
        // closed and fixed, we'll back patch it's len 'o len into
        // it's parents -- after we pop it off the stack
        int len = topLength();

        // patch to top length into our next lower value, since they
        // didn't get updated incrementally, then next-next lower
        // value will get updated when the next one pops off
        int ii=_top - 2;
        if (ii>=0) {
            _patch_lengths[_patch_stack[ii]] += len;
        }

        int lenolen = IonBinary.lenLenFieldWithOptionalNibble(len);
        _top--;
        if (lenolen > 0) {
            patch(lenolen);
        }
    }

    private final int topLength() {
        return _patch_lengths[_patch_stack[_top - 1]];
    }

    private final int topType() {
        if (_top == 0) return IonConstants.tidDATAGRAM;
        return _patch_types[_patch_stack[_top - 1]];
    }

    private final int parentType() {
        int ii = _top - 2;
        while (ii >= 0) {
            int type = _patch_types[_patch_stack[ii]];
            if (type != IonConstants.tidTypedecl) return type;
            ii--;
        }
        return IonConstants.tidDATAGRAM;
    }

    private final void startValue(IonType value_type, int value_length)
        throws IOException
    {
        int patch_len = 0;

        if (_ensure_initial_ivm) {
            writeIonVersionMarker();
        }

        // write field name
        if (_in_struct) {
            if (!isFieldNameSet()) {
                throw new IllegalStateException(ERROR_MISSING_FIELD_NAME);
            }
            int sid = super.getFieldId();
            if (sid < 1) {
                throw new UnsupportedOperationException("symbol resolution must be handled by the user writer");
            }
            patch_len += _writer.writeVarUIntValue(sid, true);
            clearFieldName();
        }

        // write annotations
        int annotations_len = 0;
        int sid_count = annotationCount();
        if (sid_count > 0) {
            int[] sids = super.internAnnotationsAndGetSids();

            // FIRST add up the length of the annotation symbols as they'll appear in the buffer
            for (int ii=0; ii<sid_count; ii++) {
                assert sids[ii] > 0;
                annotations_len += IonBinary.lenVarUInt(sids[ii]);
            }

            // THEN write the td byte, optional annotations, this is before the caller
            //      writes out the actual values (plain value) td byte and varlen
            //      an annotation is just like any parent collection header in that it needs
            //      to be in our stack for length patching purposes
            int td = (IonConstants.tidTypedecl << 4);
            if (value_length == UNKNOWN_LENGTH) {
                // if we don't know the value length we push a
                // patch point onto the backpatch stack - but first we
                // patch our parent with the fieldid len and the annotation
                // type desc byte
                patch(patch_len + 1);
                patch_len = 0;          // with the call to patch, we've taken care of the patch length
                push(IonConstants.tidTypedecl);
                _writer.write((byte)td);
            }
            else {
                // if we know the value length we can write the
                // annotation header out in full (and avoid the back patching)
                // <ann,ln><totallen><annlen><ann><valuetd,ln><valuelen><value>
                int annotation_len_o_len = IonBinary.lenVarUInt(annotations_len); // len(<annlen>)
                int total_ann_value_len = annotation_len_o_len + annotations_len + value_length;
                if (total_ann_value_len < IonConstants.lnIsVarLen) {
                    td |= (total_ann_value_len & 0xf);
                    _writer.write((byte)td);
                }
                else {
                    td |= (IonConstants.lnIsVarLen & 0xf);
                    _writer.write((byte)td);
                    // add the len of len to the patch total (since we just learned about it)
                    patch_len += _writer.writeVarUIntValue(total_ann_value_len, true);
                }
                patch_len++;  // the size of the ann type desc byte
            }

            patch_len += annotations_len;
            patch_len += _writer.writeVarUIntValue(annotations_len, true);                              /// CAS late night added "len +="
            for (int ii=0; ii<sid_count; ii++) {
                // note that len already has the sum of the actual lengths
                // added into it so that we could write it out in front
                _writer.writeVarUIntValue(sids[ii], true);
            }
            // we patch any wrapper the annotation is in with whatever we wrote here
            clearAnnotations();
        }
        if (patch_len > 0) {
            patch(patch_len);
        }
    }

    private final void closeValue()
        throws IOException
    {
        int topType = topType();

        // check for annotations, which we need to pop off now
        // since once we close a value out, we won't need to patch
        // the annotations it (might have) had
        if (topType == IonConstants.tidTypedecl) {
            pop();
        }
        else if ((topType == IonConstants.tidDATAGRAM) && _auto_flush) {
            this.flush();
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
    void writeIonVersionMarker(SymbolTable systemSymtab)
        throws IOException
    {
        if (!atDatagramLevel()) {
            throw new IllegalStateException("you can only write Ion Version Markers when you are at the datagram level");
        }
        if (! SystemSymbols.ION_1_0.equals(systemSymtab.getIonVersionId())) {
            throw new UnsupportedOperationException("This library only supports Ion 1.0");
        }

        _writer.write(IonConstants.BINARY_VERSION_MARKER_1_0);
        _ensure_initial_ivm = false;  // we've done our job, we can turn this off now

        super.writeIonVersionMarker(systemSymtab);
    }

    @Override
    void writeLocalSymtab(SymbolTable symtab)
        throws IOException
    {
        patchInSymbolTable(symtab);
        super.writeLocalSymtab(symtab);
    }



    public final void stepIn(IonType containerType) throws IOException
    {
        startValue(containerType, UNKNOWN_LENGTH);
        patch(1);

        int tid;
        switch (containerType)
        {
            case LIST:   tid = tidList;   _in_struct = false; break;
            case SEXP:   tid = tidSexp;   _in_struct = false; break;
            case STRUCT: tid = tidStruct; _in_struct = true;  break;
            default:
                throw new IllegalArgumentException();
        }

        push(tid);
        _writer.writeByte((byte)(tid << 4));
        _user_depth++;
    }

    public final void stepOut() throws IOException
    {
        if (_top < 1) {
            throw new IllegalStateException(IonMessages.CANNOT_STEP_OUT);
        }
        pop();
        closeValue();
        _in_struct = this.topInStruct();
        _user_depth--;
    }


    public void writeNull(IonType type) throws IOException
    {
        startValue(IonType.NULL, 1);
        int tid = -1;
        switch (type) {
        case NULL:      tid = IonConstants.tidNull;      break;
        case BOOL:      tid = IonConstants.tidBoolean;   break;
        case INT:       tid = IonConstants.tidPosInt;    break;
        case FLOAT:     tid = IonConstants.tidFloat;     break;
        case DECIMAL:   tid = IonConstants.tidDecimal;   break;
        case TIMESTAMP: tid = IonConstants.tidTimestamp; break;
        case SYMBOL:    tid = IonConstants.tidSymbol;    break;
        case STRING:    tid = IonConstants.tidString;    break;
        case BLOB:      tid = IonConstants.tidBlob;      break;
        case CLOB:      tid = IonConstants.tidClob;      break;
        case SEXP:      tid = IonConstants.tidSexp;      break;
        case LIST:      tid = IonConstants.tidList;      break;
        case STRUCT:    tid = IonConstants.tidStruct;    break;
        default: throw new IllegalArgumentException("Invalid type: " + type);
        }
        _writer.write((tid << 4) | IonConstants.lnIsNullAtom);
        patch(1);
    }
    public void writeBool(boolean value) throws IOException
    {
        startValue(IonType.BOOL, 1);
        int ln = value ? IonConstants.lnBooleanTrue : IonConstants.lnBooleanFalse;
        _writer.write((IonConstants.tidBoolean << 4) | ln);
        patch(1);
    }
    public void writeInt(int value) throws IOException
    {
        int len = IonBinary.lenIonInt(value);
        startValue(IonType.INT, len + 1); // int's are always less than varlen long
        if (value < 0) {
            _writer.write((IonConstants.tidNegInt << 4) | len);
            _writer.writeUIntValue(-value, len);
        }
        else {
            _writer.write((IonConstants.tidPosInt << 4) | len);
            _writer.writeUIntValue(value, len);
        }
        patch(1 + len);
    }
    public void writeInt(long value) throws IOException
    {
        int len = IonBinary.lenIonInt(value);
        startValue(IonType.INT, len + 1); // int's are always less than varlen long
        if (value < 0) {
            _writer.write((IonConstants.tidNegInt << 4) | len);
            _writer.writeUIntValue(-value, len);
        }
        else {
            _writer.write((IonConstants.tidPosInt << 4) | len);
            _writer.writeUIntValue(value, len);
        }
        patch(1 + len);
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
        int ln = len;
        int patch_len = 1 + len;
        if (len >= IonConstants.lnIsVarLen) {
            ln = IonConstants.lnIsVarLen;
            patch_len += IonBinary.lenVarUInt(len);
        }
        startValue(IonType.INT, patch_len);

        if (is_negative) {
            _writer.write((IonConstants.tidNegInt << 4) | ln);

        }
        else {
            _writer.write((IonConstants.tidPosInt << 4) | ln);
        }
        if (ln == IonConstants.lnIsVarLen) {
            _writer.writeVarUIntValue(len, false);
        }

        int wroteLen = _writer.writeUIntValue(positive, len);
        assert wroteLen == len;
        patch(patch_len);

    }

    public void writeFloat(double value) throws IOException
    {
        int len = IonBinary.lenIonFloat(value);
        startValue(IonType.FLOAT, len + 1); // int's are always less than varlen long
        _writer.write((IonConstants.tidFloat << 4) | len);
        len = _writer.writeFloatValue(value);
        patch(1 + len);
    }

    @Override
    public void writeDecimal(BigDecimal value) throws IOException
    {
        if (value == null) {
            writeNull(IonType.DECIMAL);
            return;
        }

        int patch_len = 1;
        int len = IonBinary.lenIonDecimal(value, false);
        int ln = len;
        if (len >= IonConstants.lnIsVarLen) {
            ln = IonConstants.lnIsVarLen;
            patch_len += IonBinary.lenVarUInt(len);
        }

        startValue(IonType.DECIMAL, patch_len + len);

        _writer.write((IonConstants.tidDecimal << 4) | ln);
        if (len >= IonConstants.lnIsVarLen) {
            _writer.writeVarUIntValue(len, true);
        }
        int wroteLen = _writer.writeDecimalContent(value, false);
        assert wroteLen == len;
        patch_len += wroteLen;
        patch(patch_len);
    }

    public void writeTimestamp(Timestamp value) throws IOException
    {
        if (value == null) {
            writeNull(IonType.TIMESTAMP);
            return;
        }

        Timestamp di = value;

        int patch_len = 1;
        int len = IonBinary.lenIonTimestamp(di);

        int ln = len;
        if (len >= IonConstants.lnIsVarLen) {
            ln = IonConstants.lnIsVarLen;
            patch_len += IonBinary.lenVarUInt(len);
        }

        startValue(IonType.TIMESTAMP, patch_len + len); // int's are always less than varlen long

        _writer.write((IonConstants.tidTimestamp << 4) | ln);
        if (len >= IonConstants.lnIsVarLen) {
            _writer.writeVarUIntValue(len, true);
        }
        patch_len += _writer.writeTimestamp(di);
        patch(patch_len);
    }
    public void writeString(String value) throws IOException
    {
        if (value == null) {
            writeNull(IonType.STRING);
            return;
        }

        int patch_len = 1;
        int len = IonBinary.lenIonString(value);

        int ln = len;
        if (len >= IonConstants.lnIsVarLen) {
            ln = IonConstants.lnIsVarLen;
            patch_len += IonBinary.lenVarUInt(len);
        }

        startValue(IonType.STRING, patch_len + len);

        _writer.write((IonConstants.tidString << 4) | ln);
        if (len >= IonConstants.lnIsVarLen) {
            _writer.writeVarUIntValue(len, true);
        }
        patch_len += _writer.writeStringData(value);
        patch(patch_len);
    }

    public void writeSymbol(int symbolId) throws IOException
    {
        if (symbolId == ION_1_0_SID && atDatagramLevel()) {
            // the $ion_1_0 symbol at the datagram level is ALWAYS
            // an ion version marker
            writeIonVersionMarker();
        }
        else {
            int patch_len = 1;
            int len = IonBinary.lenUInt(symbolId);
            startValue(IonType.SYMBOL, len + 1);
            _writer.write((IonConstants.tidSymbol << 4) | len);
            patch_len += _writer.writeUIntValue(symbolId, len);
            patch(patch_len);
        }
    }

    public void writeSymbol(String value) throws IOException
    {
        if (value == null) {
            writeNull(IonType.SYMBOL);
            return;
        }

        int sid = add_symbol(value);
        writeSymbol(sid);
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

        int patch_len = 1;
        int ln = len;
        if (len >= IonConstants.lnIsVarLen) {
            ln = IonConstants.lnIsVarLen;
            patch_len += IonBinary.lenVarUInt(len);
        }

        startValue(IonType.CLOB, patch_len + len);

        _writer.write((IonConstants.tidClob << 4) | ln);
        if (len >= IonConstants.lnIsVarLen) {
            _writer.writeVarUIntValue(len, true);
        }
        _writer.write(value, start, len);
        patch_len += len;
        patch(patch_len);
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

        int patch_len = 1;
        int ln = len;
        if (len >= IonConstants.lnIsVarLen) {
            ln = IonConstants.lnIsVarLen;
            patch_len += IonBinary.lenVarUInt(len);
        }

        startValue(IonType.BLOB, patch_len + len); // int's are always less than varlen long

        _writer.write((IonConstants.tidBlob << 4) | ln);
        if (len >= IonConstants.lnIsVarLen) {
            _writer.writeVarUIntValue(len, true);
        }
        _writer.write(value, start, len);
        patch_len += len;
        patch(patch_len);
    }

    static final int bool_true = (IonConstants.tidBoolean << 4) | IonConstants.lnBooleanTrue;
    static final int bool_false = (IonConstants.tidBoolean << 4) | IonConstants.lnBooleanFalse;
    @Override
    public void writeBoolList(boolean[] values) throws IOException
    {
        int len = values.length; // 1 byte per boolean
        int patch_len = 1;
        int ln = len;
        if (len >= IonConstants.lnIsVarLen) {
            ln = IonConstants.lnIsVarLen;
            patch_len += IonBinary.lenVarUInt(len);
        }

        startValue(IonType.LIST, patch_len + len); // int's are always less than varlen long

        _writer.write((IonConstants.tidList << 4) | ln);
        if (len >= IonConstants.lnIsVarLen) {
            _writer.writeVarUIntValue(len, true);
        }

        for (int ii=0; ii<values.length; ii++) {
            _writer.write(values[ii] ? bool_true : bool_false);
        }
        patch_len += len;
        patch(patch_len);
    }
    static final int int_tid_0   = (IonConstants.tidPosInt << 4);
    static final int int_tid_pos = (IonConstants.tidPosInt << 4) | 1;
    static final int int_tid_neg = (IonConstants.tidNegInt << 4) | 1;
    @Override
    public void writeIntList(byte[] values) throws IOException
    {
        int patch_len = 1;
        int len = 0;
        for (int ii=0; ii<values.length; ii++) {
            len += (values[ii] == 0) ? 1 : 2;
        }

        int ln = len;
        if (len >= IonConstants.lnIsVarLen) {
            ln = IonConstants.lnIsVarLen;
            patch_len += IonBinary.lenVarUInt(len);
        }

        startValue(IonType.LIST, patch_len + len); // int's are always less than varlen long

        _writer.write((IonConstants.tidList << 4) | ln);
        if (len >= IonConstants.lnIsVarLen) {
            _writer.writeVarUIntValue(len, true);
        }

        for (int ii=0; ii<values.length; ii++) {
            int v = values[ii];
            if (v == 0) {
                _writer.write(int_tid_0);
            }
            else if (v < 0) {
                _writer.write(int_tid_neg);
                _writer.writeUIntValue(-v, 1);
            }
            else {
                _writer.write(int_tid_pos);
                _writer.writeUIntValue(v, 1);
            }
        }
        patch_len += len;
        patch(patch_len);
    }
    @Override
    public void writeIntList(short[] values) throws IOException
    {
        int patch_len = 1;
        int len = 0;
        for (int ii=0; ii<values.length; ii++) {
            len += IonBinary.lenIonIntWithTypeDesc((long)values[ii]);
        }
        int ln = len;
        if (len >= IonConstants.lnIsVarLen) {
            ln = IonConstants.lnIsVarLen;
            patch_len += IonBinary.lenVarUInt(len);
        }

        startValue(IonType.LIST, patch_len + len); // int's are always less than varlen long

        _writer.write((IonConstants.tidList << 4) | ln);
        if (len >= IonConstants.lnIsVarLen) {
            _writer.writeVarUIntValue(len, true);
        }

        for (int ii=0; ii<values.length; ii++) {
            int v = values[ii];
            ln = IonBinary.lenIonInt(v);
            if (v == 0) {
                _writer.write(int_tid_0);
            }
            else if (v < 0) {
                _writer.write(int_tid_neg);
                _writer.writeUIntValue(-v, ln);
            }
            else {
                _writer.write(int_tid_pos);
                _writer.writeUIntValue(v, ln);
            }
        }
        patch_len += len;
        patch(patch_len);
    }
    @Override
    public void writeIntList(int[] values) throws IOException
    {
        int tmp;
        int patch_len = 1;
        int len = 0;
        for (int ii=0; ii<values.length; ii++) {
            //len++; cas 22 feb 2008, also changed to withtypedesc below
            tmp = IonBinary.lenIonIntWithTypeDesc((long)values[ii]);
            len += tmp;
        }

        int ln = len;
        if (len >= IonConstants.lnIsVarLen) {
            ln = IonConstants.lnIsVarLen;
            tmp = IonBinary.lenVarUInt(len);
            patch_len += tmp;
        }

        startValue(IonType.LIST, patch_len + len); // int's are always less than varlen long

        _writer.write((IonConstants.tidList << 4) | ln);
        if (len >= IonConstants.lnIsVarLen) {
            tmp = _writer.writeVarUIntValue(len, true);
        }

        for (int ii=0; ii<values.length; ii++) {
            int v = values[ii];
            ln = IonBinary.lenIonInt(v);
            if (v == 0) {
                _writer.write(int_tid_0);
            }
            else if (v < 0) {
                _writer.write(int_tid_neg);
                if (v == Integer.MIN_VALUE) {
                    _writer.writeUIntValue(-((long)v), ln);
                }
                else {
                    _writer.writeUIntValue(-v, ln);
                }
            }
            else {
                _writer.write(int_tid_pos);
                _writer.writeUIntValue(v, ln);
            }
        }
        patch_len += len;
        patch(patch_len);
    }
    @Override
    public void writeIntList(long[] values) throws IOException
    {
        int patch_len = 1;
        int len = 0;
        for (int ii=0; ii<values.length; ii++) {
            //len++; cas 22 feb 2008, also changed to withtypedesc below
            len += IonBinary.lenIonIntWithTypeDesc(values[ii]);
        }

        int ln = len;
        if (len >= IonConstants.lnIsVarLen) {
            ln = IonConstants.lnIsVarLen;
            patch_len += IonBinary.lenVarUInt(len);
        }

        startValue(IonType.LIST, patch_len + len); // int's are always less than varlen long

        _writer.write((IonConstants.tidList << 4) | ln);
        if (len >= IonConstants.lnIsVarLen) {
            _writer.writeVarUIntValue(len, true);
        }

        for (int ii=0; ii<values.length; ii++) {
            long v = values[ii];
            ln = IonBinary.lenIonInt(v);
            if (v == 0) {
                _writer.write(int_tid_0);
            }
            else if (v < 0) {
                _writer.write(int_tid_neg);
                _writer.writeUIntValue(-v, ln);
            }
            else {
                _writer.write(int_tid_pos);
                _writer.writeUIntValue(v, ln);
            }
        }
        patch_len += len;
        patch(patch_len);
    }
    @Override
    public void writeFloatList(float[] values) throws IOException
    {
        int patch_len = 1;
        int len = 0;
        for (int ii=0; ii<values.length; ii++) {
            len++;
            len += IonBinary.lenIonFloat(values[ii]);
        }

        int ln = len;
        if (len >= IonConstants.lnIsVarLen) {
            ln = IonConstants.lnIsVarLen;
            patch_len += IonBinary.lenVarUInt(len);
        }

        startValue(IonType.LIST, patch_len + len); // int's are always less than varlen long

        _writer.write((IonConstants.tidList << 4) | ln);
        if (len >= IonConstants.lnIsVarLen) {
            _writer.writeVarUIntValue(len, true);
        }

        for (int ii=0; ii<values.length; ii++) {
            double v = values[ii];
            ln = IonBinary.lenIonFloat(v);
            _writer.write((IonConstants.tidFloat << 4) | ln);
            _writer.writeFloatValue(v);
        }
        patch_len += len;

        patch(patch_len);
    }
    @Override
    public void writeFloatList(double[] values) throws IOException
    {
        int patch_len = 1;
        int len = 0;
        for (int ii=0; ii<values.length; ii++) {
            len++;
            len += IonBinary.lenIonFloat(values[ii]);
        }

        int ln = len;
        if (len >= IonConstants.lnIsVarLen) {
            ln = IonConstants.lnIsVarLen;
            patch_len += IonBinary.lenVarUInt(len);
        }

        startValue(IonType.LIST, patch_len + len); // int's are always less than varlen long

        _writer.write((IonConstants.tidList << 4) | ln);
        if (len >= IonConstants.lnIsVarLen) {
            _writer.writeVarUIntValue(len, true);
        }

        for (int ii=0; ii<values.length; ii++) {
            double v = values[ii];
            ln = IonBinary.lenIonFloat(v);
            _writer.write((IonConstants.tidFloat << 4) | ln);
            _writer.writeFloatValue(v);
        }
        patch_len += len;

        patch(patch_len);
    }
    @Override
    public void writeStringList(String[] values) throws IOException
    {
        String s;

        int patch_len = 1;
        int len = 0;
        for (int ii=0; ii<values.length; ii++) {
            len++;
            s = values[ii];
            if (s != null) {
                int vlen = IonBinary.lenIonString(s);
                if (vlen >= IonConstants.lnIsVarLen) {
                    len += IonBinary.lenVarUInt(vlen);
                }
                len += vlen;
            }
        }

        int ln = len;
        if (len >= IonConstants.lnIsVarLen) {
            ln = IonConstants.lnIsVarLen;
            patch_len += IonBinary.lenVarUInt(len);
        }

        startValue(IonType.LIST, patch_len + len); // int's are always less than varlen long

        _writer.write((IonConstants.tidList << 4) | ln);
        if (len >= IonConstants.lnIsVarLen) {
            _writer.writeVarUIntValue(len, true);
        }

        for (int ii=0; ii<values.length; ii++) {
            s = values[ii];
            if (s == null) {
                _writer.write((IonConstants.tidString << 4) | IonConstants.lnIsNullAtom);
            }
            else {
                int vlen = IonBinary.lenIonString(s);
                if (vlen < IonConstants.lnIsVarLen) {
                    _writer.write((IonConstants.tidString << 4) | vlen);
                }
                else {
                    _writer.write((IonConstants.tidString << 4) | IonConstants.lnIsVarLen);
                    len += IonBinary.lenVarUInt(vlen);
                }
                _writer.writeStringData(s);
            }
        }
        patch_len += len;

        patch(patch_len);
    }

    // TODO make private after IonBinaryWriter is removed
    /**
     * Writes everything we've got into the output stream, performing all
     * necessary patches along the way.
     *
     * This implements {@link com.amazon.ion.IonBinaryWriter#writeBytes(OutputStream)}
     * via our subclass {@link IonWriterBinaryCompatibility.System}.
     */
    int writeBytes(OutputStream userstream) throws IOException
    {
        int buffer_length = _manager.buffer().size();
        if (buffer_length == 0) return 0;

        int pos = 0;
        int total_written = 0;
        BlockedByteInputStream datastream =
            new BlockedByteInputStream(_manager.buffer());

        int patch_idx = 0;
        int patch_pos;
        if (patch_idx < _patch_count) {
            patch_pos = _patch_offsets[patch_idx];
        }
        else {
            patch_pos = buffer_length + 1;
        }

        // loop through the data buffer merging in
        // symbol table and length patching as needed
        // symbol tables first then lengths if they
        // are at the same offset.  Symbol table
        // patches are represented as a _patch_types[i]
        // of TID_FOR_SYMBOL_TABLE_PATCH.  Then the
        // _patch_length is the index to the symbol table
        // into _patch_symbol_tables[].
        while (pos < buffer_length)
        {
            // first write whatever data needs to be
            // written to get us to the patch location
            if (pos < patch_pos) {
                int len;
                if (patch_pos > buffer_length) {
                    len = buffer_length - pos ;
                }
                else {
                    len = patch_pos - pos ;
                }

                // write the user data
                pos += datastream.writeTo(userstream, len);

                total_written += len;
                if (pos >= buffer_length) break;
            }

            // extract the next patch to emit
            int vlen = _patch_lengths[patch_idx];
            int ptd  = _patch_types[patch_idx];
            switch (ptd) {
            case TID_FOR_SYMBOL_TABLE_PATCH:
                int table_idx = _patch_table_idx[patch_idx];
                SymbolTable symtab = _patch_symbol_tables[table_idx];
                int symtab_len = write_symbol_table(userstream, symtab);
                total_written += symtab_len;
                break;
            case IonConstants.tidNull:      // 0
            case IonConstants.tidBoolean:   // 1
            case IonConstants.tidPosInt:    // 2
            case IonConstants.tidNegInt:    // 3
            case IonConstants.tidFloat:     // 4
            case IonConstants.tidDecimal:   // 5
            case IonConstants.tidTimestamp: // 6
            case IonConstants.tidSymbol:    // 7
            case IonConstants.tidString:    // 8
            case IonConstants.tidClob:      // 9
            case IonConstants.tidBlob:      // 10 A
            case IonConstants.tidList:      // 11 B
            case IonConstants.tidSexp:      // 12 C
            case IonConstants.tidStruct:    // 13 D
            case IonConstants.tidTypedecl:  // 14 E
                // there is not type desc byte for the datagram, but there is
                // for all other containers
                int type_desc_len = IonBinary.writeTypeDescWithLength(userstream, ptd, vlen);
                total_written += type_desc_len;

                // skip the typedesc byte we wrote into the buffered stream
                // TODO: don't waste our time writing the type desc byte
                //       we clearly don't care about
                pos += datastream.skip(1);
                break;

            case IonConstants.tidDATAGRAM:
                // do nothing
                break;
            default:
                throw new IonException("Internal Error: invalid type id ["+ptd+"] encountered while patching binary output");
            }

            // find the next patch point, if there's one left
            patch_idx++;
            if (patch_idx < _patch_count) {
                patch_pos = _patch_offsets[patch_idx];
            }
            else {
                patch_pos = buffer_length + 1;
            }
        }

        return total_written;
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
            if (vlen >= IonConstants.lnIsVarLen) {
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
