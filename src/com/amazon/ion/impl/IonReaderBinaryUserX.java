// Copyright (c) 2009-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.SymbolTable.UNKNOWN_SYMBOL_ID;
import static com.amazon.ion.SystemSymbols.ION_1_0_SID;
import static com.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE_SID;
import static com.amazon.ion.impl._Private_Utils.newLocalSymtab;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.OffsetSpan;
import com.amazon.ion.SeekableReader;
import com.amazon.ion.Span;
import com.amazon.ion.SpanProvider;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.impl.UnifiedInputStreamX.FromByteArray;
import com.amazon.ion.impl.UnifiedSavePointManagerX.SavePoint;
import com.amazon.ion.impl._Private_ScalarConversions.AS_TYPE;
import java.io.IOException;
import java.util.Iterator;

final class IonReaderBinaryUserX
    extends IonReaderBinarySystemX
    implements _Private_ReaderWriter
{
    /**
     * This is the physical start-of-stream offset when this reader was created.
     * It must be subtracted from the logical offsets exposed by
     * {@link OffsetSpan}s.
     */
    private final int _physical_start_offset;

    SymbolTable _symbols;
    IonCatalog  _catalog;

    private static final class IonReaderBinarySpan
        extends DowncastingFaceted
        implements Span, OffsetSpan
    {
        State       _state;
        long        _offset;
        long        _limit;
        SymbolTable _symbol_table;

        public long getStartOffset()
        {
            return _offset;
        }

        public long getFinishOffset()
        {
            return _limit;
        }
    }

    @Deprecated
    public IonReaderBinaryUserX(IonSystem system, IonCatalog catalog,
                                byte[] bytes, int offset, int length)
    {
        super(system, bytes, offset, length);
        _physical_start_offset = offset;
        init_user(catalog);
    }

    public IonReaderBinaryUserX(IonSystem system,
                                IonCatalog catalog,
                                UnifiedInputStreamX userBytes,
                                int physicalStartOffset)
    {
        super(system, userBytes);
        _physical_start_offset = physicalStartOffset;
        init_user(catalog);
    }

    public IonReaderBinaryUserX(IonSystem system,
                                IonCatalog catalog,
                                UnifiedInputStreamX userBytes)
    {
        super(system, userBytes);
        _physical_start_offset = 0;
        init_user(catalog);
    }

    //FIXME: PERF_TEST was :private
    final void init_user(IonCatalog catalog)
    {
        _symbols = _system.getSystemSymbolTable();
        _catalog = catalog;
    }


    /**
     * Determines the abstract position of the reader, such that one can
     * later {@link #seek} back to it.
     * <p>
     * The current implementation only works when the reader is positioned on
     * a value (not before, between, or after values). In other words, one
     * should only call this method when {@link #getType()} is non-null.
     *
     * @return the current position; not null.
     *
     * @throws IllegalStateException if the reader doesn't have a current
     * value.
     */
    public Span getCurrentPosition()
    {
        IonReaderBinarySpan pos = new IonReaderBinarySpan();

        if (getType() == null)
        {
            String message = "IonReader isn't positioned on a value";
            throw new IllegalStateException(message);
        }

        if (_position_start == -1)  // TODO remove? should be unreachable.
        {
            // special case of the position before the first call to next
            pos._offset = _input._pos;
            pos._limit = _input._limit;
            pos._symbol_table = _symbols;
        }
        else
        {
            pos._offset = _position_start - _physical_start_offset;
            pos._limit = pos._offset + _position_len;
            pos._symbol_table = _symbols;
        }

        pos._state = _state;

        return pos;
    }


    public void seek(IonReaderBinarySpan position)
    {
        IonReaderBinarySpan pos = position;

        if (pos == null)
        {
            throw new IllegalArgumentException("Position invalid for binary reader");
        }
        if (!(_input instanceof FromByteArray))
        {
            throw new UnsupportedOperationException("Binary seek not implemented for non-byte array backed sources");
        }

        // TODO test that span is within the bounds of the input byte[]

        // manually reset the input specific type of input stream
        FromByteArray input = (FromByteArray)_input;
        input._pos   = (int) (pos._offset + _physical_start_offset);
        input._limit = (int) (pos._limit  + _physical_start_offset);

        // TODO: these (eof and save points) should be put into
        //       a re-init method on the input stream
        input._eof = false;
        for (;;) {
            SavePoint sp = input._save_points._active_stack;
            if (sp == null) break;
            input._save_points.savePointPopActive(sp);
            sp.free();
        }

        // reset the raw reader
        re_init_raw();

        // reset the system reader
        // - nothing to do

        // reset the user reader
        init_user(this._catalog);

        // now we need to set our symbol table
        _symbols = pos._symbol_table;

        // and the other misc state variables we had
        // read past before getPosition gets called
        //   jonker: Don't do this, we'll re-read the data from the stream.
        //           Otherwise, this reader will be in the wrong state.
        //           For example, getType() will return non-null but that
        //           shouldn't happen until the user calls next().
//        _state         = pos._state;
//        _value_type    = pos._value_type;
//        _value_is_null = pos._value_is_null;
//        _value_is_true = pos._value_is_true;

//        _is_in_struct = false;
    }


    @Override
    public IonType next()
    {
        IonType t = null;
        if (hasNext()) {
            _has_next_needed = true;
            t = _value_type;
        }
        return t;
    }

    @Override
    public boolean hasNext()
    {
        if (!_eof && _has_next_needed) {
            clear_system_value_stack();
            try {
                while (!_eof && _has_next_needed) {
                    has_next_helper_user();
                }
            }
            catch (IOException e) {
                error(e);
            }
        }
        return !_eof;
    }


    private final void has_next_helper_user() throws IOException
    {
        super.hasNext();
        if (getDepth() == 0 && !_value_is_null) {
            if (_value_tid == _Private_IonConstants.tidSymbol) {
                if (load_annotations() == 0) {
                    // $ion_1_0 is read as an IVM only if it is not annotated
                    load_cached_value(AS_TYPE.int_value);
                    int sid = _v.getInt();
                    if (sid == ION_1_0_SID) {
                        _symbols = _system.getSystemSymbolTable();
                        push_symbol_table(_symbols);
                        _has_next_needed = true;
                    }
                }
            }
            else if (_value_tid == _Private_IonConstants.tidStruct) {
                int count = load_annotations();
                for(int ii=0; ii<count; ii++) {
                    if (_annotation_ids[ii] == ION_SYMBOL_TABLE_SID) {
                        _symbols =
                            newLocalSymtab(_system,
                                           // TODO should be current symtab:
                                           _system.getSystemSymbolTable(),
                                           _catalog,
                                           this,
                                           false);
                        push_symbol_table(_symbols);
                        _has_next_needed = true;
                        break;
                    }
                }
            }
            else {
                assert (_value_tid != _Private_IonConstants.tidTypedecl);
            }
        }
    }

    @Override
    public String getFieldName()
    {
        String name;
        if (_value_field_id == SymbolTable.UNKNOWN_SYMBOL_ID) {
            name = null;
        }
        else {
            name = _symbols.findSymbol(_value_field_id);
        }
        return name;
    }

    @Override
    public final SymbolToken getFieldNameSymbol()
    {
        if (_value_field_id == SymbolTable.UNKNOWN_SYMBOL_ID) return null;
        int sid = _value_field_id;
        String text = _symbols.findKnownSymbol(sid);
        return new SymbolTokenImpl(text, sid);
    }

    @Override
    public Iterator<String> iterateTypeAnnotations()
    {
        String[] annotations = getTypeAnnotations();
        return _Private_Utils.stringIterator(annotations);
    }

    @Override
    public String[] getTypeAnnotations()
    {
        load_annotations();
        String[] anns;
        if (_annotation_count < 1) {
            anns = _Private_Utils.EMPTY_STRING_ARRAY;
        }
        else {
            anns = new String[_annotation_count];
            for (int ii=0; ii<_annotation_count; ii++) {
                anns[ii] = _symbols.findSymbol(_annotation_ids[ii]);
            }
        }
        return anns;
    }

    @Override // TODO this override shouldn't be necessary
    public String stringValue()
    {
        if (! IonType.isText(_value_type)) throw new IllegalStateException();
        if (_value_is_null) return null;

        if (_value_type == IonType.SYMBOL) {
            if (!_v.hasValueOfType(AS_TYPE.string_value)) {
                int sid = getSymbolId();
                String sym = _symbols.findSymbol(sid);
                _v.addValue(sym);
            }
        }
        else {
            prepare_value(AS_TYPE.string_value);
        }
        return _v.getString();
    }

    @Override
    public SymbolToken symbolValue()
    {
        if (_value_type != IonType.SYMBOL) throw new IllegalStateException();
        if (_value_is_null) return null;

        int sid = getSymbolId();
        assert sid != UNKNOWN_SYMBOL_ID;
        String text = _symbols.findKnownSymbol(sid);

        return new SymbolTokenImpl(text, sid);
    }

    @Override
    public SymbolTable getSymbolTable()
    {
        return _symbols;
    }
    //
    //  This code handles the skipped symbol table
    //  support - it is cloned in IonReaderTextUserX,
    //  IonReaderBinaryUserX and IonWriterBaseImpl
    //
    //  SO ANY FIXES HERE WILL BE NEEDED IN THOSE
    //  THREE LOCATIONS AS WELL.
    //
    private int _symbol_table_top = 0;
    private SymbolTable[] _symbol_table_stack = new SymbolTable[3]; // 3 is rare, IVM followed by a local sym tab with open content
    private void clear_system_value_stack()
    {
        while (_symbol_table_top > 0) {
            _symbol_table_top--;
            _symbol_table_stack[_symbol_table_top] = null;
        }
    }
    private void push_symbol_table(SymbolTable symbols)
    {
        assert(symbols != null);
        if (_symbol_table_top >= _symbol_table_stack.length) {
            int new_len = _symbol_table_stack.length * 2;
            SymbolTable[] temp = new SymbolTable[new_len];
            System.arraycopy(_symbol_table_stack, 0, temp, 0, _symbol_table_stack.length);
            _symbol_table_stack = temp;
        }
        _symbol_table_stack[_symbol_table_top++] = symbols;
    }
    @Override
    public SymbolTable pop_passed_symbol_table()
    {
        if (_symbol_table_top <= 0) {
            return null;
        }
        _symbol_table_top--;
        SymbolTable symbols = _symbol_table_stack[_symbol_table_top];
        _symbol_table_stack[_symbol_table_top] = null;
        return symbols;
    }


    //========================================================================
    // Facet support


    @Override
    public <T> T asFacet(Class<T> facetType)
    {
        if (facetType == SpanProvider.class)
        {
            return facetType.cast(new SpanProviderFacet());
        }

        // TODO ION-243 support seeking over InputStream
        if (_input instanceof FromByteArray)
        {
            if (facetType == SeekableReader.class)
            {
                return facetType.cast(new SeekableReaderFacet());
            }
        }

        if (facetType == ByteTransferReader.class)
        {
            // This is a rather sketchy use of Facets, since the availability
            // of the facet depends upon the current state of this subject,
            // and that can change over time.

            // TODO ION-241 Our {@link #transferCurrentValue} doesn't handle
            //  field names and annotations.

            // Ensure there's a contiguous buffer we can copy.
            if (_input instanceof UnifiedInputStreamX.FromByteArray
                && getTypeAnnotationIds().length == 0
                && ! isInStruct())
            {
                return facetType.cast(new ByteTransferReaderFacet());
            }
        }

        return super.asFacet(facetType);
    }


    private class SpanProviderFacet implements SpanProvider
    {
        public Span currentSpan()
        {
            return getCurrentPosition();
        }
    }


    private class SeekableReaderFacet
        extends SpanProviderFacet
        implements SeekableReader
    {
        public void hoist(Span span)
        {
            if (! (span instanceof IonReaderBinarySpan))
            {
                throw new IllegalArgumentException("Span isn't compatible with this reader.");
            }

            seek((IonReaderBinarySpan) span);
        }
    }


    private class ByteTransferReaderFacet implements ByteTransferReader
    {
        public void transferCurrentValue(IonWriterSystemBinary writer)
            throws IOException
        {
            // Ensure there's a contiguous buffer we can copy.
            // TODO Copy from a stream should also be possible.
            if (! (_input instanceof UnifiedInputStreamX.FromByteArray))
            {
                throw new UnsupportedOperationException();
            }

            // TODO ION-241 wrong if current value has a field name or
            //   annotations since the position is in the wrong place.
            // TODO when implementing that, be careful to handle the case where
            //   the writer already holds a pending field name or annotations!
            //   Meaning: the user has set it and then called writeValue().

            int inOffset = (int) _position_start;
            int inLen    = (int) _position_len;

            writer._writer.write(_input._bytes, inOffset, inLen);
            writer.patch(inLen);
        }
    }
}
