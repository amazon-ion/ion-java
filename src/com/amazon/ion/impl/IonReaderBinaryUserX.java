// Copyright (c) 2009-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.OctetSpan;
import com.amazon.ion.Span;
import com.amazon.ion.SpanReader;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.impl.IonScalarConversionsX.AS_TYPE;
import com.amazon.ion.impl.UnifiedInputStreamX.FromByteArray;
import com.amazon.ion.impl.UnifiedSavePointManagerX.SavePoint;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

class IonReaderBinaryUserX
    extends IonReaderBinarySystemX
    implements IonReaderWriterPrivate, IonReaderWithPosition, SpanReader
{
    SymbolTable _symbols;
    IonCatalog  _catalog;

    private static class IonReaderBinaryPosition
        extends IonReaderPositionBase
        implements IonReaderOctetPosition, OctetSpan
    {
        State       _state;
        int         _offset;
        int         _limit;
        IonType     _value_type;
        boolean     _value_is_null;
        boolean     _value_is_true;
        SymbolTable _symbol_table;

        public long getOffset()
        {
            return _offset;
        }

        public long getLength()
        {
            return _limit - _offset;
        }

        public long getStartOffset()
        {
            return _offset;
        }

        public long getFinishOffset()
        {
            return _limit;
        }
    }

    public IonReaderBinaryUserX(IonSystem system, IonCatalog catalog, byte[] bytes, int offset, int length) {
        super(system, bytes, offset, length);
        init_user(catalog);
    }
    public IonReaderBinaryUserX(IonSystem system, IonCatalog catalog, InputStream userBytes) {
        super(system, userBytes);
        init_user(catalog);
    }
    public IonReaderBinaryUserX(IonSystem system, IonCatalog catalog, UnifiedInputStreamX userBytes) {
        super(system, userBytes);
        init_user(catalog);
    }

    //FIXME: PERF_TEST was :private
    final void init_user(IonCatalog catalog)
    {
        _symbols = _system.getSystemSymbolTable();
        _catalog = catalog;
    }


    /**
     *
     */
    @Override
    public <T> T asFacet(Class<T> facetType)
    {
        if ((facetType == IonReaderWithPosition.class) ||
            (facetType == SpanReader.class))
        {
            return facetType.cast(this);
        }
        return super.asFacet(facetType);
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
    public IonReaderBinaryPosition getCurrentPosition()
    {
        // check to see that the reader is in a valid position
        // to mark it
        //     - not in the middle of a value
        //        TODO what does that mean?

        IonReaderBinaryPosition pos = new IonReaderBinaryPosition();

        if (getType() == null)
        {
            String message = "IonReader isn't positioned on a value";
            throw new IllegalStateException(message);
        }

        if (_position_start == -1)
        {
            // special case of the position before the first call to next
            pos._offset = _input._pos;
            pos._limit = _input._limit;
            pos._symbol_table = _symbols;
        }
        else
        {
            pos._offset = _position_start;
            pos._limit = _position_len + _position_start;
            pos._symbol_table = _symbols;
        }

        pos._state         = _state;
        pos._value_type    = _value_type;
        pos._value_is_null = _value_is_null;
        pos._value_is_true = _value_is_true;

        return pos;
    }


    public Span currentSpan()
    {
        return getCurrentPosition();
    }


    public void seek(IonReaderPosition position)
    {
        IonReaderBinaryPosition pos = position.asFacet(IonReaderBinaryPosition.class);

        if (pos == null)
        {
            throw new IllegalArgumentException("Position invalid for binary reader");
        }
        if (!(_input instanceof FromByteArray))
        {
            throw new UnsupportedOperationException("Binary seek not implemented for non-byte array backed sources");
        }

        // manually reset the input specific type of input stream
        FromByteArray input = (FromByteArray)_input;
        input._pos = pos._offset;
        input._limit = pos._limit;

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
        _state         = pos._state;
        _value_type    = pos._value_type;
        _value_is_null = pos._value_is_null;
        _value_is_true = pos._value_is_true;

        _is_in_struct = false;
    }

    public void hoist(Span span)
    {
        if (! (span instanceof IonReaderBinaryPosition))
        {
            throw new IllegalArgumentException("Span isn't compatible with this reader.");
        }

        seek((IonReaderBinaryPosition) span);
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
        if (!_eof &&_has_next_needed) {
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
            if (_value_tid == IonConstants.tidSymbol) {
                load_cached_value(AS_TYPE.int_value);
                int sid = _v.getInt();
                if (sid == UnifiedSymbolTable.ION_1_0_SID) {
                    _symbols = _system.getSystemSymbolTable();
                    push_symbol_table(_symbols);
                    _has_next_needed = true;
                }
            }
            else if (_value_tid == IonConstants.tidStruct) {
                int count = load_annotations();
                for(int ii=0; ii<count; ii++) {
                    if (_annotation_ids[ii] == UnifiedSymbolTable.ION_SYMBOL_TABLE_SID) {

                        //stepIn();
                        //an empty struct is actually ok, just not very interesting
                        //if (!hasNext()) {
                        //    this.error_at("local symbol table with an empty struct encountered");
                        //}
                        UnifiedSymbolTable symtab =
                                UnifiedSymbolTable.makeNewLocalSymbolTable(
                                    _system
                                  , _catalog
                                  , this
                                  , false // false failed do list encountered, but removed call to stepIn above // true failed for testBenchmark singleValue
                        );
                        _symbols = symtab;
                        push_symbol_table(_symbols);
                        _has_next_needed = true;
                        break;
                    }
                }
            }
            else {
                assert (_value_tid != IonConstants.tidTypedecl);
            }
        }
    }

    @Override
    public String getFieldName()
    {
        String name;
        if (_value_field_id == UnifiedSymbolTable.UNKNOWN_SID) {
            name = null;
        }
        else {
            name = _symbols.findSymbol(_value_field_id);
        }
        return name;
    }

    @Override
    public Iterator<String> iterateTypeAnnotations()
    {
        String[] annotations = getTypeAnnotations();
        return IonImplUtils.stringIterator(annotations);
    }

    @Override
    public String[] getTypeAnnotations()
    {
        try {
            load_annotations();
        }
        catch (IOException e) {
            error(e);
        }
        String[] anns;
        if (_annotation_count < 1) {
            anns = IonImplUtils.EMPTY_STRING_ARRAY;
        }
        else {
            anns = new String[_annotation_count];
            for (int ii=0; ii<_annotation_count; ii++) {
                anns[ii] = _symbols.findSymbol(_annotation_ids[ii]);
            }
        }
        return anns;
    }

    @Override
    public String stringValue()
    {
        if (_value_is_null) {
            return null;
        }
        if (IonType.SYMBOL.equals(_value_type)) {
            if (!_v.hasValueOfType(AS_TYPE.string_value)) {
                int sid = intValue();
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
}
