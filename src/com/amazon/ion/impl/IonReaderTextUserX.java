// Copyright (c) 2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.SymbolTable;
import java.io.File;
import java.io.InputStream;
import java.io.Reader;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 *    The text user reader add support for symbols and recognizes,
 *    and consumes (and processes), the system values $ion_1_0 and
 *    local symbol tables (tagged with $ion_symbol_table).
 *
 *    Should this materialize and "symbolate" all the symbol
 *    values as they come through?  - No.
 *
 *    Probably if we want the symbol id's to be the same for this
 *    reader as it is for other variants.  Hmmm, that's expensive
 *    when you don't need it (which is most of the time).
 *
 *    This will not auto-populate a symbol table.  In the event
 *    a symbol is a '$<digits>' symbol id symbol this will return
 *    that value.  If the string is present in the current symbol
 *    table it will return the id, which would be true if the
 *    symbol is a system symbol or if there is a local symbol
 *    table in the input stream.  Otherwise it return the
 *    undefined symbol value.
 *
 */
public class IonReaderTextUserX
    extends IonReaderTextSystemX
{
    IonSystem   _system;
    IonCatalog  _catalog;
    SymbolTable _symbols;


    public IonReaderTextUserX(IonSystem system, CharSequence chars) {
        super(chars);
        initUserReader(system);
    }
    public IonReaderTextUserX(IonSystem system, CharSequence chars, int offset, int length) {
        super(chars, offset, length);
        initUserReader(system);
    }
    public IonReaderTextUserX(IonSystem system, Reader userChars) {
        super(userChars);
        initUserReader(system);
    }
    public IonReaderTextUserX(IonSystem system, byte[] bytes) {
        super(bytes);
        initUserReader(system);
    }
    public IonReaderTextUserX(IonSystem system, byte[] bytes, int offset, int length) {
        super(bytes, offset, length);
        initUserReader(system);
    }
    public IonReaderTextUserX(IonSystem system, InputStream userBytes) {
        super(userBytes);
        initUserReader(system);
    }
    public IonReaderTextUserX(IonSystem system, File file) {
        super(file);
        initUserReader(system);
    }
    public IonReaderTextUserX(IonSystem system, UnifiedInputStreamX uis) {
        super(uis);
        initUserReader(system);
    }
    private void initUserReader(IonSystem system) {
        _system = system;
        _catalog = system.getCatalog();
        _symbols = system.getSystemSymbolTable();
    }


    /**
     * this looks forward to see if there is an upcoming value
     * and if there is it returns true.  It may have to clean up
     * any value that's partially complete (for example a
     * collection whose annotation has been read and loaded
     * but the use has chosen not to step into the collection).
     *
     * The user reader variant of hasNext also looks for system
     * values to process.  System values are the Ion version
     * marker ($ion_1_0) and local symbol tables.  If either of
     * these is encountered the symbol table processing will be
     * handled and the value will be "skipped".
     *
     * @return true if more data remains, false on eof
     */
    @Override
    public boolean hasNext()
    {
        boolean has_next = has_next_user_value();
        return has_next;
    }
    private final boolean has_next_user_value()
    {
     // changed to 'while' since consumed
        // values will not be counted
        while (!_has_next_called)
        {
            // first move to the next value regardless of whether
            // it's a system value or a user value
            has_next_raw_value();

            // system values are only at the datagram level
            // we don't care about them if they're buried
            // down in some other value - note that _value_type
            // will be null at eof and on as yet undetermined
            // numeric types (which are never system values)
            if (_value_type != null && IonType.DATAGRAM.equals(getContainerType())) {
                switch (_value_type) {
                case STRUCT:
                    if (_annotation_count > 0) {
                        for (int ii=0; ii<_annotation_count; ii++) {
                            String a = _annotations[ii];
                            if (UnifiedSymbolTable.ION_SYMBOL_TABLE.equals(a)) {
                                symbol_table_load();
                                _has_next_called = false;
                                break;
                            }
                        }
                    }
                    break;
                case SYMBOL:
                    String sym = stringValue();
                    if (UnifiedSymbolTable.ION_1_0.equals(sym)) {
                        symbol_table_reset();
                        _has_next_called = false;
                    }
                    break;
                default:
                    break;
                }
            }
        }
        return (_eof != true);
    }
    private final void symbol_table_load()
    {
        _symbols = UnifiedSymbolTable.loadLocalSymbolTable(this, _catalog);
        return;
    }
    private final void symbol_table_reset()
    {
        IonType t = next();
        assert( IonType.SYMBOL.equals(t) );
        _symbols = _symbols.getSystemSymbolTable();
        return;
    }

    @Override
    public int getFieldId()
    {
        if (!isInStruct()) {
            // TODO: should this be return IllegalStateException ?
            return SymbolTable.UNKNOWN_SYMBOL_ID;
        }
        String fieldname = getFieldName();
        int    id        = _symbols.findSymbol(fieldname);
        return id;
    }

    @Override
    public int getSymbolId()
    {
        if (getType() != IonType.SYMBOL) {
            throw new IllegalStateException("only valid if the value is a symbol");
        }
        String symbol = stringValue();
        int    id     = _symbols.findSymbol(symbol);
        return id;
    }

    @Override
    public SymbolTable getSymbolTable()
    {
        if (_symbols == null) {
            _symbols = _system.getSystemSymbolTable();
        }
        return _symbols;
    }

    @Override
    public int[] getTypeAnnotationIds()
    {
        String[] syms = getTypeAnnotations();
        int      len  = syms.length;
        int[]    ids  = new int[len];

        for (int ii=0; ii<len; ii++) {
            String sym = stringValue();
            ids[ii] = _symbols.findSymbol(sym);
        }
        return ids;
    }

    @Override
    public Iterator<Integer> iterateTypeAnnotationIds()
    {
        int[] ids = getTypeAnnotationIds();
        if (ids == null) return IntIterator.EMPTY_ITERATOR;
        return new IntIterator(ids);
    }

    static final class IntIterator implements Iterator<Integer>
    {
        static IntIterator EMPTY_ITERATOR = new IntIterator(null);

        int[] _values;
        int   _length;
        int   _pos;

        IntIterator(int[] values) {
            _values = values;
            _length = (values == null) ? 0 : values.length;
        }
        public boolean hasNext() {
            return (_pos < _length);
        }
        public Integer next() {
            if (!hasNext()) throw new NoSuchElementException();
            return _values[_pos++];
        }
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
