// Copyright (c) 2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonIterationType;
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
    implements IonReaderWriterPrivate
{
    // IonSystem   _system; now in IonReaderTextSystemX where it could be null
    IonCatalog  _catalog;
    SymbolTable _symbols;

    protected IonReaderTextUserX(IonSystem system, IonCatalog catalog, char[] chars, int offset, int length) {
        super(system, chars, offset, length);
        initUserReader(system, catalog);
    }
    protected IonReaderTextUserX(IonSystem system, IonCatalog catalog, CharSequence chars, int offset, int length) {
        super(system, chars, offset, length);
        initUserReader(system, catalog);
    }
    protected IonReaderTextUserX(IonSystem system, IonCatalog catalog, Reader userChars) {
        super(system, userChars);
        initUserReader(system, catalog);
    }
    protected IonReaderTextUserX(IonSystem system, IonCatalog catalog, byte[] bytes, int offset, int length) {
        super(system, bytes, offset, length);
        initUserReader(system, catalog);
    }
    protected IonReaderTextUserX(IonSystem system, IonCatalog catalog, InputStream userBytes) {
        super(system, userBytes);
        initUserReader(system, catalog);
    }
    protected IonReaderTextUserX(IonSystem system, IonCatalog catalog, File file) {
        super(system, file);
        initUserReader(system, catalog);
    }
    protected IonReaderTextUserX(IonSystem system, IonCatalog catalog, UnifiedInputStreamX uis) {
        super(system, uis);
        initUserReader(system, catalog);
    }
    private void initUserReader(IonSystem system, IonCatalog catalog) {
        if (system == null) {
            throw new IllegalArgumentException();
        }
        _system = system;
        if (catalog != null) {
            _catalog = catalog;
        }
        else {
            _catalog = system.getCatalog();
        }
        // not needed, getSymbolTable will force this when necessary
        //  _symbols = system.getSystemSymbolTable();
    }

    @Override
    public IonIterationType getIterationType()
    {
        return IonIterationType.USER_TEXT;
    }

    @Override
    public IonSystem getSystem()
    {
        return _system;
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
        // clear out our previous value
        clear_system_value_stack();

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
            if (_value_type != null && !isNullValue() && IonType.DATAGRAM.equals(getContainerType())) {
                switch (_value_type) {
                case STRUCT:
                    if (_annotation_count > 0) {
                        for (int ii=0; ii<_annotation_count; ii++) {
                            String a = _annotations[ii];
                            if (UnifiedSymbolTable.ION_SYMBOL_TABLE.equals(a)) {
                                _symbols = UnifiedSymbolTable.makeNewLocalSymbolTable(_system, _catalog, this, true);
                                push_symbol_table(_symbols);
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
                        push_symbol_table(_system.getSystemSymbolTable());
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
    private final void symbol_table_reset()
    {
        IonType t = next();
        assert( IonType.SYMBOL.equals(t) );
        _symbols = null; // was: _symbols.getSystemSymbolTable(); - the null is fixed in getSymbolTable()
        return;
    }

    @Override
    public int getFieldId()
    {
        if (!isInStruct()) {
            // TODO: should this be return IllegalStateException ?
            return SymbolTable.UNKNOWN_SYMBOL_ID;
        }
        String      fieldname = getFieldName();
        SymbolTable symbols   = getSymbolTable();
        int         id        = symbols.findSymbol(fieldname);
        return id;
    }

    @Override
    public int getSymbolId()
    {
        if (getType() != IonType.SYMBOL) {
            throw new IllegalStateException("only valid if the value is a symbol");
        }
        String symbol = stringValue();
        SymbolTable symbols   = getSymbolTable();
        // if (!_symbols.isLocalTable()) {
        //     UnifiedSymbolTable local;
        //     if (_symbols.isSystemTable()) {
        //         local = UnifiedSymbolTable.makeNewLocalSymbolTable(_system, _system.getSystemSymbolTable());
        //     }
        //     else { // if (_symbols.isSharedTable()) {
        //         local = UnifiedSymbolTable.makeNewLocalSymbolTable(_system, _system.getSystemSymbolTable(), _symbols);
        //     }
        //     _symbols = local;
        // }
        int    id     = symbols.addSymbol(symbol);
        return id;
    }

    @Override
    public SymbolTable getSymbolTable()
    {
        if (_symbols == null) {
            SymbolTable system_symbols = _system.getSystemSymbolTable();
            _symbols = UnifiedSymbolTable.makeNewLocalSymbolTable(system_symbols);
        }
        return _symbols;
    }

    private static int[] _empty_int_array = new int[0];
    @Override
    public int[] getTypeAnnotationIds()
    {
        int[]    ids;
        String[] syms = getTypeAnnotations();
        int      len  = syms.length;

        if (len == 0) {
            ids  = _empty_int_array;
        }
        else {
            SymbolTable symbols = getSymbolTable();
            ids  = new int[len];
            for (int ii=0; ii<len; ii++) {
                String sym = stringValue();
                ids[ii] = symbols.findSymbol(sym);
            }
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

    public static final class IntIterator implements Iterator<Integer>
    {
        static IntIterator EMPTY_ITERATOR = new IntIterator(null);

        int[] _values;
        int   _length;
        int   _pos;

        public IntIterator(int[] values) {
            _values = values;
            _length = (values == null) ? 0 : values.length;
        }
        public IntIterator(int[] values, int offset, int len) {
            _values = values;
            _pos = offset;
            _length = len;
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

    //
    //  This code handles the skipped symbol table
    //  support - it is cloned in IonReaderTreeUserX
    //  and IonReaderBinaryUserX
    //
    //  SO ANY FIXES HERE WILL BE NEEDED IN THOSE
    //  TWO LOCATIONS AS WELL.
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
