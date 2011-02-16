// Copyright (c) 2009-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.impl.IonScalarConversionsX.AS_TYPE;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

/**
 *
 */
class IonReaderBinaryUserX
    extends IonReaderBinarySystemX
    implements IonReaderWriterPrivate
{
    SymbolTable _symbols;
    IonCatalog  _catalog;

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

    private final void init_user(IonCatalog catalog)
    {
        _symbols = _system.getSystemSymbolTable();
        _catalog = catalog;
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
            name = _symbols.findKnownSymbol(_value_field_id);
        }
        return name;
    }

    @Override
    public Iterator<String> iterateTypeAnnotations()
    {
        String[] anns = getTypeAnnotations();
        IonReaderTextRawX.StringIterator it = new IonReaderTextRawX.StringIterator(anns);
        return it;
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
