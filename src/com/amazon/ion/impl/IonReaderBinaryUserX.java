// Copyright (c) 2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

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
public class IonReaderBinaryUserX extends IonReaderBinarySystemX
{
    IonSystem   _system;
    SymbolTable _symbols;

    public IonReaderBinaryUserX(IonSystem system, byte[] bytes) {
        super(bytes);
        init_user(system);
    }
    public IonReaderBinaryUserX(IonSystem system, byte[] bytes, int offset, int length) {
        super(bytes, offset, length);
        init_user(system);
    }
    public IonReaderBinaryUserX(IonSystem system, InputStream userBytes) {
        super(userBytes);
        init_user(system);
    }
    public IonReaderBinaryUserX(IonSystem system, UnifiedInputStreamX userBytes) {
        super(userBytes);
        init_user(system);
    }

    private final void init_user(IonSystem system) {
        _system = system;
        _symbols = system.getSystemSymbolTable();
    }

    @Override
    public boolean hasNext()
    {
        try {
            while (!_eof && _has_next_needed) {
                has_next_helper_user();
            }
        }
        catch (IOException e) {
            error(e);
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
                    _has_next_needed = true;
                }
            }
            else if (_value_tid == IonConstants.tidStruct) {
                int count = load_annotations();
                for(int ii=0; ii<count; ii++) {
                    if (_annotation_ids[ii] == UnifiedSymbolTable.ION_SYMBOL_TABLE_SID) {
                        stepIn();
                        UnifiedSymbolTable symtab = new
                            UnifiedSymbolTable(_system.getSystemSymbolTable()
                                               ,this
                                               ,_system.getCatalog()
                            );
                        stepOut();
                        _symbols = symtab;
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

    //
    // unsupported public methods that require a symbol table
    // to operate - which is only supported on a user reader
    //
    @Override
    public String getFieldName()
    {
        String name = _symbols.findKnownSymbol(_value_field_id);
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
        String[] anns = new String[_annotation_count];
        for (int ii=0; ii<_annotation_count; ii++) {
            anns[ii] = _symbols.findKnownSymbol(_annotation_ids[ii]);
        }
        return anns;
    }
    @Override
    public String stringValue()
    {
        if (IonType.SYMBOL.equals(_value_type)) {
            if (!_v.hasValueOfType(AS_TYPE.string_value)) {
                int sid = intValue();
                String sym = _symbols.findSymbol(sid);
                _v.setValue(sym);
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
}
