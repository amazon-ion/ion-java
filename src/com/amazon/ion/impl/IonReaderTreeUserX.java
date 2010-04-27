// Copyright (c) 2010 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonIterationType;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.SymbolTable;

/**
 *
 */
class IonReaderTreeUserX
    extends IonReaderTreeSystem
{

    public IonReaderTreeUserX(IonValue value)
    {
        super(value);
    }

    @Override
    public IonIterationType getIterationType()
    {
        return IonIterationType.USER_ION_VALUE;
    }

    @Override
    public boolean hasNext()
    {
        return next_helper_user();
    }

    @Override
    public IonType next()
    {
        if (!next_helper_user()) {
            return null;
        }
        this._curr = this._next;
        this._next = null;

        if (this._symbols != null) {
            if (this._root != null) {
                _root.setSymbolTable(this._symbols);
            }
        }

        return this._curr.getType();
    }

    boolean next_helper_user()
    {
        if (_eof) return false;
        if (_next != null) return true;

        // read values from the system
        // reader and if they are system values
        // process them.  Return when we've
        // read all the immediate system values
        IonType next_type;
        for (;;) {
            next_type = next_helper_system();

            if (_top == 0 && _parent instanceof IonDatagram) {
                if (IonType.SYMBOL.equals(next_type)) {
                    assert(_next instanceof IonSymbol);
                    IonSymbol sym = (IonSymbol)_next;
                    if (sym.isNullValue()) {
                        // there are no null values we will consume here
                        break;
                    }
                    int sid = sym.getSymbolId();
                    if (sid == -1) {
                        String name = sym.stringValue();
                        if (name != null) {
                            sid = _system.getSystemSymbolTable().findSymbol(name);
                        }
                    }
                    if (sid == UnifiedSymbolTable.ION_1_0_SID) {
                        set_symbol_table(_system.getSystemSymbolTable());
                        _next = null;
                        continue;
                    }
                }
                else if (IonType.STRUCT.equals(next_type)
                      && _next.hasTypeAnnotation(UnifiedSymbolTable.ION_SYMBOL_TABLE)
                ) {
                    assert(_next instanceof IonStruct);
                    // read a local symbol table
                    IonReader reader = new IonReaderTreeUserX(_next);
                    SymbolTable symtab = UnifiedSymbolTable.makeNewLocalSymbolTable(_system, reader, false);
                    set_symbol_table(symtab);
                    _next = null;
                    continue;
                }
            }
            // if we get here we didn't process a system
            // value, if we had we would have 'continue'd
            // so this is a value the user gets
            break;
        }
        return (next_type != null);
    }
}
