// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl;

import static com.amazon.ion.SymbolTable.UNKNOWN_SYMBOL_ID;
import static com.amazon.ion.SystemSymbols.ION_1_0_SID;
import static com.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE;
import static com.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE_SID;
import static com.amazon.ion.impl.IonReaderTextUserX.isIonVersionMarker;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.SeekableReader;
import com.amazon.ion.Span;
import com.amazon.ion.SpanProvider;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;


final class IonReaderTreeUserX
    extends IonReaderTreeSystem
    implements _Private_ReaderWriter
{

    private final _Private_LocalSymbolTableFactory _lstFactory;

    IonCatalog _catalog;
    private SymbolTable _symbols;

    public IonReaderTreeUserX(IonValue value, IonCatalog catalog, _Private_LocalSymbolTableFactory lstFactory)
    {
        super(value); // calls re_init
        _catalog = catalog;
        _lstFactory = lstFactory;
    }

    @Override
    void re_init(IonValue value, boolean hoisted)
    {
        super.re_init(value, hoisted);
        _symbols = _system_symtab;
    }

    //========================================================================

    @Override
    public SymbolTable getSymbolTable()
    {
        return _symbols;
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
            this._curr = null;
            return null;
        }
        this._curr = this._next;
        this._next = null;
        return this._curr.getType();
    }

    boolean next_helper_user()
    {
        if (_eof) return false;
        if (_next != null) return true;

        clear_system_value_stack();

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
                    if (sid == UNKNOWN_SYMBOL_ID) {
                        String name = sym.stringValue();
                        if (name != null) {
                            sid = _system_symtab.findSymbol(name);
                        }
                    }
                    boolean isIVM = isIonVersionMarker(sym.symbolValue().getText()) || sid == ION_1_0_SID;
                    if (isIVM
                        && _next.getTypeAnnotationSymbols().length == 0) {
                        // $ion_1_0 is read as an IVM only if it is not annotated
                        SymbolTable symbols = _system_symtab;
                        _symbols = symbols;
                        push_symbol_table(symbols);
                        _next = null;
                        continue;
                    }
                }
                else if (IonType.STRUCT.equals(next_type)
                    && _next_has_ion_symbol_table_annotation()
                ) {
                    assert(_next instanceof IonStruct);
                    // read a local symbol table
                    IonReaderTreeUserX reader = new IonReaderTreeUserX(_next, _catalog, _lstFactory);
                    // The child reader's symbol table is the symbol table that was active when the value began.
                    reader._symbols = _symbols;
                    SymbolTable symtab = _lstFactory.newLocalSymtab(_catalog, reader, false);
                    _symbols = symtab;
                    push_symbol_table(symtab);
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

    private boolean _next_has_ion_symbol_table_annotation() {
        SymbolToken[] annotations = _next.getTypeAnnotationSymbols();
        if (annotations.length == 0) return false;
        return annotations[0].getSid() == ION_SYMBOL_TABLE_SID
            || annotations[0].getText() == ION_SYMBOL_TABLE;
    }

    //
    //  This code handles the skipped symbol table
    //  support - it is cloned in IonReaderTextUserX
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


    private static final class TreeSpan
        extends DowncastingFaceted
        implements Span
    {
        IonValue _value;
    }

    private final Span currentSpanImpl()
    {
        if (this._curr == null) {
            throw new IllegalStateException("Reader has no current value");
        }

        TreeSpan span = new TreeSpan();
        span._value = this._curr;

        return span;
    }


    private void hoistImpl(Span span)
    {
        if (span instanceof TreeSpan) {
            TreeSpan treeSpan = (TreeSpan)span;
            this.re_init(treeSpan._value, /* hoisted */ true);
        }
        else {
            // TODO custom exception
            throw new IllegalArgumentException("Span not appropriate for this reader");
        }
    }


    //========================================================================
    // Facet support


    @Override
    public <T> T asFacet(Class<T> facetType)
    {
        if ((facetType == SeekableReader.class) ||
            (facetType == SpanProvider.class))
        {
            return facetType.cast(new SeekableReaderFacet());
        }

        return super.asFacet(facetType);
    }


    private class SeekableReaderFacet implements SeekableReader
    {
        public Span currentSpan()
        {
            return currentSpanImpl();
        }

        public void hoist(Span span)
        {
            hoistImpl(span);
        }
    }
}
