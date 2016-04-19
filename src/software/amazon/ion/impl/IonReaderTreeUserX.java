/*
 * Copyright 2010-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package software.amazon.ion.impl;

import static software.amazon.ion.SymbolTable.UNKNOWN_SYMBOL_ID;
import static software.amazon.ion.SystemSymbols.ION_1_0_SID;
import static software.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE;
import static software.amazon.ion.impl.PrivateUtils.newLocalSymtab;

import software.amazon.ion.IonCatalog;
import software.amazon.ion.IonDatagram;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonStruct;
import software.amazon.ion.IonSymbol;
import software.amazon.ion.IonType;
import software.amazon.ion.IonValue;
import software.amazon.ion.SeekableReader;
import software.amazon.ion.Span;
import software.amazon.ion.SpanProvider;
import software.amazon.ion.SymbolTable;

final class IonReaderTreeUserX
    extends IonReaderTreeSystem
    implements PrivateReaderWriter
{
    IonCatalog _catalog;

    public IonReaderTreeUserX(IonValue value, IonCatalog catalog)
    {
        super(value);
        _catalog = catalog;
    }


    //========================================================================

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

    private boolean next_helper_user()
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
                    int sid = sym.symbolValue().getSid();
                    if (sid == UNKNOWN_SYMBOL_ID) {
                        String name = sym.stringValue();
                        if (name != null) {
                            sid = _system.getSystemSymbolTable().findSymbol(name);
                        }
                    }
                    if (sid == ION_1_0_SID
                        && _next.getTypeAnnotationSymbols().length == 0) {
                        // $ion_1_0 is read as an IVM only if it is not annotated
                        SymbolTable symbols = _system.getSystemSymbolTable();
                        set_symbol_table(symbols);
                        push_symbol_table(symbols);
                        _next = null;
                        continue;
                    }
                }
                else if (IonType.STRUCT.equals(next_type)
                      && _next.hasTypeAnnotation(ION_SYMBOL_TABLE)
                ) {
                    assert(_next instanceof IonStruct);
                    // read a local symbol table
                    IonReader reader = new IonReaderTreeUserX(_next, _catalog);
                    SymbolTable symtab =
                        newLocalSymtab(_system,
                                       _system.getSystemSymbolTable(),
                                       _system.getCatalog(),
                                       reader, false);
                    set_symbol_table(symtab);
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
