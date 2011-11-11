// Copyright (c) 2007-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.impl.UnifiedSymbolTable.makeNewLocalSymbolTable;

import com.amazon.ion.IonLob;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSequence;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.ValueFactory;
import java.util.Iterator;
import java.util.NoSuchElementException;


public class IonIteratorImpl
    implements Iterator<IonValue>
{
    private final ValueFactory _valueFactory;

    private IonReader    _reader;
    private SymbolTable  _current_symbols;

    private boolean      _at_eof;
    private IonValue _curr;
    private IonValuePrivate _next;



    /**
     * @throws NullPointerException if any parameter is null.
     */
    public IonIteratorImpl(ValueFactory valueFactory,
                           IonReader input)
    {
        if (valueFactory == null || input == null)
        {
            throw new NullPointerException();
        }

        _valueFactory = valueFactory;
        _reader = input;
    }


    /**
     * Returns true if the iteration has more elements.
     * here we actually walk ahead and get the next value (it's
     * the only way we know if there are more and clear out the
     * various $ion noise out of the way
     */
    public boolean hasNext() {
        if (_at_eof) return false;
        if (_next != null) return true;
        return (prefetch() != null);
    }

    private IonValue prefetch()
    {
        assert !_at_eof && _next == null;

        IonType type = _reader.next();
        if (type == null)
        {
            _at_eof = true;
        }
        else
        {
            IonValue v = readValue(_reader);
            _next = (IonValuePrivate) v;
            SymbolTable symbols = _next.getAssignedSymbolTable();
            if (UnifiedSymbolTable.isTrivialTable(symbols) == true
             && UnifiedSymbolTable.isTrivialTable(this._current_symbols) == false
            ) {
                // TODO why is this changing the symtab?
                symbols = this._current_symbols;
                _next.setSymbolTable(symbols);
            }

            if (UnifiedSymbolTable.isRealLocalTable(symbols) == false) {
                // TODO why is this changing the symtab?
                // so we have to make it a real
                assert(symbols == null || symbols.isSharedTable() || symbols.isSystemTable());
                IonSystem system = _next.getSystem();
                SymbolTable local =
                    makeNewLocalSymbolTable(system, system.getSystemSymbolTable());
                _next.setSymbolTable(local);
                symbols = local;
            }

            this._current_symbols = symbols;
        }

        return _next;
    }


    private IonValue readValue(IonReader reader)
    {
        IonType type = _reader.getType();

        String [] annotations = _reader.getTypeAnnotations();

        IonValue v;

        if (_reader.isNullValue())
        {
            v = _valueFactory.newNull(type);
        }
        else
        {
            switch (type) {
                case NULL:
                    // Handled above
                    throw new IllegalStateException();
                case BOOL:
                    v = _valueFactory.newBool(_reader.booleanValue());
                    break;
                case INT:
                    // FIXME should use bigInteger
                    v = _valueFactory.newInt(_reader.longValue());
                    break;
                case FLOAT:
                    v = _valueFactory.newFloat(_reader.doubleValue());
                    break;
                case DECIMAL:
                    v = _valueFactory.newDecimal(_reader.decimalValue());
                    break;
                case TIMESTAMP:
                    v = _valueFactory.newTimestamp(_reader.timestampValue());
                    break;
                case STRING:
                    v = _valueFactory.newString(_reader.stringValue());
                    break;
                case SYMBOL:
                    // FIXME handle case where only SID is known
                    v = _valueFactory.newSymbol(_reader.stringValue());
                    break;
                case BLOB:
                {
                    IonLob lob = _valueFactory.newNullBlob();
                    lob.setBytes(reader.newBytes());
                    v = lob;
                    break;
                }
                case CLOB:
                {
                    IonLob lob = _valueFactory.newNullClob();
                    lob.setBytes(reader.newBytes());
                    v = lob;
                    break;
                }
                case STRUCT:
                {
                    IonStruct struct = _valueFactory.newEmptyStruct();
                    _reader.stepIn();
                    while (_reader.next() != null)
                    {
                        String fieldName = _reader.getFieldName();
                        IonValue child = readValue(_reader);
                        struct.add(fieldName, child);
                    }
                    _reader.stepOut();
                    v = struct;
                    break;
                }
                case LIST:
                {
                    IonSequence seq = _valueFactory.newEmptyList();
                    _reader.stepIn();
                    while (_reader.next() != null)
                    {
                        IonValue child = readValue(_reader);
                        seq.add(child);
                    }
                    _reader.stepOut();
                    v = seq;
                    break;
                }
                case SEXP:
                {
                    IonSequence seq = _valueFactory.newEmptySexp();
                    _reader.stepIn();
                    while (_reader.next() != null)
                    {
                        IonValue child = readValue(_reader);
                        seq.add(child);
                    }
                    _reader.stepOut();
                    v = seq;
                    break;
                }
                default:
                    throw new IllegalStateException();
            }
        }

        // TODO this is too late in the case of system reading
        // when v is a local symtab (it will get itself, not the prior symtab)
        SymbolTable symtab = _reader.getSymbolTable();
        ((IonValuePrivate)v).setSymbolTable(symtab);

        if (annotations.length != 0) {
            v.setTypeAnnotations(annotations);
        }

        return v;
    }


    public IonValue next() {
        if (! _at_eof) {
            _curr = null;
            if (_next == null) {
                prefetch();
            }
            if (_next != null) {
                _curr = _next;
                _next = null;
                return _curr;
            }
        }
        throw new NoSuchElementException();
    }


    public void remove() {
        throw new UnsupportedOperationException();
    }
}
