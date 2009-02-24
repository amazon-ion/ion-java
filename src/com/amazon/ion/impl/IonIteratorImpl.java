/*
 * Copyright (c) 2007-2009 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.impl;

import com.amazon.ion.IonLob;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSequence;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.SymbolTable;
import java.util.Iterator;
import java.util.NoSuchElementException;


public class IonIteratorImpl
    implements Iterator<IonValue>
{
    private final IonSystemImpl _system;

    private IonReader _reader;

    private boolean      _at_eof;
    private IonValueImpl _curr;
    private IonValueImpl _next;



    /**
     * @throws NullPointerException if any parameter is null.
     */
    public IonIteratorImpl(IonSystemImpl system,
                           IonReader input)
    {
        if (system == null || input == null)
        {
            throw new NullPointerException();
        }

        _system = system;
        _reader = input;
    }


    public IonSystemImpl getSystem() {
        return _system;
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

        if (! _reader.hasNext())
        {
            _at_eof = true;
        }
        else
        {
            IonType type = _reader.next();
            // FIXME second clause shouldn't be needed.  ION-27
//            assert !_reader.isInStruct() || type==IonType.STRUCT;

            IonValue v = readValue(_reader);
            _next = (IonValueImpl) v;
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
            v = _system.newNull(type);
        }
        else
        {
            switch (type) {
                case NULL:
                    // Handled above
                    throw new IllegalStateException();
                case BOOL:
                    v = _system.newBool(_reader.booleanValue());
                    break;
                case INT:
                    // FIXME should use bigInteger
                    v = _system.newInt(_reader.longValue());
                    break;
                case FLOAT:
                    v = _system.newFloat(_reader.doubleValue());
                    break;
                case DECIMAL:
                    v = _system.newDecimal(_reader.bigDecimalValue());
                    break;
                case TIMESTAMP:
                    v = _system.newTimestamp(_reader.timestampValue());
                    break;
                case STRING:
                    v = _system.newString(_reader.stringValue());
                    break;
                case SYMBOL:
                    // FIXME handle case where only SID is known
                    v = _system.newSymbol(_reader.stringValue());
                    break;
                case BLOB:
                {
                    IonLob lob = _system.newNullBlob();
                    lob.setBytes(reader.newBytes());
                    v = lob;
                    break;
                }
                case CLOB:
                {
                    IonLob lob = _system.newNullClob();
                    lob.setBytes(reader.newBytes());
                    v = lob;
                    break;
                }
                case STRUCT:
                {
                    IonStruct struct = _system.newEmptyStruct();
                    _reader.stepIn();
                    while (_reader.hasNext())
                    {
                        _reader.next();
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
                    IonSequence seq = _system.newEmptyList();
                    _reader.stepIn();
                    while (_reader.hasNext())
                    {
                        _reader.next();
                        IonValue child = readValue(_reader);
                        seq.add(child);
                    }
                    _reader.stepOut();
                    v = seq;
                    break;
                }
                case SEXP:
                {
                    IonSequence seq = _system.newEmptySexp();
                    _reader.stepIn();
                    while (_reader.hasNext())
                    {
                        _reader.next();
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

        SymbolTable symtab = _reader.getSymbolTable();
        ((IonValueImpl)v).setSymbolTable(symtab);

        if (annotations != null) {
            // FIXME should have IonValue.setTypeAnnotations(String[])
            for (int i = 0; i < annotations.length; i++)
            {
                String annot = annotations[i];
                v.addTypeAnnotation(annot);
            }
        }

        return v;
    }


    public IonValueImpl next() {
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
