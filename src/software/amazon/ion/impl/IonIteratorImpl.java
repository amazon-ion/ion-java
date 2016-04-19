/*
 * Copyright 2007-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import java.util.Iterator;
import java.util.NoSuchElementException;
import software.amazon.ion.IonLob;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonSequence;
import software.amazon.ion.IonStruct;
import software.amazon.ion.IonType;
import software.amazon.ion.IonValue;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.SymbolToken;
import software.amazon.ion.ValueFactory;


final class IonIteratorImpl
    implements Iterator<IonValue>
{
    private final ValueFactory _valueFactory;
    private final IonReader _reader;
    private boolean  _at_eof;
    private IonValue _curr;
    private IonValue _next;



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
            _next = readValue();
        }

        return _next;
    }


    private IonValue readValue()
    {
        IonType type = _reader.getType();

        SymbolToken[] annotations = _reader.getTypeAnnotationSymbols();

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
                    v = _valueFactory.newInt(_reader.bigIntegerValue());
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
                    // TODO always pass the SID?  Is it correct?
                    v = _valueFactory.newSymbol(_reader.symbolValue());
                    break;
                case BLOB:
                {
                    IonLob lob = _valueFactory.newNullBlob();
                    lob.setBytes(_reader.newBytes());
                    v = lob;
                    break;
                }
                case CLOB:
                {
                    IonLob lob = _valueFactory.newNullClob();
                    lob.setBytes(_reader.newBytes());
                    v = lob;
                    break;
                }
                case STRUCT:
                {
                    IonStruct struct = _valueFactory.newEmptyStruct();
                    _reader.stepIn();
                    while (_reader.next() != null)
                    {
                        SymbolToken name = _reader.getFieldNameSymbol();
                        IonValue child = readValue();
                        struct.add(name, child);
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
                        IonValue child = readValue();
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
                        IonValue child = readValue();
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
        ((PrivateIonValue)v).setSymbolTable(symtab);

        if (annotations.length != 0) {
            ((PrivateIonValue)v).setTypeAnnotationSymbols(annotations);
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
