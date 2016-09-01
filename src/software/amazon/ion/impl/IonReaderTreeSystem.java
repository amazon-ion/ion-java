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

import static software.amazon.ion.impl.PrivateUtils.readFully;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.Iterator;
import software.amazon.ion.Decimal;
import software.amazon.ion.IntegerSize;
import software.amazon.ion.IonBool;
import software.amazon.ion.IonContainer;
import software.amazon.ion.IonDatagram;
import software.amazon.ion.IonDecimal;
import software.amazon.ion.IonException;
import software.amazon.ion.IonFloat;
import software.amazon.ion.IonInt;
import software.amazon.ion.IonLob;
import software.amazon.ion.IonNull;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonStruct;
import software.amazon.ion.IonSymbol;
import software.amazon.ion.IonSystem;
import software.amazon.ion.IonText;
import software.amazon.ion.IonTimestamp;
import software.amazon.ion.IonType;
import software.amazon.ion.IonValue;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.SymbolToken;
import software.amazon.ion.Timestamp;
import software.amazon.ion.impl.PrivateIonValue.SymbolTableProvider;

class IonReaderTreeSystem
    implements IonReader, PrivateReaderWriter
{
    protected IonSystem           _system;
    protected SymbolTable         _symbols;
    protected Iterator<IonValue>  _iter;
    protected IonValue            _parent;
    protected PrivateIonValue   _next;
    protected PrivateIonValue   _curr;
    protected boolean             _eof;

    /** Holds pairs: IonValue parent, Iterator<IonValue> cursor */
    private   Object[]           _stack = new Object[10];
    protected int                _top;
    private   boolean            _hoisted;

    // Interface that allows access to the _symbols value (whatever that might be in terms of
    // stream processing context)
    private   final SymbolTableProvider _symbolTableAccessor;

    public IonReaderTreeSystem(IonValue value)
    {
        if (value == null) {
            // do nothing
            _symbolTableAccessor = null;
        }
        else {
            _system = value.getSystem();
            re_init(value, /* hoisted */ false);
            _symbolTableAccessor = new SymbolTableProvider()
            {
                public SymbolTable getSymbolTable()
                {
                    return null == _symbols ? _system.getSystemSymbolTable() : _symbols;
                }
            };
        }
    }


    /**
     * @return This implementation always returns null.
     */
    public <T> T asFacet(Class<T> facetType)
    {
        return null;
    }

    //========================================================================

    void re_init(IonValue value, boolean hoisted)
    {
        _symbols = null;
        _curr = null;
        _eof = false;
        _top = 0;
        _hoisted = hoisted;
        if (value instanceof IonDatagram) {
            // datagrams interacting with these readers must be
            // IonContainerPrivate containers
            assert(value instanceof PrivateIonContainer);
            IonDatagram dg = (IonDatagram) value;
            _parent = dg;
            _next = null;
            _iter = dg.systemIterator(); // we want a system reader not: new Children(dg);
        }
        else {
            _parent = (hoisted ? null : value.getContainer());
            _next = (PrivateIonValue) value;
        }
    }

    public void close()
    {
        _eof = true;
    }

    protected void set_symbol_table(SymbolTable symtab)
    {
        _symbols = symtab;
        return;
    }

    private void push() {
        int oldlen = _stack.length;
        if (_top + 1 >= oldlen) { // we're going to do a "+2" on top so we need extra space
            int newlen = oldlen * 2;
            Object[] temp = new Object[newlen];
            System.arraycopy(_stack, 0, temp, 0, oldlen);
            _stack = temp;
        }
        _stack[_top++] = _parent;
        _stack[_top++] = _iter;
    }

    @SuppressWarnings("unchecked")
    private void pop() {
        assert _top >= 2;

        _top--;
        _iter = (Iterator<IonValue>)_stack[_top];
        _stack[_top] = null;  // Allow iterator to be garbage collected!

        _top--;
        _parent = (IonValue)_stack[_top];
        _stack[_top] = null;

        // We don't know if we're at the end of the container, so check again.
        _eof = false;
    }

    public IonType next()
    {
        if (this._next == null && next_helper_system() == null) {
            this._curr = null;
            return null;
        }
        this._curr = this._next;
        this._next = null;

        return this._curr.getType();
    }

    IonType next_helper_system()
    {
        if (this._eof) return null;
        if (this._next != null) return this._next.getType();

        if (this._iter != null && this._iter.hasNext()) {
            this._next = (PrivateIonValue) this._iter.next();
        }

        if ((this._eof =(this._next == null)) == true) {
            return null;
        }
        return this._next.getType();
    }

    public final void stepIn()
    {
        if (!(this._curr instanceof IonContainer)) {
            throw new IllegalStateException("current value must be a container");
        }
        push();
        _parent = _curr;
        _iter = new Children(((IonContainer)this._curr));
        _curr = null;
    }

    public final void stepOut()
    {
        if (this._top < 1) {
            throw new IllegalStateException(IonMessages.CANNOT_STEP_OUT);
        }
        pop();
        _curr = null;
    }

    public final int getDepth() {
        return _top/2;
    }

    public SymbolTable getSymbolTable()
    {
        SymbolTable symboltable = null;

        if (_curr != null) {
            symboltable = _curr.getSymbolTable();
        }
        else if (_parent != null) {
            symboltable = _parent.getSymbolTable();
        }

        return symboltable;
    }

    public IonType getType()
    {
        return (_curr == null) ? null : _curr.getType();
    }

    public final String[] getTypeAnnotations()
    {
        if (_curr == null) {
            throw new IllegalStateException();
        }
        return _curr.getTypeAnnotations();
    }


    public final SymbolToken[] getTypeAnnotationSymbols()
    {
        if (_curr == null) {
            throw new IllegalStateException();
        }
        // TODO should this localize the symbols?
        return _curr.getTypeAnnotationSymbols(_symbolTableAccessor);
    }

    public final Iterator<String> iterateTypeAnnotations()
    {
        String [] annotations = getTypeAnnotations();
        return PrivateUtils.stringIterator(annotations);
    }


    public boolean isInStruct()
    {
        return (_parent instanceof IonStruct);
    }

    public boolean isNullValue()
    {
        if (_curr instanceof IonNull) return true;
        if (_curr == null) {
            throw new IllegalStateException("must call next() before isNullValue()");

        }
        return _curr.isNullValue();
    }

    public String getFieldName()
    {
        return (_curr == null || (_hoisted && _top == 0)) ? null : _curr.getFieldName();
    }

    public final SymbolToken getFieldNameSymbol()
    {
        if (_curr == null || (_hoisted && _top == 0)) return null;
        return _curr.getFieldNameSymbol(_symbolTableAccessor);
    }


    public boolean booleanValue()
    {
        if (_curr instanceof IonBool) {
            return ((IonBool)_curr).booleanValue();
        }
        throw new IllegalStateException("current value is not a boolean");

    }

    public int intValue()
    {
        if (_curr instanceof IonInt)  {
            return ((IonInt)_curr).intValue();
        }
        if (_curr instanceof IonFloat)  {
            return (int)((IonFloat)_curr).doubleValue();
        }
        if (_curr instanceof IonDecimal)  {
            return (int)((IonDecimal)_curr).doubleValue();
        }
        throw new IllegalStateException("current value is not an ion int, float, or decimal");
    }

    public long longValue()
    {
        if (_curr instanceof IonInt)  {
            return ((IonInt)_curr).longValue();
        }
        if (_curr instanceof IonFloat)  {
            return (long)((IonFloat)_curr).doubleValue();
        }
        if (_curr instanceof IonDecimal)  {
            return (long)((IonDecimal)_curr).doubleValue();
        }
        throw new IllegalStateException("current value is not an ion int, float, or decimal");
    }

    public BigInteger bigIntegerValue()
    {
        if (_curr instanceof IonInt)  {
            return ((IonInt)_curr).bigIntegerValue();
        }
        if (_curr instanceof IonFloat)  {
            // To avoid decapitating values that are > Long.MAX_VALUE, we must
            // convert to BigDecimal first.
            BigDecimal bd = ((IonFloat)_curr).bigDecimalValue();
            return (bd == null ? null : bd.toBigInteger());
        }
        if (_curr instanceof IonDecimal)  {
            BigDecimal bd = ((IonDecimal)_curr).bigDecimalValue();
            return (bd == null ? null : bd.toBigInteger());
        }
        throw new IllegalStateException("current value is not an ion int, float, or decimal");
    }

    public double doubleValue()
    {
        if (_curr instanceof IonFloat)  {
            return ((IonFloat)_curr).doubleValue();
        }
        if (_curr instanceof IonDecimal)  {
            return ((IonDecimal)_curr).doubleValue();
        }
        throw new IllegalStateException("current value is not an ion float or decimal");
    }

    public BigDecimal bigDecimalValue()
    {
        if (_curr instanceof IonDecimal)  {
            return ((IonDecimal)_curr).bigDecimalValue();
        }
        throw new IllegalStateException("current value is not an ion decimal");
    }

    public Decimal decimalValue()
    {
        if (_curr instanceof IonDecimal)  {
            return ((IonDecimal)_curr).decimalValue();
        }
        throw new IllegalStateException("current value is not an ion decimal");
    }

    public Timestamp timestampValue()
    {
        if (_curr instanceof IonTimestamp) {
            return ((IonTimestamp)_curr).timestampValue();
        }
        throw new IllegalStateException("current value is not a timestamp");
    }

    public Date dateValue()
    {
        if (_curr instanceof IonTimestamp)  {
            return ((IonTimestamp)_curr).dateValue();
        }
        throw new IllegalStateException("current value is not an ion timestamp");
    }

    public String stringValue()
    {
        if (_curr instanceof IonText) {
            return ((IonText)_curr).stringValue();
        }
        throw new IllegalStateException("current value is not a symbol or string");
    }

    public SymbolToken symbolValue()
    {
        if (! (_curr instanceof IonSymbol))
        {
            throw new IllegalStateException();
        }
        if (_curr.isNullValue()) return null;
        return ((IonSymbol)_curr).symbolValue();
    }

    public int byteSize()
    {
        if (_curr instanceof IonLob) {
            IonLob lob = (IonLob)_curr;
            return lob.byteSize();
        }
        throw new IllegalStateException("current value is not an ion blob or clob");
    }

    public byte[] newBytes()
    {
        if (_curr instanceof IonLob) {
            IonLob lob = (IonLob)_curr;
            int loblen = lob.byteSize();
            byte[] buffer = new byte[loblen];
            InputStream is = lob.newInputStream();
            int retlen;
            try {
                retlen = readFully(is, buffer, 0, loblen);
                is.close();
            }
            catch (IOException e) {
                throw new IonException(e);
            }
            assert (retlen == -1 ? loblen == 0 : retlen == loblen);
            return buffer;
        }
        throw new IllegalStateException("current value is not an ion blob or clob");
    }

    public int getBytes(byte[] buffer, int offset, int len)
    {
        if (_curr instanceof IonLob) {
            IonLob lob = (IonLob)_curr;
            int loblen = lob.byteSize();
            if (loblen > len) {
                throw new IllegalArgumentException("insufficient space in buffer for this value");
            }
            InputStream is = lob.newInputStream();
            int retlen;
            try {
                retlen = readFully(is, buffer, 0, loblen);
                is.close();
            }
            catch (IOException e) {
                throw new IonException(e);
            }
            assert retlen == loblen;
            return retlen;
        }
        throw new IllegalStateException("current value is not an ion blob or clob");
    }

    public IonValue getIonValue(IonSystem sys)
    {
        return _curr;
    }

    public String valueToString()
    {
        return (_curr == null) ? null : _curr.toString();
    }


    private static final class Children implements Iterator<IonValue>
    {
        boolean             _eof;
        int                 _next_idx;
        PrivateIonContainer _parent;
        IonValue            _curr;

        Children(IonContainer parent)
        {
            if (parent instanceof PrivateIonContainer) {
                _parent = (PrivateIonContainer)parent;
                _next_idx = 0;
                _curr = null;
                if (_parent.isNullValue()) {
                    // otherwise the empty contents member will cause trouble
                    _eof = true;
                }
            }
            else {
                throw new UnsupportedOperationException("this only supports IonContainerImpl instances");
            }
        }

        public boolean hasNext()
        {
            if (_eof) return false;

            int len = _parent.get_child_count();

            if (_next_idx > 0) {
                // first we have to verify the position of the
                // current value, since it might move if local
                // symbol tables get created.  In which case it
                // will be moved down the list.
                int ii = _next_idx - 1;
                _next_idx = len; // if we can't find our current
                                 // value we'll be at eof anyway
                while (ii<len) {
                    if (_curr == _parent.get_child(ii)) {
                        _next_idx = ii+1;
                        break;
                    }
                }
            }
            // if there anything left?
            if (_next_idx >= _parent.get_child_count()) {
                _eof = true;
            }
            return !_eof;
        }

        public IonValue next()
        {
            // the hasNext() is needed to adjust our _next_idx
            // value if the underlying arraylist moved under us
            if (!hasNext()) {
                _curr = null;
            }
            else {
                _curr = _parent.get_child(_next_idx);
                _next_idx++;
            }
            return _curr;
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }

    // system readers don't skip any symbol tables
    public SymbolTable pop_passed_symbol_table()
    {
        return null;
    }


    @Override
    public IntegerSize getIntegerSize()
    {
        if(_curr instanceof IonInt)
        {
            return ((IonInt)_curr).getIntegerSize();

        }
        return null;
    }
}
