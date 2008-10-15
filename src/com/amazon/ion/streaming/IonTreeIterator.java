/*
 * Copyright (c) 2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.streaming;

import com.amazon.ion.IonBool;
import com.amazon.ion.IonContainer;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonDecimal;
import com.amazon.ion.IonException;
import com.amazon.ion.IonFloat;
import com.amazon.ion.IonInt;
import com.amazon.ion.IonLob;
import com.amazon.ion.IonNull;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonText;
import com.amazon.ion.IonTimestamp;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.TtTimestamp;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.Date;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Provides a concrete implementation of {@link IonReader} that operates
 * over an {@link IonValue}, typically an {@link IonDatagram}.
 */
public final class IonTreeIterator
    extends IonIterator
{
    Iterator<IonValue> _iter;
    IonValue _root;
    IonValue _next;
    IonValue _curr;
    boolean  _eof;

    Object[] _stack = new Object[10];
    int      _top;

    void push() {
        if (_top > (_stack.length - 2)) {
            Object[] temp = new Object[_stack.length * 2];
            System.arraycopy(_stack, 0, temp, 0, _top);
            _stack = temp;
        }
        _stack[_top++] = _root;
        _stack[_top++] = _iter;
    }

    @SuppressWarnings("unchecked")
    void pop() {
        assert _top >= 2;
        assert _stack != null;
        assert (_stack[_top - 1] instanceof Iterator);
        assert (_stack[_top - 2] instanceof IonValue);

        _top--;
        _iter = (Iterator<IonValue>)_stack[_top];
        _top--;
        _root = (IonValue)_stack[_top];
    }

    public IonTreeIterator(IonValue value) {
        _root = value;
        _curr = null;
        _eof = false;
        if (value instanceof IonContainer) {
            _iter = ((IonContainer)value).iterator();
        }
        else if (value instanceof IonDatagram) {
            _iter = ((IonDatagram)value).iterator();
        }
        else {
            _iter = null;
        }
    }


    @Override
    public boolean hasNext()
    {
        if (this._eof) return false;
        if (this._next != null) return true;

        if (this._iter == null) {
            this._next = this._root;
        }
        else if (this._iter.hasNext()) {
            this._next = this._iter.next();
        }
        this._eof = (this._next == null);
        return !this._eof;
    }

    @Override
    public IonType next()
    {
        if (this._next == null && !this.hasNext()) {
            throw new NoSuchElementException();
        }
        this._curr = this._next;
        this._next = null;

        return this._curr.getType();
    }

    @Override
    public int getContainerSize() {
        if (!(this._curr instanceof IonContainer)) {
            throw new IllegalStateException("current iterator value must be a container");
        }
        return ((IonContainer)_curr).size();
    }

    @Override
    public void stepInto()
    {
        if (!(this._curr instanceof IonContainer)) {
            throw new IllegalStateException("current iterator value must be a container");
        }
        push();
        _root = _curr;
        _iter = ((IonContainer)this._curr).iterator();
        _curr = null;
    }

    @Override
    public void stepOut()
    {
        if (this._top < 1) {
            throw new IllegalStateException("current iterator must be in a stepped into container");
        }
        pop();
    }

    @Override
    public int getDepth() {
        return _top/2;
    }

    @Deprecated
    public void position(IonReader other)
    {
        if (!(other instanceof IonTreeIterator)) {
            throw new IllegalArgumentException("invalid iterator type, classes must match");
        }
        IonTreeIterator iother = (IonTreeIterator)other;

        this._eof = iother._eof;
        this._curr = iother._curr;
        this._root = iother._root;

        if (iother._iter == null) {
            this._iter = null;
        }
        else {
            assert iother._root instanceof IonContainer;
            this._iter = ((IonContainer)iother._root).iterator();
            while (this.hasNext()) {
                this.next();
                if (this._curr == iother._curr) break;
            }
        }
    }

    @Override
    public UnifiedSymbolTable getSymbolTable()
    {
        UnifiedSymbolTable utable = null;
        SymbolTable symboltable = null;

        if (_curr != null) {
            symboltable = _curr.getSymbolTable();
        }
        else if (_root != null) {
            symboltable = _root.getSymbolTable();
        }
        if (symboltable instanceof UnifiedSymbolTable) {
            utable = (UnifiedSymbolTable)symboltable;
        }
        else {
            utable = new UnifiedSymbolTable(symboltable);
        }

        return utable;
    }

    @Override
    public IonType getType()
    {
        return (_curr == null) ? null : _curr.getType();
    }

    @Override
    public int getTypeId()
    {
        return (_curr == null) ? -1 : _curr.getType().ordinal();
    }

    @Override
    public String[] getTypeAnnotations()
    {
        if (_curr == null) {
            throw new IllegalStateException();
        }
        String [] annotations = _curr.getTypeAnnotations();
        if (annotations == null || annotations.length < 1) {
            return null;
        }

        return annotations;
    }

    @Override
    public int[] getTypeAnnotationIds()
    {
        String [] annotations = getTypeAnnotations();
        if (annotations == null)  return null;

        int [] ids = new int[annotations.length];
        SymbolTable sym = _curr.getSymbolTable();

        for (int ii=0; ii<annotations.length; ii++) {
            ids[ii] = sym.findSymbol(annotations[ii]);
        }

        return ids;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterator<Integer> iterateTypeAnnotationIds()
    {
        int [] ids = getTypeAnnotationIds();
        if (ids == null) return (Iterator<Integer>) EMPTY_ITERATOR;
        return new IdIterator(ids);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterator<String> iterateTypeAnnotations()
    {
        String [] annotations = getTypeAnnotations();
        if (annotations == null) return (Iterator<String>) EMPTY_ITERATOR;
        return new StringIterator(annotations);
    }


    @Override
    public boolean isInStruct()
    {
        Object r = _root;
        if (_top > 1) {
            r = _stack[_top - 1];
        }
        return (r instanceof IonStruct);
    }

    @Override
    public boolean isNullValue()
    {
        if (_curr instanceof IonNull) return true;
        if (_curr == null) {
            throw new IllegalStateException("curr of iterator is not yet set");

        }
        return _curr.isNullValue();
    }

    @Override
    public int getFieldId()
    {
        // FIXME why does this return null? What does that even mean for int?
        // FIXME IonValueImpl.getFieldId doesn't return -1 as specced here!
        return (_curr == null) ? null : _curr.getFieldId();
    }

    @Override
    public String getFieldName()
    {
        return (_curr == null) ? null : _curr.getFieldName();
    }

    @Override
    public IonValue getIonValue(IonSystem sys)
    {
        return _curr;
    }

    @Override
    public boolean booleanValue()
    {
        if (_curr instanceof IonBool) {
            return ((IonBool)_curr).booleanValue();
        }
        throw new IllegalStateException("current value is not a boolean");

    }

    @Override
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

    @Override
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

    @Override
    public double doubleValue()
    {
        if (_curr instanceof IonFloat)  {
            return (int)((IonFloat)_curr).doubleValue();
        }
        if (_curr instanceof IonDecimal)  {
            return (int)((IonDecimal)_curr).doubleValue();
        }
        throw new IllegalStateException("current value is not an ion float or decimal");
    }

    @Override
    public BigDecimal bigDecimalValue()
    {
        if (_curr instanceof IonDecimal)  {
            return ((IonDecimal)_curr).bigDecimalValue();
        }
        throw new IllegalStateException("current value is not an ion decimal");
    }

    @Override
    public TtTimestamp timestampValue()
    {
        if (_curr instanceof IonTimestamp) {
            return ((IonTimestamp)_curr).timestampValue();
        }
        throw new IllegalStateException("current value is not a timestamp");
    }

    @Override
    public Date dateValue()
    {
        if (_curr instanceof IonTimestamp)  {
            return ((IonTimestamp)_curr).dateValue();
        }
        throw new IllegalStateException("current value is not an ion timestamp");
    }

    @Override
    public String stringValue()
    {
        if (_curr == null) return null;
        if (_curr instanceof IonText) {
            return ((IonText)_curr).stringValue();
        }
        throw new IllegalStateException("current value is not a symbol or string");
    }

    @Override
    public int getSymbolId()
    {
        if (_curr == null) return -1;
        if (_curr instanceof IonSymbol) {
            return ((IonSymbol)_curr).getSymbolId();
        }
        throw new IllegalStateException("current value is not a symbol");
    }

    public int byteSize()
    {
        if (_curr instanceof IonLob) {
            IonLob lob = (IonLob)_curr;
            return lob.byteSize();
        }
        throw new IllegalStateException("current value is not an ion blob or clob");
    }

    @Override
    public byte[] newBytes()
    {
        if (_curr instanceof IonLob) {
            IonLob lob = (IonLob)_curr;
            int loblen = lob.byteSize();
            byte[] buffer = new byte[loblen];
            InputStream is = lob.newInputStream();
            int retlen;
            try {
                retlen = is.read(buffer, 0, loblen);
                is.close();
            }
            catch (IOException e) {
                throw new IonException(e);
            }
            assert retlen == loblen;
            return buffer;
        }
        throw new IllegalStateException("current value is not an ion blob or clob");
    }

    @Override
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
                retlen = is.read(buffer, offset, loblen);
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

    @Override
    public String valueToString()
    {
        return (_curr == null) ? null : _curr.toString();
    }

    static final class StringIterator implements Iterator<String>
    {
        String [] _values;
        int       _pos;

        StringIterator(String[] values) {
            _values = values;
        }
        public boolean hasNext() {
            return (_pos < _values.length);
        }
        public String next() {
            if (!hasNext()) throw new NoSuchElementException();
            return _values[_pos++];
        }
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    static final class IdIterator implements Iterator<Integer>
    {
        int []  _values;
        int     _pos;

        IdIterator(int[] values) {
            _values = values;
        }
        public boolean hasNext() {
            return (_pos < _values.length);
        }
        public Integer next() {
            if (!hasNext()) throw new NoSuchElementException();
            int value = _values[_pos++];
            return value;
        }
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}

