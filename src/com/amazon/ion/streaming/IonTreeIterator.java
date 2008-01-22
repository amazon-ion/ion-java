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
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonText;
import com.amazon.ion.IonTimestamp;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.impl.IonTokenReader.Type.timeinfo;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.Date;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 *
 */
public class IonTreeIterator
    extends IonIterator
{
    Iterator<IonValue> _iter;
    IonValue _root;
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
    
    IonTreeIterator(IonValue value) {
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
        if (this._iter == null) return true;
        return this._iter.hasNext();
    }

    @Override
    public IonType next()
    {
        if (this._eof) return null;
        if (this._iter == null) {
            this._curr = this._root;
            this._eof = true;
        }
        else {
            this._curr = this._iter.next();
            this._eof = (this._curr == null);
        }
        return (this._curr == null) ? null : this._curr.getType();
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
    public void position(IonIterator other)
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
    public void setSymbolTable(UnifiedSymbolTable  externalsymboltable) 
    {
        // TODO: this is being ignored right now or is it a ...
        // BUGBUG:
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
    public String[] getAnnotations()
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
    public int[] getAnnotationIds()
    {
        String [] annotations = getAnnotations();
        if (annotations == null)  return null;

        int [] ids = new int[annotations.length];
        SymbolTable sym = _curr.getSymbolTable();
        
        for (int ii=0; ii<annotations.length; ii++) {
            ids[ii] = sym.findSymbol(annotations[ii]);
        }
    
        return ids;
    }

    @Override
    public Iterator<Integer> getAnnotationIdIterator()
    {
        int [] ids = getAnnotationIds();
        if (ids == null) return null;
        return new IdIterator(ids);
    }
    
    @Override
    public Iterator<String> getAnnotationIterator()
    {
        String [] annotations = getAnnotations();
        if (annotations == null) return null;
        
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
    public boolean isNull()
    {
        if (_curr instanceof IonNull) return true;
        if (_curr == null) {
            throw new IllegalStateException("current is nujll");
    
        }
        return _curr.isNullValue();
    }

    @Override
    public int getFieldId()
    {
        return (_curr == null) ? null : _curr.getFieldNameId();
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
    public boolean getBool()
    {
        if (_curr instanceof IonBool) {
            return ((IonBool)_curr).booleanValue();
        }
        throw new IllegalStateException("current value is not a boolean");
    
    }

    @Override
    public int getInt()
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
    public long getLong()
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
    public double getDouble()
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
    public BigDecimal getBigDecimal()
    {
        if (_curr instanceof IonDecimal)  {
            return ((IonDecimal)_curr).toBigDecimal();
        }
        throw new IllegalStateException("current value is not an ion decimal");
    }

    @Override
    public timeinfo getTimestamp()
    {
        if (_curr instanceof IonTimestamp) {
            timeinfo ti = new timeinfo();
            ti.d = ((IonTimestamp)_curr).dateValue();
            ti.localOffset = ((IonTimestamp)_curr).getLocalOffset();
            return ti;
        }
        throw new IllegalStateException("current value is not a timestamp");    
    }

    @Override
    public Date getDate()
    {
        if (_curr instanceof IonTimestamp)  {
            return ((IonTimestamp)_curr).dateValue();
        }
        throw new IllegalStateException("current value is not an ion timestamp");
    }

    @Override
    public String getString()
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
            return ((IonSymbol)_curr).intValue();
        }
        throw new IllegalStateException("current value is not a symbol");
    }

    @Override
    public byte[] getBytes()
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
    public String getValueAsString()
    {
        return (_curr == null) ? null : _curr.toString();
    }

    static class StringIterator implements Iterator<String>
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

    static class IdIterator implements Iterator<Integer>
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

