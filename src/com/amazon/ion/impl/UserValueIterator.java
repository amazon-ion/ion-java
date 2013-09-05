// Copyright (c) 2007-2013 Amazon.com, Inc.  All rights reserved.
package com.amazon.ion.impl;

import com.amazon.ion.IonException;
import com.amazon.ion.IonValue;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.impl.IonBinary.BufferManager;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

class UserValueIterator
    implements Iterator<IonValue>
{
    /** if true we reset the system reader for each user value */
    private boolean        _recycle_buffer;

    private SystemValueIterator _systemReader;

    /**
     * The system reader changes it's local symtab on next(), but we call
     * that to determine hasNext().  Therefore we need to keep our own
     * symtab reference so we maintain the precondition that it doesn't
     * change until after calling our next.
     */
    private SymbolTable    _localSymbolTable;

    private boolean        _at_eof;
    private IonValue       _next;


    /**
     * Unless {@link #setBufferToRecycle()} is called, this iterator will
     * intrementally load the encode the whole input stream into a buffer!
     */
    public UserValueIterator(SystemValueIterator systemReader)
    {
        _systemReader = systemReader;
        _localSymbolTable = null; // systemReader.getLocalSymbolTable();
    }

    public void setBufferToRecycle() {
        this._recycle_buffer = true;
    }
    public void clearBufferRecycling() {
        this._recycle_buffer = false;
    }

    //Returns true if the iteration has more elements.
    //here we actually walk ahead and get the next value (it's
    //the only way we know if there are more and clear out the
    //various $ion noise out of the way
    public boolean hasNext() {
        if (_at_eof) return false;
        if (_next != null) return true;
        return (prefetch() != null);
    }

    private IonValue prefetch()
    {
        assert !_at_eof;

        while (_next == null) {
            if (this._recycle_buffer) {
                this._systemReader.resetBuffer();
            }
            if (!this._systemReader.hasNext()) {
                _at_eof = true;
                break;
            }

            _next = this._systemReader.next();
            assert _next != null;

            if (this._systemReader.currentIsHidden()) {
                _localSymbolTable = null; // we'll be resetting this shortly
                _next = null;
            }
        }

        return _next;
    }


    public IonValue next() {
        if (! _at_eof) {
            if (_next == null) {
                prefetch();
            }
            if (_next != null) {
                IonValue retval = _next;
                _next = null;
                _localSymbolTable = _systemReader.getSymbolTable();
                if (this._recycle_buffer) {
                    ((IonValueImpl)retval).clear_position_and_buffer();
                }
                return retval;
            }
        }

        throw new NoSuchElementException();
    }


    public void remove() {
        throw new UnsupportedOperationException();
    }

    public byte[] getBytes() {
        BufferManager buffer = this._systemReader.getBuffer();
        int len = buffer.buffer().size();
        byte[] bytes = new byte[len];
        try {
            buffer.reader(0).read(bytes, 0, len);
        }
        catch (IOException e){
            throw new IonException(e);
        }
        return bytes;
    }

    public BufferManager getBuffer() {
        return this._systemReader.getBuffer();
    }

    public SymbolTable getLocalSymbolTable()
    {
        if (_localSymbolTable == null) {
            _localSymbolTable = _systemReader.getLocalSymbolTable();
        }
        return _localSymbolTable;
    }

//    /**
//     * This cannot be called between {@link #hasNext()} and {@link #next()}.
//     * @param symbols must be local, not shared.
//     */
//    public void setLocalSymbolTable(SymbolTable symbols)
//    {
//        if (_next != null) {
//            throw new IllegalStateException();
//        }
//        _systemReader.setLocalSymbolTable(symbols);
//        _localSymbolTable = symbols;
//    }

//    public boolean canSetLocalSymbolTable()
//    {
//        return _systemReader.canSetLocalSymbolTable();
//    }

    public void close()
    {
        _at_eof = true;
        _next = null;
        try {
            _systemReader.close();
        }
        catch (IOException e) {
            throw new IonException(e);
        }
    }
}
