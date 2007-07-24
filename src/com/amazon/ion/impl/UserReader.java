/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.impl;

import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonValue;
import com.amazon.ion.LocalSymbolTable;
import com.amazon.ion.impl.IonBinary.BufferManager;
import com.amazon.ion.system.StandardIonSystem;
import java.io.IOException;
import java.io.Reader;
import java.util.NoSuchElementException;

public class UserReader
    implements IonReader
{
    private SystemReader _systemReader;

    /**
     * The system reader changes it's local symtab on next(), but we call
     * that to determine hasNext().  Therefore we need to keep our own
     * symtab reference so we maintain the precondition that it doesn't
     * change until after calling our next.
     */
    private LocalSymbolTable    _localSymbolTable;

    private boolean             _at_eof;
    private IonValue            _next;


    public UserReader(StandardIonSystem system,
                      LocalSymbolTable initialSymbolTable,
                      Reader input)
    {
        this(new SystemReader(system,
                              system.getCatalog(),
                              initialSymbolTable,
                              input));
    }


    public UserReader(SystemReader systemReader)
    {
        _systemReader = systemReader;
        _localSymbolTable = systemReader.getLocalSymbolTable();
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
            if (!this._systemReader.hasNext()) {
                _at_eof = true;
                break;
            }

            _next = this._systemReader.next();
            assert _next != null;

            if (this._systemReader.currentIsHidden()) {
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
                _localSymbolTable = _systemReader.getLocalSymbolTable();
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

    public LocalSymbolTable getLocalSymbolTable()
    {
        return _localSymbolTable;
    }

    /**
     * This cannot be called between {@link #hasNext()} and {@link #next()}.
     * @param symbols
     */
    public void setLocalSymbolTable(LocalSymbolTable symbols)
    {
        if (_next != null) {
            throw new IllegalStateException();
        }
        _systemReader.setLocalSymbolTable(symbols);
        _localSymbolTable = symbols;
    }

    public boolean canSetLocalSymbolTable()
    {
        return _systemReader.canSetLocalSymbolTable();
    }

    public void close()
    {
        _at_eof = true;
        _next = null;
        _systemReader.close();
    }
}