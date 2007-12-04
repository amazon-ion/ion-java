/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.impl;

import static com.amazon.ion.impl.IonConstants.BINARY_VERSION_MARKER_SIZE;
import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonValue;
import com.amazon.ion.LocalSymbolTable;
import com.amazon.ion.StaticSymbolTable;
import com.amazon.ion.impl.IonBinary.BufferManager;
import com.amazon.ion.system.StandardIonSystem;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.NoSuchElementException;


public class SystemReader
    implements IonReader
{
    private final StandardIonSystem _system;
    private final IonCatalog _catalog;

    private Reader           _input;
    private IonParser        _parser;
    private BufferManager    _buffer;
    private int              _buffer_offset;

    private LocalSymbolTable _currentSymbolTable;

    private boolean      _at_eof;
    private boolean      _currentIsHidden;
    private IonValue     _curr;
    private IonValue     _next;


    /**
     * @throws NullPointerException if any parameter is null.
     */
    public SystemReader(StandardIonSystem system, String s) {
        this(system, system.getCatalog(), new StringReader(s));
    }

    /**
     * @throws NullPointerException if any parameter is null.
     */
    public SystemReader(StandardIonSystem system,
                        IonCatalog catalog,
                        Reader input)
    {
        this(system, catalog, system.newLocalSymbolTable(), input);
    }

    /**
     * @throws NullPointerException if any parameter is null.
     */
    public SystemReader(StandardIonSystem system,
                        IonCatalog catalog,
                        LocalSymbolTable initialSymboltable,
                        Reader input)
    {
        if (system == null || catalog == null || initialSymboltable == null)
        {
            throw new NullPointerException();
        }

        _system = system;
        _catalog = catalog;
        _currentSymbolTable = initialSymboltable;
        initialize(input, 0);
    }


    /**
     * @throws NullPointerException if input is null.
     */
    private void initialize(Reader input, int limit) {
        _input = input;
        _parser = new IonParser(_input);
        _buffer = _parser.getByteBuffer();
    }


    /**
     * @throws NullPointerException if any parameter is null.
     */
    public SystemReader(StandardIonSystem system, BufferManager buffer)
    {
        this(system, system.getCatalog(), buffer);
    }

    /**
     * Creates a new system reader using a specific catalog, reading data from
     * the start of a buffer.
     *
     * @throws NullPointerException if any parameter is null.
     */
    public SystemReader(StandardIonSystem system,
                        IonCatalog catalog,
                        BufferManager buffer)
    {
        if (catalog == null)  // Others are dereferenced below.
        {
            throw new NullPointerException();
        }

        IonBinary.Reader reader = buffer.reader();
        IonBinary.verifyBinaryVersionMarker(reader);

        _system = system;
        _catalog = catalog;
        // TODO this should be an unmodifiable bootstram symtab.
        _currentSymbolTable = system.newLocalSymbolTable();
        _buffer = buffer;
        _buffer_offset = reader.position();
    }


    public StandardIonSystem getSystem() {
        return _system;
    }

    public LocalSymbolTable getLocalSymbolTable() {
        return _currentSymbolTable;
    }


    public boolean canSetLocalSymbolTable()
    {
        // If parser is set, we're scanning binary data.
        return (_parser != null);
    }

    /**
     * Cannot be called between {@link #hasNext()} and {@link #next()}.
     * @param symbolTable
     */
    public void setLocalSymbolTable(LocalSymbolTable symbolTable) {
        if (_parser == null) {
            throw new UnsupportedOperationException();
        }
        if (_next != null) {
            throw new IllegalStateException();
        }
        _currentSymbolTable = symbolTable;
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

        BufferManager buffer = _buffer;
        // just to make the other code easier to read, and write

        // ok, now we walk on ahead
        if (buffer.buffer().size() <= _buffer_offset) {
            // we used up the buffer we've seen so far ...
            // so parse another value out of the input
            if (_parser != null) {
                boolean freshBuffer = _buffer_offset == 0;
                _parser.parse(_currentSymbolTable
                              ,_buffer_offset
                              ,freshBuffer
                              ,0
                );
                if (freshBuffer) {
                    // We wrote a magic cookie; skip it.
                    _buffer_offset = BINARY_VERSION_MARKER_SIZE;
                }
            }
            if (buffer.buffer().size() <= _buffer_offset) {
                // we didn't make any progress,
                // so there's no more data for us
                _at_eof = true;
            }
        }
        if (!_at_eof) {
            // there is some sort a value, we'll get it and check it out
            // until we find something we like
            IonValueImpl value =
                IonValueImpl.makeValueFromBuffer(0
                                                 ,_buffer_offset
                                                 ,buffer
                                                 ,this._currentSymbolTable
                                                 ,null
            );

            // move along on the buffer
            _buffer_offset = value.pos_getOffsetofNextValue();

            _next = value;
        }

        return _next;
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

                checkCurrentForHiddens();

                return _curr;
            }
        }
        throw new NoSuchElementException();
    }

    private void checkCurrentForHiddens()
    {
        LocalSymbolTable newLocalSymbtab =
            _system.handleLocalSymbolTable(_catalog, _curr);
        if (newLocalSymbtab != null)
        {
            // Note that we may be replacing the encoded systemId symbol
            // with a struct, in which case the tree view will be out of
            // sync with the binary.  That's okay, though: if the bytes are
            // requested it will be updated.

            _curr = newLocalSymbtab.getIonRepresentation();
            _currentSymbolTable = newLocalSymbtab;
            _currentIsHidden = true;
        }
        // $ion_symbol_table
        else if (_system.valueIsStaticSymbolTable(_curr))
        {
            StaticSymbolTable newTable =
                new StaticSymbolTableImpl(_system, (IonStruct) _curr);
            _catalog.putTable(newTable);
            _currentIsHidden = true;
        }
        else
        {
            _currentIsHidden = false;
        }
    }


    /**
     * Only valid after call to {@link #next()}.
     */
    boolean currentIsHidden() {
        if (_curr == null) {
            throw new IllegalStateException();
        }
        return _currentIsHidden;
    }


    public void remove() {
        throw new UnsupportedOperationException();
    }

    public BufferManager getBuffer() {
        return _buffer;
    }

    public void close()
    {
        _at_eof = true;
        _curr = _next = null;

        if (_input != null)
        {
            try
            {
                _input.close();
            }
            catch (IOException e)
            {
                throw new IonException("Error closing reader: "
                                           + e.getMessage(),
                                       e);
            }
            finally
            {
                _input = null;
            }
        }
    }
}
