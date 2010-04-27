// Copyright (c) 2010 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonContainer;
import com.amazon.ion.IonException;
import com.amazon.ion.IonIterationType;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.impl.IonWriterUserText.TextOptions;
import java.io.IOException;
import java.io.OutputStream;

/**
 *  This is the factory class for constructing writers
 *  with various capabilities.
 */
class IonWriterFactory
{
    public enum Flags { AUTO_FLUSH, SUPRESS_ION_VERSION_MARKER }

    boolean             _in_progress;
    IonIterationType    _type;
    IonSystem           _system;
    SymbolTable         _symbols;
    SymbolTable[]       _imports;
    OutputStream        _out;
    Appendable          _chars;
    TextOptions         _options;
    IonContainer        _container;
    boolean             _auto_flush;
    boolean             _assure_ivm = true;

    public synchronized void startWriter()
    {
        if (_in_progress) {
            throw new IonException("a writer is already in progress");
        }
        _in_progress = true;
    }
    public synchronized void reset()
    {
        // we don't care if one is or is not in progress - just reset everything
        _type = null;
        _system = null;
        _symbols = null;
        _imports = null;
        _out = null;
        _chars = null;
        _options = null;

        _in_progress = false;
    }
    private void force_in_progress()
    {
        _in_progress = true;
    }
    public synchronized IonWriter getWriter()
    {
        _in_progress = false;
        if (_type == null) {
            throw new IllegalStateException("the writer type is required to construct the writer");
        }

        check_for_required_properties();
        check_for_excess_properties();
        construct_derived_properties();

        IonWriter writer;

        // check to make
        switch (_type) {
        case SYSTEM_TEXT:
            writer = new IonWriterSystemText(_system.getSystemSymbolTable(),
                                             _chars, _options);
            break;
        case SYSTEM_BINARY:
            writer = new IonWriterSystemBinary(_system.getSystemSymbolTable(),
                                               _out, _auto_flush, _assure_ivm);
            break;
        case SYSTEM_ION_VALUE:
            writer = new IonWriterSystemTree(_system, _container);
            break;
        case USER_TEXT:
            if (_out != null) {
                writer = new IonWriterUserText(_system, _out, _options);
            }
            else if (_chars != null) {
                writer = new IonWriterUserText(_system, _chars, _options);
            }
            else {
                assert("this should never be allowed by the previous checks".length() < 0);
                writer = null;
            }
            break;
        case USER_BINARY:
            IonWriterSystemBinary binary_system =
                new IonWriterSystemBinary(_system.getSystemSymbolTable(), _out,
                                          _auto_flush, _assure_ivm);
            writer = new IonWriterUserBinary(_system, binary_system);
            break;
        case USER_ION_VALUE:
            IonWriterSystemTree tree_system = new IonWriterSystemTree(_system, _container);
            writer = new IonWriterUserTree(tree_system);
            break;
        default:
            throw new IonException("unexpected writer type encountered "+_type.toString());
        }

        if (_symbols != null) {
            try {
                writer.setSymbolTable(_symbols);
            }
            catch (IOException e) {
                throw new IonException(e);
            }
        }

        // TODO: should we really reset this factory?
        //       should we just null out values that are
        //       "used up" (like containers, and output streams)?
        //       or do nothing of the sort
        reset();

        return writer;
    }

    public synchronized void set(IonIterationType type)
    {
        force_in_progress();
        _type = type;
    }

    public synchronized void set(IonSystem system)
    {
        force_in_progress();
        _system = system;
    }

    public synchronized void set(SymbolTable symbolTable)
    {
        if (symbolTable != null) {
            if (!(symbolTable.isLocalTable() || symbolTable.isSystemTable())) {
                throw new IonException("symbol table must be null, or a local, or a system symbol table");
            }
        }
        force_in_progress();
        _symbols = symbolTable;
    }

    public synchronized void set(SymbolTable[] imports)
    {
        if (imports != null && imports.length > 0) {
            for (int ii=0; ii<imports.length; ii++) {
                if (!imports[ii].isSharedTable()) {
                    throw new IonException("import symbol tables must be a shared symbol table");
                }
            }
        }
        force_in_progress();
        _imports = imports;
    }

    public synchronized void set(OutputStream out)
    {
        force_in_progress();
        _out = out;
    }

    public synchronized void set(Appendable chars)
    {
        force_in_progress();
        _chars = chars;
    }

    public synchronized void set(TextOptions options)
    {
        force_in_progress();
        _options = options;
    }

    public synchronized void set(Flags flag)
    {
        force_in_progress();
        if (flag == null) {
            throw new IllegalArgumentException("flag value cannot be null");
        }
        switch(flag) {
            case AUTO_FLUSH:
                _auto_flush = true;
                break;
            case SUPRESS_ION_VERSION_MARKER:
                _assure_ivm = false;
                break;
            default:
                throw new IllegalArgumentException("flag value not recognised: "+flag);
        }
    }

    public synchronized void set(IonContainer container)
    {
        force_in_progress();
        _container = container;
    }

    private void property_error(String message)
    {
        throw new IonException(message);
    }

    private void check_for_required_properties()
    {
        switch (_type) {
        case USER_TEXT:
        case SYSTEM_TEXT:
            if (_out == null && _chars == null) {
                property_error("text writers require an output");
            }
            break;
        case USER_BINARY:
        case SYSTEM_BINARY:
            if (_out == null) {
                property_error("binary writers require an output stream");
            }
            break;
        case USER_ION_VALUE:
        case SYSTEM_ION_VALUE:
            if (_system == null) {
                property_error("tree writers require an IonSystem");
            }
            break;
        default:
            throw new IonException("unexpected writer type encountered "+_type.toString());
        }
    }

    private void check_for_excess_properties()
    {
        // this is true for all writers
        if (_symbols != null && _imports != null) {
            property_error("you can only specify the import list or the local symbol table, not both");
        }
        // text writers can't have two destinations
        switch (_type) {
        case SYSTEM_TEXT:
        case USER_TEXT:
            if (_out != null && _chars != null) {
                property_error("you can only specify the output stream or the appendable destination for text writers, not both");
            }
            break;
        default:
            break;
        }

        // text writers can't have two destinations
        switch (_type) {
        case SYSTEM_TEXT:
        case USER_TEXT:
            if (_out != null && _chars != null) {
                property_error("you can only specify the output stream or the appendable destination for text writers, not both");
            }
            break;
        default:
            break;
        }

        // only text supports appendable
        switch (_type) {
        case SYSTEM_BINARY:
        case SYSTEM_ION_VALUE:
        case USER_BINARY:
        case USER_ION_VALUE:
            if (_chars != null) {
                property_error("you can only specify an Appendable as the output destination for text writers");
            }
            break;
        default:
            throw new IonException("unexpected writer type encountered "+_type.toString());
        }

        // containers are only good on tree writers
        switch (_type) {
        case SYSTEM_TEXT:
        case SYSTEM_BINARY:
        case USER_TEXT:
        case USER_BINARY:
            if (_container != null) {
                property_error("a container is not valid for a text writer");
            }
            break;
        default:
            break;
        }

    }

    private void construct_derived_properties()
    {
        // text writer must have options set
        if (_options == null && _type.isText()) {
            boolean filter_symbol_tables = _type.isUser();
            _options = new TextOptions(false, true, filter_symbol_tables);
        }

        // if we have imports then we should cons up a local symbol table
        if (_imports != null) {
            assert(_symbols == null);
            if (_system == null) {
                property_error("an IonSystem is required to change an import to a local symbol table");
            }
            _symbols = _system.newLocalSymbolTable(_imports);
            _imports = null; // just one or the other, not both
        }

        // let's make sure they get a good converter if they want us to write
        // our characters to a byte based output stream
        if (_out == null && _chars != null && _type.isText()) {
            _chars = new IonUTF8.CharToUTF8(_out);
            _out = null; // again there can be only 1 output destination
        }

        // a tree writer needs a container, a datagram if the user didn't offer one
        if (_container == null && _type.isIonValue()) {
            assert(_system != null); // this is checked earlier
            _container = _system.newDatagram();
        }
    }
}
