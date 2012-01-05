// Copyright (c) 2010-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.impl.UnifiedSymbolTable.isNonSystemSharedTable;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonContainer;
import com.amazon.ion.IonException;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import java.io.IOException;
import java.io.OutputStream;

/**
 *  This is the factory class for constructing writers
 *  with various capabilities.
 */
public class IonWriterFactory
{
    public enum Flags { AUTO_FLUSH, SUPRESS_ION_VERSION_MARKER }

    boolean             _in_progress;
    IonIterationType    _type;
    IonSystem           _system;
    SymbolTable         _symbols;
    SymbolTable[]       _imports;
    IonCatalog          _catalog;
    OutputStream        _out;
    Appendable          _chars;
    _Private_TextOptions _options;
    IonContainer        _container;
    boolean             _auto_flush;
    private boolean     _suppress_ivm;
    boolean             _stream_copy_optimized;

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

        IonWriterBaseImpl writer;

        // check to make
        SymbolTable systemSymbolTable = _system.getSystemSymbolTable();
        switch (_type) {
        case SYSTEM_TEXT:
            writer = new IonWriterSystemText(systemSymbolTable,
                                             _chars, _options);
            break;
        case SYSTEM_BINARY:
            writer = new IonWriterSystemBinary(systemSymbolTable,
                                               _out, _auto_flush,
                                               true /* ensureInitialIvm */);
            break;
        case SYSTEM_ION_VALUE:
        {
            IonContainer c =
                (_container != null ? _container : _system.newDatagram());
            writer = new IonWriterSystemTree(systemSymbolTable, _catalog, c);
            break;
        }
        case USER_TEXT:
        {
            SymbolTable symtab = _system.getSystemSymbolTable();
            IonWriterSystemText systemWriter;
            if (_out != null) {
                systemWriter = new IonWriterSystemText(symtab, _out, _options);
            }
            else if (_chars != null) {
                systemWriter = new IonWriterSystemText(symtab, _chars, _options);
            }
            else {
                throw new IllegalStateException();
            }
            writer = new IonWriterUserText(_catalog, _system, systemWriter,
                                           _options);
            break;
        }
        case USER_BINARY:
            IonWriterSystemBinary binary_system =
                new IonWriterSystemBinary(systemSymbolTable, _out,
                                          _auto_flush, _suppress_ivm);
            writer = new IonWriterUserBinary(_catalog, _system, binary_system,
                                             _stream_copy_optimized);
            break;
        case USER_ION_VALUE:
        {
            IonContainer c =
                (_container != null ? _container : _system.newDatagram());
            IonWriterSystemTree tree_system =
                new IonWriterSystemTree(systemSymbolTable, _catalog, c);
            writer =
                new IonWriterUserTree(_catalog, _system, tree_system);
            break;
        }
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

    public synchronized void set(IonCatalog catalog)
    {
        force_in_progress();
        _catalog = catalog;
    }

    public synchronized void set(SymbolTable symbolTable)
    {
        if (isNonSystemSharedTable(symbolTable)) {
            throw new IonException("symbol table must be null, or a local, or a system symbol table");
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

    public synchronized void set(_Private_TextOptions options)
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
                _suppress_ivm = true;
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

        switch (_type) {
        case USER_TEXT:
        case USER_BINARY:
            if (_system == null) {
                property_error("user writers require an IonSystem");
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
            _options = new _Private_TextOptions(false, true, filter_symbol_tables);
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

        if (_catalog == null && _system != null) {
            // when a catalog is needed we also need a system (which was checked)
            _catalog = _system.getCatalog();
        }
    }

    /**
     * static short cut methods to construct IonWriters
     * quickly.
     * @param container must not be null.
     */
    public static IonWriter makeWriter(IonContainer container)
    {
        IonSystem sys = container.getSystem();
        IonCatalog cat = sys.getCatalog();
        IonWriter writer = makeWriter(cat, container);
        return writer;
    }

    /**
     * @param container must not be null.
     */
    public static IonWriter makeWriter(IonCatalog catalog,
                                       IonContainer container)
    {
        IonSystem sys = container.getSystem();
        SymbolTable defaultSystemSymtab = sys.getSystemSymbolTable();

        IonWriterSystemTree system_writer =
            new IonWriterSystemTree(defaultSystemSymtab, catalog, container);

        IonWriter writer = new IonWriterUserTree(catalog, sys, system_writer);
        return writer;
    }


    public static IonWriterUserBinary newBinaryWriter(IonSystem system,
                                                      IonCatalog catalog,
                                                      boolean streamCopyOptimized,
                                                      OutputStream output,
                                                      SymbolTable... imports)
    {
        SymbolTable defaultSystemSymtab = system.getSystemSymbolTable();

        IonWriterSystemBinary system_writer =
            new IonWriterSystemBinary(defaultSystemSymtab, output,
                                      /* autoFlush */    false,
                                      /* ensureInitialIvm */ true);

        IonWriterUserBinary writer =
            new IonWriterUserBinary(catalog, system, system_writer,
                                    streamCopyOptimized, imports);
        return writer;
    }

    public static final _Private_TextOptions DEFAULT_OPTIONS = null;

    /**
     * Doesn't write a local symtab.
     */
    public static IonWriterBaseImpl makeWriter(IonSystem system,
                                               Appendable output,
                                               _Private_TextOptions options)
    {
        return makeWriter(system, null, output, options);
    }

    /**
     * Writes a local symtab only if there's a real import.
     */
    public static IonWriterBaseImpl makeWriter(IonSystem system,
                                               IonCatalog catalog,
                                               Appendable output,
                                               _Private_TextOptions options,
                                               SymbolTable... imports)
    {
        if (catalog == null) catalog = system.getCatalog();

        if (options == null)
        {
            options = new _Private_TextOptions(false /* prettyPrint */,
                                              true /* printAscii */,
                                              true /* filterOutSymbolTables */);
        }

        SymbolTable defaultSystemSymtab = system.getSystemSymbolTable();

        IonWriterSystemText systemWriter =
            new IonWriterSystemText(defaultSystemSymtab, output, options);

        return new IonWriterUserText(catalog, system, systemWriter, options,
                                     imports);
    }

    /**
     * Doesn't write a local symtab.
     */
    public static IonWriterBaseImpl makeWriter(IonSystem system,
                                               OutputStream output,
                                               _Private_TextOptions options)
    {
        return makeWriter(system, null, output, options);
    }

    /**
     * Writes a local symtab only if there's a real import.
     */
    public static IonWriterBaseImpl makeWriter(IonSystem system,
                                               IonCatalog catalog,
                                               OutputStream output,
                                               _Private_TextOptions options,
                                               SymbolTable... imports)
    {
        if (catalog == null) catalog = system.getCatalog();

        if (options == null)
        {
            options = new _Private_TextOptions(false /* prettyPrint */,
                                              true /* printAscii */,
                                              true /* filterOutSymbolTables */);
        }

        SymbolTable defaultSystemSymtab = system.getSystemSymbolTable();

        IonWriterSystemText systemWriter =
            new IonWriterSystemText(defaultSystemSymtab, output, options);

        return new IonWriterUserText(catalog, system, systemWriter, options,
                                     imports);
    }

    /**
     * @param container must not be null.
     */
    public static IonWriter makeSystemWriter(IonContainer container)
    {
        IonSystem sys = container.getSystem();
        IonCatalog cat = sys.getCatalog();
        SymbolTable defaultSystemSymtab = sys.getSystemSymbolTable();
        IonWriter writer =
            new IonWriterSystemTree(defaultSystemSymtab, cat, container);
        return writer;
    }
}
