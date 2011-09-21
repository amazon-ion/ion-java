// Copyright (c) 2010-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.SystemSymbols.SYMBOLS;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.SymbolTable;
import java.io.IOException;
import java.io.OutputStream;


/**
 *
 */
public class IonWriterUserText
    extends IonWriterUser
{
    static public class TextOptions
    {
        private final static CharSequence SPACE_CHARACTER = " ";
        private final static CharSequence LINE_SEPARATOR = System.getProperty("line.separator");

        final boolean       _pretty_print;
        final boolean       _ascii_only;
        final CharSequence  _line_separator;
        final boolean       _filter_symbol_tables;
        final boolean       _suppress_ion_version_marker;


        public TextOptions(boolean prettyPrint, boolean printAscii)
        {
            _pretty_print = prettyPrint;
            _ascii_only   = printAscii;
            if (_pretty_print) {
                _line_separator = LINE_SEPARATOR;
            }
            else {
                _line_separator = SPACE_CHARACTER;
            }
            _filter_symbol_tables = false;
            _suppress_ion_version_marker = false;
        }
        public TextOptions(boolean prettyPrint, boolean printAscii, boolean filterOutSymbolTables)
        {
            _pretty_print = prettyPrint;
            _ascii_only   = printAscii;
            if (_pretty_print) {
                _line_separator = LINE_SEPARATOR;
            }
            else {
                _line_separator = SPACE_CHARACTER;
            }
            _filter_symbol_tables = filterOutSymbolTables;
            _suppress_ion_version_marker = false;
        }
        public TextOptions(boolean prettyPrint, boolean printAscii, boolean filterOutSymbolTables, boolean suppressIonVersionMarker)
        {
            _pretty_print = prettyPrint;
            _ascii_only   = printAscii;
            if (_pretty_print) {
                _line_separator = LINE_SEPARATOR;
            }
            else {
                _line_separator = SPACE_CHARACTER;
            }
            _filter_symbol_tables = filterOutSymbolTables;
            _suppress_ion_version_marker = suppressIonVersionMarker;
        }

        public final boolean isPrettyPrintOn() {
            return _pretty_print;
        }
        public final boolean isAsciiOutputOn() {
            return _ascii_only;
        }
        public final boolean isFilterSymbolTablesOn() {
            return _filter_symbol_tables;
        }
        public final boolean issuppressIonVersionMarkerOn() {
            return this._suppress_ion_version_marker;
        }
        public final CharSequence lineSeparator() {
            return _line_separator;
        }
    }

    final private boolean _filter_symbol_tables;

    protected IonWriterUserText(IonSystem sys, IonCatalog catalog, OutputStream out, TextOptions options) {
        super(sys, new IonWriterSystemText(sys, sys.getSystemSymbolTable(), out, options),
              catalog, options.issuppressIonVersionMarkerOn());
        _filter_symbol_tables = options.isFilterSymbolTablesOn();
    }
    protected IonWriterUserText(IonSystem sys, IonCatalog catalog, Appendable out, TextOptions options) {
        super(sys, new IonWriterSystemText(sys, sys.getSystemSymbolTable(), out, options),
              catalog, options.issuppressIonVersionMarkerOn());
        _filter_symbol_tables = options.isFilterSymbolTablesOn();
    }


    @Override
    public void set_symbol_table_helper(SymbolTable prev_symbols, SymbolTable new_symbols)
        throws IOException
    {
        // for the text user writer if the symbol table
        // isn't changing we don't care
        if (prev_symbols == new_symbols) {
            return;
        }

        // cases are system symbol table - after an IVM (or not)
        // local symbol table, with or without imports
        boolean requires_this_local_table = false;
        if (new_symbols.isLocalTable()) {
            SymbolTable[] imports = new_symbols.getImportedTables();
            if (imports != null && imports.length > 0) {
                requires_this_local_table = true;
            }
        }

        boolean needs_ivm = !_after_ion_version_marker;
        if (_filter_symbol_tables) {
            if (!requires_this_local_table) {
                needs_ivm = false;
            }
        }

        assert(_system_writer == _current_writer);
        if (needs_ivm) {
            // system writer call won't recurse back on us
            _system_writer.writeIonVersionMarker();
            _after_ion_version_marker = true;
            // and no other state needs updating as we're
            // about to write and set the local table next anyway
        }

        if (requires_this_local_table) {
            // TODO: remove cast below with update IonReader over symbol table
            IonReader reader = ((UnifiedSymbolTable)new_symbols).getReader(this._system);
            // move onto and write the struct header
            IonType t = reader.next();
            assert(IonType.STRUCT.equals(t));
            String[] a = reader.getTypeAnnotations();
            assert(a != null && a.length >= 1); // you (should) always have the $ion_symbol_table annotation

            // now we'll start a local symbol table struct
            // in the underlying system writer
            _system_writer.setTypeAnnotations(a);
            _system_writer.stepIn(IonType.STRUCT);

            // step into the symbol table struct and
            // write the values - EXCEPT the symbols field
            reader.stepIn();
            for (;;) {
                t = reader.next();
                if (t == null) break;
                // get the field name and skip over 'symbols'
                String name = reader.getFieldName();
                if (SYMBOLS.equals(name)) {
                    continue;
                }
                _system_writer.writeValue(reader);
            }

            // we're done step out and move along
            _system_writer.stepOut();
        }
    }

    @Override
    UnifiedSymbolTable inject_local_symbol_table() throws IOException
    {
        // no catalog since it doesn't matter as this is a
        // pure local table, with no imports
        // we let the system writer handle this work
        assert(_system_writer instanceof IonWriterSystemText);
        UnifiedSymbolTable symbols
            = ((IonWriterSystemText)_system_writer).inject_local_symbol_table();
        return symbols;
    }
}
