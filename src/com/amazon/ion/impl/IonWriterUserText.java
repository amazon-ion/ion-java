// Copyright (c) 2010 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonIterationType;
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

        public TextOptions(boolean prettyPrint, boolean printAscii) {
            _pretty_print = prettyPrint;
            _ascii_only   = printAscii;
            _filter_symbol_tables = false;
            if (_pretty_print) {
                _line_separator = LINE_SEPARATOR;
            }
            else {
                _line_separator = SPACE_CHARACTER;
            }
        }
        public TextOptions(boolean prettyPrint, boolean printAscii, boolean filterOutSymbolTables) {
            _pretty_print = prettyPrint;
            _ascii_only   = printAscii;
            _filter_symbol_tables = filterOutSymbolTables;
            if (_pretty_print) {
                _line_separator = LINE_SEPARATOR;
            }
            else {
                _line_separator = SPACE_CHARACTER;
            }
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
        public final CharSequence lineSeparator() {
            return _line_separator;
        }
    }

    final private boolean _filter_symbol_tables;

    protected IonWriterUserText(IonSystem sys, OutputStream out, TextOptions options) {
        super(new IonWriterSystemText(sys, out, options), null);
        _filter_symbol_tables = options.isFilterSymbolTablesOn();
    }
    protected IonWriterUserText(IonSystem sys, Appendable out, TextOptions options) {
        super(new IonWriterSystemText(sys, out, options), null);
        _filter_symbol_tables = options.isFilterSymbolTablesOn();
    }

    @Override
    public IonIterationType getIterationType()
    {
        return IonIterationType.USER_TEXT;
    }

    @Override
    public void setSymbolTable(SymbolTable symbols)
        throws IOException
    {
        if (getDepth() > 0) {
            throw new IllegalStateException("you cannot set the symbol table unless you are at the top level");
        }

        // This ensures that symbols is system or local
        super.setSymbolTable(symbols);

        // cases are system symbol table - after an IVM (or not)
        // local symbol table, with or without imports
        boolean needs_ivm = !_after_ion_version_marker;
        boolean needs_local_table = false;
        if (symbols.isLocalTable()) {
            SymbolTable[] imports = symbols.getImportedTables();
            if (imports != null && imports.length > 0) {
                needs_local_table = true;
            }
        }
        if (_filter_symbol_tables) {
            if (!needs_local_table) {
                needs_ivm = false;
            }
        }

        if (needs_ivm) {
            _system_writer.writeIonVersionMarker();
            // no other state needs updating as we're
            // about to write the local table next anyway
        }

        if (needs_local_table) {
            IonReader reader = symbols.getReader();
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
                if (UnifiedSymbolTable.SYMBOLS.equals(name)) {
                    continue;
                }
                _system_writer.writeValue(reader);
            }

            // we're done step out and move along
            _system_writer.stepOut();
        }
    }
}
