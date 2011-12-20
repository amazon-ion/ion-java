// Copyright (c) 2010-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.SystemSymbols.SYMBOLS;

import com.amazon.ion.InternedSymbol;
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
    @Deprecated
    static public class TextOptions extends $PrivateTextOptions
    {
        public TextOptions(boolean prettyPrint, boolean printAscii)
        {
            super(prettyPrint, printAscii);
        }
        public TextOptions(boolean prettyPrint, boolean printAscii,
                           boolean filterOutSymbolTables)
        {
            super(prettyPrint, printAscii, filterOutSymbolTables);
        }
        public TextOptions(boolean prettyPrint, boolean printAscii,
                           boolean filterOutSymbolTables,
                           boolean suppressIonVersionMarker)
        {
            super(prettyPrint, printAscii, filterOutSymbolTables,
                  suppressIonVersionMarker);
        }
    }

    final private boolean _filter_symbol_tables;

    protected IonWriterUserText(IonSystem sys, IonCatalog catalog,
                                OutputStream out, $PrivateTextOptions options)
    {
        super(catalog, sys,
              new IonWriterSystemText(sys.getSystemSymbolTable(),
                                      out, options),
              true /* rootIsDatagram */,
              options.issuppressIonVersionMarkerOn());
        _filter_symbol_tables = options.isFilterSymbolTablesOn();
    }

    protected IonWriterUserText(IonSystem sys, IonCatalog catalog,
                                Appendable out, $PrivateTextOptions options)
    {
        super(catalog, sys,
              new IonWriterSystemText(sys.getSystemSymbolTable(),
                                      out, options),
              true /* rootIsDatagram */,
              options.issuppressIonVersionMarkerOn());
        _filter_symbol_tables = options.isFilterSymbolTablesOn();
    }


    @Override
    public void set_symbol_table_helper(SymbolTable prev_symbols,
                                        SymbolTable new_symbols)
        throws IOException
    {
        // for the text user writer if the symbol table
        // isn't changing we don't care
        if (prev_symbols == new_symbols) {
            return;
        }

        // cases are system symbol table - after an IVM (or not)
        // local symbol table, with or without imports
        boolean newSymtabIsLocalWithImports = false;
        if (new_symbols.isLocalTable()) {

            // TODO this always ignores local symtabs w/o imports

            SymbolTable[] imports = new_symbols.getImportedTables();
            if (imports != null && imports.length > 0) {
                newSymtabIsLocalWithImports = true;
            }
        }

        // TODO This looks wrong.  We don't require an IVM just because we're
        // changing symtabs.
        boolean needs_ivm = !_previous_value_was_ivm;
        if (_filter_symbol_tables && !newSymtabIsLocalWithImports) {
            // new_symbols is system, or local w/no imports
            needs_ivm = false;
        }

        // system table
        //   if _filter_symbol_tables, do nothing
        //       TODO seems wrong in general, esp beyond Ion 1.0
        //   if _previous_value_was_ivm, do nothing
        //   else write IVM

        // local table no imports
        //   if _filter_symbol_tables, do nothing
        //   if _previous_value_was_ivm, do nothing
        //   else write IVM
        //      TODO seems wrong. Why write anything?

        // local table w/ imports
        //   if _previous_value_was_ivm, write symtab (skipping symbols)
        //   else write IVM, then write symtab (skipping symbols)
        //      TODO Wrong. Could break open content in the symtab.


        assert(_system_writer == _current_writer);
        if (needs_ivm) {
            // system writer call won't recurse back on us
            _system_writer.writeIonVersionMarker();
            _previous_value_was_ivm = true;
            // and no other state needs updating as we're
            // about to write and set the local table next anyway
        }

        if (newSymtabIsLocalWithImports) {
            // TODO: remove cast below with update IonReader over symbol table
            IonReader reader =
                ((UnifiedSymbolTable)new_symbols).getReader();
            // move onto and write the struct header
            IonType t = reader.next();
            assert(IonType.STRUCT.equals(t));
            InternedSymbol[] a = reader.getTypeAnnotationSymbols();
            // you (should) always have the $ion_symbol_table annotation
            assert(a != null && a.length >= 1);

            // now we'll start a local symbol table struct
            // in the underlying system writer
            _system_writer.setTypeAnnotationSymbols(a);
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
}
