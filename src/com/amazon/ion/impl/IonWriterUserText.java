// Copyright (c) 2010-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.SystemSymbols.SYMBOLS;
import static com.amazon.ion.impl.UnifiedSymbolTable.initialSymbolTable;

import com.amazon.ion.InternedSymbol;
import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonException;
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
    static public class TextOptions extends _Private_TextOptions
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


    protected IonWriterUserText(IonCatalog catalog,
                                IonSystem system,
                                _Private_TextOptions options,
                                IonWriterSystemText systemWriter,
                                SymbolTable... imports)
    {
        super(catalog, system, systemWriter, true /* rootIsDatagram */,
              options.issuppressIonVersionMarkerOn());

        _filter_symbol_tables = options.isFilterSymbolTablesOn();

        if (imports != null && imports.length != 0)
        {
            SymbolTable initialSymtab = initialSymbolTable(system, imports);

            try {
                setSymbolTable(initialSymtab);
            }
            catch (IOException e) {
                throw new IonException(e);
            }
        }
    }


    /** @deprecated */
    @Deprecated
    protected IonWriterUserText(IonSystem sys, IonCatalog catalog,
                                OutputStream out, _Private_TextOptions options)
    {
        super(catalog, sys,
              new IonWriterSystemText(sys.getSystemSymbolTable(),
                                      out, options),
              true /* rootIsDatagram */,
              options.issuppressIonVersionMarkerOn());
        _filter_symbol_tables = options.isFilterSymbolTablesOn();
    }

    /** @deprecated */
    @Deprecated
    protected IonWriterUserText(IonSystem sys, IonCatalog catalog,
                                Appendable out, _Private_TextOptions options)
    {
        super(catalog, sys,
              new IonWriterSystemText(sys.getSystemSymbolTable(),
                                      out, options),
              true /* rootIsDatagram */,
              options.issuppressIonVersionMarkerOn());
        _filter_symbol_tables = options.isFilterSymbolTablesOn();
    }


    @Override
    public void set_symbol_table_helper(SymbolTable new_symbols)
        throws IOException
    {
        // TODO this should be the "minimize distant IVMs" option

        assert(_system_writer == _current_writer);

        // TODO implement _filter_symbol_tables

        if (new_symbols.isSystemTable())
        {
            // system writer call won't recurse back on us
            _system_writer.writeIonVersionMarker(new_symbols);
            _previous_value_was_ivm = true;
        }
        else // local symtab
        {
            // TODO this always ignores local symtabs w/o imports
            SymbolTable[] imports = new_symbols.getImportedTables();
            if (imports.length > 0) {
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
}
