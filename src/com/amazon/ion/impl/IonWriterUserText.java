// Copyright (c) 2010-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.system.IonWriterBuilder.InitialIvmHandling.SUPPRESS;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonSystem;
import com.amazon.ion.ValueFactory;
import com.amazon.ion.system.IonTextWriterBuilder;
import java.io.OutputStream;


/**
 *
 */
public class IonWriterUserText
    extends IonWriterUser
{
    @Deprecated
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
        public TextOptions(boolean prettyPrint, boolean printAscii,
                           boolean filterOutSymbolTables)
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
        public TextOptions(boolean prettyPrint, boolean printAscii,
                           boolean filterOutSymbolTables,
                           boolean suppressIonVersionMarker)
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


    private static _Private_IonTextWriterBuilder builderFor(IonCatalog catalog,
                                                            TextOptions options)
    {
        _Private_IonTextWriterBuilder b =
            _Private_IonTextWriterBuilder.standard();
        if (options._pretty_print)
        {
            b.withPrettyPrinting();
        }
        if (options._ascii_only)
        {
            b.setCharset(IonTextWriterBuilder.ASCII);
        }
        if (options._suppress_ion_version_marker)
        {
            b.setInitialIvmHandling(SUPPRESS);
        }
        b.setCatalog(catalog);
        return b;
    }



    IonWriterUserText(ValueFactory symtabValueFactory,
                      IonWriterSystemText systemWriter)
    {
        super(systemWriter.getOptions().getCatalog(),
              symtabValueFactory,
              systemWriter,
              systemWriter.getOptions().getInitialIvmHandling() == SUPPRESS,
              systemWriter.getOptions().getImports());
    }


    /** @deprecated */
    @Deprecated
    protected IonWriterUserText(IonSystem sys, IonCatalog catalog,
                                OutputStream out, TextOptions options)
    {
        this(sys,
             new IonWriterSystemText(sys.getSystemSymbolTable(),
                                     builderFor(catalog, options),
                                     out));
    }

    /** @deprecated */
    @Deprecated
    protected IonWriterUserText(IonSystem sys, IonCatalog catalog,
                                Appendable out, TextOptions options)
    {
        this(sys,
             new IonWriterSystemText(sys.getSystemSymbolTable(),
                                     builderFor(catalog, options),
                                     out));
    }
}
