// Copyright (c) 2010-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.util.Printer;

public class _Private_TextOptions
{
    private final static CharSequence SPACE_CHARACTER = " ";
    private final static CharSequence LINE_SEPARATOR = System.getProperty("line.separator");

    final boolean       _pretty_print;
    final boolean       _ascii_only;
    final CharSequence  _line_separator;
    final boolean       _filter_symbol_tables;
    final boolean       _suppress_ion_version_marker;

    /** Strings and clobs longer than this length will be rendered as
     * long-strings, but will only line-break on extant '\n' code points.
     */
    public int _long_string_threshold = Integer.MAX_VALUE;

    public boolean _blob_as_string;
    public boolean _clob_as_string;
    public boolean _decimal_as_float;
    public boolean _sexp_as_list;
    public boolean _skip_annotations;
    public boolean _string_as_json;
    public boolean _symbol_as_string;
    public boolean _timestamp_as_millis;
    public boolean _timestamp_as_string;
    public boolean _untyped_nulls;

    /**
     * Behaves like {@link Printer#setJsonMode()}
     */
    public static _Private_TextOptions prettyJson()
    {
        _Private_TextOptions o =
            new _Private_TextOptions(/*prettyPrint*/ true,
                                    /*printAscii*/ true,
                                    /*filterOutSymbolTables*/ true,
                                    /*suppressIonVersionMarker*/ true);
        o._blob_as_string      = true;
        o._clob_as_string      = true;
        // TODO datagram as list
        o._decimal_as_float    = true;
        o._sexp_as_list        = true;
        o._skip_annotations    = true;
        o._string_as_json      = true;
        o._symbol_as_string    = true;
        o._timestamp_as_millis = true;
        o._timestamp_as_string = false;
        o._untyped_nulls       = true;

        return o;
    }

    public _Private_TextOptions(boolean prettyPrint, boolean printAscii)
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
    public _Private_TextOptions(boolean prettyPrint, boolean printAscii,
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
    public _Private_TextOptions(boolean prettyPrint, boolean printAscii,
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