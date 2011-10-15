// Copyright (c) 2010-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

public class $PrivateTextOptions
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

    public $PrivateTextOptions(boolean prettyPrint, boolean printAscii)
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
    public $PrivateTextOptions(boolean prettyPrint, boolean printAscii, boolean filterOutSymbolTables)
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
    public $PrivateTextOptions(boolean prettyPrint, boolean printAscii, boolean filterOutSymbolTables, boolean suppressIonVersionMarker)
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