/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;


/**
 * A read-only symbol table containing predefined Ion system symbols.
 */
public interface SystemSymbolTable
    extends SymbolTable
{
    public static final String[] SYSTEM_SYMBOLS =
    {
        SystemSymbolTable.ION,
        SystemSymbolTable.ION_1_0,
        SystemSymbolTable.ION_SYMBOL_TABLE,
        SystemSymbolTable.NAME,
        SystemSymbolTable.VERSION,
        SystemSymbolTable.IMPORTS,
        SystemSymbolTable.SYMBOLS,
        SystemSymbolTable.MAX_ID,
        SystemSymbolTable.ION_EMBEDDED_VALUE
    };

    /**
     * The symbol name prefix reserved for use by Ion implementations.
     */
    public static final String ION_RESERVED_PREFIX = "$ion_";

    /**
     * The system symbol <tt>'$ion'</tt>, as defined by Ion 1.0.
     */
    public static final String ION = "$ion";
    public static final int    ION_SID = 1;

    /**
     * The system symbol <tt>'$ion_1_0'</tt>, as defined by Ion 1.0.
     */
    public static final String ION_1_0 = "$ion_1_0";
    public static final int    ION_1_0_SID = 2;

    /**
     * The system symbol <tt>'$ion_symbol_table'</tt>, as defined by Ion 1.0.
     */
    public static final String ION_SYMBOL_TABLE = "$ion_symbol_table";
    public static final int    ION_SYMBOL_TABLE_SID = 3;

    /**
     * The system symbol <tt>'name'</tt>, as defined by Ion 1.0.
     */
    public static final String NAME = "name";
    public static final int    NAME_SID = 4;

    /**
     * The system symbol <tt>'version'</tt>, as defined by Ion 1.0.
     */
    public static final String VERSION = "version";
    public static final int    VERSION_SID = 5;

    /**
     * The system symbol <tt>'imports'</tt>, as defined by Ion 1.0.
     */
    public static final String IMPORTS = "imports";
    public static final int    IMPORTS_SID = 6;

    /**
     * The system symbol <tt>'symbols'</tt>, as defined by Ion 1.0.
     */
    public static final String SYMBOLS = "symbols";
    public static final int    SYMBOLS_SID = 7;

    /**
     * The system symbol <tt>'max_id'</tt>, as defined by Ion 1.0.
     */
    public static final String MAX_ID = "max_id";
    public static final int    MAX_ID_SID = 8;

    /**
     * The system symbol <tt>'$ion_embedded_value'</tt>, as defined by Ion 1.0.
     */
    public static final String ION_EMBEDDED_VALUE = "$ion_embedded_value";
    public static final int    ION_EMBEDDED_VALUE_SID = 9;
}
