/*
 * Copyright (c) 2007-2011 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;


/**
 * A read-only symbol table containing predefined Ion system symbols.
 *
 * @deprecated Since R13.  Use {@link SystemSymbols} instead.
 */
@Deprecated
public interface SystemSymbolTable  // TODO Remove this.  ION-251
    extends SymbolTable
{
    @Deprecated
    public static final String[] SYSTEM_SYMBOLS =
    {
        SystemSymbols.ION,
        SystemSymbols.ION_1_0,
        SystemSymbols.ION_SYMBOL_TABLE,
        SystemSymbols.NAME,
        SystemSymbols.VERSION,
        SystemSymbols.IMPORTS,
        SystemSymbols.SYMBOLS,
        SystemSymbols.MAX_ID,
        SystemSymbols.ION_SHARED_SYMBOL_TABLE
    };

    @Deprecated
    public static final int ION_1_0_MAX_ID = 9;


    /**
     * The symbol name prefix reserved for use by Ion implementations.
     */
    @Deprecated
    public static final String ION_RESERVED_PREFIX = "$ion_";

    /**
     * The system symbol <tt>'$ion'</tt>, as defined by Ion 1.0.
     */
    @Deprecated
    public static final String ION = SystemSymbols.ION;
    @Deprecated
    public static final int    ION_SID = SystemSymbols.ION_SID;

    /**
     * The system symbol <tt>'$ion_1_0'</tt>, as defined by Ion 1.0.
     * This value is the Version Identifier for Ion 1.0.
     */
    @Deprecated
    public static final String ION_1_0 = SystemSymbols.ION_1_0;
    @Deprecated
    public static final int    ION_1_0_SID = SystemSymbols.ION_1_0_SID;

    /**
     * The system symbol <tt>'$ion_symbol_table'</tt>, as defined by Ion 1.0.
     */
    @Deprecated
    public static final String ION_SYMBOL_TABLE = SystemSymbols.ION_SYMBOL_TABLE;
    @Deprecated
    public static final int    ION_SYMBOL_TABLE_SID = SystemSymbols.ION_SYMBOL_TABLE_SID;

    /**
     * The system symbol <tt>'name'</tt>, as defined by Ion 1.0.
     */
    @Deprecated
    public static final String NAME = SystemSymbols.NAME;
    @Deprecated
    public static final int    NAME_SID = SystemSymbols.NAME_SID;

    /**
     * The system symbol <tt>'version'</tt>, as defined by Ion 1.0.
     */
    @Deprecated
    public static final String VERSION = SystemSymbols.VERSION;
    @Deprecated
    public static final int    VERSION_SID = SystemSymbols.VERSION_SID;

    /**
     * The system symbol <tt>'imports'</tt>, as defined by Ion 1.0.
     */
    @Deprecated
    public static final String IMPORTS = SystemSymbols.IMPORTS;
    @Deprecated
    public static final int    IMPORTS_SID = SystemSymbols.IMPORTS_SID;

    /**
     * The system symbol <tt>'symbols'</tt>, as defined by Ion 1.0.
     */
    @Deprecated
    public static final String SYMBOLS = SystemSymbols.SYMBOLS;
    @Deprecated
    public static final int    SYMBOLS_SID = SystemSymbols.SYMBOLS_SID;

    /**
     * The system symbol <tt>'max_id'</tt>, as defined by Ion 1.0.
     */
    @Deprecated
    public static final String MAX_ID = SystemSymbols.MAX_ID;
    @Deprecated
    public static final int    MAX_ID_SID = SystemSymbols.MAX_ID_SID;

    /**
     * The system symbol <tt>'$ion_embedded_value'</tt>, as defined by Ion 1.0.
     */
    @Deprecated
    public static final String ION_EMBEDDED_VALUE = "$ion_embedded_value";
    @Deprecated
    public static final int    ION_EMBEDDED_VALUE_SID = 9;

    /*
     * The system symbol <tt>'$ion_shared_symbol_table'</tt>, as defined by Ion 1.0.
     */
    @Deprecated
    public static final String ION_SHARED_SYMBOL_TABLE = SystemSymbols.ION_SHARED_SYMBOL_TABLE;
    @Deprecated
    public static final int    ION_SHARED_SYMBOL_TABLE_SID = SystemSymbols.ION_SHARED_SYMBOL_TABLE_SID;
}
