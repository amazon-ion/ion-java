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
    /**
     * The symbol name prefix reserved for use by Ion implementations.
     */
    public static final String ION_RESERVED_PREFIX = "$ion_";

    /**
     * The system symbol <tt>'$ion'</tt>, as defined by Ion 1.0.
     */
    public static final String ION = "$ion";

    /**
     * The system symbol <tt>'$ion_1_0'</tt>, as defined by Ion 1.0.
     */
    public static final String ION_1_0 = "$ion_1_0";

    /**
     * The system symbol <tt>'$ion_embedded_value'</tt>, as defined by Ion 1.0.
     */
    public static final String ION_EMBEDDED_VALUE = "$ion_embedded_value";

    /**
     * The system symbol <tt>'$ion_symbol_table'</tt>, as defined by Ion 1.0.
     */
    public static final String ION_SYMBOL_TABLE = "$ion_symbol_table";

    /**
     * The system symbol <tt>'name'</tt>, as defined by Ion 1.0.
     */
    public static final String NAME = "name";

    /**
     * The system symbol <tt>'version'</tt>, as defined by Ion 1.0.
     */
    public static final String VERSION = "version";

    /**
     * The system symbol <tt>'imports'</tt>, as defined by Ion 1.0.
     */
    public static final String IMPORTS = "imports";

    /**
     * The system symbol <tt>'symbols'</tt>, as defined by Ion 1.0.
     */
    public static final String SYMBOLS = "symbols";

    /**
     * The system symbol <tt>'max_id'</tt>, as defined by Ion 1.0.
     */
    public static final String MAX_ID = "max_id";



    /**
     * Gets the system identifier for this symbol table, a string of the form
     * <code>"$ion_X_Y"</code>
     */
    public String getSystemId();
}
