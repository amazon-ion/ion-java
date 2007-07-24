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
     * Gets the system identifier for this symbol table, a string of the form
     * <code>"$ion_X_Y"</code>
     */
    public String getSystemId();
}
