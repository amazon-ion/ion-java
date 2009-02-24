/*
 * Copyright (c) 2007-2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;


/**
 * @deprecated Functionality has been lifted into {@link SymbolTable}.
 */
@Deprecated
public interface LocalSymbolTable
    extends SymbolTable
{
    /**
     * Indicates whether this local table has imported any static tables.
     */
    @Deprecated
    public boolean hasImports();


    /**
     * @param name identifies the desired table.
     * @return the used table, or <code>null</code> if it's not in use.
     * @throws NullPointerException if <code>name</code> is null.
     */
    @Deprecated
    public SymbolTable getImportedTable(String name);
}
