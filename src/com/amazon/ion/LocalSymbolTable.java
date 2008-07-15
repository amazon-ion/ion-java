/*
 * Copyright (c) 2007-2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;


/**
 *
 */
public interface LocalSymbolTable
    extends SymbolTable
{
    /**
     * Gets the system symbol table being used by this local table.
     *
     * @return not <code>null</code>.
     */
    public SymbolTable getSystemSymbolTable();


    /**
     * Indicates whether this local table has imported any static tables.
     */
    public boolean hasImports();


    /**
     * @param name identifies the desired table.
     * @return the used table, or <code>null</code> if it's not in use.
     * @throws NullPointerException if <code>name</code> is null.
     */
    public SymbolTable getImportedTable(String name);


    /**
     * Adds a new symbol to this table, or finds an existing id for it.
     *
     * @param name must be non-empty.
     * @return a value greater than zero.
     */
    public int addSymbol(String name);


    /**
     * Adds a new symbol to this table using a specific id.  An exception is
     * thrown if the given name is already defined with a different id.
     *
     * @param name must be non-empty.
     * @param id must be greater than zero.
     */
    public void defineSymbol(String name, int id);
}
