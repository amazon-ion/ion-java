/*
 * Copyright (c) 2007-2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;



/**
 * Collects shared symbol tables for use by an {@link IonSystem}.
 * It is expected that
 * applications may implement this interface to customize caching behavior.
 */
public interface IonCatalog
{
    /**
     * Gets a symbol table with a specific name and the highest version
     * possible.
     *
     * @param name identifies the desired symbol table.
     * @return a shared symbol table with the given name, or
     * {@code null} if not found.
     */
    public SymbolTable getTable(String name);


    /**
     * Gets a desired symbol table from this catalog.
     *
     * @return the shared symbol table with the given name and version,
     * or {@code null} if there's no match.
     */
    public SymbolTable getTable(String name, int version);


    /**
     * Adds a symbol table to this catalog.  This interface does not define the
     * behavior of this method if this catalog already contains a table with
     * the same name and version.
     *
     * @param sharedTable must have {@link SymbolTable#isSharedTable()}
     * return {@code true}.
     */
    public void putTable(SymbolTable sharedTable);
}
