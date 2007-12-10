/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;



/**
 * Collects static symbol tables for use by an {@link IonSystem}.
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
     * @return <code>null</code> if not found.
     */
    public StaticSymbolTable getTable(String name);


    /**
     * Gets a desired symbol table from this catalog.
     *
     * @return the table with the given name and version, or <code>null</code>
     * if there's no match.
     */
    public StaticSymbolTable getTable(String name, int version);


    /**
     * Adds a symbol table to this catalog.  This interface does not define the
     * behavior of this method if this catalog already contains a table with
     * the same name and version.
     *
     * @param table must have a valid name and version.
     */
    public void putTable(StaticSymbolTable table);
}
