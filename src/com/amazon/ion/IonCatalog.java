// Copyright (c) 2007-2009 Amazon.com, Inc.  All rights reserved.

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
     * {@code null} if this catalog has no table with the name.
     */
    public SymbolTable getTable(String name);


    /**
     * Gets a desired symbol table from this catalog.
     *
     * @return the shared symbol table with the given name and version, when an
     * exact match is possible. Otherwise, returns the highest known version of
     * the requested table.  If the catalog has no table with the name, then
     * this method returns {@code null}.
     */
    public SymbolTable getTable(String name, int version);


    /**
     * Adds a symbol table to this catalog.  This interface does not define the
     * behavior of this method if this catalog already contains a table with
     * the same name and version.
     *
     * @param sharedTable must be shared but not a system table.
     *
     * @deprecated The Ion libraries do not need to insert symbol tables, so
     * this method is being removed.
     */
    @Deprecated
    public void putTable(SymbolTable sharedTable);
}
