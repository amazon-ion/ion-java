// Copyright (c) 2007-2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import com.amazon.ion.system.SimpleCatalog;
import com.amazon.ion.system.SystemFactory;



/**
 * Collects shared symbol tables for use by an {@link IonSystem}.
 * <p>
 * It is expected that many applications will implement this interface to
 * customize behavior beyond that provided by the default {@link SimpleCatalog}.
 * A typical implementation would retrieval symbol tables from some external
 * source.
 * <p>
 * To utilize a custom catalog, it must be passed to
 * {@link SystemFactory#newSystem(IonCatalog)} when a system is created, or to
 * selected methods of the {@link IonSystem} for localized use.
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
}
