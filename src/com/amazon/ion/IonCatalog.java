// Copyright (c) 2007-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import com.amazon.ion.system.IonSystemBuilder;
import com.amazon.ion.system.SimpleCatalog;


/**
 * Collects shared symbol tables for use by an {@link IonSystem}.
 * <p>
 * It is expected that many applications will implement this interface to
 * customize behavior beyond that provided by the default {@link SimpleCatalog}.
 * A typical implementation would retrieve symbol tables from some external
 * source.
 * <p>
 * To utilize a custom catalog, it must be given to the
 * {@link IonSystemBuilder} before a system is built, or to
 * selected methods of the {@link IonSystem} for localized use.
 *
 * <h2>Notes for Implementors</h2>
 * This interface defines two methods with subtly different semantics.
 * The first variant takes only a symbol table name, and returns the highest
 * version possible. The second takes a version number and attempts to match it
 * exactly, and if that's not possible falls back to the the "best match"
 * possible:
 * <ul>
 *   <li>If any versions <em>larger</em> than the requested version are
 *     available, select the smallest among them (still larger than requested).
 *   <li>Otherwise all available versions are <em>smaller</em> than the
 *     requested version, so return the largest of them.
 * </ul>
 * <p>
 * When encoding Ion binary data, its always best to use an exact match to the
 * requested version whenever possible.  Earlier versions are very likely to be
 * missing symbols that are needed by the data.  Later versions of the table
 * could have the same problem, but that's less likely under best practices.
 * <p>
 * While "get latest version" is generally okay for encoding, it's not
 * universally acceptable: one can imagine a client/server protocol where the
 * client declares what symtab/versions it can handle, and the server needs to
 * meet those requirements.
 * <p>
 * Binary <em>decoding</em> prefers an exact match, and in a couple edge cases,
 * requires it. Therefore a single "get latest version" method is insufficient.
 * See <a href="https://w?Ion/Symbols">https://w?Ion/Symbols</a> for more
 * details on this topic.
 * <p>
 * It's expected that many if not most applications will implement a dynamic
 * catalog that can fetch symtabs from some source.  In such cases the catalog
 * should make its best effort to find an exact match, and if that's not
 * possible fall back to the best match it can acquire.
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
     * Gets a desired symbol table from this catalog, using an exact match if
     * possible.
     * <p>
     * Implentations must make a best effort to find an exact match.
     * If an exact match cannot be found, then this method must make a best
     * effort to find the best match available.
     *
     * @return the shared symbol table with the given name and version, when an
     * exact match is possible. Otherwise, returns the lowest possible version
     * larger than requested.  Otherwise, return the largest possible version
     * lower than requested.  If no table with the name can be found, then
     * this method returns {@code null}.
     */
    public SymbolTable getTable(String name, int version);
}
