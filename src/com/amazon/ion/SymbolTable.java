// Copyright (c) 2007-2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import java.io.IOException;
import java.util.Iterator;

/**
 * A symbol table maps symbols between their textual form and an integer ID
 * used in the binary encoding.
 * <p>
 * Implementations of this interface must be safe for use by multiple threads.
 */
public interface SymbolTable
{
    /**
     * Indicates that a symbol's integer ID could not be determined.  That's
     * generally the case when constructing value instances that are not yet
     * contained by a datagram.
     */
    public final static int UNKNOWN_SYMBOL_ID = -1;


    /**
     * Determines whether this symbol table is local, and therefore unnamed,
     * unversioned, and modifiable.
     * <p>
     * If this method returns {@code true}, then both {@link #isSharedTable()}
     * and {@link #isSystemTable()} will return {@code false}.
     */
    public boolean isLocalTable();

    /**
     * Determines whether this symbol table is shared, and therefore named,
     * versioned, and unmodifiable.
     * <p>
     * If this method returns {@code true}, then {@link #isLocalTable()}
     * will return {@code false}.
     */
    public boolean isSharedTable();


    /**
     * Determines whether this symbol table is a system symbol table, and
     * therefore shared, named, versioned, and unmodifiable.
     * <p>
     * If this method returns {@code true}, then {@link #isLocalTable()}
     * will return {@code false} and {@link #isSharedTable()} will return
     * {@code true}.
     */
    public boolean isSystemTable();


    /**
     * Gets the unique name of this symbol table.
     *
     * @return the unique name, or {@code null} if {@link #isLocalTable()}.
     */
    public String getName();


    /**
     * Gets the version of this symbol table.
     *
     * @return at least one, or zero if {@link #isLocalTable()}.
     */
    public int getVersion();


    /**
     * Gets the system symbol table being used by this local table.
     * <p>
     * If {@link #isSystemTable()} then this method returns {@code this}.
     * Otherwise, if {@link #isSharedTable()} then this method returns
     * {@code null}.
     *
     * @return not <code>null</code>, except for non-system shared tables.
     */
    public SymbolTable getSystemSymbolTable();


    /**
     * Gets the identifier for the system symbol table used by this table.
     * The system identifier is a string of the form {@code "$ion_X_Y"}.
     *
     * @return the system identifier; or {@code null} for non-system shared
     *  tables.
     *
     * @deprecated Renamed to {@link #getIonVersionId()}.
     */
    @Deprecated
    public String getSystemId();

    /**
     * Gets the identifier for the Ion version (and thus the system symbol
     * table) used by this table.
     * The version identifier is a string of the form {@code "$ion_X_Y"}.
     *
     * @return the version identifier; or {@code null} for non-system shared
     *  tables.
     */
    public String getIonVersionId();


    /**
     * Gets the sequence of shared symbol tables imported by this (local)
     * symbol table. The result does not include a system table.
     * <p>
     * If this local table imported a shared table that was not available in
     * the appropriate {@link IonCatalog}, then that entry will be a dummy
     * table with no known symbol text.
     *
     * @return {@code null} if this is a shared or system table, otherwise a
     * non-null but potentially zero-length array of shared tables (but no
     * system table).
     */
    public SymbolTable[] getImportedTables();


    /**
     * Gets the highest symbol id reserved by this table's imports (including
     * system symbols). Any id higher than this value is a local symbol
     * declared by this table. This value is zero for shared symbol tables,
     * since they do not utilize imports.
     */
    public int getImportedMaxId();


    /**
     * Gets the highest symbol id reserved by this table.
     *
     * @return the largest integer such that {@link #findSymbol(int)} could
     * return a non-<code>null</code> result.  Note that there is no promise
     * that it <em>will</em> return a name, only that any larger id will not
     * have a name defined.
     */
    public int getMaxId();


    /**
     * Returns the number of symbols in this table, not counting imported
     * symbols (when {@link #isLocalTable()} is {@code true}).
     * If it contains more than <code>Integer.MAX_VALUE</code> elements,
     * returns <code>Integer.MAX_VALUE</code>.
     *
     * @return the number of symbols in this table.
     * @deprecated Turns out this isn't particularly meaningful
     */
    @Deprecated
    public int size();


    /**
     *
     * @param name must not be null or empty.
     * @return the id of the requested symbol, or
     * {@link #UNKNOWN_SYMBOL_ID} if it's not defined.
     */
    public int findSymbol(String name);


    /**
     * Gets a name for a symbol id, whether or not a definition is known.
     * If the id is unknown then a generic name is returned.
     *
     * @param id must be greater than zero.
     * @return not <code>null</code>.
     */
    public String findSymbol(int id);


    /**
     * Gets a defined name for a symbol id.
     *
     * @param id must be greater than zero.
     * @return the name associated with the symbol id, or <code>null</code> if
     * the name is not known.
     */
    public String findKnownSymbol(int id);


    /**
     * Adds a new symbol to this table, or finds an existing id for it.
     *
     * @param name must be non-empty.
     * @return a value greater than zero.
     *
     * @throws UnsupportedOperationException if {@link #isSharedTable()}
     * and the requested symbol is not already defined.
     */
    public int addSymbol(String name);


    /**
     * Adds a new symbol to this table using a specific id.  An exception is
     * thrown if the given name is already defined with a different id.
     *
     * @param name must be non-empty.
     * @param id must be greater than zero.
     *
     * @throws UnsupportedOperationException if {@link #isSharedTable()}
     * and the requested symbol is not already defined.
     *
     * @deprecated Use {@link #addSymbol(String)}.
     */
    @Deprecated
    public void defineSymbol(String name, int id);


    /**
     * Creates an iterator that will return all non-imported symbol names, in
     * order of their symbol IDs. The iterator will return {@code null} where
     * there is an undefined sid.
     * <p>
     * The first string returned by the iterator has a symbol ID that is one
     * more than {@link #getImportedMaxId()}, and the last string has symbol
     * ID equals to {@link #getMaxId()}.
     *
     * @return a new iterator.
     */
    public Iterator<String> iterateDeclaredSymbolNames();


    /**
     * Gets the Ion structure representing this symbol table.  Changes to this
     * object are reflected in the struct; it would be
     * very unwise to modify the return value directly.
     *
     * @return a non-null struct.
     *
     * @deprecated For internal use only.
     */
    @Deprecated
    public IonStruct getIonRepresentation();


    /**
     * Writes an Ion representation of this symbol table.
     *
     * @param writer must not be null.
     * @throws IOException if thrown by the writer.
     */
    public void writeTo(IonWriter writer)
        throws IOException;
}
