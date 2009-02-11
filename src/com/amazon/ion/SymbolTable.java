// Copyright (c) 2007-2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

/**
 * A symbol table maps symbols between their textual form and an integer ID
 * used in the binary encoding.
 * <p>
 * Implementations of this interface must be safe for use by multiple threads.
 */
public interface SymbolTable
{
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
     */
    public String getSystemId();


    /**
     * Gets the sequence of shared symbol tables imported by this (local)
     * symbol table. The result does not include a system table.
     *
     * @return {@code null} if this is a shared or system table, otherwise a
     * non-null but potentially zero-length array of shared tables (but no
     * system table).
     */
    public SymbolTable[] getImportedTables();


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
     * Determines whether this symbol table defines any non-system symbols.
     * A trivial symtab is either a shared symtab with no symbols (maxId == 0)
     * or a local symtab with no imports and no local symbols.
     * Such a table can be safely discarded in some circumstances.
     */
    public boolean isTrivial();


    /**
     *
     * @param name must not be null or empty.
     * @return the id of the requested symbol, or
     * {@link IonSymbol#UNKNOWN_SYMBOL_ID} if it's not defined.
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
     * Gets the Ion structure representing this symbol table.  Changes to this
     * object are reflected in the struct; it would be
     * very unwise to modify the return value directly.
     *
     * @return a non-null struct.
     */
    public IonStruct getIonRepresentation();

    /**
     * Compares the two symbol table to determine if this symbol
     * table is a strict superset of the other symbol table. A
     * strict superset in the case is that this symbol table contains
     * all symbols in the other symbol table and the symbols are
     * assigned to the same ids.
     * @param other possible strict subset
     * @return true if this is a strict superset of other
     */
    public boolean isCompatible(SymbolTable other);

}
