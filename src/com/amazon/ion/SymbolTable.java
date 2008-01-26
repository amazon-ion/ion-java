/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

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
     * symbols (in the case of a {@link LocalSymbolTable}).
     * If it contains more than <code>Integer.MAX_VALUE</code> elements,
     * returns <code>Integer.MAX_VALUE</code>.
     *
     * @return the number of symbols in this table.
     */
    public int size();


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
