// Copyright (c) 2010 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonValue;
import com.amazon.ion.SymbolTable;

/**
 *
 */
public interface IonValuePrivate
    extends IonValue
{
    /**
     *
     * @return int the offset of this value in its containers member list
     */
    public int         getElementId();


    /**
     * make this symbol table current for this value.
     * This may directly apply to this IonValue if this
     * value is either loose or a top level datagram
     * member.  Or it may be delegated to the IonContainer
     * this value is a contained in.
     *
     * Assigning null forces any symbol values to be
     * resolved to strings and any associated symbol
     * table will be removed.
     *
     * @param symbols
     */
    public void        setSymbolTable(SymbolTable symbols);

    /**
     * this returns the symbol table that is actually
     * assigned to this value.  Values that are contained
     * will return null as they don't actually own
     * their own symbol table.
     * <p>
     * FIXME: I believe that last sentence is incorrect.
     * Values contained by a lite datagram, at least, have assigned symtabs.
     *
     * DG Lite returns null.  DG lazy returns its own.  TODO what is that?
     *
     * @return SymbolTable if this value is the real
     *         owner, otherwise null
     */
    public SymbolTable getAssignedSymbolTable();

    /**
     * this returns the current values symbol table,
     * which is typically
     * owned by the top level value, if the current
     * symbol table is a local symbol table.  Otherwise
     * this replaces the current symbol table with a
     * new local symbol table based on the current Ion system version.
     * @return SymbolTable that is updatable (i.e. a local symbol table)
     */
    public SymbolTable getUpdatableSymbolTable();

    /**
     * checks in the current symbol table for this
     * symbol (name) and returns the symbol id if
     * this symbol is defined.
     *
     * @param name text for the symbol of interest
     * @return int symbol id if found or
     *          UnifiedSymbolTable.UNKNOWN_SID if
     *          it is not already defined
     */
    public int resolveSymbol(String name);

    /**
     * checks in the current symbol table for this
     * symbol id (sid) and returns the symbol text if
     * this symbol is defined.
     *
     * @param sid symbol id of interest
     * @return String symbol text if found or
     *          null if it is not already defined
     */
    public String resolveSymbol(int sid);

    /**
     * adds a symbol name to the current symbol
     * table.  This may change the current symbol
     * table if the current symbol table is either
     * null or not updatable.
     * @param name symbol text to be added
     * @return int symbol id of the existing, or
     *             newly defined, symbol
     */
    public int addSymbol(String name);

    /**
     * Force any symbols contained in this value to be
     * resolved in the local symbol table.  This causes
     * symbol ids to be assigned to all symbol values.
     * All field name and annotations to be present in
     * the current symbol table.  And it causes any
     * symbols with sids to have their symbol name
     * filled in.
     *
     * @param symbols the current symbol table or null
     * @return the symbol table after symbol resolution
     */
    public SymbolTable populateSymbolValues(SymbolTable symbols);

}
