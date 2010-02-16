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
     * Returns the top level value.  If this values
     * parent is a datagram or a system value it
     * is the root.  Otherwise it is the root
     * of this values container.
     *
     * @return top level owner of this value, this is never null
     */
    public IonValuePrivate getRoot();

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
     * @return SymbolTable if this value is the real
     *         owner, otherwise null
     */
    public SymbolTable getAssignedSymbolTable();

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
