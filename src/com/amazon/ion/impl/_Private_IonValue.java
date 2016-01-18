// Copyright (c) 2010-2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonValue;
import com.amazon.ion.SymbolTable;
import java.io.PrintWriter;

/**
 * NOT FOR APPLICATION USE!
 */
public interface _Private_IonValue
    extends IonValue
{
    /**
     *
     * @return int the offset of this value in its containers member list
     */
    public int         getElementId();

    /**
     * Makes this symbol table current for this value.
     * This may directly apply to this IonValue if this
     * value is either loose or a top level datagram
     * member.  Or it may be delegated to the IonContainer
     * this value is a contained in.
     * <p>
     * Assigning null forces any symbol values to be
     * resolved to strings and any associated symbol
     * table will be removed.
     * <p>
     * @param symbols must be local or system table. May be null.
     *
     * @throws UnsupportedOperationException if this is a datagram.
     */
    public void setSymbolTable(SymbolTable symbols);

    /**
     * Returns the symbol table that is directly associated with this value,
     * without doing any recursive lookup.
     * Values that are not top-level will return null as they don't actually
     * own their own symbol table.
     *
     * @throws UnsupportedOperationException if this is an {@link IonDatagram}.
     */
    public SymbolTable getAssignedSymbolTable();

    public void dump(PrintWriter out);

    public String validate();
}
