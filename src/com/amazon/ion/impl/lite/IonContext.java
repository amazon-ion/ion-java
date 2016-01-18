// Copyright (c) 2010-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import com.amazon.ion.IonContainer;
import com.amazon.ion.SymbolTable;

/**
 * Provides the parent, system and symbol table definitions that are shared
 * by one or more hierarchies of IonValues.
 * <p>
 * The type of context used by a value depends on its position.
 * <ul>
 *   <li>Top-level values have either a {@link TopLevelContext} or an
 *   {@link IonSystemLite} for a context.
 *   <li>Non-top-level values have their {@link IonContainer} for a context.
 *   <li>Datagrams have their {@link IonSystemLite} for a context.
 * </ul>
 * <p>
 *  Concrete
 *  contexts simply return the correct values (and
 *  own them).  Containers return themselves as the
 *  parent and delegate to their parent for the
 *  system and symbol table values.  Note that values
 *  contained directly by a datagram may store a
 *  symbol table locally as there may be more than
 *  one symbol table in a datagram.
 */
interface IonContext
{
    /**
     * Return the container of values associated with this context.
     * If this context is an {@link IonContainerLite} it returns itself.
     *
     * @return the container of the value;
     *  null for stand-alone top level values.
     */
    abstract IonContainerLite getContextContainer();

    /**
     * Get the IonSystem concrete object that created
     * the IonValue that is associated with this context.
     * Generally this delegates to the parent.
     *
     * @return not null
     */
    abstract IonSystemLite getSystem();


    /**
     * Returns the symbol table that is directly assigned to this context.
     * For {@link TopLevelContext} it is the symbol table member.
     * For {@link IonContainerLite} and {@link IonSystemLite} it is null.
     *
     * @return the directly assigned symbol table, with no recursive lookup.
     */
    abstract SymbolTable getContextSymbolTable();
}
