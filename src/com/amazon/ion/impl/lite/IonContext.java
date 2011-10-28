// Copyright (c) 2010 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import com.amazon.ion.ReadOnlyValueException;
import com.amazon.ion.SymbolTable;

/**
 *  This interface is implemented by the various
 *  Ion containers and by a concrete IonConcreteContext.
 *  This is used to abstract the parent, system and
 *  symbol table definitions that are shared by all
 *  IonValues.
 * <p>
 *  Generally top level values have a Concrete context
 *  and containers implement these interfaces.  Concrete
 *  contexts simply return the correct values (and
 *  own them).  Containers return themselves as the
 *  parent and delegate to their parent for the
 *  system and symbol table values.  Note that values
 *  contained directly by a datagram may store a
 *  symbol table locally as there may be more than
 *  one symbol table in a datagram.
 * <p>
 *  The IonSystemLite is a context too.  It returns
 *  null for parent, this for system, and the system symbol
 *  table.  For a local symbol table it constructs
 *  a local symbol table and attaches it to the calling
 *  child object ... how?
 */
public interface IonContext
{
    /**
     * Return the IonValue that is the parent
     * of the value associated with this context.  This name
     * is a bit misleading in that the method is called
     * from the contained value (the child) and
     * in the case of most IonContainers (like IonStruct)
     * the method simply returns this.
     *
     * @return IonContainer that is the parent of the value
     */
    abstract IonContainerLite getParentThroughContext();

    /**
     * Get the IonSystem concrete object that created
     * the IonValue that is associated with this context.
     * Generally this delegates to the parent.
     *
     * @return not null
     */
    abstract IonSystemLite getSystem();

    /**
     * Get the SymbolTable for values in this context.
     * Generally this delegates to the parent.
     *
     * @return SymbolTable
     */
    abstract SymbolTable getSymbolTable();

    /**
     * Returns the symbol table that is directly assigned to this context.
     * For {@link IonConcreteContext} it is the symbol table member.
     * For {@link IonContainerLite} and {@link IonSystemLite} it is null.
     *
     * @return the directly assigned symbol table, with no recursive lookup.
     */
    abstract SymbolTable getContextSymbolTable();

    /**
     * Get a SymbolTable associated with this
     * attached IonValue the can be used to
     * define symbols.  A local symbol table
     * as distinct from a read only symbol table,
     * such as a system symbol table.
     *
     * Generally this delegates to the parent
     * who may need to create a new symbol
     * table.
     *
     * @param child the IonValue of the child requesting the local symbol table, used to back patch
     *
     * @return SymbolTable updatable symbol table
     *
     * @throws ReadOnlyValueException if the IonValue is read only
     */
    abstract SymbolTable getLocalSymbolTable(IonValueLite child);

    /**
     *  Set the SymbolTable of the associated IonValue.
     *  Only a system symbol table or a local symbol
     *  table is a valid parameter.
     *
     *  This may delegate to the parent container or
     *  in the case of a concrete context it will
     *  store the reference itself.
     *
     *  It is not valid to overwrite a current local
     *  symbol table with another local symbol table.
     *
     * @param symbols the system or local symbol table for this IonValue
     * @param child the child requesting its symbol table be set used to back patch the context if necessary
     *
     * @throws IllegalArgumentException if the symbol table is null, or a shared symbol table
     * @throws IllegalStateException if a local symbol table is already the current symbol table
     */
    abstract void setSymbolTableOfChild(SymbolTable symbols, IonValueLite child);

    /**
     * clears the reference to any local symbol table.  This is used after
     * (or during) the clear symbol ID process called when marking a value
     * as read only.
     */
    abstract void clearLocalSymbolTable();

    /**
     * Sets the parent container for this values context object.  Generally
     * used when a local symbol table is being created for a top level
     * value and so an intermediate context is needed to find the value.
     * @param newParent the parent of the value this context is bound to
     */
    abstract void setParentThroughContext(IonValueLite child, IonContext newParent);

}
