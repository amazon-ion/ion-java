/* Copyright (c) 2007-2008 Amazon.com, Inc.  All rights reserved. */

package com.amazon.ion;

/**
 * An Ion <code>symbol</code> value.
 */
public interface IonSymbol
    extends IonText
{
    /**
     * Indicates that a symbol's integer ID could not be determined.  That's
     * generally the case when constructing value instances that are not yet
     * contained by a datagram.
     */
    public final static int UNKNOWN_SYMBOL_ID = -1;

    /**
     * Gets the characters that name this symbol.
     *
     * @return the text of the symbol, or <code>null</code> if this is
     * <code>null.symbol</code>.
     */
    public String stringValue();

    /**
     * Gets the integer symbol id used in the binary encoding of this symbol.
     *
     * @return an integer greater than zero, if this value has an associated
     * symbol table.  Otherwise, return {@link #UNKNOWN_SYMBOL_ID}.
     *
     * @throws NullValueException if this is <code>null.symbol</code>.
     * @deprecated Use {@link #getSymbolId()} instead.
     */
    @Deprecated
    public int intValue()
        throws NullValueException;

    /**
     * Gets the integer symbol id used in the binary encoding of this symbol.
     *
     * @return an integer greater than zero, if this value has an associated
     * symbol table.  Otherwise, return {@link #UNKNOWN_SYMBOL_ID}.
     *
     * @throws NullValueException if this is <code>null.symbol</code>.
     */
    public int getSymbolId()
        throws NullValueException;


    /**
     * Changes the value of this element.
     *
     * @param value the new value of this symbol;
     * may be <code>null</code> to make this <code>null.symbol</code>.
     *
     * @throws EmptySymbolException if <code>value</code> is the empty string.
     */
    public void setValue(String value)
        throws EmptySymbolException;


    public IonSymbol clone();
}
