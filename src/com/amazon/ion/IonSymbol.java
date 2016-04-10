// Copyright (c) 2007-2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

/**
 * An Ion <code>symbol</code> value.
 * <p>
 * <b>WARNING:</b> This interface should not be implemented or extended by
 * code outside of this library.
 */
public interface IonSymbol
    extends IonText
{
    /**
     * Gets the text content of this symbol.
     *
     * @return the text of the symbol, or <code>null</code> if this is
     * <code>null.symbol</code>.
     *
     * @throws UnknownSymbolException if this symbol has unknown text.
     */
    public String stringValue()
        throws UnknownSymbolException;

    /**
     * Returns this value as a symbol token (text + ID).
     *
     * @return null if {@link #isNullValue()}
     *
     * @since IonJava R15
     */
    public SymbolToken symbolValue();


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


    public IonSymbol clone()
        throws UnknownSymbolException;
}
