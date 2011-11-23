// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;


/**
 * Represents a symbol that's been interned into a {@link SymbolTable},
 * providing both the symbol text and the assigned symbol ID.
 * <p>
 * Any instance will have at least one of the two properties defined.
 */
public interface InternedSymbol
{
    /**
     * Gets the text of this symbol.
     * <p>
     * If the text is not known (usually due to a shared symbol table being
     * unavailable) then this method returns null.
     *  In such cases {@link #getSymbolId()} will be non-negative.
     *
     * @return the text of this symbol, or null if the text is unknown.
     */
    public String stringValue();

    /**
     * Gets the ID of this symbol.
     * <p>
     * If no ID has yet been assigned (as may be the case when processing Ion
     * text-formatted data), this method returns
     * {@link SymbolTable#UNKNOWN_SYMBOL_ID}.
     * In such cases {@link #stringValue()} will be non-null.
     *
     * @return the symbol ID (sid) of this symbol, or
     * {@link SymbolTable#UNKNOWN_SYMBOL_ID} if the sid unknown.
     */
    public int getSymbolId();
}
