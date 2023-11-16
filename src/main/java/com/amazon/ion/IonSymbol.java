/*
 * Copyright 2007-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

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
     * Gets the integer symbol id used in the binary encoding of this symbol.
     *
     * @return an integer greater than zero, if this value has an associated
     * symbol table.  Otherwise, return {@link SymbolTable#UNKNOWN_SYMBOL_ID}.
     *
     * @throws NullValueException if this is <code>null.symbol</code>.
     *
     * @deprecated Use {@link #symbolValue()} instead.
     */
    @Deprecated
    public int getSymbolId()
        throws NullValueException;


    /**
     * Returns this value as a symbol token (text + ID).
     *
     * @return null if {@link #isNullValue()}
     *
     */
    public SymbolToken symbolValue();


    /**
     * Changes the value of this element.
     *
     * @param value the new value of this symbol;
     * may be <code>null</code> to make this <code>null.symbol</code>.
     *
     */
    public void setValue(String value);

    public IonSymbol clone()
        throws UnknownSymbolException;
}
