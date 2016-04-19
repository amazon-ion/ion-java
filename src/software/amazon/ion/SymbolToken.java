/*
 * Copyright 2011-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package software.amazon.ion;


/**
 * An Ion symbol token (field name, annotation, and symbol values)
 * providing both the symbol text and the assigned symbol ID.
 * Symbol tokens may be interned into a {@link SymbolTable}.
 * <p>
 * Any instance will have at least one of the two properties defined.
 * <p>
 * <b>WARNING:</b> This interface should not be implemented or extended by
 * code outside of this library.
 *
 */
public interface SymbolToken
{
    /**
     * A zero-length array.
     */
    public static final SymbolToken[] EMPTY_ARRAY = new SymbolToken[0];


    /**
     * Gets the text of this symbol.
     * <p>
     * If the text is not known (usually due to a shared symbol table being
     * unavailable) then this method returns null.
     * In such cases {@link #getSid()} will be non-negative.
     *
     * @return the text of this symbol, or null if the text is unknown.
     */
    public String getText();


    /**
     * Gets the text of this symbol, throwing an exception if its unknown.
     *
     * @return the text of the symbol, not null.
     *
     * @throws UnknownSymbolException if the symbol text isn't known.
     */
    public String assumeText();


    /**
     * Gets the ID of this symbol token.
     * <p>
     * If no ID has yet been assigned (as may be the case when processing Ion
     * text-formatted data), this method returns
     * {@link SymbolTable#UNKNOWN_SYMBOL_ID}.
     * In such cases {@link #getText()} will be non-null.
     *
     * @return the symbol ID (sid) of this symbol, or
     * {@link SymbolTable#UNKNOWN_SYMBOL_ID} if the sid is unknown.
     */
    public int getSid();
}
