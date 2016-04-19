/*
 * Copyright 2007-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
 * Common functionality of Ion <code>string</code> and <code>symbol</code>
 * types.
 * <p>
 * <b>WARNING:</b> This interface should not be implemented or extended by
 * code outside of this library.
 */
public interface IonText
    extends IonValue
{
    /**
     * Gets the characters of this text value.
     *
     * @return the text of this Ion value, or <code>null</code> if
     * <code>this.isNullValue()</code>.
     *
     * @throws UnknownSymbolException if this is a symbol with unknown text.
     *
     * @see IonSymbol#symbolValue()
     */
    public String stringValue();

    /**
     * Changes the content.
     *
     * @param value the new value of this text value;
     * may be <code>null</code> to make this an Ion null value.
     *
     * @throws EmptySymbolException if this is an {@link IonSymbol} and
     * <code>value</code> is the empty string.
     */
    public void setValue(String value)
        throws EmptySymbolException;

    public IonText clone()
        throws UnknownSymbolException;
}
