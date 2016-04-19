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
 * An Ion <code>string</code> value.
 * <p>
 * <b>WARNING:</b> This interface should not be implemented or extended by
 * code outside of this library.
 */
public interface IonString
    extends IonText
{
    /**
     * Gets the characters of this string.
     *
     * @return the text of the string, or <code>null</code> if this is
     * <code>null.string</code>.
     */
    public String stringValue();

    /**
     * Changes the value of this string.
     *
     * @param value the new value of this string;
     * may be <code>null</code> to make this <code>null.string</code>.
     */
    public void setValue(String value);

    public IonString clone()
        throws UnknownSymbolException;
}
