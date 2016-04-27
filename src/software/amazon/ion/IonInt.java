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

import java.math.BigInteger;

/**
 * An Ion <code>int</code> value.
 * <p>
 * <b>WARNING:</b> This interface should not be implemented or extended by
 * code outside of this library.
 */
public interface IonInt
    extends IonValue
{
    /**
     * Gets the content of this Ion <code>int</code> as a Java
     * <code>int</code> value.
     *
     * @return the int value.
     * @throws NullValueException if <code>this.isNullValue()</code>.
     */
    public int intValue()
        throws NullValueException;

    /**
     * Gets the content of this Ion <code>int</code> as a Java
     * <code>long</code> value.
     *
     * @return the long value.
     * @throws NullValueException if <code>this.isNullValue()</code>.
     */
    public long longValue()
        throws NullValueException;


    /**
     * Gets the content of this Ion <code>int</code> as a Java
     * {@link BigInteger} value.
     *
     * @return the <code>BigInteger</code> value,
     * or <code>null</code> if this is <code>null.int</code>.
     */
    public BigInteger bigIntegerValue();

    /**
     * Gets an {@link IntegerSize} representing the smallest-possible
     * Java type of the underlying content, or {@code null} if this is
     * {@code null.int}.
     *
     * @see IonReader#getIntegerSize()
     */
    public IntegerSize getIntegerSize();

    /**
     * Sets the content of this value.
     */
    public void setValue(int value);

    /**
     * Sets the content of this value.
     */
    public void setValue(long value);

    /**
     * Sets the content of this value.
     * The integer portion of the number is used, any fractional portion is
     * ignored.
     *
     * @param content the new content of this int;
     * may be <code>null</code> to make this <code>null.int</code>.
     */
    public void setValue(Number content);

    public IonInt clone()
        throws UnknownSymbolException;
}
