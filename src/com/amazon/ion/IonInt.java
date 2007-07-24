/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;

import java.math.BigInteger;

/**
 * An Ion <code>int</code> value.
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
    public BigInteger toBigInteger();


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
}
