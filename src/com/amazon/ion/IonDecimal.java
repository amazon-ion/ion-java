/* Copyright (c) 2007-2013 Amazon.com, Inc.  All rights reserved. */

package com.amazon.ion;

import java.math.BigDecimal;

/**
 * An Ion <code>decimal</code> value.
 * <p>
 * <b>WARNING:</b> This interface should not be implemented or extended by
 * code outside of this library.
 */
public interface IonDecimal
    extends IonNumber
{
    /**
     * Gets the value of this Ion {@code decimal} as a Java
     * <code>float</code> value.
     *
     * @return the float value.
     * @throws NullValueException if <code>this.isNullValue()</code>.
     */
    public float floatValue()
        throws NullValueException;

    /**
     * Gets the value of this Ion {@code decimal} as a Java
     * <code>double</code> value.
     *
     * @return the double value.
     * @throws NullValueException if <code>this.isNullValue()</code>.
     */
    public double doubleValue()
        throws NullValueException;

    /**
     * Gets the value of this Ion {@code decimal} as a {@link BigDecimal}.
     * If you need negative zeros, use {@link #decimalValue()}.
     *
     * @return the {@code BigDecimal} value,
     * or {@code null} if {@code this.isNullValue()}.
     *
     * @see #decimalValue()
     */
    public BigDecimal bigDecimalValue();

    /**
     * Gets the value of this Ion {@code decimal} as a {@link Decimal},
     * which extends {@link BigDecimal} with support for negative zeros.
     *
     * @return the {@code Decimal} value,
     * or {@code null} if {@code this.isNullValue()}.
     *
     * @see #bigDecimalValue()
     */
    public Decimal decimalValue();

    /**
     * Sets the value of this element.
     */
    public void setValue(long value);

    /**
     * Sets the value of this element.
     * This method behaves like {@link BigDecimal#valueOf(double)} in that it
     * uses the {@code double}'s canonical string representation provided by
     * {@link Double#toString(double)}}.
     *
     * @see Decimal#valueOf(double)
     */
    public void setValue(float value);

    /**
     * Sets the value of this element.
     * This method behaves like {@link BigDecimal#valueOf(double)} in that it
     * uses the {@code double}'s canonical string representation provided by
     * {@link Double#toString(double)}}.
     *
     * @see Decimal#valueOf(double)
     */
    public void setValue(double value);

    /**
     * Sets the value of this element.
     * To set a negative zero value, pass an {@link Decimal}.
     *
     * @param value the new value of this decimal;
     * may be <code>null</code> to make this <code>null.decimal</code>.
     */
    public void setValue(BigDecimal value);


    public IonDecimal clone()
        throws UnknownSymbolException;
}
