/* Copyright (c) 2007-2008 Amazon.com, Inc.  All rights reserved. */

package com.amazon.ion;

import java.math.BigDecimal;

/**
 * An Ion <code>decimal</code> value.
 */
public interface IonDecimal
    extends IonNumber
{
    /**
     * Gets the value of this Ion <code>float</code> as a Java
     * <code>float</code> value.
     *
     * @return the float value.
     * @throws NullValueException if <code>this.isNullValue()</code>.
     */
    public float floatValue()
        throws NullValueException;

    /**
     * Gets the value of this Ion <code>int</code> as a Java
     * <code>double</code> value.
     *
     * @return the double value.
     * @throws NullValueException if <code>this.isNullValue()</code>.
     */
    public double doubleValue()
        throws NullValueException;

    /**
     * Gets the value of this Ion <code>int</code> as a Java
     * {@link BigDecimal} value.
     *
     * @return the <code>BigDecimal</code> value,
     * or <code>null</code> if <code>this.isNullValue()</code>.
     *
     * @deprecated Renamed to {@link #bigDecimalValue()}.
     */
    @Deprecated
    public BigDecimal toBigDecimal();

    /**
     * Gets the value of this Ion <code>int</code> as a Java
     * {@link BigDecimal} value.
     *
     * @return the <code>BigDecimal</code> value,
     * or <code>null</code> if <code>this.isNullValue()</code>.
     */
    public BigDecimal bigDecimalValue();

    /**
     * Sets the value of this element.
     */
    public void setValue(float value);

    /**
     * Sets the value of this element.
     */
    public void setValue(double value);

    /**
     * Sets the value of this element.
     *
     * @param value the new value of this decimal;
     * may be <code>null</code> to make this <code>null.decimal</code>.
     */
    public void setValue(BigDecimal value);

    /**
     * Sets the value of this element.  The {@link java.math.BigDecimal} class
     * does not support the IEEE negative zero value.  This method allows
     * you to force the sign bit onto a BigDecimal zero value so that
     * the IonDecimal correctly represents the negative zero value.
     *
     * @param value the new value of this decimal;
     * may be <code>null</code> to make this <code>null.decimal</code>.
     * @param classification forces the value into the given state.
     *
     * @throws IllegalArgumentException
     * if {@code classification} is {@link IonNumber.Classification#NEGATIVE_ZERO}
     * and {@code value} is non-zero, or if {@code classification} is anything
     * other than {@link IonNumber.Classification#NORMAL} or
     * {@link IonNumber.Classification#NEGATIVE_ZERO}.
     */
    public void setValue(BigDecimal value, Classification classification);

    public IonDecimal clone();
}
