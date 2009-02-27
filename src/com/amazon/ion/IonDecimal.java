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
     * Sets the value of this element.  The <code>java.math.BigDecimal</code> class
     * does not support the IEEE negative zero value.  This method allows
     * you to force the sign bit onto a BigDecimal zero value so that
     * the IonDecimal correctly represents the negative zero value.  If
     * <code>Classification.NEGATIVE_ZERO</code> is true a non-zero BigDecimal value will result
     * in an IllegalArgumentException being thrown.
     * Only <code>Classification.NEGATIVE_ZERO</code> and <code>Classification.NORMAL</code> are
     * valid for IonDecimal values other Classifications will result in an
     * IllegalArgumentException being thrown.
     *
     * @param value the new value of this decimal;
     * may be <code>null</code> to make this <code>null.decimal</code>.
     * @param <code>valueClassification</code> when <code>Classification.NEGATIVE_ZERO</code> will force the value zero to be the special negative zero.
     */
    public void setValue(BigDecimal value, IonNumber.Classification valueClassification);
    
    public IonDecimal clone();
}
