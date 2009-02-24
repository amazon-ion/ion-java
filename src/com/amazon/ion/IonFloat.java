/* Copyright (c) 2007-2009 Amazon.com, Inc.  All rights reserved. */

package com.amazon.ion;
import java.math.BigDecimal;


/**
 * An Ion <code>float</code> value.
 */
public interface IonFloat
    extends IonValue
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
     * Gets the value of this Ion <code>float</code> as a Java
     * <code>double</code> value.
     *
     * @return the double value.
     * @throws NullValueException if <code>this.isNullValue()</code>.
     */
    public double doubleValue()
        throws NullValueException;

    /**
     * Gets the value of this Ion <code>float</code> as a Java
     * {@link BigDecimal} value.
     *
     * @return the <code>BigDecimal</code> value,
     * or <code>null</code> if this is <code>null.float</code>.
     *
     * @deprecated Renamed to {@link #bigDecimalValue()}.
     */
    @Deprecated
    public BigDecimal toBigDecimal()
        throws NullValueException;


    // TODO add isSpecial to detect nan/+inf/-inf
    // would be useful before calling bigDecimalValue

    /**
     * Gets the value of this Ion <code>float</code> as a Java
     * {@link BigDecimal} value.
     *
     * @return the <code>BigDecimal</code> value,
     * or <code>null</code> if <code>this.isNullValue()</code>.
     *
     * @throws NumberFormatException if this value is {@code nan},
     * {@code +inf}, or {@code -inf}, because {@link BigDecimal} cannot
     * represent those values.
     */
    public BigDecimal bigDecimalValue()
        throws NullValueException;

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
     * Since Ion {@code float}s are essentially Java {@code double}s,
     * this performs a narrowing conversion as described by
     * {@link BigDecimal#doubleValue()}.
     *
     * @param value the new value of this float;
     * may be <code>null</code> to make this <code>null.float</code>.
     */
    public void setValue(BigDecimal value);


    public IonFloat clone();
}
