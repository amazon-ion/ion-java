/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

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

    /**
     * Gets the value of this Ion <code>float</code> as a Java
     * {@link BigDecimal} value.
     *
     * @return the <code>BigDecimal</code> value,
     * or <code>null</code> if <code>this.isNullValue()</code>.
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
     *
     * @param value the new value of this float;
     * may be <code>null</code> to make this <code>null.float</code>.
     */
    public void setValue(BigDecimal value);
}
