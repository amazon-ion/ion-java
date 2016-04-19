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
