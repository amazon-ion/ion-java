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
 * An Ion <code>float</code> value.
 * <p>
 * <b>WARNING:</b> This interface should not be implemented or extended by
 * code outside of this library.
 *
 * <h2>Precision Problems</h2>
 *
 * Use of binary floating-point numbers is prone to countless problems.
 * The vast majority of applications should use {@code decimal} values instead.
 * Please read the
 * <a href="http://speleotrove.com/decimal/decifaq1.html">Decimal Arithmetic
 * FAQ</a> for horror stories.
 * <p>
 * If you have any doubt whatsoever on whether you should use {@code float} or
 * {@code decimal}, then you should use {@code decimal}.
 *
 * @see IonDecimal
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
     * Gets the value of this Ion {@code float} as a Java {@link BigDecimal}.
     * This follows the behavior of {@link BigDecimal#valueOf(double)}. It's
     * recommended to call {@link IonFloat#isNumericValue()} before calling
     * this method.
     *
     * @return the {@link BigDecimal} value, or {@code null} if
     * {@code this.isNullValue()}.
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

    /**
     * Determines whether this value is numeric. Returns true if this value
     * is none of {@code null}, {@code nan}, {@code +inf}, and {@code -inf},
     * and false if it is any of them.
     *
     * @return a checked condition whether this value is numeric.
     */
    public boolean isNumericValue();

    public IonFloat clone()
        throws UnknownSymbolException;
}
