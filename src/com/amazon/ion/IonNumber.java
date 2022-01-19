/*
 * Copyright 2007-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.ion;

import java.math.BigDecimal;

/**
 * Common functionality of Ion {@code int}, {@code decimal}, and {@code float}
 * types.
 * <p>
 * <b>WARNING:</b> This interface should not be implemented or extended by
 * code outside of this library.
 */
public interface IonNumber
    extends IonValue
{
    /**
     * Gets the value of this Ion number as a {@link BigDecimal}.
     *
     * <p>This method will throw an exception for non-null, non-numeric values.
     * It's recommended to call {@link #isNumericValue()} before calling
     * this method.</p>
     *
     * <p>Negative zero is supported by {@link IonDecimal} and {@link IonFloat},
     * but not by {@link BigDecimal}. If you need to distinguish positive and
     * negative zero, you should call {@link IonDecimal#decimalValue()} or
     * {@link IonFloat#doubleValue()} after casting to the appropriate type.</p>
     *
     * @return the {@link BigDecimal} value, or {@code null} if
     * {@code this.isNullValue()}.
     *
     * @throws NumberFormatException if this value is {@code nan},
     * {@code +inf}, or {@code -inf}, because {@link BigDecimal} cannot
     * represent those values.
     */
    public BigDecimal bigDecimalValue();

    /**
     * Determines whether this value is numeric. Returns true if this value
     * is none of {@code null}, {@code nan}, {@code +inf}, and {@code -inf},
     * and false if it is any of them.
     *
     * @return a checked condition whether this value is numeric.
     */
    public boolean isNumericValue();
}
