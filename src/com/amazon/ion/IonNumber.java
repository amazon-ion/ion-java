// Copyright (c) 2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

/**
 * The IonNumber interface is a fore runner of a common base for the
 * ion numeric value types.  Currently only IonDecimal extends this
 * interface. In due course IonFloat and IonInt will be added to the
 * the family.
 *
 * @author csuver
 *
 */
public interface IonNumber
    extends IonValue
{
    public enum Classification {
        NORMAL,
        NEGATIVE_ZERO,
        NAN,
        NEGATIVE_INFINITY,
        POSITIVE_INFINITY
    }

    /**
     * Sets the value of this element to be <code>-0d0</code>.  Note that
     * the <code>java.math.BigDecimal</code> class does not support the
     * IEEE negative zero value.  This method allows
     * you to force this element to have special values such as negative zero.
     *
     * @param valueClassification classification to the value
     */
    public void setValueSpecial(Classification valueClassification);


    /**
     * This allows you to detect the various special values floating
     * point and decimal numbers might contain.  IonDecimal
     * values may only be NORMAL or NEGATIVE_ZERO.  IonFloat values
     * may have any Classification.
     *
     * @return Classification of the value this element
     */
    public Classification getClassification();

}
