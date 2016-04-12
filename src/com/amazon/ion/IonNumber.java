// Copyright (c) 2009-2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

/**
 * The IonNumber interface is a fore runner of a common base for the
 * ion numeric value types.  Currently only IonDecimal extends this
 * interface. In due course IonFloat and IonInt will be added to the
 * the family.
 * <p>
 * <b>WARNING:</b> This interface should not be implemented or extended by
 * code outside of this library.
 */
public interface IonNumber // TODO ION-95 Complete this interface
    extends IonValue
{
}
