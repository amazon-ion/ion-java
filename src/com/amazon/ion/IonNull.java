/* Copyright (c) 2007-2013 Amazon.com, Inc.  All rights reserved. */

package com.amazon.ion;

/**
 * The Ion <code>null</code> value, also known as <code>null.null</code>.
 * <p>
 * <b>WARNING:</b> This interface should not be implemented or extended by
 * code outside of this library.
 * <p>
 * Because this value is always null, there's no interesting functionality
 * beyond what's defined by {@link IonValue}.
 */
public interface IonNull
    extends IonValue
{
    public IonNull clone()
        throws UnknownSymbolException;
}
