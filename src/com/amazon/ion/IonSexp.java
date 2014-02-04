/* Copyright (c) 2007-2013 Amazon.com, Inc.  All rights reserved. */

package com.amazon.ion;

import java.util.Collection;


/**
 * An Ion <code>sexp</code> (S-expression) value.
 * <p>
 * <b>WARNING:</b> This interface should not be implemented or extended by
 * code outside of this library.
 */
public interface IonSexp
    extends IonValue, IonSequence, Collection<IonValue>
{
    public IonSexp clone()
        throws UnknownSymbolException;
}
