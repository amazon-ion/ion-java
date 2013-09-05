/* Copyright (c) 2007-2013 Amazon.com, Inc.  All rights reserved. */

package com.amazon.ion;

import java.util.Collection;


/**
 * An Ion <code>list</code> value.
 * <p>
 * <b>WARNING:</b> This interface should not be implemented or extended by
 * code outside of this library.
 */
public interface IonList
    extends IonValue, IonSequence, Collection<IonValue>
{
    public IonList clone()
        throws UnknownSymbolException;
}
