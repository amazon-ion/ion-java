/* Copyright (c) 2007-2009 Amazon.com, Inc.  All rights reserved. */

package com.amazon.ion;

import java.util.Collection;


/**
 * An Ion <code>list</code> value.
 */
public interface IonList
    extends IonValue, IonSequence, Collection<IonValue>
{
    public IonList clone();
}
