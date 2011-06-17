/* Copyright (c) 2007-2009 Amazon.com, Inc.  All rights reserved. */

package com.amazon.ion;

import java.util.Collection;


/**
 * An Ion <code>sexp</code> (S-expression) value.
 */
public interface IonSexp
    extends IonValue, IonSequence, Collection<IonValue>
{
    public IonSexp clone();
}
