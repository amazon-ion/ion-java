/* Copyright (c) 2007-2008 Amazon.com, Inc.  All rights reserved. */

package com.amazon.ion;


/**
 * An Ion <code>sexp</code> (S-expression) value.
 */
public interface IonSexp
    extends IonValue, IonSequence
{
    public IonSexp clone();
}
