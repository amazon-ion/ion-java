// Copyright (c) 2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

/**
 * An error caused by an operation that requires an exact match on an import
 * within the catalog.
 *
 * @see SymbolTable#isSubstitute()
 */
// TODO amznlabs/ion-java#40 Provide some useful info to assist callers with handling this
//      exception. E.g. reference to the substitute import in violation.
public class SubstituteSymbolTableException
    extends IonException
{
    private static final long serialVersionUID = 2885122600422187914L;

    public SubstituteSymbolTableException()
    {
        super();
    }

    public SubstituteSymbolTableException(String message)
    {
        super(message);
    }
}
