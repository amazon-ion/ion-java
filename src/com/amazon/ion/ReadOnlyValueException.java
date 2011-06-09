// Copyright (c) 2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;


/**
 * Signals an attempt to modify an {@link IonValue} that is read-only.
 *
 * @see IonValue#makeReadOnly()
 */
public class ReadOnlyValueException
    extends IonException
{
    private static final long serialVersionUID = 1L;

    public ReadOnlyValueException()
    {
        super("Read-only IonValue cannot be modified");
    }
}
