// Copyright (c) 2009-2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;


/**
 * An error caused by an attempt to modify a read-only component.
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

    public ReadOnlyValueException(Class type)
    {
        super("Cannot modify read-only instance of " + type);
    }
}
