/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package software.amazon.ion;

/**
 * An error caused by adding an {@link IonValue} into a container when it's
 * already contained elsewhere.
 */
public class ContainedValueException
    extends IonException
{
    private static final long serialVersionUID = 1L;

    public ContainedValueException()
    {
        super();
    }

    /**
     * @param message
     */
    public ContainedValueException(String message)
    {
        super(message);
    }
}
