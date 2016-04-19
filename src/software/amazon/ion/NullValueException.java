/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package software.amazon.ion;


/**
 * An error caused by invoking an inappropriate method on an Ion
 * <code>null</code> value.
 */
public class NullValueException
    extends IonException
{
    private static final long serialVersionUID = 1L;

    public NullValueException()
    {
        super();
    }

    /**
     * @param message
     * @param cause
     */
    public NullValueException(String message, Throwable cause)
    {
        super(message, cause);
    }

    /**
     * @param message
     */
    public NullValueException(String message)
    {
        super(message);
    }

    /**
     * @param cause
     */
    public NullValueException(Throwable cause)
    {
        super(cause);
    }
}
