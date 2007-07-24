/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;


/**
 * An error caused by invoking an inappropriate method on an Ion
 * <code>null</code> value.
 */
public class UnexpectedEofException
    extends IonException
{
    private static final long serialVersionUID = 1L;

    /**
     *
     */
    public UnexpectedEofException()
    {
        super();
    }

    /**
     * @param message
     * @param cause
     */
    public UnexpectedEofException(String message, Throwable cause)
    {
        super(message, cause);
    }

    /**
     * @param message
     */
    public UnexpectedEofException(String message)
    {
        super(message);
    }

    /**
     * @param cause
     */
    public UnexpectedEofException(Throwable cause)
    {
        super(cause);
    }
}
