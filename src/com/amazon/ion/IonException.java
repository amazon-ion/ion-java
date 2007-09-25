/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;

/**
 *
 */
public class IonException extends RuntimeException
{
    private static final long serialVersionUID = 5769577011706279252L;

    public IonException() { super(); }
    public IonException(String message) { super(message); }
    public IonException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a new exception with the given cause, copying the message
     * from the cause into this instance.
     * @param cause
     *     the root cause of the exception; must not be null.
     */
    public IonException(Throwable cause) { super(cause.getMessage(), cause); }

}
