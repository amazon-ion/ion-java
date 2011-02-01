// Copyright (c) 2007-2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

/**
 * Base class for exceptions thrown throughout this library.
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


    /**
     * Finds the first exception in the {@link #getCause()} chain that's not
     * an {@link IonException}.
     *
     * @return null if there's no cause that's not an {@link IonException}.
     */
    Throwable externalCause()
    {
        Throwable cause = getCause();
        while (cause != null && cause instanceof IonException)
        {
            cause = cause.getCause();
        }
        return cause;
    }


    /**
     * Finds the first exception in the {@link #getCause()} chain that is
     * an instance of the given type.
     *
     * @return null if there's no cause of the given type.
     */
    @SuppressWarnings("unchecked")
    public <T extends Throwable> T causeOfType(Class<T> type)
    {
        Throwable cause = getCause();
        while (cause != null && ! type.isInstance(cause))
        {
            cause = cause.getCause();
        }
        return (T) cause;
    }

    /**
     * Rethrows the first exception in the {@link #getCause()} chain that is
     * an instance of the given type.  If no such cause is found, returns
     * normally.
     *
     * FIXME Doesn't work under Java 5
     */
    @SuppressWarnings("unused")
    private // until I can get this to work
    <T extends Throwable> void rethrowCauseOfType(Class<T> type)
        throws Throwable // this should be throws T
    {
        T cause = causeOfType(type);

        // rethrow acts as if its always throwing Throwable, not the subclass.
        if (cause != null) throw cause;
    }
}
