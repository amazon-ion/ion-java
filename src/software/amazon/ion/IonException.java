/*
 * Copyright 2007-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package software.amazon.ion;

import java.io.IOException;
import java.util.IdentityHashMap;

/**
 * Base class for exceptions thrown throughout this library.  In most cases,
 * external exceptions (a common example being {@link IOException}) are not
 * propagated directly but are instead wrapped in one or more
 * {@link IonException}s.
 * <p>
 * This library does not promise that such an "external cause" will be the
 * direct {@link IonException#getCause() cause} of the thrown exception: there
 * there may be a chain of multiple {@link IonException} before getting to the
 * external cause.  Here's an example of how to deal with this in a situation
 * where the caller wants to propagate {@link IOException}s:
 *<pre>
 *    try {
 *        // Call some API
 *    }
 *    catch (IonException e) {
 *        IOException io = e.causeOfType(IOException.class);
 *        if (io != null) throw io;
 *        throw e;
 *    }
 *</pre>
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
        IdentityHashMap<Throwable, Throwable> seen =
            new IdentityHashMap<Throwable, Throwable>();

        Throwable cause = getCause();
        while (cause instanceof IonException)
        {
            if (seen.put(cause, cause) != null)  // cycle check
            {
                return null;
            }
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
        IdentityHashMap<Throwable, Throwable> seen =
            new IdentityHashMap<Throwable, Throwable>();

        Throwable cause = getCause();
        while (cause != null && ! type.isInstance(cause))
        {
            if (seen.put(cause, cause) != null)  // cycle check
            {
                return null;
            }
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
