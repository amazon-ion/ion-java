// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.junit.Test;

/**
 *
 */
public class IonExceptionTest
{
    IonException wrap(Throwable cause)
    {
        return new IonException(cause);
    }

    @Test
    public void testExternalCause()
    {
        IonException ion = new IonException();
        assertNull(ion.externalCause());
        ion = wrap(ion);
        assertNull(ion.externalCause());
        ion = wrap(ion);
        assertNull(ion.externalCause());

        IOException io = new IOException("boom");
        ion = wrap(io);
        assertSame(io, ion.externalCause());
        ion = wrap(ion);
        assertSame(io, ion.externalCause());
        ion = wrap(ion);
        assertSame(io, ion.externalCause());
    }

    @Test
    public void testCauseOfType()
    {
        IonException ion = new IonException();
        assertNull(ion.causeOfType(IOException.class));
        ion = wrap(ion);
        assertNull(ion.causeOfType(IOException.class));

        // External cause isn't the requested type
        ion = wrap(new Error("boom"));
        assertNull(ion.causeOfType(IOException.class));

        IOException io = new IOException("boom");
        ion = wrap(io);
        assertSame(io, ion.causeOfType(IOException.class));
        ion = wrap(ion);
        assertSame(io, ion.causeOfType(IOException.class));
        ion = wrap(ion);
        assertSame(io, ion.causeOfType(IOException.class));

        // External cause is a subtype of the requested type
        io = new FileNotFoundException("boom");
        ion = wrap(io);
        assertSame(io, ion.causeOfType(IOException.class));
        ion = wrap(ion);
        assertSame(io, ion.causeOfType(IOException.class));
        ion = wrap(ion);
        assertSame(io, ion.causeOfType(IOException.class));
    }
}
