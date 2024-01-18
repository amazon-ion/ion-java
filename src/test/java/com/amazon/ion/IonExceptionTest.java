/*
 * Copyright 2007-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.ion;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.io.IOException;

import com.amazon.ion.system.IonSystemBuilder;
import org.junit.Test;


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
    public void testExternalCauseWithCycle()
    {
        IonException ie1 = new IonException();
        IonException ie2 = new IonException(ie1);
        ie1.initCause(ie2);
        IonException ion = new IonException(ie1);
        assertNull(ion.externalCause());
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

    @Test
    public void testCauseOfTypeWithCycle()
    {
        IonException ie1 = new IonException();
        IonException ie2 = new IonException(ie1);
        ie1.initCause(ie2);
        IonException ion = new IonException(ie1);
        assertNull(ion.causeOfType(IOException.class));
    }

    @Test
    public void testCauseOfWrongEncoding() {
        try {
            byte[] bytes_input = new byte[]{
                    (byte) 0x27, (byte) 0x31, (byte) -0xB, (byte) 0x31, (byte) 0x31, (byte) 0x31, (byte) 0x27};

            IonSystemBuilder.standard().build().newLoader().load(bytes_input);
        } catch (IonException e) {
            // The exception should be caught here
            assertEquals(e.getMessage(), "Invalid encoding: encountered non-Unicode character.");
            return;
        } catch (Exception ignore) {}

        fail();
    }
}
