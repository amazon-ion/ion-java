// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.system;

import com.amazon.ion.IonWriter;
import org.junit.Test;

import java.io.IOException;

import static com.amazon.ion.SystemSymbols.ION_1_0;
import static com.amazon.ion.system.IonWriterBuilder.InitialIvmHandling.SUPPRESS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

public class IonTextWriterBuilder_1_0_Test extends IonTextWriterBuilderTestBase {

    @Override
    IonTextWriterBuilder standard() {
        return IonTextWriterBuilder.standard();
    }

    @Override
    String ivm() {
        return ION_1_0;
    }

    @Test
    public void testInitialIvmHandling()
    {
        IonTextWriterBuilder b = standard();
        b.setInitialIvmHandling(SUPPRESS);
        assertSame(SUPPRESS, b.getInitialIvmHandling());

        // Test with...() on mutable builder

        IonTextWriterBuilder b2 = b.withInitialIvmHandling(null);
        assertSame(b, b2);
        assertSame(null, b.getInitialIvmHandling());

        // Test with...() on immutable builder

        b2 = b.immutable();
        assertSame(null, b2.getInitialIvmHandling());
        IonTextWriterBuilder b3 = b2.withInitialIvmHandling(SUPPRESS);
        assertNotSame(b2, b3);
        assertSame(null, b2.getInitialIvmHandling());
        assertSame(SUPPRESS, b3.getInitialIvmHandling());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testInitialIvmHandlingImmutability()
    {
        IonTextWriterBuilder b = standard();
        b.setInitialIvmHandling(SUPPRESS);

        IonTextWriterBuilder b2 = b.immutable();
        assertSame(SUPPRESS, b2.getInitialIvmHandling());
        b2.setInitialIvmHandling(null);
    }

    @Test
    public void testInitialIvmSuppression()
        throws IOException
    {
        IonTextWriterBuilder b = standard();

        StringBuilder out = new StringBuilder();
        IonWriter writer = b.build(out);
        writer.writeSymbol(ivm());
        writer.writeNull();
        writer.writeSymbol(ivm());
        writer.close();
        assertEquals(ivm() + " null " + ivm(), out.toString());

        b.withInitialIvmHandling(SUPPRESS);
        out.setLength(0);
        writer = b.build(out);
        writer.writeSymbol(ivm());
        writer.writeSymbol(ivm());
        writer.writeNull();
        writer.writeSymbol(ivm());
        writer.close();
        assertEquals("null " + ivm(), out.toString());
    }
}
