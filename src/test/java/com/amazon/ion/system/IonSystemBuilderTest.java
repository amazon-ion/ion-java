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

package com.amazon.ion.system;

import static com.amazon.ion.impl.lite._Private_LiteDomTrampoline.isLiteSystem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonWriter;
import com.amazon.ion.impl._Private_IonWriter;
import java.io.ByteArrayOutputStream;
import org.junit.Test;


public class IonSystemBuilderTest
{
    @Test(expected = UnsupportedOperationException.class)
    public void testDefaultIsLocked()
    {
        IonSystemBuilder.standard().setCatalog(null);
    }

    @Test
    public void testStandard()
    {
        assertEquals(null, IonSystemBuilder.standard().getCatalog());

        IonSystem ion = IonSystemBuilder.standard().build();
        assertTrue(isLiteSystem(ion));
        assertSame(SimpleCatalog.class, ion.getCatalog().getClass());
    }



    @Test
    public void testFrozen()
    {
        IonSystemBuilder b = IonSystemBuilder.standard();
        IonSystemBuilder fb = b.immutable();
        assertSame(b, fb);
    }

    @Test
    public void testMutable()
    {
        IonSystemBuilder b = IonSystemBuilder.standard();
        IonSystemBuilder mb = b.mutable();
        assertNotSame(b, mb);
        assertSame(mb, mb.mutable());
    }


    @Test(expected = UnsupportedOperationException.class)
    public void testCatalogLockCheck()
    {
        IonSystemBuilder b = IonSystemBuilder.standard().copy().immutable();
        b.setCatalog(null);
    }

    @Test
    public void testNullCatalog()
    {
        IonSystemBuilder b = IonSystemBuilder.standard().copy();
        b.setCatalog(null);
        IonSystem ion = b.build();
        assertSame(SimpleCatalog.class, ion.getCatalog().getClass());
    }

    @Test
    public void testCustomCatalog()
    {
        IonCatalog catalog = new SimpleCatalog();

        IonSystemBuilder b = IonSystemBuilder.standard().copy();
        b.setCatalog(catalog);
        assertSame(catalog, b.getCatalog());

        IonSystem ion = b.build();
        assertSame(catalog, ion.getCatalog());
    }

    @Test
    public void testWithCatalog()
    {
        IonCatalog catalog = new SimpleCatalog();

        IonSystemBuilder b =
            IonSystemBuilder.standard().withCatalog(catalog);
        assertSame(catalog, b.getCatalog());
    }

    //-------------------------------------------------------------------------

    @Test
    public void testStreamCopyOptimized()
    {
        IonSystemBuilder b = IonSystemBuilder.standard().copy();
        b.setStreamCopyOptimized(true);
        IonSystem ion = b.build();
        assertTrue(isLiteSystem(ion));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonWriter w = ion.newBinaryWriter(out);
        assertTrue(((_Private_IonWriter)w).isStreamCopyOptimized());
    }


    @Test(expected = UnsupportedOperationException.class)
    public void testStreamCopyOptimizedImmutability()
    {
        IonSystemBuilder b = IonSystemBuilder.standard().copy();
        b.setStreamCopyOptimized(true);

        IonSystemBuilder b2 = b.immutable();
        assertTrue(b2.isStreamCopyOptimized());
        b2.setStreamCopyOptimized(false);
    }

    //-------------------------------------------------------------------------

    @Test
    public void testIonTextWriterBuilder()
    {
        IonCatalog textWriterCatalog = new SimpleCatalog(); // should be ignored
        IonTextWriterBuilder textWriterBuilder = IonTextWriterBuilder.standard().withCatalog(textWriterCatalog);

        IonSystemBuilder systemBuilder = IonSystemBuilder.standard().withIonTextWriterBuilder(textWriterBuilder);
        assertSame(textWriterBuilder, systemBuilder.getIonTextWriterBuilder());

        IonCatalog systemCatalog = new SimpleCatalog();
        IonSystem system = systemBuilder.withCatalog(systemCatalog).build();
        assertSame(systemCatalog, system.getCatalog());
    }

    @Test
    public void testIonBinaryWriterBuilder()
    {
        IonCatalog binaryWriterCatalog = new SimpleCatalog(); // should be ignored
        IonBinaryWriterBuilder binaryWriterBuilder = IonBinaryWriterBuilder.standard().withCatalog(binaryWriterCatalog);

        IonSystemBuilder systemBuilder = IonSystemBuilder.standard().withIonBinaryWriterBuilder(binaryWriterBuilder);
        assertSame(binaryWriterBuilder, systemBuilder.getIonBinaryWriterBuilder());

        IonCatalog systemCatalog = new SimpleCatalog();
        IonSystem system = systemBuilder.withCatalog(systemCatalog).build();
        assertSame(systemCatalog, system.getCatalog());
    }

    @Test
    public void testIonReaderBuilder()
    {
        IonCatalog readerCatalog = new SimpleCatalog(); // should be ignored
        IonReaderBuilder readerBuilder = IonReaderBuilder.standard().withCatalog(readerCatalog);

        IonSystemBuilder systemBuilder = IonSystemBuilder.standard().withReaderBuilder(readerBuilder);
        assertSame(readerBuilder, systemBuilder.getReaderBuilder());

        IonCatalog systemCatalog = new SimpleCatalog();
        IonSystem system = systemBuilder.withCatalog(systemCatalog).build();
        assertSame(systemCatalog, system.getCatalog());
    }

    //-------------------------------------------------------------------------

    @Test
    public void testFluidStyle()
    {
        IonCatalog catalog = new SimpleCatalog();
        IonSystem ion = IonSystemBuilder.standard()
                                        .withCatalog(catalog)
                                        .withStreamCopyOptimized(true)
                                        .build();
        assertSame(catalog, ion.getCatalog());
    }

    @Test
    public void testCopy()
    {
        IonCatalog catalog = new SimpleCatalog();
        IonSystemBuilder b1 = IonSystemBuilder.standard()
                                              .withCatalog(catalog)
                                              .withStreamCopyOptimized(true);
        IonSystemBuilder b2 = b1.copy();
        assertNotSame(b1, b2);
        assertSame(b1.getCatalog(),     b2.getCatalog());
        assertSame(b1.isStreamCopyOptimized(), b2.isStreamCopyOptimized());
    }
}
