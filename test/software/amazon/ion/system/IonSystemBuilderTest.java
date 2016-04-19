/*
 * Copyright 2011-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.ion.system;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static software.amazon.ion.impl.lite.PrivateLiteDomTrampoline.isLiteSystem;

import java.io.ByteArrayOutputStream;
import org.junit.Test;
import software.amazon.ion.IonCatalog;
import software.amazon.ion.IonSystem;
import software.amazon.ion.IonWriter;
import software.amazon.ion.impl.PrivateIonWriter;
import software.amazon.ion.system.IonSystemBuilder;
import software.amazon.ion.system.SimpleCatalog;

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

    @Test
    public void testLite()
    {
        IonSystemBuilder b = IonSystemBuilder.standard().copy();
        IonSystem ion = b.build();
        assertTrue(isLiteSystem(ion));
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
        assertTrue(((PrivateIonWriter)w).isStreamCopyOptimized());
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
