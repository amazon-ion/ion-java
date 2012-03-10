// Copyright (c) 2011-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.system;

import static com.amazon.ion.impl._Private_LazyDomTrampoline.isLazySystem;
import static com.amazon.ion.impl.lite._Private_LiteDomTrampoline.isLiteSystem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonWriter;
import com.amazon.ion.impl.IonWriterUserBinary;
import java.io.ByteArrayOutputStream;
import org.junit.Test;

/**
 *
 */
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
        assertEquals(false, IonSystemBuilder.standard().isBinaryBacked());
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


    @Test(expected = UnsupportedOperationException.class)
    public void testBinaryBackedLockCheck()
    {
        IonSystemBuilder b = IonSystemBuilder.standard().copy().immutable();
        b.setBinaryBacked(true);
    }

    @Test
    public void testLite()
    {
        IonSystemBuilder b = IonSystemBuilder.standard().copy();
        b.setBinaryBacked(false);
        IonSystem ion = b.build();
        assertTrue(isLiteSystem(ion));
    }

    @Test
    public void testBinaryBacked()
    {
        IonSystemBuilder b = IonSystemBuilder.standard().copy();
        b.setBinaryBacked(true);
        IonSystem ion = b.build();
        assertTrue(isLazySystem(ion));
    }

    @Test
    public void testStreamCopyOptimized()
    {
        IonSystemBuilder b = IonSystemBuilder.standard().copy();
        b.setStreamCopyOptimized(true);
        IonSystem ion = b.build();
        assertTrue(isLiteSystem(ion));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonWriter w = ion.newBinaryWriter(out);
        assertTrue(((IonWriterUserBinary)w).myStreamCopyOptimized);
    }

    @Test
    public void testFluidStyle()
    {
        IonCatalog catalog = new SimpleCatalog();
        IonSystem ion = IonSystemBuilder.standard()
                                        .withCatalog(catalog)
                                        .withBinaryBacked(true)
                                        .withStreamCopyOptimized(true)
                                        .build();
        assertSame(catalog, ion.getCatalog());
        assertTrue(isLazySystem(ion));
    }

    @Test
    public void testCopy()
    {
        IonCatalog catalog = new SimpleCatalog();
        IonSystemBuilder b1 = IonSystemBuilder.standard()
                                              .withCatalog(catalog)
                                              .withBinaryBacked(true)
                                              .withStreamCopyOptimized(true);
        IonSystemBuilder b2 = b1.copy();
        assertNotSame(b1, b2);
        assertSame(b1.getCatalog(),     b2.getCatalog());
        assertSame(b1.isBinaryBacked(), b2.isBinaryBacked());
        assertSame(b1.isStreamCopyOptimized(), b2.isStreamCopyOptimized());
    }
}
