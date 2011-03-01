// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.system;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonSystem;
import com.amazon.ion.impl.IonSystemImpl;
import com.amazon.ion.impl.lite.IonSystemLite;
import org.junit.Test;

/**
 *
 */
public class IonSystemBuilderTest
{
    @Test(expected = UnsupportedOperationException.class)
    public void testDefaultIsLocked()
    {
        IonSystemBuilder.defaultBuilder().setCatalog(null);
    }

    @Test
    public void testDefault()
    {
        assertEquals(false, IonSystemBuilder.defaultBuilder().isBinaryBacked());
        assertEquals(null, IonSystemBuilder.defaultBuilder().getCatalog());

        IonSystem ion = IonSystemBuilder.defaultBuilder().build();
        assertSame(IonSystemLite.class, ion.getClass());
        assertSame(SimpleCatalog.class, ion.getCatalog().getClass());
    }



    @Test
    public void testFrozen()
    {
        IonSystemBuilder b = IonSystemBuilder.defaultBuilder();
        IonSystemBuilder fb = b.immutable();
        assertSame(b, fb);
    }

    @Test
    public void testMutable()
    {
        IonSystemBuilder b = IonSystemBuilder.defaultBuilder();
        IonSystemBuilder mb = b.mutable();
        assertNotSame(b, mb);
        assertSame(mb, mb.mutable());
    }


    @Test(expected = UnsupportedOperationException.class)
    public void testCatalogLockCheck()
    {
        IonSystemBuilder b = IonSystemBuilder.defaultBuilder().copy().immutable();
        b.setCatalog(null);
    }

    @Test
    public void testNullCatalog()
    {
        IonSystemBuilder b = IonSystemBuilder.defaultBuilder().copy();
        b.setCatalog(null);
        IonSystem ion = b.build();
        assertSame(SimpleCatalog.class, ion.getCatalog().getClass());
    }

    @Test
    public void testCustomCatalog()
    {
        IonCatalog catalog = new SimpleCatalog();

        IonSystemBuilder b = IonSystemBuilder.defaultBuilder().copy();
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
            IonSystemBuilder.defaultBuilder().withCatalog(catalog);
        assertSame(catalog, b.getCatalog());
    }


    @Test(expected = UnsupportedOperationException.class)
    public void testBinaryBackedLockCheck()
    {
        IonSystemBuilder b = IonSystemBuilder.defaultBuilder().copy().immutable();
        b.setBinaryBacked(true);
    }

    @Test
    public void testLite()
    {
        IonSystemBuilder b = IonSystemBuilder.defaultBuilder().copy();
        b.setBinaryBacked(false);
        IonSystem ion = b.build();
        assertSame(IonSystemLite.class, ion.getClass());
    }

    @Test
    public void testBinaryBacked()
    {
        IonSystemBuilder b = IonSystemBuilder.defaultBuilder().copy();
        b.setBinaryBacked(true);
        IonSystem ion = b.build();
        assertSame(IonSystemImpl.class, ion.getClass());
    }

    @Test
    public void testFluidStyle()
    {
        IonCatalog catalog = new SimpleCatalog();
        IonSystem ion = IonSystemBuilder.defaultBuilder()
                                        .withCatalog(catalog)
                                        .withBinaryBacked(true)
                                        .build();
        assertSame(catalog, ion.getCatalog());
        assertSame(IonSystemImpl.class, ion.getClass());
    }

    @Test
    public void testCopy()
    {
        IonCatalog catalog = new SimpleCatalog();
        IonSystemBuilder b1 = IonSystemBuilder.defaultBuilder()
                                              .withCatalog(catalog)
                                              .withBinaryBacked(true);
        IonSystemBuilder b2 = b1.copy();
        assertNotSame(b1, b2);
        assertSame(b1.getCatalog(),     b2.getCatalog());
        assertSame(b1.isBinaryBacked(), b2.isBinaryBacked());
    }
}
