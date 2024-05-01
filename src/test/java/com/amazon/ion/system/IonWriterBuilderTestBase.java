// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.system;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.impl.Symtabs;
import com.amazon.ion.impl._Private_IonWriter;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

/**
 * Base tests for classes that inherit from {@link IonWriterBuilderBase}.
 * @param <Builder> the concrete type of the class under test.
 */
abstract class IonWriterBuilderTestBase<Builder extends IonWriterBuilderBase<Builder>> {

    /**
     * @return a new, standard builder of the relevant type.
     */
    abstract Builder standard();

    public void testBuildNull(Builder b)
    {
        try {
            b.build((OutputStream)null);
            fail("Expected exception");
        }
        catch (RuntimeException e) { }
    }

    @Test
    public void testStandard()
    {
        Builder b = standard();
        Assert.assertNotNull(b);

        testBuildNull(b);

        OutputStream out = new ByteArrayOutputStream();
        IonWriter writer = b.build(out);
        Assert.assertNotNull(writer);

        assertNotSame(b, standard());
    }

    @Test
    public void testCustomCatalog()
    {
        IonCatalog catalog = new SimpleCatalog();

        Builder b = standard();
        b.setCatalog(catalog);
        assertSame(catalog, b.getCatalog());

        OutputStream out = new ByteArrayOutputStream();
        IonWriter writer = b.build(out);
        assertSame(catalog, ((_Private_IonWriter)writer).getCatalog());

        IonCatalog catalog2 = new SimpleCatalog();
        b.setCatalog(catalog2);
        assertSame(catalog2, b.getCatalog());

        // Test with...() on mutable builder

        Builder b2 = b.withCatalog(catalog);
        assertSame(b, b2);
        assertSame(catalog, b2.getCatalog());

        // Test with...() on immutable builder

        b2 = b.immutable();
        assertSame(catalog, b2.getCatalog());
        Builder b3 = b2.withCatalog(catalog2);
        assertNotSame(b2, b3);
        assertSame(catalog, b2.getCatalog());
        assertSame(catalog2, b3.getCatalog());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCatalogImmutability()
    {
        IonCatalog catalog = new SimpleCatalog();

        Builder b = standard();
        b.setCatalog(catalog);

        Builder b2 = b.immutable();
        assertSame(catalog, b2.getCatalog());
        b2.setCatalog(null);
    }

    @Test
    public void testImports()
    {
        SymbolTable f = Symtabs.CATALOG.getTable("fred", 1);
        SymbolTable g = Symtabs.CATALOG.getTable("ginger", 1);

        SymbolTable[] symtabsF = new SymbolTable[] { f };
        SymbolTable[] symtabsG = new SymbolTable[] { g };

        Builder b = standard();
        b.setImports(f);

        OutputStream out = new ByteArrayOutputStream();
        IonWriter writer = b.build(out);
        SymbolTable st = writer.getSymbolTable();
        assertArrayEquals(symtabsF, st.getImportedTables());

        // Test with...() on mutable builder

        Builder b2 = b.withImports(g);
        assertSame(b, b2);
        assertArrayEquals(symtabsG, b2.getImports());

        // Test with...() on immutable builder

        b2 = b.immutable();
        assertArrayEquals(symtabsG, b2.getImports());
        Builder b3 = b2.withImports(f);
        assertNotSame(b2, b3);
        assertArrayEquals(symtabsG, b2.getImports());
        assertArrayEquals(symtabsF, b3.getImports());

        // Test cloning of array

        SymbolTable[] symtabs = new SymbolTable[] { f };
        b3.setImports(symtabs);
        assertNotSame(symtabs, b3.getImports());
        assertArrayEquals(symtabsF, b3.getImports());

        symtabs[0] = g;
        assertArrayEquals(symtabsF, b3.getImports());

        b3.getImports()[0] = g;
        assertArrayEquals(symtabsF, b3.getImports());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testImportsImmutability()
    {
        SymbolTable f = Symtabs.CATALOG.getTable("fred", 1);
        SymbolTable[] symtabs = new SymbolTable[] { f };

        Builder b = standard();
        b.setImports(f);

        Builder b2 = b.immutable();
        assertArrayEquals(symtabs, b2.getImports());
        b2.setImports();
    }

    @Test
    public void testImportsNull()
    {
        SymbolTable f = Symtabs.CATALOG.getTable("fred", 1);
        SymbolTable[] symtabs = new SymbolTable[] { f };

        Builder b = standard();
        b.setImports(symtabs);
        b.setImports((SymbolTable[])null);
        assertSame(null, b.getImports());

        b.setImports(new SymbolTable[0]);
        assertArrayEquals(new SymbolTable[0], b.getImports());
    }
}
