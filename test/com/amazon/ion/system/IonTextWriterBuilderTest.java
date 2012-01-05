// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.system;

import static com.amazon.ion.system.IonTextWriterBuilder.ASCII;
import static com.amazon.ion.system.IonTextWriterBuilder.UTF8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.Symtabs;
import com.amazon.ion.impl.IonWriterUserText;
import com.amazon.ion.impl._Private_IonWriter;
import java.io.OutputStream;
import java.nio.charset.Charset;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class IonTextWriterBuilderTest
{
    public void testBuildNull(IonTextWriterBuilder b)
    {
        try {
            b.build((Appendable)null);
            fail("Expected exception");
        }
        catch (NullPointerException e) { }

        try {
            b.build((OutputStream)null);
            fail("Expected exception");
        }
        catch (NullPointerException e) { }
    }

    @Test
    public void testStandard()
    {
        IonTextWriterBuilder b = IonTextWriterBuilder.standard();
        Assert.assertNotNull(b);

        testBuildNull(b);

        StringBuilder out = new StringBuilder();
        IonWriter writer = b.build(out);
        Assert.assertNotNull(writer);
        Assert.assertTrue(writer instanceof IonWriterUserText);

        assertNotSame(b, IonTextWriterBuilder.standard());
    }


    @Test
    public void testSimplifiedAscii()
    {
        IonTextWriterBuilder b = IonTextWriterBuilder.simplifiedAscii();
        assertSame(ASCII, b.getCharset());

    }

    //-------------------------------------------------------------------------

    @Test
    public void testCustomCatalog()
    {
        IonCatalog catalog = new SimpleCatalog();

        IonTextWriterBuilder b = IonTextWriterBuilder.standard();
        b.setCatalog(catalog);
        assertSame(catalog, b.getCatalog());

        StringBuilder out = new StringBuilder();
        IonWriter writer = b.build(out);
        assertSame(catalog, ((_Private_IonWriter)writer).getCatalog());

        IonCatalog catalog2 = new SimpleCatalog();
        b.setCatalog(catalog2);
        assertSame(catalog2, b.getCatalog());

        // Test with...() on mutable builder

        IonTextWriterBuilder b2 = b.withCatalog(catalog);
        assertSame(b, b2);
        assertSame(catalog, b2.getCatalog());

        // Test with...() on immutable builder

        b2 = b.immutable();
        assertSame(catalog, b2.getCatalog());
        IonTextWriterBuilder b3 = b2.withCatalog(catalog2);
        assertNotSame(b2, b3);
        assertSame(catalog, b2.getCatalog());
        assertSame(catalog2, b3.getCatalog());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCatalogImmutability()
    {
        IonCatalog catalog = new SimpleCatalog();

        IonTextWriterBuilder b = IonTextWriterBuilder.standard();
        b.setCatalog(catalog);

        IonTextWriterBuilder b2 = b.immutable();
        assertSame(catalog, b2.getCatalog());
        b2.setCatalog(null);
    }

    //-------------------------------------------------------------------------

    @Test
    public void testCharset()
    {
        IonTextWriterBuilder b = IonTextWriterBuilder.standard();
        b.setCharset(ASCII);
        assertSame(ASCII, b.getCharset());
        b.setCharset(null);
        assertSame(null, b.getCharset());

        // Test with...() on mutable builder

        IonTextWriterBuilder b2 = b.withCharset(UTF8);
        assertSame(b, b2);
        assertSame(UTF8, b2.getCharset());

        // Test with...() on immutable builder

        b2 = b.immutable();
        assertSame(UTF8, b2.getCharset());
        IonTextWriterBuilder b3 = b2.withCharset(ASCII);
        assertNotSame(b2, b3);
        assertSame(UTF8, b2.getCharset());
        assertSame(ASCII, b3.getCharset());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCharsetImmutability()
    {
        IonTextWriterBuilder b = IonTextWriterBuilder.standard();
        b.setCharset(ASCII);
        assertSame(ASCII, b.getCharset());


        IonTextWriterBuilder b2 = b.immutable();
        assertSame(ASCII, b2.getCharset());
        b2.setCharset(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCharsetValidation()
    {
        Charset iso = Charset.forName("ISO-8859-1");
        IonTextWriterBuilder b = IonTextWriterBuilder.standard();
        b.setCharset(iso);
    }

    //-------------------------------------------------------------------------

    @Test
    public void testImports()
    {
        SymbolTable f = Symtabs.CATALOG.getTable("fred", 1);
        SymbolTable g = Symtabs.CATALOG.getTable("ginger", 1);

        SymbolTable[] symtabsF = new SymbolTable[] { f };
        SymbolTable[] symtabsG = new SymbolTable[] { g };

        IonTextWriterBuilder b = IonTextWriterBuilder.standard();
        b.setImports(f);

        StringBuilder out = new StringBuilder();
        IonWriter writer = b.build(out);
        SymbolTable st = writer.getSymbolTable();
        assertArrayEquals(symtabsF, st.getImportedTables());

        // Test with...() on mutable builder

        IonTextWriterBuilder b2 = b.withImports(g);
        assertSame(b, b2);
        assertArrayEquals(symtabsG, b2.getImports());

        // Test with...() on immutable builder

        b2 = b.immutable();
        assertArrayEquals(symtabsG, b2.getImports());
        IonTextWriterBuilder b3 = b2.withImports(f);
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

        IonTextWriterBuilder b = IonTextWriterBuilder.standard();
        b.setImports(f);

        IonTextWriterBuilder b2 = b.immutable();
        assertArrayEquals(symtabs, b2.getImports());
        b2.setImports();
    }

    @Test
    public void testImportsNull()
    {
        SymbolTable f = Symtabs.CATALOG.getTable("fred", 1);
        SymbolTable[] symtabs = new SymbolTable[] { f };

        IonTextWriterBuilder b = IonTextWriterBuilder.standard();
        b.setImports(symtabs);
        b.setImports((SymbolTable[])null);
        assertSame(null, b.getImports());

        b.setImports(new SymbolTable[0]);
        assertArrayEquals(new SymbolTable[0], b.getImports());
    }
}
