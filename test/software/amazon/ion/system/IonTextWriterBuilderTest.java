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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static software.amazon.ion.SystemSymbols.ION_1_0;
import static software.amazon.ion.system.IonTextWriterBuilder.ASCII;
import static software.amazon.ion.system.IonTextWriterBuilder.UTF8;
import static software.amazon.ion.system.IonTextWriterBuilder.LstMinimizing.EVERYTHING;
import static software.amazon.ion.system.IonTextWriterBuilder.LstMinimizing.LOCALS;
import static software.amazon.ion.system.IonWriterBuilder.InitialIvmHandling.SUPPRESS;
import static software.amazon.ion.system.IonWriterBuilder.IvmMinimizing.ADJACENT;
import static software.amazon.ion.system.IonWriterBuilder.IvmMinimizing.DISTANT;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.ion.IonCatalog;
import software.amazon.ion.IonWriter;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.Symtabs;
import software.amazon.ion.impl.PrivateIonWriter;
import software.amazon.ion.system.IonTextWriterBuilder;
import software.amazon.ion.system.SimpleCatalog;

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

        assertNotSame(b, IonTextWriterBuilder.standard());
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
        assertSame(catalog, ((PrivateIonWriter)writer).getCatalog());

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
    public void testInitialIvmHandling()
    {
        IonTextWriterBuilder b = IonTextWriterBuilder.standard();
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
        IonTextWriterBuilder b = IonTextWriterBuilder.standard();
        b.setInitialIvmHandling(SUPPRESS);

        IonTextWriterBuilder b2 = b.immutable();
        assertSame(SUPPRESS, b2.getInitialIvmHandling());
        b2.setInitialIvmHandling(null);
    }

    @Test
    public void testInitialIvmSuppression()
        throws IOException
    {
        IonTextWriterBuilder b = IonTextWriterBuilder.standard();

        StringBuilder out = new StringBuilder();
        IonWriter writer = b.build(out);
        writer.writeSymbol(ION_1_0);
        writer.writeNull();
        writer.writeSymbol(ION_1_0);
        writer.close();
        assertEquals(ION_1_0 + " null " + ION_1_0, out.toString());

        b.withInitialIvmHandling(SUPPRESS);
        out.setLength(0);
        writer = b.build(out);
        writer.writeSymbol(ION_1_0);
        writer.writeSymbol(ION_1_0);
        writer.writeNull();
        writer.writeSymbol(ION_1_0);
        writer.close();
        assertEquals("null " + ION_1_0, out.toString());
    }

    //-------------------------------------------------------------------------

    @Test
    public void testIvmMinimizing()
    {
        IonTextWriterBuilder b = IonTextWriterBuilder.standard();
        assertEquals(null, b.getIvmMinimizing());
        b.setIvmMinimizing(ADJACENT);
        assertSame(ADJACENT, b.getIvmMinimizing());

        // Test with...() on mutable builder

        IonTextWriterBuilder b2 = b.withIvmMinimizing(null);
        assertSame(b, b2);
        assertSame(null, b.getIvmMinimizing());

        // Test with...() on immutable builder

        b2 = b.immutable();
        assertSame(null, b2.getIvmMinimizing());
        IonTextWriterBuilder b3 = b2.withIvmMinimizing(ADJACENT);
        assertNotSame(b2, b3);
        assertSame(null, b2.getIvmMinimizing());
        assertSame(ADJACENT, b3.getIvmMinimizing());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testIvmMinimizingImmutability()
    {
        IonTextWriterBuilder b = IonTextWriterBuilder.standard();
        b.setIvmMinimizing(ADJACENT);

        IonTextWriterBuilder b2 = b.immutable();
        assertSame(ADJACENT, b2.getIvmMinimizing());
        b2.setIvmMinimizing(null);
    }

    @Test
    public void testIvmMinimization()
        throws IOException
    {
        IonTextWriterBuilder b = IonTextWriterBuilder.standard();

        StringBuilder out = new StringBuilder();
        IonWriter writer = b.build(out);
        writer.writeSymbol(ION_1_0);
        writer.writeSymbol(ION_1_0);
        writer.close();
        assertEquals(ION_1_0 + " " + ION_1_0, out.toString());

        b.withIvmMinimizing(ADJACENT);
        out.setLength(0);
        writer = b.build(out);
        writer.writeSymbol(ION_1_0);
        writer.writeSymbol(ION_1_0);
        writer.writeNull();
        writer.writeSymbol(ION_1_0);
        writer.writeSymbol(ION_1_0);
        writer.close();
        assertEquals(ION_1_0 + " null " + ION_1_0, out.toString());

        b.withIvmMinimizing(DISTANT);
        out.setLength(0);
        writer = b.build(out);
        writer.writeSymbol(ION_1_0);
        writer.writeSymbol(ION_1_0);
        writer.writeNull();
        writer.writeSymbol(ION_1_0);
        writer.writeSymbol(ION_1_0);
        writer.close();
        assertEquals(ION_1_0 + " null", out.toString());
    }

    //-------------------------------------------------------------------------

    @Test
    public void testLstMinimizing()
    {
        IonTextWriterBuilder b = IonTextWriterBuilder.standard();
        b.setLstMinimizing(EVERYTHING);
        assertSame(EVERYTHING, b.getLstMinimizing());

        // Test with...() on mutable builder

        IonTextWriterBuilder b2 = b.withLstMinimizing(null);
        assertSame(b, b2);
        assertSame(null, b.getLstMinimizing());

        // Test with...() on immutable builder

        b2 = b.immutable();
        assertSame(null, b2.getLstMinimizing());
        IonTextWriterBuilder b3 = b2.withLstMinimizing(LOCALS);
        assertNotSame(b2, b3);
        assertSame(null, b2.getLstMinimizing());
        assertSame(LOCALS, b3.getLstMinimizing());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testLstMinimizingImmutability()
    {
        IonTextWriterBuilder b = IonTextWriterBuilder.standard();
        b.setLstMinimizing(EVERYTHING);

        IonTextWriterBuilder b2 = b.immutable();
        assertSame(EVERYTHING, b2.getLstMinimizing());
        b2.setLstMinimizing(null);
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

        // Building shouldn't mutate the builder.
        b.build(new StringBuilder());
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

    //-------------------------------------------------------------------------


    @Test(expected = UnsupportedOperationException.class)
    public void testLongStringThresholdImmutability()
    {
        IonTextWriterBuilder b = IonTextWriterBuilder.standard();
        b.setLongStringThreshold(99);

        IonTextWriterBuilder b2 = b.immutable();
        assertEquals(99, b2.getLongStringThreshold());
        b2.setLongStringThreshold(80);
    }
}
