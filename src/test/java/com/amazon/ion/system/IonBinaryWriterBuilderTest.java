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

import static com.amazon.ion.TestUtils.symbolTableEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.TestUtils;
import com.amazon.ion.impl.Symtabs;
import com.amazon.ion.impl._Private_IonBinaryWriterBuilder;
import com.amazon.ion.impl._Private_IonWriter;
import com.amazon.ion.impl._Private_Utils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import org.junit.Assert;
import org.junit.Test;


public class IonBinaryWriterBuilderTest
{
    public void testBuildNull(IonBinaryWriterBuilder b)
    {
        try {
            b.build((OutputStream)null);
            fail("Expected exception");
        }
        catch (NullPointerException e) { }
    }

    @Test
    public void testStandard()
    {
        IonBinaryWriterBuilder b = IonBinaryWriterBuilder.standard();
        Assert.assertNotNull(b);

        testBuildNull(b);

        OutputStream out = new ByteArrayOutputStream();
        IonWriter writer = b.build(out);
        Assert.assertNotNull(writer);

        assertNotSame(b, IonBinaryWriterBuilder.standard());
    }


    //-------------------------------------------------------------------------

    @Test
    public void testCustomCatalog()
    {
        IonCatalog catalog = new SimpleCatalog();

        IonBinaryWriterBuilder b = IonBinaryWriterBuilder.standard();
        b.setCatalog(catalog);
        assertSame(catalog, b.getCatalog());

        OutputStream out = new ByteArrayOutputStream();
        IonWriter writer = b.build(out);
        assertSame(catalog, ((_Private_IonWriter)writer).getCatalog());

        IonCatalog catalog2 = new SimpleCatalog();
        b.setCatalog(catalog2);
        assertSame(catalog2, b.getCatalog());

        // Test with...() on mutable builder

        IonBinaryWriterBuilder b2 = b.withCatalog(catalog);
        assertSame(b, b2);
        assertSame(catalog, b2.getCatalog());

        // Test with...() on immutable builder

        b2 = b.immutable();
        assertSame(catalog, b2.getCatalog());
        IonBinaryWriterBuilder b3 = b2.withCatalog(catalog2);
        assertNotSame(b2, b3);
        assertSame(catalog, b2.getCatalog());
        assertSame(catalog2, b3.getCatalog());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCatalogImmutability()
    {
        IonCatalog catalog = new SimpleCatalog();

        IonBinaryWriterBuilder b = IonBinaryWriterBuilder.standard();
        b.setCatalog(catalog);

        IonBinaryWriterBuilder b2 = b.immutable();
        assertSame(catalog, b2.getCatalog());
        b2.setCatalog(null);
    }


    //-------------------------------------------------------------------------

    @Test
    public void testStreamCopyOptimized()
    {
        IonBinaryWriterBuilder b = IonBinaryWriterBuilder.standard();
        b.setStreamCopyOptimized(true);
        assertTrue(b.isStreamCopyOptimized());

        OutputStream out = new ByteArrayOutputStream();
        IonWriter w = b.build(out);
        assertTrue(((_Private_IonWriter)w).isStreamCopyOptimized());
    }


    @Test(expected = UnsupportedOperationException.class)
    public void testStreamCopyOptimizedImmutability()
    {
        IonBinaryWriterBuilder b = IonBinaryWriterBuilder.standard();
        b.setStreamCopyOptimized(true);

        IonBinaryWriterBuilder b2 = b.immutable();
        assertTrue(b2.isStreamCopyOptimized());
        b2.setStreamCopyOptimized(false);
    }


    //-------------------------------------------------------------------------

    @Test
    public void testSetIsFloatBinary32Enabled() throws IOException
    {
        IonSystem system = IonSystemBuilder.standard().build();

        IonBinaryWriterBuilder b = IonBinaryWriterBuilder.standard();
        b.setIsFloatBinary32Enabled(true);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonWriter writer = b.build(out);
        writer.writeFloat(1.0);
        writer.close();
        assertEquals(9, out.size());
        assertEquals(system.newFloat(1.0), system.singleValue(out.toByteArray()));

        b.setIsFloatBinary32Enabled(false);

        out = new ByteArrayOutputStream();
        writer = b.build(out);
        writer.writeFloat(1.0);
        writer.close();
        assertEquals(13, out.size());
        assertEquals(system.newFloat(1.0), system.singleValue(out.toByteArray()));
    }

    @Test
    public void testWithFloatBinary32Enabled() throws IOException
    {
        IonSystem system = IonSystemBuilder.standard().build();

        IonBinaryWriterBuilder b = IonBinaryWriterBuilder.standard()
            .withFloatBinary32Enabled();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonWriter writer = b.build(out);
        writer.writeFloat(1.0);
        writer.close();
        assertEquals(9, out.size());
        assertEquals(system.newFloat(1.0), system.singleValue(out.toByteArray()));
    }

    @Test
    public void testWithFloatBinary32Disabled() throws IOException
    {
        IonSystem system = IonSystemBuilder.standard().build();

        IonBinaryWriterBuilder b = IonBinaryWriterBuilder.standard()
            .withFloatBinary32Disabled();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonWriter writer = b.build(out);
        writer.writeFloat(1.0);
        writer.close();
        assertEquals(13, out.size());
        assertEquals(system.newFloat(1.0), system.singleValue(out.toByteArray()));
    }

    //-------------------------------------------------------------------------


    @Test
    public void testSymtabValueFactory()
    {
        IonSystem system = IonSystemBuilder.standard().build();

        _Private_IonBinaryWriterBuilder b =
            _Private_IonBinaryWriterBuilder.standard();
        b.setSymtabValueFactory(system);
        assertSame(system, b.getSymtabValueFactory());

        // The value factory isn't visible through other APIs so we can't
        // really test any further.
    }


    @Test(expected = UnsupportedOperationException.class)
    public void testSymtabValueFactoryImmutability()
    {
        _Private_IonBinaryWriterBuilder b =
            _Private_IonBinaryWriterBuilder.standard();
        b.setSymtabValueFactory(IonSystemBuilder.standard().build());

        _Private_IonBinaryWriterBuilder b2 = b.immutable();
        b2.setSymtabValueFactory(null);
    }


    //-------------------------------------------------------------------------


    @Test
    public void testInitialSymtab()
        throws IOException
    {
        SymbolTable sst = _Private_Utils.systemSymtab(1);

        SymbolTable lst0 = Symtabs.localSymbolTableFactory().newLocalSymtab(sst);
        lst0.intern("hello");

        _Private_IonBinaryWriterBuilder b =
            _Private_IonBinaryWriterBuilder.standard();
        b.setInitialSymbolTable(lst0);
        assertSame(lst0, b.getInitialSymbolTable());

        OutputStream out = new ByteArrayOutputStream();
        IonWriter writer = b.build(out);
        assertEquals(sst.getMaxId() + 1,
                     writer.getSymbolTable().findSymbol("hello"));
        // Builder makes a copy of the symtab
        SymbolTable lst1 = writer.getSymbolTable();
        assertNotSame(lst0, lst1);
        assertSame(lst0, b.getInitialSymbolTable());

        // Second call to build, we get another copy.
        writer = b.build(out);
        SymbolTable lst2 = writer.getSymbolTable();
        assertNotSame(lst0, lst2);
        assertNotSame(lst1, lst2);
        writer.writeSymbol("addition");

        // Now the LST has been extended, so the builder should make a copy
        // with the original max_id.
        writer = b.build(out);
        SymbolTable lst3 = writer.getSymbolTable();
        assertEquals(sst.getMaxId() + 1,
                     lst3.findSymbol("hello"));
        assertEquals(sst.getMaxId() + 1, lst3.getMaxId());
        assertNotSame(lst0, lst3);
        assertNotSame(lst1, lst3);
        assertNotSame(lst2, lst3);
        assertSame(lst0, b.getInitialSymbolTable());
    }

    @Test
    public void testImmutableInitialSymtab()
    {
        SymbolTable sst = _Private_Utils.systemSymtab(1);

        // Immutable local symtabs shouldn't get copied.
        SymbolTable lst = Symtabs.localSymbolTableFactory().newLocalSymtab(sst);
        lst.intern("hello");
        lst.makeReadOnly();

        _Private_IonBinaryWriterBuilder b =
            _Private_IonBinaryWriterBuilder.standard();
        b.setInitialSymbolTable(lst);
        assertSame(lst, b.getInitialSymbolTable());

        OutputStream out = new ByteArrayOutputStream();
        IonWriter writer = b.build(out);
        assertTrue(symbolTableEquals(lst, writer.getSymbolTable()));

        writer = b.build(out);
        assertTrue(symbolTableEquals(lst, writer.getSymbolTable()));
    }

    @Test
    public void testInitialSymtabFromReader()
        throws Exception
    {
        SymbolTable lst;
        try (IonReader reader = IonReaderBuilder.standard().build(TestUtils.ensureBinary(IonSystemBuilder.standard().build(), "foo".getBytes(StandardCharsets.UTF_8)))) {
            assertEquals(IonType.SYMBOL, reader.next());
            lst = reader.getSymbolTable();
        }

        _Private_IonBinaryWriterBuilder b =
            _Private_IonBinaryWriterBuilder.standard();
        b.setInitialSymbolTable(lst);
        assertSame(lst, b.getInitialSymbolTable());

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonWriter writer = b.build(out);
        assertTrue(symbolTableEquals(lst, writer.getSymbolTable()));

        writer = b.build(out);
        assertTrue(symbolTableEquals(lst, writer.getSymbolTable()));

        writer.writeSymbol("bar");
        writer.writeSymbol("foo");
        writer.close();

        try (IonReader reader = IonReaderBuilder.standard().build(out.toByteArray())) {
            assertEquals(IonType.SYMBOL, reader.next());
            assertEquals("bar", reader.stringValue());
            assertEquals(IonType.SYMBOL, reader.next());
            assertEquals("foo", reader.stringValue());
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testInitialSymtabImmutability()
    {
        _Private_IonBinaryWriterBuilder b =
            _Private_IonBinaryWriterBuilder.standard();
        b.setInitialSymbolTable(null);

        _Private_IonBinaryWriterBuilder b2 = b.immutable();
        b2.setInitialSymbolTable(null);
    }


    //-------------------------------------------------------------------------

    @Test
    public void testImports()
    {
        SymbolTable f = Symtabs.CATALOG.getTable("fred", 1);
        SymbolTable g = Symtabs.CATALOG.getTable("ginger", 1);

        SymbolTable[] symtabsF = new SymbolTable[] { f };
        SymbolTable[] symtabsG = new SymbolTable[] { g };

        IonBinaryWriterBuilder b = IonBinaryWriterBuilder.standard();
        b.setImports(f);

        OutputStream out = new ByteArrayOutputStream();
        IonWriter writer = b.build(out);
        SymbolTable st = writer.getSymbolTable();
        assertArrayEquals(symtabsF, st.getImportedTables());

        // Test with...() on mutable builder

        IonBinaryWriterBuilder b2 = b.withImports(g);
        assertSame(b, b2);
        assertArrayEquals(symtabsG, b2.getImports());

        // Test with...() on immutable builder

        b2 = b.immutable();
        assertArrayEquals(symtabsG, b2.getImports());
        IonBinaryWriterBuilder b3 = b2.withImports(f);
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

        IonBinaryWriterBuilder b = IonBinaryWriterBuilder.standard();
        b.setImports(f);

        IonBinaryWriterBuilder b2 = b.immutable();
        assertArrayEquals(symtabs, b2.getImports());
        b2.setImports();
    }

    @Test
    public void testImportsNull()
    {
        SymbolTable f = Symtabs.CATALOG.getTable("fred", 1);
        SymbolTable[] symtabs = new SymbolTable[] { f };

        IonBinaryWriterBuilder b = IonBinaryWriterBuilder.standard();
        b.setImports(symtabs);
        b.setImports((SymbolTable[])null);
        assertSame(null, b.getImports());

        b.setImports(new SymbolTable[0]);
        assertArrayEquals(new SymbolTable[0], b.getImports());
    }
}
