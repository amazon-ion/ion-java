// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.impl.UnifiedSymbolTable.ION_SYMBOL_TABLE;
import static com.amazon.ion.junit.IonAssert.assertIonIteratorEquals;

import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonException;
import com.amazon.ion.IonList;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.Symtabs;
import com.amazon.ion.junit.IonAssert;
import java.io.OutputStream;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 */
public class BinaryWriterTest
    extends OutputStreamWriterTestCase
{
    @Override
    protected IonWriter makeWriter(OutputStream out, SymbolTable... imports)
        throws Exception
    {
        return system().newBinaryWriter(out, imports);
    }

    private byte[] copyAllFrom(IonReader ir)
    throws Exception
    {
        while (ir.next() != null)
        {
            iw.writeValue(ir);
        }
        iw.close();
        return outputByteArray();
    }

    @Test
    public void testOptimizedWriteValue()
    throws Exception
    {
        String importFred =
            ION_SYMBOL_TABLE
            + "::{imports:[{name:\"fred\",version:1,max_id:2}]}";
        String declareFred =
            ION_SYMBOL_TABLE
            + "::{symbols:[\"fred_1\"]}";
        String body =
            "fred_1 { fred_1: 12 } null [ (fred_1) ] fred_1::fred_1";

        SymbolTable fred1   = Symtabs.register("fred",   1, catalog());
        SymbolTable fred2   = Symtabs.register("fred",   2, catalog());
        SymbolTable ginger1 = Symtabs.register("ginger", 1, catalog());

        // Copy with no declared symbol table; after first symbol this is
        // optimized since the writer then has the only needed symbol.
        byte[] source = encode(body);
        IonReader ir = system().newReader(source);
        iw = makeWriter();
        byte[] copy = copyAllFrom(ir);
        Assert.assertArrayEquals(source, copy);

        // Copy with matching local symbols; similar to above.
        source = encode(declareFred + body);
        ir = system().newReader(source);
        iw = makeWriter();
        copy = copyAllFrom(ir);
        Assert.assertArrayEquals(source, copy);

        // Copy with identical imported symbol tables: fully optimized.
        source = encode(importFred + body);
        ir = system().newReader(source);
        iw = makeWriter(fred1);
        copy = copyAllFrom(ir);
        Assert.assertArrayEquals(source, copy);

        // Copy with superset import on the writer: (should be) fully optimized.
        // TODO at the moment the compatability code requires exact-match on imports.
        ir = system().newReader(source);
        iw = makeWriter(fred2);
        copy = copyAllFrom(ir);
        assertIonIteratorEquals(system().iterate(source),
                                system().iterate(copy));

        // Reader and writer use different symtabs entirely, no optimization
        ir = system().newReader(source);
        iw = makeWriter(ginger1);
        copy = copyAllFrom(ir);
        assertIonIteratorEquals(system().iterate(source),
                                system().iterate(copy));


        // TODO reader is subset of writer: optimize
        // TODO reader is superset of writer: no optimize
    }


    @Test
    public void testOptimizedWriteValueIntoContainer()
    throws Exception
    {
        String body =
            "fred_1 { fred_1: 12 } null [ (fred_1) ] fred_1::fred_1";
        byte[] source = encode(body);
        IonReader ir = system().newReader(source);

        iw = makeWriter();
        iw.stepIn(IonType.LIST);
        while (ir.next() != null) iw.writeValue(ir);
        iw.stepOut();

        IonDatagram dg = loader().load(outputByteArray());
        IonList list = (IonList) dg.get(0);

        assertIonIteratorEquals(system().iterate(source),
                                list.iterator());
    }

    @Test
    public void testOptimizedWriteValueBetweenStruct()
    throws Exception
    {
        String body =
            "fred_1 { fred_1: 12, fred_1: 13 }";
        byte[] source = encode(body);
        IonReader ir = system().newReader(source);
        ir.next();
        ir.next();
        ir.stepIn();

        iw = makeWriter();
        // Prime the local symtab so it matches the source symtab.
        iw.writeSymbol("fred_1");
        iw.stepIn(IonType.STRUCT);
        while (ir.next() != null) iw.writeValue(ir);
        iw.stepOut();

        byte[] copy = outputByteArray();

        assertIonIteratorEquals(system().iterate(source),
                                system().iterate(copy));
    }


    @Test(expected = IonException.class)
    public void testWriteSymbolWithLockedSymtab()
        throws Exception
    {
        iw = makeWriter();
        PrivateDmsdkUtils.lockLocalSymbolTable(iw.getSymbolTable());
        iw.writeSymbol("s");
    }

    @Test(expected = IonException.class)
    public void testAddTypeAnnotationWithLockedSymtab()
        throws Exception
    {
        iw = makeWriter();
        PrivateDmsdkUtils.lockLocalSymbolTable(iw.getSymbolTable());
        iw.addTypeAnnotation("a");
        iw.writeNull();
    }

    @Test(expected = IonException.class)
    public void testSetFieldNameWithLockedSymtab()
        throws Exception
    {
        iw = makeWriter();
        PrivateDmsdkUtils.lockLocalSymbolTable(iw.getSymbolTable());
        iw.stepIn(IonType.STRUCT);
        iw.setFieldName("f");
        iw.writeNull();
    }


    @Test
    public void testFlushingUnlockedSymtab()
    throws Exception
    {
        iw = makeWriter();
        SymbolTable symtab = iw.getSymbolTable();
        symtab.addSymbol("fred_1");
        symtab.addSymbol("fred_2");

        iw.writeSymbol("fred_1");
        iw.flush();
        byte[] bytes = myOutputStream.toByteArray();
        assertEquals(0, bytes.length);
    }

    @Test
    public void testFlushingUnlockedSymtabWithImports()
    throws Exception
    {
        SymbolTable fred1 = Symtabs.register("fred",   1, catalog());
        iw = makeWriter(fred1);
        iw.writeSymbol("fred_1");
        iw.flush();
        byte[] bytes = myOutputStream.toByteArray();
        assertEquals(0, bytes.length);
    }

    @Test @Ignore // TODO ION-218  and see below for another case
    public void testMinimalSymtab()
    throws Exception
    {
        iw = makeWriter();
        iw.writeNull();
        byte[] bytes = outputByteArray();

        // 4 bytes for IVM, 1 byte for null
        assertEquals(5, bytes.length);
    }

    @Test
    public void testBinaryWriterReuseWithNoSymbols()
        throws Exception
    {
        testBinaryWriterReuseWithSymbols(null, false);
    }

    @Test
    public void testBinaryWriterReuseWithSystemSymbols()
        throws Exception
    {
        testBinaryWriterReuseWithSymbols("name", false);
    }

    @Test
    public void testBinaryWriterReuseWithUserSymbols()
        throws Exception
    {
        testBinaryWriterReuseWithSymbols("s", true);
    }

    public void testBinaryWriterReuseWithSymbols(String symbol,
                                                 boolean fixedIon218)
        throws Exception
    {
        iw = makeWriter();
        iw.writeSymbol(symbol);
        iw.finish();

        byte[] bytes1 = myOutputStream.toByteArray();
        myOutputStream.reset();
        IonValue dg1 = loader().load(bytes1);

        iw.writeSymbol(symbol);
        iw.finish();
        byte[] bytes2 = myOutputStream.toByteArray();
        myOutputStream.reset();
        IonValue dg2 = loader().load(bytes2);

        IonAssert.assertIonEquals(dg1, dg2);
        // FIXME ION-218 this fails because bytes1 has an empty local symtab.
        if (fixedIon218) {
            Assert.assertArrayEquals(bytes1, bytes2);
        }

        iw.writeSymbol(symbol);
        iw.finish();
        byte[] bytes3 = myOutputStream.toByteArray();
        IonValue dg3 = loader().load(bytes3);

        IonAssert.assertIonEquals(dg1, dg3);
        Assert.assertArrayEquals(bytes2, bytes3);
    }
}
