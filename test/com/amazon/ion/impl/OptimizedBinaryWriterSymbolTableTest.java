// Copyright (c) 2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.Symtabs.printLocalSymtab;
import static com.amazon.ion.impl._Private_Utils.isNonSymbolScalar;
import static com.amazon.ion.impl._Private_Utils.symtabExtends;
import static com.amazon.ion.junit.IonAssert.assertIonEquals;
import static com.amazon.ion.junit.IonAssert.assertIonIteratorEquals;

import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonType;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.Symtabs;
import org.junit.Test;

/**
 * {@link OptimizedBinaryWriterTestCase} tests related to
 * {@link _Private_Utils#symtabExtends(SymbolTable, SymbolTable)} checks.
 */
public class OptimizedBinaryWriterSymbolTableTest
    extends OptimizedBinaryWriterTestCase
{
    /**
     * Checks that the writer's symtab is not an extension of the reader's,
     * and that optimized write has taken place depending on whether the
     * the writer {@link #isStreamCopyOptimized()} and reader's current value
     * is a non-symbol scalar.
     */
    private void checkWriteValueWithIncompatibleSymtab()
        throws Exception
    {
        IonType type = ir.next();

        assertFalse(symtabExtends(iw.getSymbolTable(), ir.getSymbolTable()));
        checkWriteValue(isStreamCopyOptimized() && isNonSymbolScalar(type));
    }

    /**
     * Checks that the writer's symtab is an extension of the reader's,
     * and that optimized write has taken place depending on whether
     * the writer {@link #isStreamCopyOptimized()}.
     */
    private void checkWriteValueWithCompatibleSymtab()
        throws Exception
    {
        ir.next();

        assertTrue(symtabExtends(iw.getSymbolTable(), ir.getSymbolTable()));
        checkWriteValue(isStreamCopyOptimized());
    }


    /**
     * Writer's LST identical to Reader's - optimize.
     */
    @Test
    public void testOptimizedWriteValueSameLST1()
        throws Exception
    {
        String readerLST = printLocalSymtab("amazon", "website");
        byte[] source = encode(readerLST + "amazon website");
        ir = makeReaderProxy(source);
        iw = makeWriter();

        // Prime the writer with LST symbols "amazon" and "website"
        // TODO-373 The only way to guarantee that the writer has the same LST
        //          as the reader would be to pass the LST over. Clean this up
        //          when this API is available.
        iw.writeSymbol("amazon");                         // amazon
        iw.writeSymbol("website");                        // website

        checkWriteValueWithCompatibleSymtab();            // amazon
        checkWriteValueWithCompatibleSymtab();            // website

        iw.close();

        IonDatagram expected = loader().load("amazon website amazon website");
        IonDatagram actual   = loader().load(outputByteArray());
        assertIonEquals(expected, actual);
    }

    /**
     * Writer's LST identical to Reader's - optimize.
     *
     * Writer has no declared symbol table to begin with, after writing first
     * symbol this is optimized since the writer now has the only needed symbol.
     */
    @Test
    public void testOptimizedWriteValueSameLST2()
        throws Exception
    {
        byte[] source = encode("amazon amazon 123");
        ir = makeReaderProxy(source);
        iw = makeWriter();

        checkWriteValueWithIncompatibleSymtab();   // amazon
        checkWriteValueWithCompatibleSymtab();     // amazon
        checkWriteValueWithCompatibleSymtab();     // 123

        iw.close();
        assertArrayEquals(source, outputByteArray());
    }

    // TODO ION-377
    // Test for the use-case where there are gaps in reader's LSTs.
    // This covers the case in UnifiedSymbolTable.isExtensionOf(SymbolTable),
    // for which at least one of its symbol's text are unknown.
    // We can't test this at the moment as there's no way to forcibly inject
    // gaps into a writer's LST.

    /**
     * Writer's LST subset of Reader's - no optimize.
     */
    @Test
    public void testOptimizedWriteValueSubsetWriterLST()
        throws Exception
    {
        String readerLST = printLocalSymtab("amazon", "website");
        byte[] source = encode(readerLST + "amazon website");
        ir = makeReaderProxy(source);
        iw = makeWriter();

        // Prime the writer with LST symbol "amazon"
        iw.writeSymbol("amazon");                             // amazon

        checkWriteValueWithIncompatibleSymtab();              // amazon
        checkWriteValueWithIncompatibleSymtab();              // website

        iw.close();

        IonDatagram expected = loader().load("amazon amazon website");
        IonDatagram actual   = loader().load(outputByteArray());
        assertIonEquals(expected, actual);
    }

    /**
     * Writer's LST superset of Reader's - optimize.
     */
    @Test
    public void testOptimizedWriteValueSupersetWriterLST()
        throws Exception
    {
        String readerLST = printLocalSymtab("amazon");
        byte[] source = encode(readerLST + "amazon");
        ir = makeReaderProxy(source);
        iw = makeWriter();

        // Prime the writer with LST symbols "amazon" and "website"
        iw.writeSymbol("amazon");                         // amazon
        iw.writeSymbol("website");                        // website

        checkWriteValueWithCompatibleSymtab();            // amazon

        iw.close();

        IonDatagram expected = loader().load("amazon website amazon");
        IonDatagram actual   = loader().load(outputByteArray());
        assertIonEquals(expected, actual);
    }

    /**
     * Writer's imports identical to Reader's: fully optimized.
     */
    @Test
    public void testOptimizedWriteValueSameImports()
        throws Exception
    {
        SymbolTable fred1 = Symtabs.register("fred", 1, catalog());

        byte[] source = encode(importFred1 + "fred_1 123 null");
        ir = makeReaderProxy(source);
        iw = makeWriter(fred1);

        checkWriteValueWithCompatibleSymtab();
        checkWriteValueWithCompatibleSymtab();
        checkWriteValueWithCompatibleSymtab();

        iw.close();
        assertArrayEquals(source, outputByteArray());
    }

    /**
     * Writer's imports different from Reader's, no optimization.
     */
    @Test
    public void testOptimizedWriteValueDiffImports()
        throws Exception
    {
        SymbolTable ginger1 = Symtabs.register("ginger", 1, catalog());

        byte[] source = encode(importFred1 + "fred_1 123 null");
        ir = makeReaderProxy(source);
        iw = makeWriter(ginger1);

        checkWriteValueWithIncompatibleSymtab();
        checkWriteValueWithIncompatibleSymtab();
        checkWriteValueWithIncompatibleSymtab();

        iw.close();
        assertIonIteratorEquals(system().iterate(source),
                                system().iterate(outputByteArray()));
    }

    /**
     * Writer's imports superset of Reader's: (could be) fully optimized.
     * TODO ION-253 at the moment the compatability code requires
     *      exact-match on imports.
     */
    @Test
    public void testOptimizedWriteValueSupersetWriterImport()
        throws Exception
    {
        SymbolTable fred2 = Symtabs.register("fred", 2, catalog());

        byte[] source = encode(importFred1 + "fred_1 123 null");
        ir = makeReaderProxy(source);
        iw = makeWriter(fred2);

        checkWriteValueWithIncompatibleSymtab();
        checkWriteValueWithIncompatibleSymtab();
        checkWriteValueWithIncompatibleSymtab();

        iw.close();
        assertIonIteratorEquals(system().iterate(source),
                                system().iterate(outputByteArray()));
    }

    /**
     * Writer's imports subset of Reader's - no optimize.
     */
    @Test
    public void testOptimizedWriteValueSubsetWriterImport()
        throws Exception
    {
        SymbolTable fred1 = Symtabs.register("fred", 1, catalog());

        byte[] source = encode(importFred2 + "fred_1 123 null");
        ir = makeReaderProxy(source);
        iw = makeWriter(fred1);

        checkWriteValueWithIncompatibleSymtab();
        checkWriteValueWithIncompatibleSymtab();
        checkWriteValueWithIncompatibleSymtab();

        iw.close();
        assertIonIteratorEquals(system().iterate(source),
                                system().iterate(outputByteArray()));
    }

}
