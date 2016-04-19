/*
 * Copyright 2013-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.ion.impl;

import static software.amazon.ion.Symtabs.printLocalSymtab;
import static software.amazon.ion.impl.PrivateUtils.isNonSymbolScalar;
import static software.amazon.ion.impl.PrivateUtils.symtabExtends;
import static software.amazon.ion.junit.IonAssert.assertIonEquals;
import static software.amazon.ion.junit.IonAssert.assertIonIteratorEquals;

import org.junit.Test;
import software.amazon.ion.IonDatagram;
import software.amazon.ion.IonType;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.Symtabs;
import software.amazon.ion.impl.PrivateUtils;

/**
 * {@link OptimizedBinaryWriterTestCase} tests related to
 * {@link PrivateUtils#symtabExtends(SymbolTable, SymbolTable)} checks.
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
        iw = makeWriterWithLocalSymtab("amazon", "website");

        // writer's symtab same as reader's
        checkWriteValueWithCompatibleSymtab(); // amazon
        checkWriteValueWithCompatibleSymtab(); // website

        iw.close();

        IonDatagram expected = loader().load("amazon website");
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

    /**
     * Writer's LST subset of Reader's - no optimize.
     */
    @Test
    public void testOptimizedWriteValueSubsetWriterLST1()
        throws Exception
    {
        String readerLST = printLocalSymtab("amazon", "website");
        byte[] source = encode(readerLST + "amazon website");
        ir = makeReaderProxy(source);
        iw = makeWriterWithLocalSymtab("amazon");

        checkWriteValueWithIncompatibleSymtab();              // amazon
        checkWriteValueWithIncompatibleSymtab();              // website

        iw.close();

        IonDatagram expected = loader().load("amazon website");
        IonDatagram actual   = loader().load(outputByteArray());
        assertIonEquals(expected, actual);
    }

    /**
     * Writer's LST subset of Reader's - no optimize.
     * Reader's symtab has a gap at the end.
     */
    @Test
    public void testOptimizedWriteValueSubsetWriterLST2()
        throws Exception
    {
        // Create reader with LST with a gap at the end
        String readerLST = printLocalSymtab("amazon", "website", null);
        byte[] source = encode(readerLST + "amazon website");
        ir = makeReaderProxy(source);
        iw = makeWriterWithLocalSymtab("amazon", "website");

        checkWriteValueWithIncompatibleSymtab();              // amazon
        checkWriteValueWithIncompatibleSymtab();              // website

        iw.close();

        IonDatagram expected = loader().load("amazon website");
        IonDatagram actual   = loader().load(outputByteArray());
        assertIonEquals(expected, actual);
    }

    /**
     * Writer's LST superset of Reader's - optimize.
     */
    @Test
    public void testOptimizedWriteValueSupersetWriterLST1()
        throws Exception
    {
        String readerLST = printLocalSymtab("amazon");
        byte[] source = encode(readerLST + "amazon");
        ir = makeReaderProxy(source);
        iw = makeWriterWithLocalSymtab("amazon", "website");

        checkWriteValueWithCompatibleSymtab();            // amazon

        iw.close();

        IonDatagram expected = loader().load("amazon");
        IonDatagram actual   = loader().load(outputByteArray());
        assertIonEquals(expected, actual);
    }

    /**
     * Writer's LST superset of Reader's - optimize.
     * Writer's symtab has a gap at the end.
     */
    @Test
    public void testOptimizedWriteValueSupersetWriterLST2()
        throws Exception
    {
        String readerLST = printLocalSymtab("amazon", "website");
        byte[] source = encode(readerLST + "amazon website");
        ir = makeReaderProxy(source);
        iw = makeWriterWithLocalSymtab("amazon", "website", null);

        checkWriteValueWithCompatibleSymtab();              // amazon
        checkWriteValueWithCompatibleSymtab();              // website

        iw.close();

        IonDatagram expected = loader().load("amazon website");
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
     * TODO amznlabs/ion-java#18 at the moment the compatability code requires
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

    /**
     * Reader's source contains interspersed LSTs.
     * TODO amznlabs/ion-java#39 Investigate allowing a config. option to copy reader's LST
     *              over to the writer.
     */
    @Test
    public void testOptimizedWriteValue_InterspersedReaderLSTs()
        throws Exception
    {
        String readerLST1 = printLocalSymtab("a", "aa");
        String readerLST2 = printLocalSymtab("b", "bb");
        byte[] source = encode(readerLST1 + "a aa " +
                               readerLST2 + "b bb");
        ir = makeReaderProxy(source);
        iw = makeWriterWithLocalSymtab("a", "aa");

        // writer's symtab same as reader's - optimize
        checkWriteValueWithCompatibleSymtab();            // a
        checkWriteValueWithCompatibleSymtab();            // aa

        // writer's symtab diff. from reader's - no optimize
        checkWriteValueWithIncompatibleSymtab();          // b
        checkWriteValueWithIncompatibleSymtab();          // bb

        iw.close();

        IonDatagram expected = loader().load("a aa b bb");
        IonDatagram actual   = loader().load(outputByteArray());
        assertIonEquals(expected, actual);
    }

}
