// Copyright (c) 2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.Symtabs.printLocalSymtab;
import static com.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE;
import static com.amazon.ion.impl._Private_Utils.symtabExtends;
import static com.amazon.ion.junit.IonAssert.assertIonEquals;
import static com.amazon.ion.junit.IonAssert.assertIonIteratorEquals;
import static java.lang.reflect.Proxy.newProxyInstance;

import com.amazon.ion.IonContainer;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonList;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSequence;
import com.amazon.ion.IonSexp;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.Symtabs;
import com.amazon.ion.SystemSymbols;
import com.amazon.ion.junit.Injected.Inject;
import com.amazon.ion.system.IonSystemBuilder;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import org.junit.Test;

/**
 * Tests for {@link IonWriter#writeValue(IonReader)}, with and without
 * copy optimization enabled.
 *
 * @see IonSystemBuilder#withStreamCopyOptimized(boolean)
 */
public class OptimizedBinaryWriterTest
    extends OutputStreamWriterTestCase
{
    @Inject("copySpeed")
    public static final StreamCopySpeed[] COPY_SPEEDS =
        StreamCopySpeed.values();

    /**
     * Denotes whether the
     * {@link ByteTransferReader#transferCurrentValue(IonWriterSystemBinary)}
     * has been called after an {@link IonWriter#writeValue(IonReader)}.
     */
    private boolean isTransferCurrentValueInvoked = false;

    private static final String importFred1 = ION_SYMBOL_TABLE +
        "::{imports:[{name:\"fred\",version:1,max_id:2}]}";

    private static final String importFred2 = ION_SYMBOL_TABLE +
        "::{imports:[{name:\"fred\",version:2,max_id:4}]}";

    protected IonReader ir;

    @Override
    protected IonWriter makeWriter(OutputStream out, SymbolTable... imports)
        throws Exception
    {
        myOutputForm = OutputForm.BINARY;

        IonWriter iw = system().newBinaryWriter(out, imports);

        if (isStreamCopyOptimized())
        {
            assertTrue("IonWriter should be instance of IonWriterUserBinary",
                       iw instanceof IonWriterUserBinary);
            IonWriterUserBinary iwUserBinary = (IonWriterUserBinary) iw;
            assertTrue("IonWriterUserBinary should be stream copy optimized",
                       iwUserBinary.myStreamCopyOptimized);
        }

        return iw;
    }

    @Override
    protected void checkFlushedAfterTopLevelValueWritten()
    {
        checkFlushed(false);
    }

    private class TransferCurrentValueWatchingReader
        implements ByteTransferReader
    {
        private final ByteTransferReader myDelegate;

        TransferCurrentValueWatchingReader(ByteTransferReader byteTransferReader)
        {
            myDelegate = byteTransferReader;
        }

        public void transferCurrentValue(IonWriterSystemBinary writer)
            throws IOException
        {
            OptimizedBinaryWriterTest.this.isTransferCurrentValueInvoked = true;
            myDelegate.transferCurrentValue(writer);
        }
    }

    /**
     * Obtains a dynamic proxy of {@link IonReader} over the passed in byte[],
     * with an invocation handler hook over {@link ByteTransferReader} facet,
     * so as to verify whether the transferCurrentValue() method is actually
     * being called.
     *
     * @see TransferCurrentValueWatchingReader
     */
    private IonReader makeReaderProxy(byte[] bytes)
    {
        final IonReader reader = system().newReader(bytes);

        InvocationHandler handler = new InvocationHandler()
        {
            public Object invoke(Object proxy, Method method, Object[] args)
                throws Throwable
            {
                if (method.getName().equals("asFacet") &&
                    args.length == 1 &&
                    args[0] == ByteTransferReader.class)
                {
                    ByteTransferReader transferReader =
                        (ByteTransferReader) method.invoke(reader, args);

                    if (transferReader == null)
                        return null;

                    return new TransferCurrentValueWatchingReader(transferReader);
                }

                return method.invoke(reader, args);
            }
        };

        return (IonReader) newProxyInstance(reader.getClass().getClassLoader(),
                                            new Class[] { IonReader.class },
                                            handler);
    }


    /**
     * Calls {@code iw.writeValue(ir)} and checks whether copy optimization
     * has been made through the {@link #isTransferCurrentValueInvoked} boolean.
     *
     * @param expectedTransferInvoked
     *          the expected value of {@link #isTransferCurrentValueInvoked}
     *          after {@link IonWriter#writeValue(IonReader)} is called.
     */
    private void checkWriteValue(boolean expectedTransferInvoked)
        throws Exception
    {
        // Reset flag before calling IonWriter.writeValue
        isTransferCurrentValueInvoked = false;

        // TODO ION-241 - Currently, doesn't copy annotations or field names,
        //      so we always expect no transfer of raw bytes
        if (ir.isInStruct() || ir.getTypeAnnotationSymbols().length > 0)
        {
            expectedTransferInvoked = false;
        }

        iw.writeValue(ir); // method in test

        assertEquals(expectedTransferInvoked, isTransferCurrentValueInvoked);
    }

    //==========================================================================
    // Tests related to symtab extension checks.

    /**
     * Checks that the writer's symtab is not an extension of the reader's,
     * and that optimized write has not taken place.
     */
    private void checkWriteValueWithIncompatibleSymtab()
        throws Exception
    {
        ir.next();

        assertFalse(symtabExtends(iw.getSymbolTable(), ir.getSymbolTable()));
        checkWriteValue(false);
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

    //==========================================================================

    /**
     * Helper method for {@link #testOptimizedWriteValueBetweenContainers()}.
     * Copies children values from the reader to the writer.
     */
    private void copyContainerElements()
        throws Exception
    {
        ir.next();

        ir.stepIn();
        {
            while (ir.next() != null)
            {
                checkWriteValue(isStreamCopyOptimized());
            }
        }
        ir.stepOut();
    }

    /**
     * Test copy optimized writeValue when the IonReader is stepped-in.
     */
    @Test
    public void testOptimizedWriteValueBetweenContainers()
        throws Exception
    {
        String body =
            "{ fred_1: 123, fred_1: fred_1, fred_1: fred_1::null } " + // struct
            "[ 123, fred_1, fred_1::null ] " +                         // list
            "( 123 fred_1 fred_1::null ) " +                           // sexp
            "[[[ 123, fred_1, fred_1::null ]]]" +                      // deeply nested list
            "[ 123, fred_1, fred_1::null ]";                           // list

        byte[] source = encode(body);
        ir = makeReaderProxy(source);
        iw = makeWriter();

        // Prime the local symtab so it matches the source's symtab
        iw.writeSymbol("fred_1");

        //=== Struct ===
        copyContainerElements();

        //=== List ===
        copyContainerElements();

        //=== Sexp ===
        copyContainerElements();

        //=== deeply nested List ===
        ir.next();
        ir.stepIn();
        {
            ir.next();
            ir.stepIn();
            {
                copyContainerElements();
            }
            ir.stepOut();
        }
        ir.stepOut();

        //=== copy to a stepped-in-sexp writer ===
        iw.stepIn(IonType.SEXP);
        {
            copyContainerElements();
        }
        iw.stepOut();

        // Create the expected Ion data as text, which will be loaded into
        // a datagram later for equality check.

        StringBuilder sb = new StringBuilder();
        String scalars = "123 fred_1 fred_1::null "; // scalars in each container
        sb.append("fred_1 ");                   // from priming of local symtab
        sb.append(scalars);                     // from struct
        sb.append(scalars);                     // from list
        sb.append(scalars);                     // from sexp
        sb.append(scalars);                     // from deeply nested list
        sb.append("(" + scalars + ")");         // stepped-in-sexp writer

        String expectedIonText = sb.toString();

        IonDatagram expected = loader().load(expectedIonText);
        IonDatagram actual   = loader().load(outputByteArray());
        assertIonEquals(expected, actual);
    }

    //==========================================================================
    // Tests related to patching mechanism of binary writers.

    /**
     * Variants of different container values as Ion text of different type
     * descriptor lengths (i.e., L in the type desc octet, not VarUInt Length).
     * Refer to Ion's binary format wiki for more info.
     */
    protected enum ContainerTypeDescLengthVariant
    {
        LIST_L0         ("[]"),
        LIST_L1         ("[ 0 ]"),
        LIST_L2         ("[ 1 ]"),
        LIST_L13        ("[ 0, 1, 2, 3, 4, 5, 6 ]"),
        LIST_L14        ("[ 1, 2, 3, 4, 5, 6, 7, 8, 9 ]"),
        LIST_L15        ("null.list"),

        SEXP_L0         ("()"),
        SEXP_L1         ("( 0 )"),
        SEXP_L2         ("( 1 )"),
        SEXP_L13        ("( 0 1 2 3 4 5 6 )"),
        SEXP_L14        ("( 1 2 3 4 5 6 7 8 9 )"),
        SEXP_L15        ("null.sexp"),

        STRUCT_L0       ("{}"),
        STRUCT_L1       ("{ name: 1 }"), // NB: using system symbol
        STRUCT_L15      ("null.struct"),
        STRUCT_LEXISTS  ("{ name: 1, version: 2 }") // NB: using system symbols
        ;

        private String myText;

        ContainerTypeDescLengthVariant(String text)
        {
            this.myText = text;
        }

        // Not overriding toString() here as that'll mess with JUnit's reporting
        public String getText()
        {
            return myText;
        }
    }

    private void writeTypeDescLengthVariants()
        throws Exception
    {
        for (ContainerTypeDescLengthVariant variant
            : ContainerTypeDescLengthVariant.values())
        {
            byte[] bytes = encode(variant.getText());

            ir = makeReaderProxy(bytes);
            ir.next();

            if (iw.isInStruct())
            {
                iw.setFieldName(SystemSymbols.NAME); // NB: system symbol
            }
            checkWriteValue(isStreamCopyOptimized());
        }
    }

    private void addTypeDescLengthVariants(IonContainer container)
        throws Exception
    {
        if (container instanceof IonSequence)
        {
            IonSequence seq = (IonSequence) container;
            for (ContainerTypeDescLengthVariant variant
                : ContainerTypeDescLengthVariant.values())
            {
                seq.add(oneValue(variant.getText()));
            }
        }
        else
        {
            IonStruct struct = (IonStruct) container;
            for (ContainerTypeDescLengthVariant variant
                : ContainerTypeDescLengthVariant.values())
            {
                struct.add(SystemSymbols.NAME, oneValue(variant.getText()));
            }
        }
    }

    /**
     * Helper method for {@link #testOptimizedWriteLengthPatching()}.
     */
    private void writeAndAddTypeDescLengthVariants(IonContainer container)
        throws Exception
    {
        writeTypeDescLengthVariants();
        addTypeDescLengthVariants(container);
    }

    /**
     * Tests that the internal implementation of binary {@link IonWriter}'s
     * patching mechanism is correct.
     * <p>
     * Fixes ION-373.
     *
     * @see IonWriterSystemBinary
     */
    @Test
    public void testOptimizedWriteLengthPatching()
    throws Exception
    {
        iw = makeWriter();

        // The expected datagram - as values are copied to the IonWriter, we
        // book-keep the values written so as to do an equality check at the end.
        IonDatagram expected = system().newDatagram();

        //=== top level ===
        writeAndAddTypeDescLengthVariants(expected);

        //=== nested list ===
        iw.stepIn(IonType.LIST);
        IonList expectedNestedList = expected.add().newEmptyList();
        {
            writeAndAddTypeDescLengthVariants(expectedNestedList);
        }
        iw.stepOut();

        //=== nested sexp ===
        iw.stepIn(IonType.SEXP);
        IonSexp expectedNestedSexp = expected.add().newEmptySexp();
        {
            writeAndAddTypeDescLengthVariants(expectedNestedSexp);
        }
        iw.stepOut();

        //=== nested struct ===
        iw.stepIn(IonType.STRUCT);
        IonStruct expectedNestedStruct = expected.add().newEmptyStruct();
        {
            writeAndAddTypeDescLengthVariants(expectedNestedStruct);
        }
        iw.stepOut();

        //=== deeply nested list ===
        iw.stepIn(IonType.LIST);
        IonList nestedList1 = expected.add().newEmptyList();
        {
            writeAndAddTypeDescLengthVariants(nestedList1);

            iw.stepIn(IonType.LIST);
            {
                IonList nestedList2 = nestedList1.add().newEmptyList();

                writeAndAddTypeDescLengthVariants(nestedList2);
            }
            iw.stepOut();
        }
        iw.stepOut();

        IonDatagram actual = loader().load(outputByteArray());

        assertIonEquals(expected, actual);
    }

}
