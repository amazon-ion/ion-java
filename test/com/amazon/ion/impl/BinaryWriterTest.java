// Copyright (c) 2011-2016 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.Symtabs;
import com.amazon.ion.junit.IonAssert;
import java.io.OutputStream;
import org.junit.Assert;
import org.junit.Test;

public class BinaryWriterTest
    extends OutputStreamWriterTestCase
{
    @Override
    protected IonWriter makeWriter(OutputStream out, SymbolTable... imports)
        throws Exception
    {
        myOutputForm = OutputForm.BINARY;
        return system().newBinaryWriter(out, imports);
    }

    @Test(expected = IonException.class)
    public void testWriteSymbolWithLockedSymtab()
        throws Exception
    {
        iw = makeWriter();
        iw.writeSymbol("force a local symtab"); // TODO ION-165
        iw.getSymbolTable().makeReadOnly();
        iw.writeSymbol("s");
    }

    @Test(expected = IonException.class)
    public void testAddTypeAnnotationWithLockedSymtab()
        throws Exception
    {
        iw = makeWriter();
        iw.writeSymbol("force a local symtab"); // TODO ION-165
        iw.getSymbolTable().makeReadOnly();
        iw.addTypeAnnotation("a");
        iw.writeNull();
    }

    @Test(expected = IonException.class)
    public void testSetFieldNameWithLockedSymtab()
        throws Exception
    {
        iw = makeWriter();
        iw.writeSymbol("force a local symtab"); // TODO ION-165
        iw.getSymbolTable().makeReadOnly();
        iw.stepIn(IonType.STRUCT);
        iw.setFieldName("f");
        iw.writeNull();
    }


    @Test
    public void testFlushingUnlockedSymtab()
    throws Exception
    {
        iw = makeWriter();
        iw.writeSymbol("force a local symtab"); // TODO ION-165
        SymbolTable symtab = iw.getSymbolTable();
        symtab.intern("fred_1");
        symtab.intern("fred_2");

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

    @Test
    public void testMinimalSymtab()
    throws Exception
    {
        iw = makeWriter();
        iw.writeNull();
        byte[] bytes = outputByteArray();

        // 4 bytes for IVM, 1 byte for null
        assertEquals(5, bytes.length);
    }

    private static final int MANY_ANNOTATION_LENGTH = 14;
    private static final String MANY_ANNOTATION_SYMBOL_TEXT = "x";
    @Test
    public void testManyAnnotations()
        throws Exception
    {
        // TT72090065 - >= 14 bytes of annotation causes a length miscalculation
        final IonWriter out = makeWriter();
        out.stepIn(IonType.SEXP);
        {
            for (int i = 0; i < MANY_ANNOTATION_LENGTH; i++) {
                out.addTypeAnnotation(MANY_ANNOTATION_SYMBOL_TEXT);
            }
            out.writeSymbol(MANY_ANNOTATION_SYMBOL_TEXT);
        }
        out.stepOut();
        out.close();

        final IonReader in = reread();
        assertEquals(IonType.SEXP, in.next());
        in.stepIn();
        {
            assertEquals(IonType.SYMBOL, in.next());
            final String[] ann = in.getTypeAnnotations();
            assertEquals(MANY_ANNOTATION_LENGTH, ann.length);
            for (int i = 0; i < MANY_ANNOTATION_LENGTH; i++) {
                assertEquals(MANY_ANNOTATION_SYMBOL_TEXT, ann[i]);
            }
            assertEquals(null, in.next());
        }
        in.stepOut();
        assertEquals(null, in.next());
    }

    @Test
    public void testBinaryWriterReuseWithNoSymbols()
        throws Exception
    {
        testBinaryWriterReuseWithSymbols(null);
    }

    @Test
    public void testBinaryWriterReuseWithSystemSymbols()
        throws Exception
    {
        testBinaryWriterReuseWithSymbols("name");
    }

    @Test
    public void testBinaryWriterReuseWithUserSymbols()
        throws Exception
    {
        testBinaryWriterReuseWithSymbols("s");
    }

    public void testBinaryWriterReuseWithSymbols(String symbol)
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
        Assert.assertArrayEquals(bytes1, bytes2);

        iw.writeSymbol(symbol);
        iw.finish();
        byte[] bytes3 = myOutputStream.toByteArray();
        IonValue dg3 = loader().load(bytes3);

        IonAssert.assertIonEquals(dg1, dg3);
        Assert.assertArrayEquals(bytes2, bytes3);
    }

    @Override
    protected void checkFlushedAfterTopLevelValueWritten()
    {
        checkFlushed(false);
    }
}
