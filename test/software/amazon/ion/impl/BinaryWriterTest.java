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

package software.amazon.ion.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import software.amazon.ion.IonException;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonSystem;
import software.amazon.ion.IonType;
import software.amazon.ion.IonValue;
import software.amazon.ion.IonWriter;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.SymbolToken;
import software.amazon.ion.Symtabs;
import software.amazon.ion.junit.IonAssert;
import software.amazon.ion.system.IonBinaryWriterBuilder;
import software.amazon.ion.util.NullOutputStream;

public class BinaryWriterTest
    extends OutputStreamWriterTestCase
{

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Override
    protected IonWriter makeWriter(OutputStream out, SymbolTable... imports)
        throws Exception
    {
        myOutputForm = OutputForm.BINARY;
        return system().newBinaryWriter(out, imports);
    }

    @Test
    public void testWriteSymbolWithLockedSymtab()
        throws Exception
    {
        iw = makeWriter();
        iw.writeSymbol("force a local symtab"); // TODO amznlabs/ion-java#8
        iw.getSymbolTable().makeReadOnly();
        thrown.expect(IonException.class);
        iw.writeSymbol("s");
    }

    @Test
    public void testAddTypeAnnotationWithLockedSymtab()
        throws Exception
    {
        iw = makeWriter();
        iw.writeSymbol("force a local symtab"); // TODO amznlabs/ion-java#8
        iw.getSymbolTable().makeReadOnly();
        thrown.expect(IonException.class);
        iw.addTypeAnnotation("a");
    }

    @Test
    public void testSetFieldNameWithLockedSymtab()
        throws Exception
    {
        iw = makeWriter();
        iw.writeSymbol("force a local symtab"); // TODO amznlabs/ion-java#8
        iw.getSymbolTable().makeReadOnly();
        iw.stepIn(IonType.STRUCT);
        thrown.expect(IonException.class);
        iw.setFieldName("f");
    }

    @Test
    public void testInternLockedSymtab()
        throws Exception
    {
        iw = makeWriter();
        iw.writeSymbol("force a local symtab");
        SymbolTable symtab = iw.getSymbolTable();
        symtab.makeReadOnly();
        assertTrue(symtab.isReadOnly());
        thrown.expect(IonException.class);
        symtab.intern("d");
    }

    @Test
    public void testInternUnlockedSymtab()
        throws Exception
    {
        iw = makeWriter();
        iw.writeSymbol("force a local symtab");
        SymbolTable symtab = iw.getSymbolTable();
        assertTrue(!symtab.isReadOnly());
        assertTrue(symtab.isLocalTable());
        symtab.intern("d");
        iw.stepIn(IonType.STRUCT);
        {
            iw.setFieldName("e"); //this causes e to be interned
            iw.writeInt(1);
            iw.setFieldName("d"); //d was already interned
            iw.writeInt(2);
        }
        iw.stepOut();
        symtab.makeReadOnly();
        assertTrue(symtab.isReadOnly());
        SymbolToken d = symtab.find("d");
        SymbolToken e = symtab.find("e");
        assertEquals("d", d.assumeText());
        assertEquals("e", e.assumeText());
        //verify that manually interning d first worked
        assertTrue(d.getSid() < e.getSid());
    }

    @Test
    public void testFlushingUnlockedSymtab()
    throws Exception
    {
        iw = makeWriter();
        iw.writeSymbol("force a local symtab"); // TODO amznlabs/ion-java#8
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

    /*
     * Tests write of a stream larger than 2GB.
     */
    @Test
    public void testHugeWrite() throws IOException
    {
        IonSystem ion = system();
        IonWriter ionWriter = IonBinaryWriterBuilder.standard().build(new NullOutputStream());
        final int CHUNK_LENGTH = 1024;
        byte[] bytes = new byte[CHUNK_LENGTH];
        for (int i = 0; i < CHUNK_LENGTH; i++)
        {
            bytes[i] = (byte)(i % 128);
        }
        IonValue value = ion.newString(new String(bytes, "UTF-8"));
        long twoGB = 2L * 1024 * 1024 * 1024;
        long repeats = (twoGB / CHUNK_LENGTH) + 1; //go just past the limit
        long bytesWritten = 0;
        for (long i = 0; i < repeats; i++)
        {
            value.writeTo(ionWriter);
            bytesWritten += CHUNK_LENGTH;
        }
        ionWriter.close();
        assertTrue(bytesWritten >= twoGB);
    }

    @Test
    public void testNoIVMWrittenWhenNoValuesWritten() throws IOException
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonWriter writer = system().newBinaryWriter(out);
        writer.close();
        assertEquals(0, out.size()); //no IVM written
    }

    @Test(expected = IllegalStateException.class)
    public void testFinishNotAtTopLevel() throws Exception
    {
        iw = makeWriter();
        iw.stepIn(IonType.STRUCT);
        iw.finish();
    }

    @Override
    protected void checkFlushedAfterTopLevelValueWritten()
    {
        checkFlushed(false);
    }

}
