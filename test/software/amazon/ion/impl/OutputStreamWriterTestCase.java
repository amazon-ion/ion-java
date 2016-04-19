/*
 * Copyright 2009-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import org.junit.Test;
import software.amazon.ion.IonDatagram;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonType;
import software.amazon.ion.IonValue;
import software.amazon.ion.IonWriter;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.Symtabs;
import software.amazon.ion.SystemSymbols;
import software.amazon.ion.Timestamp;

public abstract class OutputStreamWriterTestCase
    extends IonWriterTestCase
{
    private IOException myFlushException = null;


    class OutputStreamWrapper extends FilterOutputStream
    {
        boolean flushed = false;
        boolean closed  = false;

        public OutputStreamWrapper(OutputStream out)
        {
            super(out);
        }

        @Override
        public void flush() throws IOException
        {
            flushed = true;
            if (myFlushException != null) throw myFlushException;
            super.flush();
        }

        @Override
        public void close() throws IOException
        {
            assertFalse("stream already closed", closed);
            closed = true;
            super.close();
        }
    }

    protected ByteArrayOutputStream myOutputStream;
    protected OutputStreamWrapper myOutputStreamWrapper;
    protected IonWriter myWriter;


    @Override
    protected final IonWriter makeWriter(SymbolTable... imports)
        throws Exception
    {
        myOutputStream = new ByteArrayOutputStream();
        myOutputStreamWrapper = new OutputStreamWrapper(myOutputStream);
        myWriter = makeWriter(myOutputStreamWrapper, imports);
        return myWriter;
    }

    protected abstract IonWriter makeWriter(OutputStream out,
                                            SymbolTable... imports)
        throws Exception;

    protected abstract void checkFlushedAfterTopLevelValueWritten();

    @Override
    protected byte[] outputByteArray() throws Exception
    {
        myWriter.close();
        checkClosed();

        byte[] bytes = myOutputStream.toByteArray();
        return bytes;
    }

    @Override
    protected void checkClosed()
    {
        assertTrue("output stream not flushed", myOutputStreamWrapper.flushed);
        assertTrue("output stream not closed",  myOutputStreamWrapper.closed);

    }

    @Override
    protected void checkFlushed(boolean expectFlushed)
    {
        assertEquals("output stream should be " +
                     (expectFlushed ? "flushed" : "not flushed"),
                     expectFlushed, myOutputStreamWrapper.flushed);
    }

    @Test
    public void testStreamIsClosedIfFinishThrows()
    throws Exception
    {
        iw = makeWriter();
        iw.writeInt(12L);

        // This will make flush() throw, which causes finish()
        // to throw because finish() calls flush().
        myFlushException = new IOException();

        try {
            iw.close();
            fail("Expected exception");
        }
        catch (IOException e)
        {
            assertSame(myFlushException, e);
        }
        checkClosed();
    }

    @Test
    public void testFlushingLockedSymtab()
        throws Exception
    {
        iw = makeWriter();

        // Force a local symtab.  TODO amznlabs/ion-java#8 Should have an API for this
        iw.addTypeAnnotation(SystemSymbols.ION_SYMBOL_TABLE);
        iw.stepIn(IonType.STRUCT);
        iw.stepOut();

        SymbolTable symtab = iw.getSymbolTable();  // TODO amznlabs/ion-java#22
        symtab.intern("fred_1");
        symtab.intern("fred_2");
        testFlushing();
    }

    @Test
    public void testFlushingLockedSymtabWithImports()
        throws Exception
    {
        SymbolTable fred1 = Symtabs.register("fred",   1, catalog());
        iw = makeWriter(fred1);
        testFlushing();
    }

    private void testFlushing()
        throws IOException
    {
        IonDatagram expected = system().newDatagram();

        iw.getSymbolTable().makeReadOnly();

        iw.writeSymbol("fred_1");
        expected.add().newSymbol("fred_1");

        iw.flush();
        checkFlushed(true);
        myOutputStreamWrapper.flushed = false;

        byte[] bytes = myOutputStream.toByteArray();
        assertEquals(expected, loader().load(bytes));

        // Try flushing when there's just a pending annotation.
        iw.addTypeAnnotation("fred_1");
        iw.flush();
        checkFlushed(true);
        myOutputStreamWrapper.flushed = false;

        bytes = myOutputStream.toByteArray();
        assertEquals(expected, loader().load(bytes));

        iw.writeSymbol("fred_2");
        expected.add().newSymbol("fred_2").addTypeAnnotation("fred_1");

        iw.flush();
        checkFlushed(true);
        myOutputStreamWrapper.flushed = false;

        bytes = myOutputStream.toByteArray();
        assertEquals(expected, loader().load(bytes));
    }

    /**
     * Checks whether the IonWriter implementation auto-flushes after writing
     * each top-level value. The actual check for the
     * implementation-specific IonWriter auto-flush mechanism is delegated to
     * the abstract method
     * {@link #checkFlushedAfterTopLevelValueWritten()}.
     */
    @Test
    public void testAutoFlushTopLevelValuesForTypedWritesContainers()
        throws Exception
    {
        iw = makeWriter();
        checkFlushed(false);


        iw.stepIn(IonType.LIST); // begin TLV
        checkFlushed(false);
        {
            iw.setTypeAnnotations("some_annot");
            iw.writeNull();
            checkFlushed(false);

            iw.writeInt(123);
            checkFlushed(false);

            iw.stepIn(IonType.SEXP);
            checkFlushed(false);
            {
                iw.setTypeAnnotations("some_annot");
                iw.writeBool(true);
                checkFlushed(false);

                iw.writeFloat(123.456e123);
                checkFlushed(false);

                iw.writeDecimal(new BigDecimal("123.456e123"));
                checkFlushed(false);

                iw.stepIn(IonType.LIST);
                checkFlushed(false);
                {
                    iw.setTypeAnnotations("some_annot");
                    iw.writeTimestamp(Timestamp
                                .forMillis(System.currentTimeMillis(), null));
                    checkFlushed(false);

                    iw.writeSymbol("some_symbol");
                    checkFlushed(false);

                    iw.writeString("some_string");
                    checkFlushed(false);

                    iw.writeClob("some_string".getBytes());
                    checkFlushed(false);

                    iw.writeBlob("some_string".getBytes());
                    checkFlushed(false);
                }
                iw.stepOut();
                checkFlushed(false);
            }
            iw.stepOut();
            checkFlushed(false);
        }
        iw.stepOut();
        checkFlushedAfterTopLevelValueWritten(); // end TLV


        iw.stepIn(IonType.STRUCT); // begin TLV
        checkFlushed(false);
        {
            iw.setFieldName("first");
            iw.writeNull();
            checkFlushed(false);

            iw.setFieldName("second");
            iw.writeInt(123);
            checkFlushed(false);
        }
        iw.stepOut();
        checkFlushedAfterTopLevelValueWritten(); // end TLV


        iw.stepIn(IonType.SEXP); // begin TLV
        checkFlushed(false);
        {
            iw.writeNull();
            checkFlushed(false);

            iw.writeInt(123);
            checkFlushed(false);
        }
        iw.stepOut();
        checkFlushedAfterTopLevelValueWritten(); // end TLV
    }

    /**
     * @see #testAutoFlushTopLevelValuesForTypedWritesContainers()
     */
    @Test
    public void testAutoFlushTopLevelValuesForTypedWritesScalars()
        throws Exception
    {
        iw = makeWriter();
        checkFlushed(false);

        iw.writeNull();
        checkFlushedAfterTopLevelValueWritten();

        iw.writeInt(123);
        checkFlushedAfterTopLevelValueWritten();

        iw.writeBool(true);
        checkFlushedAfterTopLevelValueWritten();

        iw.writeFloat(123.456e123);
        checkFlushedAfterTopLevelValueWritten();

        iw.writeDecimal(new BigDecimal("123.456e123"));
        checkFlushedAfterTopLevelValueWritten();

        iw.setTypeAnnotations("some_annot");
        iw.writeTimestamp(Timestamp
                          .forMillis(System.currentTimeMillis(), null));
        checkFlushedAfterTopLevelValueWritten();

        iw.writeSymbol("some_symbol");
        checkFlushedAfterTopLevelValueWritten();

        iw.writeString("some_string");
        checkFlushedAfterTopLevelValueWritten();

        iw.writeClob("some_string".getBytes());
        checkFlushedAfterTopLevelValueWritten();

        iw.writeBlob("some_string".getBytes());
        checkFlushedAfterTopLevelValueWritten();
    }

    /**
     * NOTE: This test method tests for the {@code write*()} methods of
     * the IonWriter that are not typed.
     *
     * @see #testAutoFlushTopLevelValuesForTypedWritesContainers()
     */
    @Test
    public void testAutoFlushTopLevelValuesForOtherWrites()
        throws Exception
    {
        IonValue val = system().singleValue("[a, [b, [c]]]");

        //================== IonValue.writeTo(IonWriter) =======================
        iw = makeWriter();
        val.writeTo(iw);
        checkFlushedAfterTopLevelValueWritten();

        //================== IonWriter.writeValues(IonReader) ==================
        iw = makeWriter();
        IonReader reader = system().newReader(val);
        iw.writeValues(reader);
        checkFlushedAfterTopLevelValueWritten();

        //================== IonWriter.writeValue(IonReader) ===================
        iw = makeWriter();
        reader = system().newReader(val);
        reader.next();
        iw.writeValue(reader);
        checkFlushedAfterTopLevelValueWritten();
    }
}
