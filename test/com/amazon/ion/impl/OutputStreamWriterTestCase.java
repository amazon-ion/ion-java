// Copyright (c) 2009-2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.Symtabs;
import com.amazon.ion.SystemSymbols;
import java.io.ByteArrayOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import org.junit.Test;

/**
 *
 */
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

        public void assertWasFlushed()
        {
            assertTrue("stream was not flushed", flushed);
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

    @Test
    public void testCloseFinishThrows()
    throws Exception
    {
        iw = makeWriter();
        iw.writeInt(12L);
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

        // Force a local symtab.  TODO ION-165 Should have an API for this
        iw.addTypeAnnotation(SystemSymbols.ION_SYMBOL_TABLE);
        iw.stepIn(IonType.STRUCT);
        iw.stepOut();

        SymbolTable symtab = iw.getSymbolTable();  // TODO ION-269
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
        myOutputStreamWrapper.assertWasFlushed();
        myOutputStreamWrapper.flushed = false;

        byte[] bytes = myOutputStream.toByteArray();
        assertEquals(expected, loader().load(bytes));

        // Try flushing when there's just a pending annotation.
        iw.addTypeAnnotation("fred_1");
        iw.flush();
        myOutputStreamWrapper.assertWasFlushed();
        myOutputStreamWrapper.flushed = false;

        bytes = myOutputStream.toByteArray();
        assertEquals(expected, loader().load(bytes));

        iw.writeSymbol("fred_2");
        expected.add().newSymbol("fred_2").addTypeAnnotation("fred_1");

        iw.flush();
        myOutputStreamWrapper.assertWasFlushed();
        myOutputStreamWrapper.flushed = false;

        bytes = myOutputStream.toByteArray();
        assertEquals(expected, loader().load(bytes));
    }
}
