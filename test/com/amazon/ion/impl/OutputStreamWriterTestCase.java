// Copyright (c) 2009-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import java.io.ByteArrayOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 *
 */
public abstract class OutputStreamWriterTestCase
    extends IonWriterTestCase
{
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
            super.flush();
        }

        @Override
        public void close() throws IOException
        {
            closed = true;
            super.close();
        }
    }

    private ByteArrayOutputStream myOutputStream;
    private OutputStreamWrapper myOutputStreamWrapper;
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
}
