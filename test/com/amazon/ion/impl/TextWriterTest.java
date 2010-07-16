// Copyright (c) 2009-2010 Amazon.com, Inc.  All rights reserved.

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
public class TextWriterTest
    extends IonWriterTestCase
{
    private ByteArrayOutputStream myOutputStream;
    private OutputStreamWrapper myOutputStreamWrapper;
    private IonWriter myWriter;


    private class OutputStreamWrapper extends FilterOutputStream
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



    @Override
    protected IonWriter makeWriter(SymbolTable... imports)
        throws Exception
    {
        myOutputStream = new ByteArrayOutputStream();
        myOutputStreamWrapper = new OutputStreamWrapper(myOutputStream);
        myWriter = system().newTextWriter(myOutputStreamWrapper, imports);
        return myWriter;
    }

    @Override
    protected byte[] outputByteArray()
        throws Exception
    {
        myWriter.close();
        assertTrue("output stream not flushed", myOutputStreamWrapper.flushed);
        assertTrue("output stream not closed",  myOutputStreamWrapper.closed);

        byte[] utf8Bytes = myOutputStream.toByteArray();

//        String ionText = new String(utf8Bytes, "UTF-8"); // for debugging

        return utf8Bytes;
    }

    protected String outputString()
        throws Exception
    {
        byte[] utf8Bytes = outputByteArray();
        return new String(utf8Bytes, "UTF-8");
    }

    public void testNotWritingSymtab()
        throws Exception
    {
        IonWriter writer = makeWriter();
        writer.writeSymbol("holla");
        String ionText = outputString();

        if (! ionText.startsWith(UnifiedSymbolTable.ION_1_0)) {
            fail("TextWriter didn't write IVM: " + ionText);
        }

        if (ionText.contains(UnifiedSymbolTable.ION_SYMBOL_TABLE)) {
            fail("TextWriter shouldn't write symtab: " + ionText);
        }
    }
}
