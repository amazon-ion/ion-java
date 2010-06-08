// Copyright (c) 2009-2010 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import java.io.ByteArrayOutputStream;

/**
 *
 */
public class TextWriterTest
    extends IonWriterTestCase
{
    private ByteArrayOutputStream myOutputStream;


    @Override
    protected IonWriter makeWriter(SymbolTable... imports)
        throws Exception
    {
        myOutputStream = new ByteArrayOutputStream();
        return system().newTextWriter(myOutputStream, imports);
    }

    @Override
    protected byte[] outputByteArray()
        throws Exception
    {
        myOutputStream.close();
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
