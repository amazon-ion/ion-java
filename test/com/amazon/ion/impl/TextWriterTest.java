// Copyright (c) 2009-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import java.io.OutputStream;
import org.junit.Test;

/**
 *
 */
public class TextWriterTest
    extends OutputStreamWriterTestCase
{

    @Override
    protected IonWriter makeWriter(OutputStream out, SymbolTable... imports)
        throws Exception
    {
        return system().newTextWriter(out, imports);
    }

    protected String outputString()
        throws Exception
    {
        byte[] utf8Bytes = outputByteArray();
        return new String(utf8Bytes, "UTF-8");
    }

    @Test
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
