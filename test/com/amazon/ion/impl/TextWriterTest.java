// Copyright (c) 2009-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.SystemSymbols.ION_1_0;
import static com.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE;

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
        return IonImplUtils.utf8(utf8Bytes);
    }

    @Test
    public void testNotWritingSymtab()
        throws Exception
    {
        iw = makeWriter();
        iw.writeSymbol("holla");
        String ionText = outputString();

        if (! ionText.startsWith(ION_1_0)) {
            fail("TextWriter didn't write IVM: " + ionText);
        }

        if (ionText.contains(ION_SYMBOL_TABLE)) {
            fail("TextWriter shouldn't write symtab: " + ionText);
        }
    }
}
