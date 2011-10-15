// Copyright (c) 2009-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.SystemSymbols.ION_1_0;
import static com.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE;

import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonSequence;
import com.amazon.ion.IonStruct;
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
    private $PrivateTextOptions options;

    @Override
    protected IonWriter makeWriter(OutputStream out, SymbolTable... imports)
        throws Exception
    {
        if (options != null)
        {
            return system().newTextWriter(out, options, imports);
        }

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


    private void expectRendering(String expected, IonDatagram original)
        throws Exception
    {
        iw = makeWriter();
        original.writeTo(iw);

        assertEquals(original, reload());

        String actual = outputString();
        assertEquals(expected, actual);
    }

    @Test
    public void testWritingLongStrings()
        throws Exception
    {
        options = new $PrivateTextOptions(/*prettyPrint*/ false,
                                          /*printAscii*/ false,
                                          /*filterOutSymbolTables*/ true,
                                          /*suppressIonVersionMarker*/ true);
        options._long_string_threshold = 10;

        // TODO support long strings at datagram and sexp level?
        // That is tricky because we must avoid triple-quoting multiple
        // long strings in such a way that they'd get concatenated together!
        IonDatagram dg = system().newDatagram();
        dg.add().newNullString();
        dg.add().newString("hello");
        dg.add().newString("hello\nnurse");
        dg.add().newString("what's\nup\ndoc");

        expectRendering("null.string \"hello\" \"hello\\nnurse\" \"what's\\nup\\ndoc\"",
                        dg);

        dg.clear();
        IonSequence seq = dg.add().newEmptySexp();
        seq.add().newNullString();
        seq.add().newString("hello");
        seq.add().newString("hello\nnurse");
        seq.add().newString("what's\nup\ndoc");

        expectRendering("(null.string \"hello\" \"hello\\nnurse\" \"what's\\nup\\ndoc\")",
                        dg);

        dg.clear();
        seq = dg.add().newEmptyList();
        seq.add().newNullString();
        seq.add().newString("hello");
        seq.add().newString("hello\nnurse");
        seq.add().newString("what's\nup\ndoc");

        expectRendering("[null.string,\"hello\",'''hello\n" +
                        "nurse''','''what\\'s\n" +
                        "up\n" +
                        "doc''']",
                        dg);

        dg.clear();
        IonStruct struct = dg.add().newEmptyStruct();
        struct.add("a").newNullString();
        struct.add("b").newString("hello");
        struct.add("c").newString("hello\nnurse");
        struct.add("d").newString("what's\nup\ndoc");

        expectRendering("{a:null.string,b:\"hello\",c:'''hello\n" +
                        "nurse''',d:'''what\\'s\n" +
                        "up\n" +
                        "doc'''}",
                        dg);
    }

    @Test
    public void testWritingLongClobs()
        throws Exception
    {
        options = new $PrivateTextOptions(/*prettyPrint*/ false,
                                          /*printAscii*/ false,
                                          /*filterOutSymbolTables*/ true,
                                          /*suppressIonVersionMarker*/ true);
        options._long_string_threshold = 3;


        IonDatagram dg = system().newDatagram();
        dg.add().newClob(new byte[]{'a', 'b', '\n'});

        expectRendering("{{\"ab\\n\"}}", dg);

        dg.clear();
        dg.add().newClob(new byte[]{'a', 'b', '\n', 'c'});
        expectRendering("{{'''ab\n" +
                        "c'''}}",
                        dg);
    }
}
