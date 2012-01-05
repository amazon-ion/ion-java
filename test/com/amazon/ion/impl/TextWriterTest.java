// Copyright (c) 2009-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.SystemSymbols.ION_1_0;
import static com.amazon.ion.system.IonWriterBuilder.InitialIvmHandling.SUPPRESS;

import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonSequence;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.system.IonTextWriterBuilder;
import java.io.OutputStream;
import org.junit.Test;

/**
 *
 */
public class TextWriterTest
    extends OutputStreamWriterTestCase
{
    private IonTextWriterBuilder options;

    @Override
    protected IonWriter makeWriter(OutputStream out, SymbolTable... imports)
        throws Exception
    {
        myOutputForm = OutputForm.TEXT;

        if (options != null) return options.withImports(imports).build(out);

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

        assertEquals("holla", ionText);
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
        options = IonTextWriterBuilder.standard();
        options.setInitialIvmHandling(SUPPRESS);
        options.setLongStringThreshold(10);

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

        options.setLongStringThreshold(0);
        expectRendering("[null.string,\"hello\",\"hello\\nnurse\",\"what's\\nup\\ndoc\"]",
            dg);

        options.setLongStringThreshold(10);
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
        options = IonTextWriterBuilder.standard();
        options.setInitialIvmHandling(SUPPRESS);
        options.setLongStringThreshold(3);


        IonDatagram dg = system().newDatagram();
        dg.add().newClob(new byte[]{'a', 'b', '\n'});

        expectRendering("{{\"ab\\n\"}}", dg);

        dg.clear();
        dg.add().newClob(new byte[]{'a', 'b', '\n', 'c'});
        expectRendering("{{'''ab\n" +
                        "c'''}}",
                        dg);
    }

    @Test
    public void testSuppressInitialIvm()
        throws Exception
    {
        iw = makeWriter();
        iw.writeSymbol(ION_1_0);
        iw.writeNull();
        iw.writeSymbol(ION_1_0);
        iw.writeNull();

        assertEquals(ION_1_0 + " null " + ION_1_0 + " null", outputString());

        options = IonTextWriterBuilder.standard().withInitialIvmHandling(SUPPRESS);

        iw = makeWriter();
        iw.writeSymbol(ION_1_0);
        iw.writeNull();
        iw.writeSymbol(ION_1_0);
        iw.writeNull();

        assertEquals("null " + ION_1_0 + " null", outputString());
    }
}
