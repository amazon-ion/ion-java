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

import static java.lang.String.format;
import static software.amazon.ion.SystemSymbols.ION_1_0;
import static software.amazon.ion.system.IonWriterBuilder.InitialIvmHandling.ENSURE;
import static software.amazon.ion.system.IonWriterBuilder.InitialIvmHandling.SUPPRESS;
import static software.amazon.ion.system.IonWriterBuilder.IvmMinimizing.DISTANT;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import org.junit.Test;
import software.amazon.ion.IonDatagram;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonSequence;
import software.amazon.ion.IonStruct;
import software.amazon.ion.IonWriter;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.Symtabs;
import software.amazon.ion.SystemSymbols;
import software.amazon.ion.impl.PrivateUtils;
import software.amazon.ion.system.IonTextWriterBuilder;
import software.amazon.ion.system.IonTextWriterBuilder.LstMinimizing;
import software.amazon.ion.system.IonWriterBuilder.IvmMinimizing;

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
        return PrivateUtils.utf8(utf8Bytes);
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


    @Test
    public void testEnsureInitialIvm()
        throws Exception
    {
        options = IonTextWriterBuilder.standard();
        iw = makeWriter();
        iw.writeNull();
        String ionText = outputString();
        assertEquals("null", ionText);

        options.setInitialIvmHandling(ENSURE);
        iw = makeWriter();
        iw.writeNull();
        ionText = outputString();
        assertEquals(ION_1_0 + " null", ionText);

        iw = makeWriter();
        iw.writeSymbol(SystemSymbols.ION_1_0);
        iw.writeNull();
        ionText = outputString();
        assertEquals(ION_1_0 + " null", ionText);
    }


    @Test
    public void testIvmMinimizing()
        throws Exception
    {
        options = IonTextWriterBuilder.standard();
        iw = makeWriter();
        iw.writeSymbol(ION_1_0);
        iw.writeSymbol(ION_1_0);
        iw.writeSymbol("foo");
        iw.writeSymbol(ION_1_0);
        iw.writeSymbol(ION_1_0);
        iw.writeSymbol(ION_1_0);
        iw.writeSymbol("bar");
        iw.writeSymbol(ION_1_0);

        String ionText = outputString();
        assertEquals(ION_1_0 + " " + ION_1_0 + " foo " +
                     ION_1_0 + " " + ION_1_0 + " " + ION_1_0 + " bar " +
                     ION_1_0,
                     ionText);

        options.setIvmMinimizing(IvmMinimizing.ADJACENT);
        iw = makeWriter();
        iw.writeSymbol(ION_1_0);
        iw.writeSymbol(ION_1_0);
        iw.writeSymbol("foo");
        iw.writeSymbol(ION_1_0);
        iw.writeSymbol(ION_1_0);
        iw.writeSymbol(ION_1_0);
        iw.writeSymbol("bar");
        iw.writeSymbol(ION_1_0);

        ionText = outputString();
        assertEquals(ION_1_0 + " foo " + ION_1_0 + " bar " + ION_1_0,
                     ionText);
    }

    @Test
    public void testLstMinimizing()
        throws Exception
    {
        SymbolTable fred1 = Symtabs.register("fred",   1, catalog());

        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        IonWriter binaryWriter = system().newBinaryWriter(buf, fred1);
        binaryWriter.writeSymbol("fred_1");
        binaryWriter.writeSymbol("ginger");
        binaryWriter.finish();
        byte[] binaryData = buf.toByteArray();

        options = IonTextWriterBuilder.standard();

        // TODO User reader still transfers local symtabs!
        IonReader binaryReader = system().newReader(binaryData);
        iw = makeWriter();
        iw.writeValues(binaryReader);

        String ionText = outputString();
        assertEquals(// TODO "$ion_1_0 " +
                     "$ion_symbol_table::{imports:[{name:\"fred\",version:1,max_id:2}],"
                     +                   "symbols:[\"ginger\"]} " +
                     "fred_1 ginger",
                     ionText);

        options.setLstMinimizing(LstMinimizing.LOCALS);
        binaryReader = system().newReader(binaryData);
        iw = makeWriter();
        iw.writeValues(binaryReader);
        ionText = outputString();
        assertEquals(// TODO "$ion_1_0 " +
                     "$ion_symbol_table::{imports:[{name:\"fred\",version:1,max_id:2}]} " +
                     "fred_1 ginger",
                     ionText);

        options.setLstMinimizing(LstMinimizing.EVERYTHING);
        binaryReader = system().newReader(binaryData);
        iw = makeWriter();
        iw.writeValues(binaryReader);
        ionText = outputString();
        assertEquals(// TODO "$ion_1_0 " +
                     "$ion_1_0 fred_1 ginger",
                     ionText);

        options.setInitialIvmHandling(SUPPRESS);
        binaryReader = system().newReader(binaryData);
        iw = makeWriter();
        iw.writeValues(binaryReader);
        ionText = outputString();
        assertEquals("fred_1 ginger",
                     ionText);
    }


    @Test
    public void testC1ControlCodes()
        throws Exception
    {
        options = IonTextWriterBuilder.standard();
        options.setInitialIvmHandling(SUPPRESS);

        iw = makeWriter();
        iw.writeString("\u0080 through \u009f"); // Note Java Unicode escapes!

        assertEquals("\"\\x80 through \\x9f\"", outputString());
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
        options.setLongStringThreshold(5);

        // This is tricky because we must avoid triple-quoting multiple
        // long strings in such a way that they'd get concatenated together!
        IonDatagram dg = system().newDatagram();
        dg.add().newNullString();
        dg.add().newString("hello");
        dg.add().newString("hello\nnurse");
        dg.add().newString("goodbye").addTypeAnnotation("a");
        dg.add().newString("what's\nup\ndoc");

        expectRendering("null.string \"hello\" '''hello\nnurse''' a::'''goodbye''' \"what's\\nup\\ndoc\"",
                        dg);

        dg.clear();
        dg.add().newString("looong");
        IonSequence seq = dg.add().newEmptySexp();
        seq.add().newString("looong");
        seq.add().newString("hello");
        seq.add().newString("hello\nnurse");
        seq.add().newString("goodbye").addTypeAnnotation("a");
        seq.add().newString("what's\nup\ndoc");

        expectRendering("'''looong''' ('''looong''' \"hello\" '''hello\nnurse''' a::'''goodbye''' \"what's\\nup\\ndoc\")",
                        dg);

        dg.clear();
        dg.add().newString("looong");
        seq = dg.add().newEmptyList();
        seq.add().newString("looong");
        seq.add().newString("hello");
        seq.add().newString("hello\nnurse");
        seq.add().newString("what's\nup\ndoc");

        expectRendering("'''looong''' ['''looong''',\"hello\",'''hello\n" +
                        "nurse''','''what\\'s\n" +
                        "up\n" +
                        "doc''']",
                        dg);

        options.setLongStringThreshold(0);
        expectRendering("\"looong\" [\"looong\",\"hello\",\"hello\\nnurse\",\"what's\\nup\\ndoc\"]",
            dg);

        options.setLongStringThreshold(5);
        dg.clear();
        dg.add().newString("looong");
        IonStruct struct = dg.add().newEmptyStruct();
        struct.add("a").newString("looong");
        struct.add("b").newString("hello");
        struct.add("c").newString("hello\nnurse");
        struct.add("d").newString("what's\nup\ndoc");

        expectRendering(
            "'''looong''' {a:'''looong''',b:\"hello\",c:'''hello\n" +
            "nurse''',d:'''what\\'s\n" +
            "up\n" +
            "doc'''}",
            dg
        );

        options.withPrettyPrinting();
        expectRendering(
            // TODO amznlabs/ion-java#57 determine if these really should be platform independent newlines
            format(
                "%n" +
                "'''looong'''%n" +
                "{%n" +
                        "  a:'''looong''',%n" +
                "  b:\"hello\",%n" +
                "  c:'''hello\n" +
                        "nurse''',%n" +
                        "  d:'''what\\'s\n" +
                        "up\n" +
                        "doc'''%n" +
                        "}"
            ),
            dg
        );
    }

    @Test
    public void testJsonLongStrings()
        throws Exception
    {
        options = IonTextWriterBuilder.json();
        options.setLongStringThreshold(5);

        IonDatagram dg = system().newDatagram();
        dg.add().newString("hello");
        dg.add().newString("hello!");
        dg.add().newString("goodbye");
        dg.add().newString("what's\nup\ndoc");

        expectRendering("\"hello\" '''hello!''' \"goodbye\" '''what\\'s\nup\ndoc'''",
                        dg);
    }

    @Test
    public void testJsonSystemMinimization()
        throws Exception
    {
        SymbolTable fred1 = Symtabs.register("fred",   1, catalog());

        options = IonTextWriterBuilder.json();
        iw = makeWriter(fred1);
        iw.writeSymbol(ION_1_0);
        iw.writeSymbol("fred_1");
        iw.writeSymbol(ION_1_0);
        iw.writeSymbol(ION_1_0);
        iw.writeSymbol("fred_1");

        assertEquals("\"fred_1\" \"fred_1\"", outputString());
    }

    @Test
    public void testWritingLongClobs()
        throws Exception
    {
        options = IonTextWriterBuilder.standard();
        options.setInitialIvmHandling(SUPPRESS);
        options.setLongStringThreshold(4);


        IonDatagram dg = system().newDatagram();
        dg.add().newClob(new byte[]{'a', '"', '\'', '\n'});

        expectRendering("{{\"a\\\"'\\n\"}}", dg);

        dg.clear();
        dg.add().newClob(new byte[]{'a', '"', '\'', '\n', 'c'});
        expectRendering("{{'''a\"\\'\n" +
                        "c'''}}",
                        dg);
    }

    @Test
    public void testWritingJsonLongClobs()
        throws Exception
    {
        options = IonTextWriterBuilder.json();
        options.setInitialIvmHandling(SUPPRESS);
        options.setLongStringThreshold(4);

        // Cannot call expectRendering as the input to reload() would be Json rather than Ion
        // test 1
        IonDatagram dg = system().newDatagram();
        dg.add().newClob(new byte[]{'a', '"', '\'', '\n'});

        // expectRendering
        iw = makeWriter();
        dg.writeTo(iw);

        IonDatagram reloaded = loader().load("{{\"a\\\"'\\n\"}}");
        assertEquals(dg, reloaded);

        String actual = outputString();
        assertEquals("\"a\\\"'\\n\"", actual);

        // test 2
        dg.clear();
        dg.add().newClob(new byte[]{'a', '"', '\'', '\n', 'c', 0x7F});

        // expectRendering
        iw = makeWriter();
        dg.writeTo(iw);

        reloaded = loader().load("{{'''a\"\\'\nc\\x7f'''}}");
        assertEquals(dg, reloaded);

        actual = outputString();
        assertEquals("\"a\\\"'\\nc\\u007f\"", actual);
    }

    @Test
    public void testSuppressInitialIvm()
        throws Exception
    {
        iw = makeWriter();
        iw.writeSymbol(ION_1_0);
        iw.writeSymbol(ION_1_0);
        iw.writeNull();
        iw.writeSymbol(ION_1_0);
        iw.writeNull();

        assertEquals(ION_1_0 + " " + ION_1_0 + " null " + ION_1_0 + " null",
                     outputString());

        options = IonTextWriterBuilder.standard().withInitialIvmHandling(SUPPRESS);

        iw = makeWriter();
        iw.writeSymbol(ION_1_0);
        iw.writeSymbol(ION_1_0);
        iw.writeNull();
        iw.writeSymbol(ION_1_0);
        iw.writeNull();

        assertEquals("null " + ION_1_0 + " null", outputString());
    }

    @Test
    public void testMinimizeDistantIvm()
        throws Exception
    {
        options = IonTextWriterBuilder.standard().withIvmMinimizing(DISTANT);

        iw = makeWriter();
        iw.writeSymbol(ION_1_0);
        iw.writeSymbol(ION_1_0);
        iw.writeNull();
        iw.writeSymbol(ION_1_0);
        iw.writeSymbol(ION_1_0);
        iw.writeNull();

        assertEquals(ION_1_0 + " null null", outputString());
    }

    @Test @Override
    public void testWritingLob()
        throws Exception
    {
        super.testWritingLob();

        options = IonTextWriterBuilder.standard();
        super.testWritingLob();

        options = IonTextWriterBuilder.pretty();
        super.testWritingLob();

        options.setLongStringThreshold(2);
        super.testWritingLob();
    }

    @Test @Override
    public void testFinishDoesReset()
        throws Exception
    {
        super.testFinishDoesReset();

        options = IonTextWriterBuilder.standard().withIvmMinimizing(DISTANT);
        super.testFinishDoesReset();
    }

    @Test @Override
    public void testAnnotationNotSetToIvmOnStartOfStream()
        throws Exception
    {
        options = IonTextWriterBuilder.standard().withInitialIvmHandling(ENSURE);

        super.testAnnotationNotSetToIvmOnStartOfStream();
    }

    @Override
    protected void checkFlushedAfterTopLevelValueWritten()
    {
        checkFlushed(true);
        myOutputStreamWrapper.flushed = false;
    }
}
