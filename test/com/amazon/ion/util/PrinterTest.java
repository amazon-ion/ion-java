// Copyright (c) 2007-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.util;


import static com.amazon.ion.SystemSymbols.ION_1_0;
import static com.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE;

import com.amazon.ion.BlobTest;
import com.amazon.ion.ClobTest;
import com.amazon.ion.IntTest;
import com.amazon.ion.IonBlob;
import com.amazon.ion.IonBool;
import com.amazon.ion.IonClob;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonDecimal;
import com.amazon.ion.IonFloat;
import com.amazon.ion.IonInt;
import com.amazon.ion.IonList;
import com.amazon.ion.IonNull;
import com.amazon.ion.IonSexp;
import com.amazon.ion.IonString;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonTestCase;
import com.amazon.ion.IonTimestamp;
import com.amazon.ion.IonValue;
import com.amazon.ion.impl._Private_Utils;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class PrinterTest
    extends IonTestCase
{
    private Printer myPrinter;


    @Before @Override
    public void setUp()
        throws Exception
    {
        super.setUp();
        myPrinter = new Printer();
    }

    public IonInt int123()
    {
        return system().newInt(123);
    }


    public IonSymbol symbol(String text)
    {
        return system().newSymbol(text);
    }

    public IonSymbol symbolHello()
    {
        return symbol("hello");
    }

    public IonSymbol symbolNotEquals()
    {
        return symbol("!=");
    }

    public void checkRendering(String expected, IonValue value)
        throws Exception
    {
        StringBuilder w = new StringBuilder();
        myPrinter.print(value, w);
        assertEquals("Printer output", expected, w.toString());
    }

    public void checkNullRendering(String image, IonValue value)
        throws Exception
    {
        boolean origUntypedNulls = myPrinter.myOptions.untypedNulls;

        myPrinter.myOptions.untypedNulls = false;
        checkRendering(image, value);

        myPrinter.myOptions.untypedNulls = true;
        checkRendering("null", value);

        myPrinter.myOptions.untypedNulls = origUntypedNulls;
    }


    //=========================================================================
    // Test cases

    @Test
    public void testPrintingAnnotations()
        throws Exception
    {
        IonNull value = (IonNull) oneValue("an::$99::null");
        checkRendering("an::$99::null", value);

        value.addTypeAnnotation("+");
        value.addTypeAnnotation("\u0000");
        value.addTypeAnnotation("$99");
        checkRendering("an::$99::'+'::'\\0'::'$99'::null", value);

        myPrinter.setPrintSymbolAsString(true);
        checkRendering("an::$99::'+'::'\\0'::'$99'::null", value);
        myPrinter.setPrintStringAsJson(true);
        checkRendering("an::$99::'+'::'\\0'::'$99'::null", value);
        myPrinter.setPrintSymbolAsString(false);
        myPrinter.setPrintStringAsJson(false);

        myPrinter.setSkipAnnotations(true);
        checkRendering("null", value);
        myPrinter.setSkipAnnotations(false);

        IonSexp s = system().newEmptySexp();
        s.add(value);
        checkRendering("(an::$99::'+'::'\\0'::'$99'::null)", s);
        myPrinter.setPrintSymbolAsString(true);
        checkRendering("(an::$99::'+'::'\\0'::'$99'::null)", s);
        myPrinter.setPrintStringAsJson(true);
        checkRendering("(an::$99::'+'::'\\0'::'$99'::null)", s);

        value.setTypeAnnotations("boo", "boo");
        checkRendering("boo::boo::null", value);
    }


    @Test
    public void testPrintingBlob()
        throws Exception
    {
        IonBlob value = system().newNullBlob();
        checkNullRendering("null.blob", value);

        for (int i = 0; i < BlobTest.TEST_DATA.length; i++)
        {
            BlobTest.TestData td = BlobTest.TEST_DATA[i];
            value.setBytes(td.bytes);

            myPrinter.setPrintBlobAsString(true);
            checkRendering("\"" + td.base64 + "\"", value);

            myPrinter.setPrintBlobAsString(false);
            checkRendering("{{" + td.base64 + "}}", value);
        }

        value = (IonBlob) oneValue("{{}}");
        checkRendering("{{}}", value);

        value.addTypeAnnotation("an");
        checkRendering("an::{{}}", value);
    }


    @Test
    public void testPrintingBool()
        throws Exception
    {
        IonBool value = system().newNullBool();
        checkNullRendering("null.bool", value);

        value.setValue(true);
        checkRendering("true", value);

        value.setValue(false);
        checkRendering("false", value);

        value.addTypeAnnotation("an");
        checkRendering("an::false", value);
    }


    @Test
    public void testPrintingClob()
        throws Exception
    {
        IonClob value = system().newNullClob();
        checkNullRendering("null.clob", value);

        value.setBytes(ClobTest.SAMPLE_ASCII_AS_UTF8);
        checkRendering("{{\"" + ClobTest.SAMPLE_ASCII + "\"}}", value);

        // TODO test "real" UTF8 and other encodings.

        value = (IonClob) oneValue("{{\"\"}}");
        checkRendering("{{\"\"}}", value);

        value.addTypeAnnotation("an");
        checkRendering("an::{{\"\"}}", value);

        myPrinter.setPrintClobAsString(true);
        checkRendering("an::\"\"", value);

        value.clearTypeAnnotations();
        value.setBytes(ClobTest.SAMPLE_ASCII_AS_UTF8);
        checkRendering("\"" + ClobTest.SAMPLE_ASCII + "\"", value);

        value = (IonClob) oneValue("{{'''Ab\\0'''}}");
        myPrinter.setPrintClobAsString(false);
        checkRendering("{{\"Ab\\0\"}}", value);
        myPrinter.setPrintClobAsString(true);
        checkRendering("\"Ab\\0\"", value);
        myPrinter.setPrintStringAsJson(true);
        checkRendering("\"Ab\\u0000\"", value);
    }


    @Test
    public void testPrintingDatagram()
        throws Exception
    {
        IonDatagram dg = loader().load("a b c");
        StringBuilder w = new StringBuilder();
        myPrinter.print(dg, w);
        String text = w.toString();
        assertTrue("missing version marker",
                   text.startsWith(ION_1_0 + ' '));
        assertTrue("missing data",
                   text.endsWith(" a b c"));

        // Just force symtab analysis and make sure output is still okay
        dg.getBytes(new byte[dg.byteSize()]);
        text = w.toString();
        assertTrue("missing version marker",
                   text.startsWith(ION_1_0 + ' '));
        assertTrue("missing data",
                   text.endsWith(" a b c"));

        // We shouldn't jnject a local table if its not needed.

        // TODO ION-165
        if (getDomType() == DomType.LITE)
        {
            // This is a hack to make the lite dom work like the original.
            // It's hiding some uglyness, disable to see.
            myPrinter.myOptions.simplifySystemValues = true;
        }

        String data = "2 '+' [2,'+']";
        String dataWithIvm = ION_1_0 + ' ' + data;
        dg = loader().load(dataWithIvm);
        checkRendering(dataWithIvm, dg);

        myPrinter.setSkipSystemValues(true);
        checkRendering(data, dg);

        myPrinter.setPrintDatagramAsList(true);
        checkRendering("[2,'+',[2,'+']]", dg);

        myPrinter.setPrintDatagramAsList(false);
        myPrinter.setSkipSystemValues(false);
        myPrinter.setJsonMode();
        checkRendering("[2,\"+\",[2,\"+\"]]", dg);
    }

    @Test
    public void testDatagramWithoutSymbols()
    throws Exception
    {
        IonDatagram dg = system().newDatagram();
        dg.add().newInt(1);
        checkRendering(ION_1_0 + " 1", dg);
    }

    @Test
    public void testSimplifyingChainedLocalSymtab()
    throws Exception
    {
        myPrinter.myOptions.simplifySystemValues = true;

        String ionText =
            ION_SYMBOL_TABLE + "::{}"
            + " x"
            + " " + ION_SYMBOL_TABLE + "::{}"
            + " y";
        IonDatagram dg = loader().load(ionText);
        checkRendering(ION_1_0 + " x y", dg);
    }

    @Test
    public void testPrintingDecimal()
        throws Exception
    {
        IonDecimal value = system().newNullDecimal();
        checkNullRendering("null.decimal", value);

        value.setValue(-123);
        checkRendering("-123.", value);

        value.setValue(456);
        checkRendering("456.", value);

        value.setValue(0);
        checkRendering("0.", value);

        value.addTypeAnnotation("an");
        checkRendering("an::0.", value);

        value = (IonDecimal) oneValue("0d42");
        checkRendering("0d42", value);

        value = (IonDecimal) oneValue("0d+42");
        checkRendering("0d42", value);

        value = (IonDecimal) oneValue("0d-42");
        checkRendering("0d-42", value);

        value = (IonDecimal) oneValue("100d-1");
        checkRendering("10.0", value);

        value = (IonDecimal) oneValue("100d3");
        checkRendering("100d3", value);

        myPrinter.setPrintDecimalAsFloat(true);
        checkRendering("100e3", value);
    }


    @Test
    public void testPrintingFloat()
        throws Exception
    {
        IonFloat value = system().newNullFloat();
        checkNullRendering("null.float", value);

        value.setValue(-123);
        checkRendering("-123e0", value);

        value.setValue(456);
        checkRendering("456e0", value);

        value.setValue(0);
        checkRendering("0e0", value);

        value.addTypeAnnotation("an");
        checkRendering("an::0e0", value);

        value = (IonFloat) oneValue("1e4");
        // TODO this prints as 10000e0 which is less than ideal.
//      checkRendering("1e4", value);

        value = (IonFloat) oneValue("1e+4");
        // TODO this prints as 10000e0 which is less than ideal.
//      checkRendering("1e4", value);

        value = (IonFloat) oneValue("125e-2");
        checkRendering("125e-2", value);
    }


    @Test
    public void testPrintingInt()
        throws Exception
    {
        IonInt value = system().newNullInt();
        checkNullRendering("null.int", value);

        value.setValue(-123);
        checkRendering("-123", value);

        value.setValue(456);
        checkRendering("456", value);

        value.setValue(IntTest.A_LONG_INT);
        checkRendering(Long.toString(IntTest.A_LONG_INT), value);

        value.setValue(0);
        checkRendering("0", value);

        value.addTypeAnnotation("an");
        checkRendering("an::0", value);
    }


    @Test
    public void testPrintingList()
        throws Exception
    {
        IonList value = system().newNullList();
        checkNullRendering("null.list", value);

        value.add(system().newNull());
        checkRendering("[null]", value);

        value.add(int123());
        checkRendering("[null,123]", value);

        value.add(symbolNotEquals());
        value.add(symbolHello());
        value.add(symbol("null"));
        checkRendering("[null,123,'!=',hello,'null']", value);

        value = (IonList) oneValue("[]");
        checkRendering("[]", value);

        value.addTypeAnnotation("an");
        checkRendering("an::[]", value);
    }


    @Test
    public void testPrintingNull()
        throws Exception
    {
        IonNull value = system().newNull();
        checkNullRendering("null", value);

        value.addTypeAnnotation("an");
        checkRendering("an::null", value);
    }


    @Test
    public void testPrintingSexp()
        throws Exception
    {
        IonSexp value = system().newNullSexp();
        checkNullRendering("null.sexp", value);

        value.add(system().newNull());
        checkRendering("(null)", value);

        value.add(int123());
        checkRendering("(null 123)", value);

        value.add(symbolNotEquals());
        value.add(symbolHello());
        value.add(symbol("null"));
        checkRendering("(null 123 != hello 'null')", value);

        myPrinter.setPrintSexpAsList(true);
        checkRendering("[null,123,'!=',hello,'null']", value);
        myPrinter.setPrintSymbolAsString(true);
        checkRendering("[null,123,\"!=\",\"hello\",\"null\"]", value);

        value = (IonSexp) oneValue("()");
        checkRendering("[]", value);
        myPrinter.setPrintSexpAsList(false);
        checkRendering("()", value);

        myPrinter.setPrintSymbolAsString(false);
        value.addTypeAnnotation("an");
        checkRendering("an::()", value);
    }


    @Test
    public void testPrintingString()
        throws Exception
    {
        IonString value = system().newNullString();
        checkNullRendering("null.string", value);

        value.setValue("Adam E");
        checkRendering("\"Adam E\"", value);

        value.setValue("Oh, \"Hello!\"");
        checkRendering("\"Oh, \\\"Hello!\\\"\"", value);

        value.addTypeAnnotation("an");
        checkRendering("an::\"Oh, \\\"Hello!\\\"\"", value);

        value = system().newString("Ab\u0000");
        checkRendering("\"Ab\\0\"", value);
        myPrinter.setPrintStringAsJson(true);
        checkRendering("\"Ab\\u0000\"", value);

        // TODO check escaping
    }


    @Test
    public void testPrintingStruct()
        throws Exception
    {
        IonStruct value = system().newNullStruct();
        checkNullRendering("null.struct", value);

        value.put("foo", system().newNull());
        checkRendering("{foo:null}", value);

        // TODO this is too strict, order shouldn't matter.
        value.put("123", system().newNull());
        checkRendering("{foo:null,'123':null}", value);

        value.add("foo", int123());
        checkRendering("{foo:null,'123':null,foo:123}", value);

        myPrinter.setPrintSymbolAsString(true);
        checkRendering("{\"foo\":null,\"123\":null,\"foo\":123}", value);

        value = (IonStruct) oneValue("{}");
        checkRendering("{}", value);

        value.addTypeAnnotation("an");
        checkRendering("an::{}", value);


        value = (IonStruct) oneValue("an::{$99:null}");
        value.addTypeAnnotation("\u0007");
        value.put("A\u0000", system().newInt(12));
        checkRendering("an::'\\a'::{\"$99\":null,\"A\\0\":12}", value);
        myPrinter.setPrintStringAsJson(true);
        checkRendering("an::'\\a'::{\"$99\":null,\"A\\u0000\":12}", value);
    }


    @Test
    public void testPrintingSymbol()
        throws Exception
    {
        IonSymbol value = system().newNullSymbol();
        checkNullRendering("null.symbol", value);

        value.setValue("Adam E");
        checkRendering("'Adam E'", value);

        // Symbols that look like keywords.
        value.setValue("null");
        checkRendering("'null'", value);
        value.setValue("true");
        checkRendering("'true'", value);
        value.setValue("false");
        checkRendering("'false'", value);
        value.setValue("null.int");
        checkRendering("'null.int'", value);

        // Operators standalone
        value.setValue("%");
        checkRendering("'%'", value);

        value.setValue("Oh, \"Hello!\"");
        checkRendering("'Oh, \"Hello!\"'", value);
        // not: checkRendering("'Oh, \\\"Hello!\\\"'", value);

        myPrinter.setPrintSymbolAsString(true);
        checkRendering("\"Oh, \\\"Hello!\\\"\"", value);
        myPrinter.setPrintSymbolAsString(false);

        value.setValue("Oh, 'Hello there!'");
        checkRendering("'Oh, \\\'Hello there!\\\''", value);

        myPrinter.setPrintSymbolAsString(true);
        checkRendering("\"Oh, 'Hello there!'\"", value);
        myPrinter.setPrintSymbolAsString(false);

        value.addTypeAnnotation("an");
        checkRendering("an::'Oh, \\\'Hello there!\\\''", value);

        // TODO check escaping

        value = system().newSymbol("Ab\u0000");
        checkRendering("'Ab\\0'", value);
        myPrinter.setPrintSymbolAsString(true);
        checkRendering("\"Ab\\0\"", value);
        myPrinter.setPrintStringAsJson(true);
        checkRendering("\"Ab\\u0000\"", value);
        myPrinter.setPrintSymbolAsString(false);
        checkRendering("'Ab\\0'", value);

        value = system().newSymbol("$99"); // Known, sidlike text
        checkRendering("'$99'", value);
        myPrinter.setPrintSymbolAsString(true);
        checkRendering("\"$99\"", value);
        myPrinter.setPrintStringAsJson(true);
        checkRendering("\"$99\"", value);
        myPrinter.setPrintSymbolAsString(false);
        checkRendering("'$99'", value);

        value = (IonSymbol) system().singleValue("$99");  // Unknown symbol
        checkRendering("$99", value);
        myPrinter.setPrintSymbolAsString(true);
        checkRendering("\"$99\"", value);
        myPrinter.setPrintStringAsJson(true);
        checkRendering("\"$99\"", value);
        myPrinter.setPrintSymbolAsString(false);
        checkRendering("$99", value);
    }

    @Test
    public void testPrintingSidlikeSymbol()
        throws Exception
    {
        IonSymbol value = system().newSymbol("$");
        checkRendering("$", value);

        value = system().newSymbol("$0");
        checkRendering("'$0'", value);

        value = system().newSymbol("$99");
        checkRendering("'$99'", value);
    }

    // TODO annotations
    // TODO field names


    @Test
    public void testPrintingTimestamp()
        throws Exception
    {
        IonTimestamp value = system().newNullTimestamp();
        checkNullRendering("null.timestamp", value);

        value = (IonTimestamp) oneValue("2007-05-15T18:45-00:00");
        checkRendering("2007-05-15T18:45-00:00", value);

        value = (IonTimestamp) oneValue("2007-05-15T18:45Z");
        checkRendering("2007-05-15T18:45Z", value);

        // offset +0 shortens to Z
        value = (IonTimestamp) oneValue("2007-05-15T18:45+00:00");
        checkRendering("2007-05-15T18:45Z", value);

        value = (IonTimestamp) oneValue("2007-05-15T18:45+01:12");
        checkRendering("2007-05-15T18:45+01:12", value);

        value = (IonTimestamp) oneValue("2007-05-15T18:45-10:01");
        checkRendering("2007-05-15T18:45-10:01", value);

        value.addTypeAnnotation("an");
        checkRendering("an::2007-05-15T18:45-10:01", value);

        myPrinter.setPrintTimestampAsString(true);
        checkRendering("an::\"2007-05-15T18:45-10:01\"", value);

        myPrinter.setJsonMode();
        checkRendering("" + value.getMillis(), value);

        // TODO test printTimestampAsMillis
    }

    private static final String Q = "\"";

    @Test
    public void testJsonEscapes()
    throws Exception
    {
        // ION-101
        String ionEscapes =
            Q + "\\0\\a\\b\\t\\n\\f\\r\\v\\\"\\'\\?\\\\\\/\\\n" + Q;
        String jsonEscapes =
            Q + "\\u0000\\u0007\\b\\t\\n\\f\\r\\u000b\\\"'?\\\\/" + Q;

        IonString value = (IonString) system().singleValue(ionEscapes);

        myPrinter.setJsonMode();
        checkRendering(jsonEscapes, value);
    }


    @Test
    public void testJsonEscapeNonBmp() throws Exception {
        // JIRA ION-33
        // JIRA ION-64
        final String literal = new StringBuilder()
            .append("'''")
            .append('\uDAF7')
            .append('\uDE56')
            .append("'''")
            .toString();

        final byte[] utf8Bytes = _Private_Utils.utf8(literal);

        final IonDatagram dg = loader().load(utf8Bytes);
        final StringBuilder out = new StringBuilder();
        final Printer json = new Printer();
        json.setJsonMode();
        json.print(dg.get(0), out);

        assertEquals(
            "\"\\uDAF7\\uDE56\"".toLowerCase(),
            out.toString().toLowerCase()
        );
    }
}
