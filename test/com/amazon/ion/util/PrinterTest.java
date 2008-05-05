/*
 * Copyright (c) 2007-2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.util;


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

/**
 *
 */
public class PrinterTest
    extends IonTestCase
{
    private Printer myPrinter;


    @Override
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
        assertEquals(expected, w.toString());
    }


    //=========================================================================
    // Test cases

    public void testPrintingAnnotations()
        throws Exception
    {
        IonNull value = (IonNull) oneValue("an::null");
        checkRendering("an::null", value);

        value.addTypeAnnotation("+");
        value.addTypeAnnotation("\u0000");
        checkRendering("an::'+'::'\\0'::null", value);

        myPrinter.setPrintSymbolAsString(true);
        checkRendering("\"an\"::\"+\"::\"\\0\"::null", value);
        myPrinter.setPrintStringAsJson(true);
        checkRendering("\"an\"::\"+\"::\"\\u0000\"::null", value);
        myPrinter.setPrintSymbolAsString(false);
        myPrinter.setPrintStringAsJson(false);

        myPrinter.setSkipAnnotations(true);
        checkRendering("null", value);
        myPrinter.setSkipAnnotations(false);

        IonSexp s = system().newEmptySexp();
        s.add(value);
        checkRendering("(an::+::'\\0'::null)", s);
        myPrinter.setPrintSymbolAsString(true);
        checkRendering("(\"an\"::\"+\"::\"\\0\"::null)", s);
        myPrinter.setPrintStringAsJson(true);
        checkRendering("(\"an\"::\"+\"::\"\\u0000\"::null)", s);
    }


    public void testPrintingBlob()
        throws Exception
    {
        IonBlob value = system().newNullBlob();
        checkRendering("null.blob", value);

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


    public void testPrintingBool()
        throws Exception
    {
        IonBool value = system().newNullBool();
        checkRendering("null.bool", value);

        value.setValue(true);
        checkRendering("true", value);

        value.setValue(false);
        checkRendering("false", value);

        value.addTypeAnnotation("an");
        checkRendering("an::false", value);
    }


    public void testPrintingClob()
        throws Exception
    {
        IonClob value = system().newNullClob();
        checkRendering("null.clob", value);

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


    public void testPrintingDatagram()
        throws Exception
    {
        String text = "a b c";
        IonDatagram dg = loader().load(text);
        checkRendering(text, dg);

//        text = "$ion_1_0 a + [a,'+']";
//        dg = loader.load(text);
//        checkRendering(text, dg);
    }


    public void testPrintingDecimal()
        throws Exception
    {
        IonDecimal value = system().newNullDecimal();
        checkRendering("null.decimal", value);

        value.setValue(-123);
        checkRendering("-123d0", value);

        value.setValue(456);
        checkRendering("456d0", value);

        value.setValue(0);
        checkRendering("0d0", value);

        value.addTypeAnnotation("an");
        checkRendering("an::0d0", value);

        value = (IonDecimal) oneValue("0d42");
        checkRendering("0d42", value);

        value = (IonDecimal) oneValue("0d+42");
        checkRendering("0d42", value);

        value = (IonDecimal) oneValue("0d-42");
        checkRendering("0d-42", value);

        value = (IonDecimal) oneValue("100d-1");
        checkRendering("100d-1", value);

        value = (IonDecimal) oneValue("100d3");
        checkRendering("100d3", value);

        myPrinter.setPrintDecimalAsFloat(true);
        checkRendering("100e3", value);
    }


    public void testPrintingFloat()
        throws Exception
    {
        IonFloat value = system().newNullFloat();
        checkRendering("null.float", value);

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


    public void testPrintingInt()
        throws Exception
    {
        IonInt value = system().newNullInt();
        checkRendering("null.int", value);

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


    public void testPrintingList()
        throws Exception
    {
        IonList value = system().newNullList();
        checkRendering("null.list", value);

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


    public void testPrintingNull()
        throws Exception
    {
        IonNull value = system().newNull();
        checkRendering("null", value);

        value.addTypeAnnotation("an");
        checkRendering("an::null", value);
    }


    public void testPrintingSexp()
        throws Exception
    {
        IonSexp value = system().newNullSexp();
        checkRendering("null.sexp", value);

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


    public void testPrintingString()
        throws Exception
    {
        IonString value = system().newNullString();
        checkRendering("null.string", value);

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


    public void testPrintingStruct()
        throws Exception
    {
        IonStruct value = system().newNullStruct();
        checkRendering("null.struct", value);

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
        checkRendering("\"an\"::{}", value);

        value.addTypeAnnotation("\u0007");
        value.put("A\u0000", system().newInt(12));
        checkRendering("\"an\"::\"\\a\"::{\"A\\0\":12}", value);
        myPrinter.setPrintStringAsJson(true);
        checkRendering("\"an\"::\"\\u0007\"::{\"A\\u0000\":12}", value);
    }


    public void testPrintingSymbol()
        throws Exception
    {
        IonSymbol value = system().newNullSymbol();
        checkRendering("null.symbol", value);

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
    }


    public void testPrintingTimestamp()
        throws Exception
    {
        IonTimestamp value = system().newNullTimestamp();
        checkRendering("null.timestamp", value);

        value = (IonTimestamp) oneValue("2007-05-15T18:45-00:00");
        checkRendering("2007-05-15T18:45:00.000-00:00", value);

        value = (IonTimestamp) oneValue("2007-05-15T18:45Z");
        checkRendering("2007-05-15T18:45:00.000Z", value);

        // offset +0 shortens to Z
        value = (IonTimestamp) oneValue("2007-05-15T18:45+00:00");
        checkRendering("2007-05-15T18:45:00.000Z", value);

        value = (IonTimestamp) oneValue("2007-05-15T18:45+01:12");
        checkRendering("2007-05-15T18:45:00.000+01:12", value);

        value = (IonTimestamp) oneValue("2007-05-15T18:45-10:01");
        checkRendering("2007-05-15T18:45:00.000-10:01", value);

        value.addTypeAnnotation("an");
        checkRendering("an::2007-05-15T18:45:00.000-10:01", value);

        myPrinter.setPrintTimestampAsString(true);
        checkRendering("an::\"2007-05-15T18:45:00.000-10:01\"", value);

        myPrinter.setJsonMode();
        checkRendering("" + value.getMillis(), value);

        // TODO test printTimestampAsMillis
    }
}
