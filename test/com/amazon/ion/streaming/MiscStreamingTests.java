// Copyright (c) 2008-2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.streaming;

import com.amazon.ion.BinaryTest;
import com.amazon.ion.IonBinaryWriter;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonString;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonTestCase;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.impl.IonImplUtils;
import com.amazon.ion.system.SystemFactory;

/**
 *
 */
public class MiscStreamingTests
    extends IonTestCase
{
    static final boolean _debug_flag = false;


    //=========================================================================
    // Test cases

    // need the extra \\'s to get at least one slash to the ion parser
    static final String _QuotingString1_ion  = "s\\\"t1";
    static final String _QuotingString1_java = "s\"t1";
    static final String _QuotingString2_ion  = "s\\\'t2";  // 5 bytes
    static final String _QuotingString2_java = "s\'t2";
    public void testQuoting()
    throws Exception
    {
        String s = " \""+_QuotingString1_ion+"\" '"+_QuotingString2_ion+"' ";
        // buffer should be 22 bytes long
        //    cookie (4 bytes)
        //    local symbol table (11 bytes):
        //        annotation           (1 byte tid, len=10)
        //              annot size       (1 byte: len=1)
        //                  $ion_1_0     (1 byte sid)
        //              struct           (1 byte tid, len=7)
        //        field 'symbols'  (1 bytes sid)
        //                  List         (1 byte tid, len=5)
        //                str2     (5 bytes)
        //    value1 string "str1" (5 bytes: 1 typedesc + 4 text)
        //    value2 symbol 'str2' (2 bytes: 1 typedesc + 1 sid)
        IonReader ir = system().newReader(s);

        IonSystem system = system();
        IonBinaryWriter wr = system.newBinaryWriter();
        wr = system.newBinaryWriter();
        wr.writeValues(ir);

        byte[] buffer = wr.getBytes();
        assertSame("this buffer length is known to be 22", buffer.length, 22);

        IonReader sir = system().newReader(s);
        IonReader bir = system().newReader(s);

        checkIteratorForQuotingTest("string", sir);
        checkIteratorForQuotingTest("binary", bir);
    }
    void checkIteratorForQuotingTest(String title, IonReader ir) {
        IonType t = ir.next();
        assertTrue("first value is string for "+title, t.equals(IonType.STRING) );
        String s = ir.stringValue();
        assertTrue("checking first value "+title, s.equals( _QuotingString1_java ) );

        t = ir.next();
        assertTrue("first value is string for "+title, t.equals(IonType.SYMBOL) );
        s = ir.stringValue();
        assertTrue("checking 2nd value "+title, s.equals( _QuotingString2_java ) );
    }


    public void testValue2()
    throws Exception
    {
        String s =
            "item_view::{item_id:\"B00096H8Q4\",marketplace_id:2,"
            +"product:{item_name:["
            +"{value:'''Method 24CT Leather Wipes''',lang:EN_CA},"
            +"{value:'''Method 24CT Chiffons de Cuir''',lang:FR_CA}],"
            +"list_price:{value:18.23,unit:EUR},}"
            +",index_suppressed:true,"
            +"offline_store_only:true,version:2,}";

        IonSystem sys = SystemFactory.newSystem();
        IonDatagram dg = sys.getLoader().load(s);
        IonValue v = dg.get(0);
        IonType t = v.getType();
        assertSame( "should be a struct", IonType.STRUCT, t );

        int tree_count = ((IonStruct)v).size();

        IonReader it = system().newReader(s);

        t = it.next();
        assertSame( "should be a struct", IonType.STRUCT, t );

        int string_count = 0;
        it.stepIn();
        while (it.next() != null) {
            string_count++;
        }
        it.stepOut();
        assertSame("tree and string iterator should have the same size",
                tree_count, string_count);

        byte[] buf = dg.getBytes();
        it = system().newReader(buf);

        t = it.next();
        assertSame( "should be a struct", IonType.STRUCT, t );

        int bin_count = 0;
        it.stepIn();
        while (it.next() != null) {
            bin_count++;
        }
        it.stepOut();
        assertSame("tree and binary iterator should have the same size",
                tree_count, bin_count);
    }

    public void testBinaryAnnotation()
    throws Exception
    {
        String s =
            "item_view::{item_id:\"B00096H8Q4\",marketplace_id:2,"
            +"product:{item_name:["
            +"{value:'''Method 24CT Leather Wipes''',lang:EN_CA},"
            +"{value:'''Method 24CT Chiffons de Cuir''',lang:FR_CA}],"
            +"list_price:{value:18.23,unit:EUR},}"
            +",index_suppressed:true,"
            +"offline_store_only:true,version:2,}";

        IonSystem sys = SystemFactory.newSystem();
        IonDatagram dg = sys.getLoader().load(s);
        IonValue v = dg.get(0);
        IonType t = v.getType();
        assertSame( "should be a struct", t, IonType.STRUCT );

        // first make sure the ion tree got it right
        assertTrue(v.hasTypeAnnotation("item_view"));
        String[] ann = v.getTypeAnnotations();
        assertTrue(ann.length == 1 && ann[0].equals("item_view"));

        // now take the string and get a text iterator and
        // make sure it got the annotation right
        IonReader it = system().newReader(s);
        t = it.next();
        assertSame( "should be a struct", t, IonType.STRUCT );
        ann = it.getTypeAnnotations();
        assertTrue(ann.length == 1 && ann[0].equals("item_view"));

        // finally get the byte array from the tree, make a
        // binary iterator and check its annotation handling
        byte[] buf = dg.getBytes();
        it = system().newReader(buf);
        t = it.next();
        assertSame( "should be a struct", t, IonType.STRUCT );
        ann = it.getTypeAnnotations();
        assertTrue(ann.length == 1 && ann[0].equals("item_view"));
    }


    public void testTextNullStringValue()
    {
        IonReader reader = system().newReader("null.string");
        testNullStringValue(reader);
    }

    public void testBinaryNullStringValue()
    {
        // TODO load this from test file
        byte[] nullSymbolBytes = BinaryTest.hexToBytes(BinaryTest.MAGIC_COOKIE
                                                       + "8f");
        IonReader reader = system().newReader(nullSymbolBytes);
        testNullStringValue(reader);
    }

    public void testTreeNullStringValue()
    {
        IonString nullString = system().newNullString();
        IonReader reader = system().newReader(nullString);
        testNullStringValue(reader);
    }


    public void testTextNullSymbolValue()
    {
        IonReader reader = system().newReader("null.symbol");
        testNullSymbolValue(reader);
    }

    public void testBinaryNullSymbolValue()
    {
        // TODO load this from test file
        byte[] nullSymbolBytes = BinaryTest.hexToBytes(BinaryTest.MAGIC_COOKIE
                                                       + "7f");
        IonReader reader = system().newReader(nullSymbolBytes);
        testNullSymbolValue(reader);
    }

    public void testTreeNullSymbolValue()
    {
        IonSymbol nullSymbol = system().newNullSymbol();
        IonReader reader = system().newReader(nullSymbol);
        testNullSymbolValue(reader);
    }

    private void testNullStringValue(IonReader reader)
    {
        testNullTextValue(reader, IonType.STRING);
    }

    private void testNullSymbolValue(IonReader reader)
    {
        testNullTextValue(reader, IonType.SYMBOL);
    }

    private void testNullTextValue(IonReader reader, IonType textType)
    {
        if (! IonImplUtils.READER_HASNEXT_REMOVED) {
            assertTrue(reader.hasNext());
        }
        assertEquals(textType, reader.next());
        assertEquals(textType, reader.getType());
        assertTrue(reader.isNullValue());
        assertEquals(null, reader.stringValue());
    }
}
