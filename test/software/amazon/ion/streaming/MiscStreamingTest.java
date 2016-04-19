/*
 * Copyright 2008-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.ion.streaming;

import static software.amazon.ion.impl.PrivateUtils.utf8;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.ion.BinaryTest;
import software.amazon.ion.IonDatagram;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonString;
import software.amazon.ion.IonStruct;
import software.amazon.ion.IonSymbol;
import software.amazon.ion.IonSystem;
import software.amazon.ion.IonTestCase;
import software.amazon.ion.IonType;
import software.amazon.ion.IonValue;
import software.amazon.ion.IonWriter;
import software.amazon.ion.TestUtils;

public class MiscStreamingTest
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

    @Test
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

        byte[] buffer = writeBinaryBytes(ir);
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


    @Test
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

        IonSystem sys = system();
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

    @Test
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

        IonSystem sys = system();
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


    @Test
    public void testTextNullStringValue()
    {
        IonReader reader = system().newReader("null.string");
        testNullStringValue(reader);
    }

    @Test
    public void testBinaryNullStringValue()
    {
        // TODO load this from test file
        byte[] nullSymbolBytes = BinaryTest.hexToBytes(BinaryTest.MAGIC_COOKIE
                                                       + "8f");
        IonReader reader = system().newReader(nullSymbolBytes);
        testNullStringValue(reader);
    }

    @Test
    public void testTreeNullStringValue()
    {
        IonString nullString = system().newNullString();
        IonReader reader = system().newReader(nullString);
        testNullStringValue(reader);
    }


    @Test
    public void testTextNullSymbolValue()
    {
        IonReader reader = system().newReader("null.symbol");
        testNullSymbolValue(reader);
    }

    @Test
    public void testBinaryNullSymbolValue()
    {
        // TODO load this from test file
        byte[] nullSymbolBytes = BinaryTest.hexToBytes(BinaryTest.MAGIC_COOKIE
                                                       + "7f");
        IonReader reader = system().newReader(nullSymbolBytes);
        testNullSymbolValue(reader);
    }

    @Test
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
        assertEquals(textType, reader.next());
        assertEquals(textType, reader.getType());
        assertTrue(reader.isNullValue());
        assertEquals(null, reader.stringValue());
    }

    @Test
    public void testSkippingListWithQuotedSymbol()
    {
        IonReader reader = system().newReader("['\\']'] 2");
        assertEquals(IonType.LIST, reader.next());
        assertEquals(IonType.INT, reader.next());
        assertEquals(2, reader.intValue());
        assertEquals(null, reader.next());
    }

    @Test
    public void testSkippingOperator()
    {
        IonReader reader = system().newReader("(+()) 2");
        assertEquals(IonType.SEXP, reader.next());
        reader.stepIn();
        assertEquals(IonType.SYMBOL, reader.next());
        reader.stepOut();
        assertEquals(IonType.INT, reader.next());
        assertEquals(2, reader.intValue());
        assertEquals(null, reader.next());
    }


    @Test
    public void testReaderDataMangling()
    throws Exception
    {
        String dataText = "a/**/b";
        byte[] dataBytes = utf8(dataText);

        IonReader reader = system().newReader(dataBytes);
        TestUtils.deepRead(reader);

        // Make sure we didn't modify the dataBytes while reading from it.
        Assert.assertArrayEquals("UTF-8 text", utf8(dataText), dataBytes);
    }


    @Test
    public void testIteratorDataMangling()
    throws Exception
    {
        String dataText = "a/**/b";
        byte[] dataBytes = utf8(dataText);

        Iterator<IonValue> reader = system().iterate(dataBytes);
        while (reader.hasNext()) reader.next();

        // Make sure we didn't modify the dataBytes while reading from it.
        Assert.assertArrayEquals("UTF-8 text", utf8(dataText), dataBytes);
    }


    @Test
    public void testTextWriterSymtabs()
    throws IOException
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonWriter writer = system().newTextWriter(out);
        system().newSymbol("foo").writeTo(writer);
        writer.close();

        assertEquals("foo", utf8(out.toByteArray()));
    }
}
