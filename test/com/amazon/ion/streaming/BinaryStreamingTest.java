/*
 * Copyright (c) 2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.streaming;

import com.amazon.ion.IonTestCase;
import com.amazon.ion.IonType;
import com.amazon.ion.impl.IonTokenReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.Date;

/**
 *
 */
public class BinaryStreamingTest
    extends IonTestCase
{
        @Override
        public void setUp()
            throws Exception
        {
            super.setUp();
        }
        
        static boolean bytesEqual(byte[] v1, byte[] v2) {
            if (v1 == null || v2 == null) {
                return v1 == v2;
            }
            if (v1.length != v2.length) {
                return false;
            }
            for (int ii=0; ii<v1.length; ii++) {
                if (v1[ii] != v2[ii]) return false;
            }
            return true;
        }


        //=========================================================================
        // Test cases
        
        static class TestValue {
            String  name;
            IonType itype;
            Object  value;
            TestValue(String tv_name, IonType tv_type, Object tv_value) {
                name = tv_name;
                itype = tv_type;
                value = tv_value;
            }
            static MathContext context = MathContext.DECIMAL128;
            static BigDecimal makeBigDecimal(double d) {
                BigDecimal bd = new BigDecimal(d, context);
                
                int scale = bd.scale();
                BigInteger bi = bd.unscaledValue();
                
                // there are only 52 significant bits in a double value, so we're going to round
                if (bi.bitLength() > 52) {
                    if (scale != scale) throw new NumberFormatException();
                }
                
                return bd;
            }
            static BigDecimal makeBigDecimal(String v) {
                BigDecimal bd = new BigDecimal(v, context);
                
                int scale = bd.scale();
                BigInteger bi = bd.unscaledValue();
                
                // there are only 52 significant bits in a double value, so we're going to round
                if (bi.bitLength() > 52 || scale != scale) {
                    throw new NumberFormatException();
                }
                
                return bd;
            }
            void writeValue(IonWriter wr) throws IOException {
                
                if (name.equals("clob_1") || name.equals("clob_2")) {
                    assertTrue( true );
                }

                wr.writeFieldname(name);
                switch (itype) {
                    case NULL:
                        if (value == null) {
                            wr.writeNull();
                        }
                        else {
                            wr.writeNull((IonType)value);
                        }
                        break;
                    case BOOL:
                        wr.writeBool((Boolean)value);
                        break;
                    case INT:
                        if (value instanceof Byte) {
                            wr.writeInt((Byte)value);
                        }
                        else if (value instanceof Short) {
                            wr.writeInt((Short)value);
                        }
                        else if (value instanceof Integer) {
                            wr.writeInt((Integer)value);
                        }
                        else if (value instanceof Long) {
                            wr.writeInt((Long)value);
                        }
                        else {
                            throw new IllegalStateException("we only write Byte, Short, Integer, or Long to an IonInt");
                        }
                        break;
                    case FLOAT:
                        if (value instanceof Float) {
                            wr.writeFloat((Float)value);
                        }
                        else if (value instanceof Double) {
                            wr.writeFloat((Double)value);
                        }
                        else {
                            throw new IllegalStateException("we only write Float or Double to an IonFloat");
                        }
                        break;
                    case DECIMAL:
                        if (value instanceof Double) {
                            BigDecimal bd = makeBigDecimal((Double)value);
                            wr.writeDecimal(bd);
                        }
                        else if (value instanceof BigDecimal) {
                            wr.writeDecimal((BigDecimal)value);
                        }
                        else {
                            throw new IllegalStateException("we only write Double and BigDecimal to an IonDecimal");
                        }
                        break;
                    case TIMESTAMP:
                        if (value instanceof Date) {
                            wr.writeTimestampUTC((Date)value);
                        }
                        else if (value instanceof String) {
                            IonTokenReader.Type.timeinfo ti = 
                                IonTokenReader.Type.timeinfo.parse((String)value);
                            wr.writeTimestamp(ti.d, ti.localOffset);
                        }
                        else if (value instanceof IonTokenReader.Type.timeinfo) {
                            IonTokenReader.Type.timeinfo ti = (IonTokenReader.Type.timeinfo)value;
                            if (ti.localOffset == null) {
                                wr.writeTimestampUTC(ti.d);
                            }
                            else {
                                wr.writeTimestamp(ti.d, ti.localOffset);
                            }
                        }
                        else {
                            throw new IllegalStateException("we only write Date (as is), a timeinfo or a String (parsed to timeinfo) to an IonTimestamp");
                        }
                        break;
                    case STRING:
                        if (value instanceof String) {
                            wr.writeString((String)value);
                        }
                        else {
                            throw new IllegalStateException("we only write String to an IonString");
                        }
                        break;
                    case SYMBOL:
                        if (value instanceof String) {
                            wr.writeSymbol((String)value);
                        }
                        else if (value instanceof Integer) {
                            wr.writeSymbol((Integer)value);
                        }
                        else {
                            throw new IllegalStateException("we only write String or Integer (a symbol id) to an IonSymbol");
                        }
                        break;
                    case BLOB:
                        if (value instanceof byte[]) {
                            wr.writeBlob((byte[])value);
                        }
                        else {
                            throw new IllegalStateException("we only write byte arrays ( byte[] ) to an IonBlob");
                        }
                        break;                  
                    case CLOB:
                        if (value instanceof byte[]) {
                            wr.writeClob((byte[])value);
                        }
                        else {
                            throw new IllegalStateException("we only write byte arrays ( byte[] ) to an IonClob");
                        }
                        break;                  
                    case STRUCT:
                    case LIST:
                    case SEXP:
                        throw new IllegalStateException("the value writer can't write complex types");
                }
                return;
            }
            void readAndTestValue(IonIterator r) throws IOException {
                
                IonType t = r.next();
                String fieldname = r.getFieldName();
                if (name.equals("bdd_d") || name.equals("bdd_c")) {
                    assertTrue( true );
                }
                
                assertTrue( name.equals( fieldname ) );
                if ( itype.equals(IonType.NULL) ) {
                    // nulls are typed so we test them differently
                    if (value != null) {
                        assertTrue( t.equals( value ) );
                    }
                    else {
                        assertTrue( t.equals( IonType.NULL) );
                    }
                }
                else {
                    assertTrue( itype.equals( t ) );
                }
                
                switch (itype) {
                    case NULL:
                        break;
                    case BOOL:
                        assertTrue( ((Boolean)value).equals(r.getBool()));
                        break;
                    case INT:
                        if (value instanceof Byte) {
                            assertTrue( ((Byte)value).equals(r.getInt()) );
                        }
                        else if (value instanceof Short) {
                            assertTrue( ((Short)value).equals(r.getInt()) );
                        }
                        else if (value instanceof Integer) {
                            assertTrue( ((Integer)value).equals(r.getInt()) );
                        }
                        else if (value instanceof Long) {
                            assertTrue( ((Long)value).equals(r.getLong()) );
                        }
                        else {
                            throw new IllegalStateException("we only test Byte, Short, Integer, or Long values against an IonInt");
                        }
                        break;
                    case FLOAT:
                        if (value instanceof Float) {
                            double d = r.getDouble();
                            assertTrue( ((Float)value).equals( (float)d ) );
                        }
                        else if (value instanceof Double) {
                            double d = r.getDouble();
                            assertTrue( ((Double)value).equals( d ) );
                        }
                        else {
                            throw new IllegalStateException("we only write Float or Double to an IonFloat");
                        }
                        break;
                    case DECIMAL:
                        BigDecimal bd1 = null;
                            BigDecimal bd2 = r.getBigDecimal();
                        if (value instanceof Double) {
                            bd1 = makeBigDecimal((Double)value);
                        }
                        else if (value instanceof BigDecimal) {
                            bd1 = (BigDecimal)value;
                        }
                        else {
                            throw new IllegalStateException("we only write Double and BigDecimal to an IonDecimal");
                        }
                        assertTrue( bd1.equals(bd2) );
                        break;
                    case TIMESTAMP:
                        IonTokenReader.Type.timeinfo ti1 = r.getTimestamp();

                        if (value instanceof Date) {
                            assertTrue( ((Date)value).equals(ti1.d) );
                        }
                        else if (value instanceof String) {
                            IonTokenReader.Type.timeinfo ti2 = 
                                IonTokenReader.Type.timeinfo.parse((String)value);
                            assertTrue( ti1.d.equals(ti2.d) );
                            assertTrue( ti1.localOffset == ti2.localOffset );
                        }
                        else if (value instanceof IonTokenReader.Type.timeinfo) {
                            IonTokenReader.Type.timeinfo ti2 = (IonTokenReader.Type.timeinfo)value;
                            assertTrue( ti1.d.equals(ti2.d) );
                            if (ti1.localOffset == null || ti2.localOffset == null) {
                                assertTrue(ti1.localOffset == ti2.localOffset);
                            }
                            else {
                                assertTrue( ti1.localOffset.equals(ti2.localOffset));
                            }
                        }
                        else {
                            throw new IllegalStateException("we only write Date (as is), a timeinfo, or a String (parsed to timeinfo) to an IonTimestamp");
                        }
                        break;
                    case STRING:
                        if (value instanceof String) {
                            assertTrue( ((String)value).equals(r.getString()) );
                        }
                        else {
                            throw new IllegalStateException("we only write String to an IonString");
                        }
                        break;
                    case SYMBOL:
                        if (value instanceof String) {
                            assertTrue( ((String)value).equals( r.getString()) );
                        }
                        else if (value instanceof Integer) {
                            assertTrue( ((Integer)value).equals( r.getSymbolId()) );
                        }
                        else {
                            throw new IllegalStateException("we only write String or Integer (a symbol id) to an IonSymbol");
                        }
                        break;
                    case BLOB:
                        
                        if (value instanceof byte[]) {
                            byte[] b1 = (byte[])value;
                            byte[] b2 = r.getBytes();
                            assertTrue( bytesEqual(b1, b2) );
                        }
                        else {
                            throw new IllegalStateException("we only write byte arrays ( byte[] ) to an IonBlob");
                        }
                        break;                  
                    case CLOB:
                        if (value instanceof byte[]) {
                            byte[] c1 = (byte[])value;
                            byte[] c2 = r.getBytes();
                            assertTrue( bytesEqual(c1, c2) );
                        }
                        else {
                            throw new IllegalStateException("we only write byte arrays ( byte[] ) to an IonClob");
                        }
                        break;                  
                    case STRUCT:
                    case LIST:
                    case SEXP:
                        throw new IllegalStateException("the value writer can't write complex types");
                }
                
            }

        }
        public void testAllValues()
        throws Exception
        {
            IonWriter wr = new IonBinaryWriter();
            byte[] buffer = null;
            
            byte[] _testbytes1 = new byte[5];
            byte[] _testbytes2 = new byte[10000];
            
            for (int ii=0; ii<5; ii++) {
                _testbytes1[ii] = (byte)('0' + ii);
            }
            for (int ii=0; ii<_testbytes2.length; ii++) {
                _testbytes2[ii] = (byte)(ii & 0xff); 
            }
            
            TestValue[] testvalues = {
                   // new TestValue( , , ),
                   new TestValue("Null",          IonType.NULL, null),
                   new TestValue("Null.null",     IonType.NULL, IonType.NULL),
                   new TestValue("Null.bool",     IonType.NULL, IonType.BOOL),
                   new TestValue("Null.int",      IonType.NULL, IonType.INT),
                   new TestValue("Null.float",    IonType.NULL, IonType.FLOAT),
                   new TestValue("Null.decimal",  IonType.NULL, IonType.DECIMAL),
                   new TestValue("Null.timestamp",IonType.NULL, IonType.TIMESTAMP),
                   new TestValue("Null.string",   IonType.NULL, IonType.STRING),
                   new TestValue("Null.symbol",   IonType.NULL, IonType.SYMBOL),
                   new TestValue("Null.clob",     IonType.NULL, IonType.CLOB),
                   new TestValue("Null.blob",     IonType.NULL, IonType.BLOB),
                   new TestValue("Null.list",     IonType.NULL, IonType.LIST),
                   new TestValue("Null.sexp",     IonType.NULL, IonType.SEXP),
                   new TestValue("Null.struct",   IonType.NULL, IonType.STRUCT),
                   
                   new TestValue("bool_true",     IonType.BOOL, Boolean.TRUE),
                   new TestValue("bool_false",    IonType.BOOL, Boolean.FALSE),
                   
                   new TestValue("int0",          IonType.INT, new Integer(0) ),
                   new TestValue("intneg1",       IonType.INT, new Integer(-1) ),
                   new TestValue("int1",          IonType.INT, new Integer(1) ),
                   new TestValue("intneg32766",   IonType.INT, new Integer(-32766) ),
                   new TestValue("int32767",      IonType.INT, new Integer(32767) ),
                   new TestValue("int-2g",        IonType.INT, new Integer(-2000000000) ),
                   new TestValue("int2g",         IonType.INT, new Integer(2000000000) ),
                   new TestValue("intmin",        IonType.INT, Integer.MIN_VALUE ),
                   new TestValue("intmax",        IonType.INT, Integer.MAX_VALUE ),

                   new TestValue("long0",         IonType.INT, new Long(0L) ),
                   new TestValue("long-1",        IonType.INT, new Long(-1L) ),
                   new TestValue("long1",         IonType.INT, new Long(1L) ),
                   new TestValue("long-4g",       IonType.INT, new Long(-4000000000L) ),
                   new TestValue("long4g",        IonType.INT, new Long(4000000000L) ),
                   new TestValue("longmin",       IonType.INT, new Long(Long.MIN_VALUE + 1) ),  // we don't reach all the way to MIN_VALUE since we have to negate it for writing (to get the positive value)
                   new TestValue("longmin2",      IonType.INT, new Long(-Long.MAX_VALUE) ),
                   new TestValue("longmax",       IonType.INT, Long.MAX_VALUE ),
                   
                   new TestValue("float_0",       IonType.FLOAT, new Float(0.0) ),
                   new TestValue("float_a",       IonType.FLOAT, new Float(-1.0) ),
                   new TestValue("float_b",       IonType.FLOAT, new Float(1.0) ),
                   new TestValue("float_c",       IonType.FLOAT, new Float(-1e10) ),
                   new TestValue("float_d",       IonType.FLOAT, new Float(1e-10) ),
                   new TestValue("float_e",       IonType.FLOAT, new Float(0.1) ),
                   new TestValue("float_f",       IonType.FLOAT, new Float(-0.1) ),
                   new TestValue("float_g",       IonType.FLOAT, Float.MIN_VALUE ),
                   new TestValue("float_h",       IonType.FLOAT, Float.MAX_VALUE ),
                   new TestValue("float_i",       IonType.FLOAT, Float.NaN ),
                   new TestValue("float_j",       IonType.FLOAT, Float.NEGATIVE_INFINITY ),
                   new TestValue("float_k",       IonType.FLOAT, Float.POSITIVE_INFINITY ),

                   new TestValue("double_0",      IonType.FLOAT, new Double(0.0) ),
                   new TestValue("double_a",      IonType.FLOAT, new Double(-1.0) ),
                   new TestValue("double_b",      IonType.FLOAT, new Double(1.0) ),
                   new TestValue("double_c",      IonType.FLOAT, new Double(-1e10) ),
                   new TestValue("double_d",      IonType.FLOAT, new Double(1e-10) ),
                   new TestValue("double_e",      IonType.FLOAT, new Double(0.1) ),
                   new TestValue("double_f",      IonType.FLOAT, new Double(-0.1) ),
                   new TestValue("double_g",      IonType.FLOAT, Double.MIN_VALUE ),
                   new TestValue("double_h",      IonType.FLOAT, Double.MAX_VALUE ),
                   new TestValue("double_i",      IonType.FLOAT, Double.NaN ),
                   new TestValue("double_j",      IonType.FLOAT, Double.NEGATIVE_INFINITY ),
                   new TestValue("double_k",      IonType.FLOAT, Double.POSITIVE_INFINITY ),
                   
                   new TestValue("bdd_0",         IonType.DECIMAL, new Double(0.0) ),
                   new TestValue("bdd_a",         IonType.DECIMAL, new Double(-1.0) ),
                   new TestValue("bdd_b",         IonType.DECIMAL, new Double(1.0) ),
                   new TestValue("bdd_c",         IonType.DECIMAL, new Double(-1e10) ),
                   new TestValue("bdd_d",         IonType.DECIMAL, new Double(1e-10) ),
                   new TestValue("bdd_e",         IonType.DECIMAL, new Double(0.1) ),
                   new TestValue("bdd_f",         IonType.DECIMAL, new Double(-0.1) ),
                   new TestValue("bdd_g",         IonType.DECIMAL, Double.MIN_VALUE ),
                   new TestValue("bdd_h",         IonType.DECIMAL, Double.MAX_VALUE ),
//                   new TestValue("bdd_i",         IonType.DECIMAL, Double.NaN ),
//                   new TestValue("bdd_j",         IonType.DECIMAL, Double.NEGATIVE_INFINITY ),
//                   new TestValue("bdd_k",         IonType.DECIMAL, Double.POSITIVE_INFINITY ),

                   new TestValue("bdbd_0",        IonType.DECIMAL, TestValue.makeBigDecimal("0.0") ),
                   new TestValue("bdbd_a",        IonType.DECIMAL, TestValue.makeBigDecimal("-1.0") ),
                   new TestValue("bdbd_b",        IonType.DECIMAL, TestValue.makeBigDecimal("1.0") ),
                   new TestValue("bdbd_c",        IonType.DECIMAL, TestValue.makeBigDecimal("-1e10") ),
                   new TestValue("bdbd_d",        IonType.DECIMAL, TestValue.makeBigDecimal("1e-10") ),
                   new TestValue("bdbd_e",        IonType.DECIMAL, TestValue.makeBigDecimal("0.1") ),
                   new TestValue("bdbd_f",        IonType.DECIMAL, TestValue.makeBigDecimal("-0.1") ),
                   new TestValue("bdbd_g",        IonType.DECIMAL, TestValue.makeBigDecimal("0.10") ),
                   new TestValue("bdbd_h",        IonType.DECIMAL, TestValue.makeBigDecimal("-0.10") ),
                   new TestValue("bdbd_i",        IonType.DECIMAL, TestValue.makeBigDecimal("100") ),
                   new TestValue("bdbd_j",        IonType.DECIMAL, TestValue.makeBigDecimal("-100.000") ),
                   
//                   new TestValue("date_d1",       IonType.TIMESTAMP, new Date("1-23-2008") ),
//                   new TestValue("date_d2",       IonType.TIMESTAMP, new Date("1-23-2008T12:53") ),
//                   new TestValue("date_d3",       IonType.TIMESTAMP, new Date("1-23-2008T12:53:24") ),

//                   new TestValue("date_t1",       IonType.TIMESTAMP, IonTokenReader.Type.timeinfo.parse("2008-02Z") ),
                   new TestValue("date_t2",       IonType.TIMESTAMP, IonTokenReader.Type.timeinfo.parse("2008-02-15") ),
                   new TestValue("date_t3",       IonType.TIMESTAMP, IonTokenReader.Type.timeinfo.parse("0001-01-01") ),
                   new TestValue("date_t4",       IonType.TIMESTAMP, IonTokenReader.Type.timeinfo.parse("9999-12-31") ),

                   new TestValue("date_t5",       IonType.TIMESTAMP, IonTokenReader.Type.timeinfo.parse("2008-02-15T12:59:59.100000-03:45") ),
                   new TestValue("date_t6",       IonType.TIMESTAMP, IonTokenReader.Type.timeinfo.parse("0001-01-01T00:00:00.000000-00:00") ),
                   new TestValue("date_t7",       IonType.TIMESTAMP, IonTokenReader.Type.timeinfo.parse("9999-12-31T23:59:59.999999+01:00") ),
                   
                   new TestValue("string_1",       IonType.STRING, ""),
                   new TestValue("string_2",       IonType.STRING, " "),
                   new TestValue("string_3",       IonType.STRING, "abcde"),
                   new TestValue("string_4",       IonType.STRING, "123456789012"),
                   new TestValue("string_5",       IonType.STRING, "1234567890123"),
                   new TestValue("string_6",       IonType.STRING, "12345678901234"),
                   new TestValue("string_7",       IonType.STRING, "123456789012345"),
                   new TestValue("string_8",       IonType.STRING, "\0\u001f\uffff"),

//                   new TestValue("symbol_1",       IonType.SYMBOL, ""),
                   new TestValue("symbol_2",       IonType.SYMBOL, " "),
                   new TestValue("symbol_3",       IonType.SYMBOL, "abcde"),
                   new TestValue("symbol_4",       IonType.SYMBOL, "123456789012"),
                   new TestValue("symbol_5",       IonType.SYMBOL, "1234567890123"),
                   new TestValue("symbol_6",       IonType.SYMBOL, "12345678901234"),
                   new TestValue("symbol_7",       IonType.SYMBOL, "123456789012345"),
                   new TestValue("symbol_8",       IonType.SYMBOL, "\0\u001f\uffff"),
                   
                   new TestValue("symbol_9",       IonType.SYMBOL, new Integer(1) ),

                   new TestValue("clob_1",       IonType.CLOB, _testbytes1),
                   new TestValue("clob_2",       IonType.CLOB, _testbytes2),

                   new TestValue("blob_1",       IonType.BLOB, _testbytes1),
                   new TestValue("blob_2",       IonType.BLOB, _testbytes2),

            };

            try {
                // we don't really need the struct, but if we use it we get to
                // label all the values
                wr.startStruct();

                for (TestValue tv : testvalues) {
                    tv.writeValue(wr);
                }

                wr.closeStruct();
                buffer = wr.getBytes();
            }
            catch (IOException e) {
                throw new Exception(e);
            }
            
            IonIterator r = IonIterator.makeIterator(buffer);
            IonType t;
            
            t = r.next();
            assertTrue( t.equals(IonType.STRUCT) );
            r.stepInto();
            r.hasNext();
            
            for (TestValue tv : testvalues) {
                tv.readAndTestValue(r);
            }
 
        }

        
        public void testBoolValue()
            throws Exception
        {
            IonWriter wr = new IonBinaryWriter();
            byte[] buffer = null;

            try {
                wr.startStruct();
                wr.writeFieldname("Foo");
                wr.writeBool(true);
                wr.closeStruct();
                buffer = wr.getBytes();
            }
            catch (IOException e) {
                throw new Exception(e);
            }

            IonIterator ir = IonIterator.makeIterator(buffer);
            if (ir.hasNext()) {
                ir.next();
                ir.stepInto();
                while (ir.hasNext()) {
                    IonType t = ir.next();
                    String name = ir.getFieldName();
                    boolean value = ir.getBool();
                    assertTrue( value );
                    System.out.println(t + " " + name +": " + value);
                }
            }
        }
        
        public void testTwoMagicCookies() {
            IonWriter wr = new IonBinaryWriter();
            byte[] buffer = null;

            try {
                wr.startStruct();
                wr.writeFieldname("Foo");
                wr.addAnnotation("boolean");
                wr.writeBool(true);
                wr.closeStruct();
                buffer = wr.getBytes();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            
            byte[] doublebuffer = new byte[buffer.length * 2];
            System.arraycopy(buffer, 0, doublebuffer, 0, buffer.length);
            System.arraycopy(buffer, 0, doublebuffer, buffer.length, buffer.length);
            
            IonIterator ir = IonIterator.makeIterator(doublebuffer);
            
            // first copy
            assertTrue(ir.next().equals(IonType.STRUCT));
            ir.stepInto();
            assertEquals(ir.next(), IonType.BOOL);
            assertEquals(ir.getFieldName(), "Foo");
            //assertEquals(ir.getAnnotations(), new String[] { "boolean" });
            String[] annotations = ir.getAnnotations();
            assertTrue(annotations != null && annotations.length == 1);
            if (annotations != null) { // just to shut eclipse up, we already tested this above
                assertTrue("boolean".equals(annotations[0]));
            }
            assertEquals(ir.getBool(), true);
            ir.stepOut();
            
            // second copy
            assertEquals(IonType.STRUCT, ir.next());
            ir.stepInto();
            assertEquals(IonType.BOOL, ir.next());
            assertEquals("Foo", ir.getFieldName());
            annotations = ir.getAnnotations();
            assertNotNull(annotations);
            assertEquals(1, annotations.length);
            assertEquals("boolean", annotations[0]);
            assertEquals(true, ir.getBool());
            ir.stepOut();
            
            assertEquals(false, ir.hasNext());
        }


        public void testBoolean() {
            IonWriter wr = new IonBinaryWriter();
            byte[] buffer = null;

            try {
                wr.startStruct();
                wr.writeFieldname("Foo");
                wr.addAnnotation("boolean");
                wr.writeBool(true);
                wr.closeStruct();
                buffer = wr.getBytes();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            IonIterator ir = IonIterator.makeIterator(buffer);
            if (ir.hasNext()) {
                ir.next();
                ir.stepInto();
                while (ir.hasNext()) {
                    assertEquals(ir.next(), IonType.BOOL);
                    assertEquals(ir.getFieldName(), "Foo");
                    //assertEquals(ir.getAnnotations(), new String[] { "boolean" });
                    String[] annotations = ir.getAnnotations();
                    assertTrue(annotations != null && annotations.length == 1);
                    if (annotations != null) { // just to shut eclipse up, we already tested this above
                        assertTrue("boolean".equals(annotations[0]));
                    }
                    assertEquals(ir.getBool(), true);
                }
            }
        }

        //Test Sample map.
        //{hello=true, Almost Done.=true, This is a test String.=true, 12242.124598129=12242.124598129, Something=null, false=false, true=true, long=9326, 12=-12}
        public void testSampleMap() {
            IonWriter wr = new IonBinaryWriter();
            byte[] buffer = null;

            try {
                wr.startStruct();
                
                wr.writeFieldname("hello");
                wr.writeBool(true);
                
                wr.writeFieldname("Almost Done.");
                wr.writeBool(true);
                
                wr.writeFieldname("This is a test String.");
                wr.writeBool(true);
                
                wr.writeFieldname("12242.124598129");
                wr.writeFloat(12242.124598129);
                
                wr.writeFieldname("Something");
                wr.writeNull();
                
                wr.writeFieldname("false");
                wr.writeBool(false);
                
                wr.writeFieldname("true");
                wr.writeBool(true);
                
                wr.writeFieldname("long");
                wr.writeInt((long) 9326);
                
                wr.writeFieldname("12");
                wr.writeInt(-12);
                
                wr.closeStruct();
                buffer = wr.getBytes();
            } catch (IOException e) {

                throw new RuntimeException(e);
            }

            IonIterator ir = IonIterator.makeIterator(buffer);
            if (ir.hasNext()) {
                ir.next();
                ir.stepInto();
                while (ir.hasNext()) {
                    assertEquals(ir.next(), IonType.BOOL);
                    assertEquals(ir.getFieldName(), "hello");
                    assertEquals(ir.getBool(), true);
                    
                    assertEquals(ir.next(), IonType.BOOL);
                    assertEquals(ir.getFieldName(), "Almost Done.");
                    assertEquals(ir.getBool(), true);
                    
                    assertEquals(ir.next(), IonType.BOOL);
                    assertEquals(ir.getFieldName(), "This is a test String.");
                    assertEquals(ir.getBool(), true);
                    
                    assertEquals(ir.next(), IonType.FLOAT);
                    assertEquals(ir.getFieldName(), "12242.124598129");
                    assertEquals(ir.getDouble(), 12242.124598129);
                    
                    assertEquals(ir.next(), IonType.NULL);
                    assertEquals(ir.getFieldName(), "Something");
                    assertTrue(ir.isNull());
                    // not:
                    //assertEquals(ir.getValueAsString(), null);
                    assertEquals(ir.getValueAsString(), "null");
                    
                    assertEquals(ir.next(), IonType.BOOL);
                    assertEquals(ir.getFieldName(), "false");
                    assertEquals(ir.getBool(), false);
                    
                    assertEquals(ir.next(), IonType.BOOL);
                    assertEquals(ir.getFieldName(), "true");
                    assertEquals(ir.getBool(), true);
                    
                    assertEquals(ir.next(), IonType.INT);
                    assertEquals(ir.getFieldName(), "long");
                    assertEquals(ir.getLong(), 9326L);
                    
                    assertEquals(ir.next(), IonType.INT);
                    assertEquals(ir.getFieldName(), "12");
                    assertEquals(ir.getInt(), -12);
                }
            }
        }
        
        public void testBenchmarkDirect() throws IOException {

            byte[] bytes ;
            
            IonWriter wr = new IonBinaryWriter();
            wr.startStruct();
            
            wr.writeFieldname("12");
            wr.addAnnotation(int.class.getCanonicalName());
            wr.writeInt(-12);
            
            wr.writeFieldname("12242.124598129");
            wr.addAnnotation(double.class.getCanonicalName());
            wr.writeFloat(12242.124598129);
            
            wr.writeFieldname("Almost Done.");
            wr.addAnnotation(boolean.class.getCanonicalName());
            wr.writeBool(true);
            
            wr.writeFieldname("This is a test String.");
            wr.addAnnotation(boolean.class.getCanonicalName());
            wr.writeBool(true);
            
            wr.writeFieldname("false");
            wr.addAnnotation(boolean.class.getCanonicalName());
            wr.writeBool(false);
            
            wr.writeFieldname("long");
            wr.addAnnotation(long.class.getCanonicalName());
            wr.writeInt((long) 9326);
            
            wr.writeFieldname("true");
            wr.addAnnotation(boolean.class.getCanonicalName());
            wr.writeBool(true);
            
            wr.closeStruct();
            
            bytes = wr.getBytes();
            
            IonIterator ir = IonIterator.makeIterator(bytes);
            assertTrue(ir.hasNext());
            ir.next();
            ir.stepInto();
            
            assertTrue(ir.hasNext());
            assertEquals(ir.next(), IonType.INT);
            assertEquals(ir.getFieldName(), "12");
            assertEquals(ir.getInt(), -12);
            
            assertTrue(ir.hasNext());
            assertEquals(ir.next(), IonType.FLOAT);
            assertEquals(ir.getFieldName(), "12242.124598129");
            assertEquals(ir.getDouble(), 12242.124598129);
            
            assertTrue(ir.hasNext());
            assertEquals(ir.next(), IonType.BOOL);
            assertEquals(ir.getFieldName(), "Almost Done.");
            assertEquals(ir.getBool(), true);
            
            assertTrue(ir.hasNext());
            assertEquals(ir.next(), IonType.BOOL);
            assertEquals(ir.getFieldName(), "This is a test String.");
            assertEquals(ir.getBool(), true);
            
            assertTrue(ir.hasNext());
            assertEquals(ir.next(), IonType.BOOL);
            assertEquals(ir.getFieldName(), "false");
            assertEquals(ir.getBool(), false);
            
            assertTrue(ir.hasNext());
            assertEquals(ir.next(), IonType.INT);
            assertEquals(ir.getFieldName(), "long");
            assertEquals(ir.getLong(), 9326L);    
            
            assertTrue(ir.hasNext());
            assertEquals(ir.next(), IonType.BOOL);
            assertEquals(ir.getFieldName(), "true");
            assertEquals(ir.getBool(), true);
            
            assertFalse(ir.hasNext());
            ir.stepOut();
            assertFalse(ir.hasNext());
        }


}
