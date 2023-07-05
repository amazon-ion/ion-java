/*
 * Copyright 2007-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.ion.streaming;

import static com.amazon.ion.impl.Symtabs.printLocalSymtab;
import static com.amazon.ion.junit.IonAssert.checkNullSymbol;

import com.amazon.ion.BinaryTest;
import com.amazon.ion.Decimal;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.ReaderMaker;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.junit.Injected.Inject;
import com.amazon.ion.junit.IonAssert;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;


public class ReaderTest
    extends ReaderTestCase
{
    @Inject("readerMaker")
    public static final ReaderMaker[] READER_MAKERS = ReaderMaker.values();


    @Test
    public void testNullInt()
    {
        {
            read("null.int");
            assertEquals(IonType.INT, in.next());
            assertTrue(in.isNullValue());
            assertEquals(null, in.bigIntegerValue());
        }
        {
            for (final String hex : Arrays.asList("E0 01 00 EA 2F", "E0 01 00 EA 3F")) {
                read(BinaryTest.hexToBytes(hex));
                assertEquals(IonType.INT, in.next());
                assertTrue(in.isNullValue());
                assertEquals(null, in.bigIntegerValue());
            }
        }
    }

    @Test
    public void testStepInOnNull() throws IOException
    {
        read("null.list null.sexp null.struct");

        assertEquals(IonType.LIST, in.next());
        assertTrue(in.isNullValue());
        in.stepIn();
        expectEof();
        in.stepOut();

        assertEquals(IonType.SEXP, in.next());
        assertTrue(in.isNullValue());
        in.stepIn();
        expectEof();
        in.stepOut();

        assertEquals(IonType.STRUCT, in.next());
        assertTrue(in.isNullValue());
        in.stepIn();
        expectEof();
        in.stepOut();
        expectEof();
    }

    @Test
    public void testStepOut() throws IOException
    {
        read("{a:{b:1,c:2},d:false}");

        in.next();
        in.stepIn();
        expectNextField("a");
        in.stepIn();
        expectNextField("b");
        in.stepOut(); // skip c
        expectNoCurrentValue();
        expectNextField("d");
        expectEof();
    }


    @Test
    public void testIterateTypeAnnotationIds()
    throws Exception
    {
        String ionText =
            printLocalSymtab("ann", "ben")
            + "ann::ben::null";

        read(ionText);

        in.next();
        SymbolToken[] tokens = in.getTypeAnnotationSymbols();
        assertEquals(2, tokens.length);
        assertEquals(10, tokens[0].getSid());
        assertEquals(11, tokens[1].getSid());

        expectEof();
    }

    @Test
    public void testStringValueOnNull()
        throws Exception
    {
        read("null.string null.symbol");

        in.next();
        expectString(null);
        in.next();
        checkNullSymbol(in);
    }

    @Test
    public void testStringValueOnNonText()
        throws Exception
    {
        // All non-text types
        read("null true 1 1e2 1d2 2011-12-01T {{\"\"}} {{}} [] () {}");

        while (in.next() != null)
        {
            try {
                in.stringValue();
                fail("expected exception on " + in.getType());
            }
            catch (IllegalStateException e) { }
        }
    }

    @Test
    public void testSymbolValue()
        throws Exception
    {
        read("null.symbol sym");
        in.next();
        checkNullSymbol(in);
        in.next();
        IonAssert.checkSymbol("sym", in);
    }

    @Test
    public void testSymbolValueOnNonSymbol()
        throws Exception
    {
        // All non-symbol types
        read("null true 1 1e2 1d2 2011-12-01T \"\" {{\"\"}} {{}} [] () {}");

        while (in.next() != null)
        {
            try {
                in.symbolValue();
                fail("expected exception on " + in.getType());
            }
            catch (IllegalStateException e) { }
        }
    }

    int readInt(String text) {
        IonReader r = myReaderMaker.newReader(system(), text);
        if (r.next() == null) {
            fail("Expected to read an integer");
        }
        return r.intValue();
    }

    long readLong(String text) {
        IonReader r = myReaderMaker.newReader(system(), text);
        if (r.next() == null) {
            fail("Expected to read a long");
        }
        return r.longValue();
    }

    double readDouble(String text) {
        IonReader r = myReaderMaker.newReader(system(), text);
        if (r.next() == null) {
            fail("Expected to read a double");
        }
        return r.doubleValue();
    }

    BigInteger readBigInteger(String text) {
        IonReader r = myReaderMaker.newReader(system(), text);
        if (r.next() == null) {
            fail("Expected to read a BigInteger");
        }
        return r.bigIntegerValue();
    }

    Decimal readDecimal(String text) {
        IonReader r = myReaderMaker.newReader(system(), text);
        if (r.next() == null) {
            fail("Expected to read a Decimal");
        }
        return r.decimalValue();
    }

    BigDecimal readBigDecimal(String text) {
        IonReader r = myReaderMaker.newReader(system(), text);
        if (r.next() == null) {
            fail("Expected to read an BigDecimal");
        }
        return r.bigDecimalValue();
    }

    @Test
    public void testReadingDecimalAsInteger()
    {
        assertEquals(readInt("0."), readInt("0"));
        assertEquals(readInt("-2147483648."), readInt("-2147483648")); // Min int
        assertEquals(readInt("2147483647."), readInt("2147483647")); // Max int
    }

    @Test
    public void testReadingDecimalAsLong()
    {
        assertEquals(readLong("0."), readLong("0"));
        assertEquals(readLong("2147483647."), readLong("2147483647")); // Max int
        assertEquals(readLong("9223372036854775807."), readLong("9223372036854775807")); // Max long
    }

    @Test(expected = IllegalStateException.class)
    public void testReadingDecimalAsBigInteger()
    {
        readBigInteger("0.");
    }

    @Test
    public void testReadingDecimalAsDouble()
    {
        assertEquals(readDouble("0."), readDouble("0e0"));
        assertEquals(readDouble("1.23"), readDouble("123e-2"));
        assertEquals(readDouble("-1.23"), readDouble("-1.23e0"));
        assertEquals(readDouble("2d24"), readDouble("2e24"));
    }

    @Test
    public void testReadingFloatAsInteger()
    {
        assertEquals(readInt("-0e0"), readInt("0"));
        assertEquals(readInt("-0e0"), readInt("0"));
        assertEquals(readInt("-2147483648e0"), readInt("-2147483648")); // Min int
        assertEquals(readInt("2147483647e0"), readInt("2147483647")); // Max int
    }

    @Test
    public void testReadingFloatAsLong()
    {
        assertEquals(readLong("0e0"), readLong("0"));
        assertEquals(readLong("0e0"), readLong("0"));
        assertEquals(readLong("-2147483648e0"), readLong("-2147483648")); // Min int
        assertEquals(readLong("2147483647e0"), readLong("2147483647")); // Max int
    }

    @Test
    public void testReadingFloatAsBigInteger()
    {
        // Note that one can't represent Long.MAX_VALUE as a double, there's
        // not enough bits in the mantissa!
        BigInteger actual = readBigInteger("null.float");
        assertNull(actual);
        BigInteger expected = readBigInteger("null.int");
        assertNull(expected);
        assertEquals(actual, expected);

        assertEquals(readBigInteger("-0e0"), readBigInteger("0"));
        assertEquals(readBigInteger("0e0"), readBigInteger("0"));
        assertEquals(readBigInteger("9223372036854776000e0"), readBigInteger("9223372036854776000"));
        assertEquals(readBigInteger("2e24"), readBigInteger("2000000000000000000000000"));
    }


    /**
     * This isn't allowed by the documentation, but it's allowed by the text
     * and binary readers.  Not allowed by the tree reader.
     *
     * We need to specify the conversion too, its non-trivial: does 0e0
     * convert to 0d0 or 0.0?  The latter is happening at this writing, but
     * it's not what one would expect.
     */
    @Test @Ignore
    public void testReadingFloatAsBigDecimal() // TODO amazon-ion/ion-java/issues/56
    {
        BigDecimal actualBd = readBigDecimal("null.float");
        assertEquals(in.isNullValue(), actualBd  == null);
        Decimal actualDec = readDecimal("null.decimal");
        assertEquals(in.isNullValue(), actualDec == null);
        assertEquals(actualBd, actualDec);

        // Note that one can't represent Long.MAX_VALUE as a double, there's
        // not enough bits in the mantissa!
        assertEquals(readBigDecimal("-0e0"), readDecimal("-0."));
        assertEquals(readBigDecimal("0e0"), readDecimal("0."));
        assertEquals(readBigDecimal("9223372036854776000e0"), readDecimal("9223372036854776000."));
        assertEquals(readBigDecimal("9223372036854776000e12"), readDecimal("9223372036854776000d12"));
        assertEquals(readBigDecimal("2e24"), readDecimal("2000000000000000000000000."));
    }


    @Test
    public void testIntValueOnNonNumber()
    {
        // All non-numeric types
        read("null true 2011-12-01T \"\" sym {{\"\"}} {{}} [] () {}");

        while (in.next() != null)
        {
            IonType type = in.getType();

            try {
                in.intValue();
                fail("expected exception from intValue on" + type);
            }
            catch (IllegalStateException e) { }

            try {
                in.longValue();
                fail("expected exception from longValue on " + type);
            }
            catch (IllegalStateException e) { }

            try {
                in.bigIntegerValue();
                fail("expected exception from bigIntegerValue on " + type);
            }
            catch (IllegalStateException e) { }
        }
    }


    private void skipThroughTopLevelContainer(String data)
    {
        read(data);

        in.next();
        in.stepIn();
        {
            while (in.next() != null)
            {
                // Just skip the children
            }
            expectEof();
        }
        in.stepOut();
        expectTopEof();
    }


    private String[] LOB_DATA = {
          "{{\"\"}}",
          "{{\"clob\"}}",

          "{{''''''}}",
          "{{''' '''}}",
          "{{'''clob'''}}",
          "{{'''c}ob'''}}",
          "{{'''c}}b'''}}",
          "{{'''c\\'''ob'''}}",

          "{{}}",
          "{{Zm9v}}",
    };

    private void testSkippingLob(String containerPrefix,
                                 String containerSuffix)
    {
        for (String lob : LOB_DATA)
        {
            String data = containerPrefix + lob + containerSuffix;
            skipThroughTopLevelContainer(data);

            data = containerPrefix + " " + lob + containerSuffix;
            skipThroughTopLevelContainer(data);

            data = containerPrefix + " " + lob + " " + containerSuffix;
            skipThroughTopLevelContainer(data);

            data = containerPrefix + lob + " " + containerSuffix;
            skipThroughTopLevelContainer(data);
        }
    }


    @Test
    public void testSkippingLobInList()
    {
        testSkippingLob("[1, { c:", " } ]");
        testSkippingLob("[1, { c:", "}]");
    }


    @Test
    public void testSkippingLobInStruct()
    {
        testSkippingLob("{a:1, b:{ c:", " } }");
        testSkippingLob("{a:1, b:{ c:", " }}");
        testSkippingLob("{a:1, b:{ c:", "}}");
    }

    @Test
    public void testReadLobUsingGetBytes() throws Exception
    {
        if (!myReaderMaker.sourceIsBinary()) {
            // TODO text implements getBytes() differently: https://github.com/amazon-ion/ion-java/issues/319
            return;
        }
        String data = "{{\"abcdefghijklmnopqrstuvwxyz\"}}";
        byte[] partialBuffer = new byte[10];
        read(data);
        assertEquals(IonType.CLOB, in.next());
        int remaining = in.byteSize();
        assertEquals(26, remaining);
        StringBuilder value = new StringBuilder();
        while (remaining > 0) {
            int bytesRead = in.getBytes(partialBuffer, 0, partialBuffer.length);
            remaining -= bytesRead;
            value.append(new String(partialBuffer, 0, bytesRead, "UTF-8"));
        }
        assertEquals("abcdefghijklmnopqrstuvwxyz", value.toString());
    }

    @Test
    public void testGetSymbolTableBeforeFirstValue()
    {
        read("123");
        SymbolTable symtab = in.getSymbolTable();
        expectNoCurrentValue();
        assertNotNull(symtab);
        assertTrue(symtab.isSystemTable());
    }

}
