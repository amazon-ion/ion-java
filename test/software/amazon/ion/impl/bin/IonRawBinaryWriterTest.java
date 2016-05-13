/*
 * Copyright 2015-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.ion.impl.bin;

import static software.amazon.ion.IonType.BLOB;
import static software.amazon.ion.IonType.BOOL;
import static software.amazon.ion.IonType.CLOB;
import static software.amazon.ion.IonType.DECIMAL;
import static software.amazon.ion.IonType.FLOAT;
import static software.amazon.ion.IonType.INT;
import static software.amazon.ion.IonType.LIST;
import static software.amazon.ion.IonType.NULL;
import static software.amazon.ion.IonType.SEXP;
import static software.amazon.ion.IonType.STRING;
import static software.amazon.ion.IonType.STRUCT;
import static software.amazon.ion.IonType.SYMBOL;
import static software.amazon.ion.IonType.TIMESTAMP;
import static software.amazon.ion.SystemSymbols.IMPORTS_SID;
import static software.amazon.ion.SystemSymbols.NAME_SID;
import static software.amazon.ion.SystemSymbols.VERSION_SID;
import static software.amazon.ion.TestUtils.hexDump;
import static software.amazon.ion.impl.bin.Symbols.systemSymbol;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import software.amazon.ion.IonException;
import software.amazon.ion.IonSystem;
import software.amazon.ion.IonType;
import software.amazon.ion.IonValue;
import software.amazon.ion.IonWriter;
import software.amazon.ion.Timestamp;
import software.amazon.ion.impl.bin.BlockAllocatorProviders;
import software.amazon.ion.impl.bin.IonRawBinaryWriter;
import software.amazon.ion.impl.bin.AbstractIonWriter.WriteValueOptimization;
import software.amazon.ion.impl.bin.IonRawBinaryWriter.PreallocationMode;
import software.amazon.ion.impl.bin.IonRawBinaryWriter.StreamCloseMode;
import software.amazon.ion.impl.bin.IonRawBinaryWriter.StreamFlushMode;
import software.amazon.ion.junit.Injected;
import software.amazon.ion.junit.IonAssert;
import software.amazon.ion.junit.Injected.Inject;
import software.amazon.ion.system.IonSystemBuilder;

// TODO incorporate this into the main reader/writer tests

@RunWith(Injected.class)
public class IonRawBinaryWriterTest extends Assert
{
    private static IonSystem SYSTEM = IonSystemBuilder.standard().build();

    protected static IonSystem system()
    {
        return SYSTEM;
    }

    protected static void assertEquals(final IonValue v1, final IonValue v2)
    {
        IonAssert.assertIonEquals(v1, v2);
    }

    @Inject("preallocationMode")
    public static final PreallocationMode[] PREALLOCATION_DIMENSION = PreallocationMode.values();

    protected ByteArrayOutputStream     buffer;
    protected IonWriter                 writer;
    protected PreallocationMode         preallocationMode;

    public void setPreallocationMode(final PreallocationMode preallocationMode)
    {
        this.preallocationMode = preallocationMode;
    }

    @Before
    public final void setup() throws Exception
    {
        buffer = new ByteArrayOutputStream();
        writer = createWriter(buffer);
        writeIVMIfPossible();
    }

    @After
    public final void teardown() throws Exception
    {
        writer.close();
        buffer = null;
        writer = null;
    }

    protected IonWriter createWriter(final OutputStream out) throws IOException
    {
        return new IonRawBinaryWriter(
            BlockAllocatorProviders.basicProvider(),
            11,
            out,
            WriteValueOptimization.NONE,
            StreamCloseMode.NO_CLOSE,
            StreamFlushMode.NO_FLUSH,
            preallocationMode,
            true
        );
    }

    private void writeIVMIfPossible() throws IOException
    {
        if (writer instanceof IonRawBinaryWriter)
        {
            ((IonRawBinaryWriter) writer).writeIonVersionMarker();
        }
    }

    /** sub-classes can provide additional checks on a value as part of {@link #assertValue(String)}. */
    protected void additionalValueAssertions(final IonValue value) {}

    protected final void assertValue(final String literal) throws IOException
    {
        writer.finish();
        final byte[] data = buffer.toByteArray();
        final IonValue actual;
        try {
            actual = system().singleValue(data);
        } catch (final Exception e) {
            throw new IonException("Bad generated data:\n" + hexDump(data), e);
        }
        final IonValue expected = system().singleValue(literal);
        assertEquals(expected, actual);

        additionalValueAssertions(actual);

        // prepare for next value
        buffer.reset();
        writeIVMIfPossible();
    }

    private static class NullDesc
    {
        public final IonType type;
        public final String literal;

        public NullDesc(final IonType type, final String literal)
        {
            this.type = type;
            this.literal = literal;
        }
    }

    private static final NullDesc[] NULL_DESCS = {
        new NullDesc(NULL,      "null.null"),
        new NullDesc(BOOL,      "null.bool"),
        new NullDesc(INT,       "null.int"),
        new NullDesc(FLOAT,     "null.float"),
        new NullDesc(DECIMAL,   "null.decimal"),
        new NullDesc(TIMESTAMP, "null.timestamp"),
        new NullDesc(SYMBOL,    "null.symbol"),
        new NullDesc(STRING,    "null.string"),
        new NullDesc(CLOB,      "null.clob"),
        new NullDesc(BLOB,      "null.blob"),
        new NullDesc(LIST,      "null.list"),
        new NullDesc(SEXP,      "null.sexp"),
        new NullDesc(STRUCT,    "null.struct")
    };

    @Test
    public void testNullNull() throws Exception
    {
        writer.writeNull();
        assertValue("null.null");

        for (final NullDesc desc : NULL_DESCS)
        {
            writer.writeNull(desc.type);
            assertValue(desc.literal);
        }
    }

    @Test
    public void testBool() throws Exception
    {
        writer.writeBool(false);
        assertValue("false");

        writer.writeBool(true);
        assertValue("true");
    }

    @Test
    public void testInt() throws Exception
    {
        writer.writeInt(BigInteger.ZERO);
        assertValue("0");

        writer.writeInt(1);
        assertValue("1");

        writer.writeInt(-1);
        assertValue("-1");

        // 2 ** 64
        writer.writeInt(new BigInteger("18446744073709551616"));
        assertValue("18446744073709551616");

        writer.writeInt(Long.MIN_VALUE);
        assertValue("-0x8000000000000000");
    }

    @Test
    public void testFloat() throws Exception
    {
        writer.writeFloat(0.0);
        assertValue("0e0");

        writer.writeFloat(Double.NaN);
        assertValue("nan");

        writer.writeFloat(Double.POSITIVE_INFINITY);
        assertValue("+inf");
    }

    public int ivmLength() {
        return 0;
    }

    @Test
    public void testVariableFloat() throws Exception
    {
        // IonRawBinaryWriterTest writes a single BVM,
        // but IonManagedBinaryWriterTest writes a BVM
        // when a value is written after a #finish()
        // call. Normalize before the test.
        writer.writeFloat(0.0);
        writer.finish();
        buffer.reset();

        // 0e0 - 32-bit
        writer.writeFloat(0.0);
        writer.finish();
        assertEquals(ivmLength() + 5,
                     buffer.size());
        buffer.reset();

        // +inf - 32-bit
        writer.writeFloat(Double.POSITIVE_INFINITY);
        writer.finish();
        assertEquals(ivmLength() + 5,
                     buffer.size());
        buffer.reset();

        // 64-bit value - fraction overflows 32-bits
        writer.writeFloat(2.147483647e9);
        writer.finish();
        assertEquals(ivmLength() + 9,
                     buffer.size());
        buffer.reset();

        // 64-bit value - exponent overflows 32-bits
        writer.writeFloat(6e128);
        writer.finish();
        assertEquals(ivmLength() + 9,
                     buffer.size());
        buffer.reset();
    }

    private static final String DECIMAL_10_DIGIT  = "1.000000001";
    private static final String DECIMAL_45_DIGIT = "1.00000000000000000000000000000000000000000001";
    @Test
    public void testDecimal() throws Exception
    {
        writer.writeDecimal(null);
        assertValue("null.decimal");

        writer.writeDecimal(BigDecimal.ZERO);
        assertValue("0d0");

        writer.writeDecimal(new BigDecimal(DECIMAL_10_DIGIT));
        assertValue(DECIMAL_10_DIGIT);

        writer.writeDecimal(new BigDecimal(DECIMAL_45_DIGIT));
        assertValue(DECIMAL_45_DIGIT);
    }

    @Test
    public void testTimestamp() throws Exception
    {
        {
            writer.writeTimestamp(null);
            assertValue("null.timestamp");
        }
        {
            final Timestamp ts = Timestamp.valueOf("2015-05-01T12:15:23Z");
            writer.writeTimestamp(ts);
            assertValue(ts.toString());
        }
        {
            final Timestamp ts = Timestamp.valueOf("2015-05-01T12:15:23.122Z");
            writer.writeTimestamp(ts);
            assertValue(ts.toString());
        }
    }

    // note that we stick to system symbols for round trip assertions
    @Test
    public void testSymbol() throws Exception
    {
        writer.writeSymbolToken(null);
        assertValue("null.symbol");

        writer.writeSymbolToken(systemSymbol(NAME_SID));
        assertValue("name");
    }

    @Test
    public void testStringInline() throws Exception
    {
        writer.writeString(null);
        assertValue("null.string");

        writer.writeString("hello");
        assertValue("'''hello'''");
    }

    private static final String STR_127 = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" +
                                          "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

    @Test
    public void testString1ByteLength() throws Exception
    {
        assertEquals(127, STR_127.length());
        writer.writeString(STR_127);
        assertValue("'''" + STR_127 + "'''");
    }

    @Test
    public void testString2ByteLength() throws Exception
    {
        assertEquals(127, STR_127.length());
        writer.writeString(STR_127 + " ");
        assertValue("'''" + STR_127 + " '''");
    }

    // this is a length that cannot fit in up to a two byte pad length (2 ** 14)
    private static final int LONG_STRING_LENGTH = 16384;
    @Test
    public void testStringSidePatch() throws Exception
    {
        final StringBuilder buf = new StringBuilder();
        for (int i = 0; i < LONG_STRING_LENGTH; i++)
        {
            buf.append("Z");
        }
        final String bigStr = buf.toString();
        writer.writeString(bigStr);
        assertValue("'''" + bigStr + "'''");
    }

    @Test
    public void testClob() throws Exception
    {
        writer.writeClob(null);
        assertValue("null.clob");

        writer.writeClob("hello".getBytes("UTF-8"));
        assertValue("{{\"hello\"}}");
    }

    @Test
    public void testBlob() throws Exception
    {
        writer.writeBlob(null);
        assertValue("null.blob");

        writer.writeBlob("hello".getBytes("UTF-8"));
        assertValue("{{aGVsbG8=}}");
    }

    @Test
    public void testSetAnnotations() throws Exception
    {
        writer.setTypeAnnotationSymbols(systemSymbol(NAME_SID), systemSymbol(VERSION_SID));
        writer.writeString("foobar");
        assertValue("name::version::'''foobar'''");
    }

    @Test
    public void testList() throws Exception
    {
        writer.setTypeAnnotationSymbols(systemSymbol(IMPORTS_SID));
        writer.stepIn(IonType.LIST);
        {
            writer.setTypeAnnotationSymbols(systemSymbol(NAME_SID));
            writer.writeString("kumo");
        }
        writer.stepOut();
        assertValue("imports::[name::\"kumo\"]");
    }

    @Test
    public void testSexp() throws Exception
    {
        writer.stepIn(IonType.SEXP);
        {
            writer.writeString("store");
            writer.stepIn(IonType.SEXP);
            {
                writer.writeString("add");
                writer.writeInt(0);
                writer.writeInt(1);
                writer.writeInt(2);
            }
            writer.stepOut();
            writer.writeString("output");
        }
        writer.stepOut();
        assertValue("('''store''' ('''add''' 0 1 2) '''output''')");
    }

    @Test
    public void testStruct() throws Exception
    {
        writer.stepIn(IonType.STRUCT);
        {
            writer.setFieldNameSymbol(systemSymbol(NAME_SID));
            writer.writeString("kumo");

            writer.setFieldNameSymbol(systemSymbol(VERSION_SID));
            writer.writeInt(1);

            writer.setFieldNameSymbol(systemSymbol(IMPORTS_SID));
            writer.stepIn(IonType.LIST);
            {
                writer.writeInt(0);
                writer.writeInt(1);
                writer.writeInt(2);
            }
            writer.stepOut();
        }
        writer.stepOut();
        assertValue("{name:\"kumo\", version:1, imports:[0, 1, 2]}");
    }

    // TODO test large stuff...
}
