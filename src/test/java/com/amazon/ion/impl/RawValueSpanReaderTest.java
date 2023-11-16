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

package com.amazon.ion.impl;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.OffsetSpan;
import com.amazon.ion.RawValueSpanProvider;
import com.amazon.ion.RawValueSpanReaderBasicTest;
import com.amazon.ion.SeekableReader;
import com.amazon.ion.Span;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.TestUtils;
import static com.amazon.ion.TestUtils.GLOBAL_SKIP_LIST;
import static com.amazon.ion.TestUtils.GOOD_IONTESTS_FILES;
import static com.amazon.ion.TestUtils.testdataFiles;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.amazon.ion.junit.Injected;
import com.amazon.ion.junit.Injected.Inject;
import com.amazon.ion.system.IonReaderBuilder;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Tests the {@link RawValueSpanProvider} reader facet, which provides access
 * to the reader's underlying byte buffer and vends OffsetSpans that provide
 * positions of the current value in that buffer.
 *
 * @see RawValueSpanReaderBasicTest
 */
@SuppressWarnings({"deprecation", "javadoc"})
@RunWith(Injected.class)
public class RawValueSpanReaderTest
{

    @Inject("testFile")
    public static final File[] FILES =
        testdataFiles(new TestUtils.And(GLOBAL_SKIP_LIST, new FilenameFilter(){

            public boolean accept(File dir, String name)
            {
                // Only accepting binary files because the RawValueSpanProvider
                // facet is currently only compatible with the binary reader.
                return name.endsWith(".10n");
            }

        }), GOOD_IONTESTS_FILES);


    private File myTestFile;

    public void setTestFile(File file)
    {
        myTestFile = file;
    }

    private IonReader reader;
    private byte[] inputBytes;
    private RawValueSpanProvider spanProvider;
    private SeekableReader seekableReader;

    /*
     * Returns a byte array that contains the value ONLY (i.e. no type code
     * or length), as encoded by Ion.
     */
    private byte[] valueBytes(OffsetSpan valueSpan)
    {
        // In a real-world use case, this byte buffer may already be allocated.
        int valueSize = (int)(valueSpan.getFinishOffset() - valueSpan.getStartOffset());
        byte[] value = new byte[valueSize];
        System.arraycopy(spanProvider.buffer(), (int)valueSpan.getStartOffset(), value, 0, value.length);
        return value;
    }

    private static class SpanTester
    {
        final Object expected;
        final byte[] expectedBytes;
        final IonType expectedType;
        final Span span;

        SpanTester(Object expected, IonType expectedType, byte[] expectedBytes, Span span)
        {
            this.expected = expected;
            this.expectedBytes = expectedBytes;
            this.expectedType = expectedType;
            this.span = span;
        }
    }

    private SpanTester generateSpan(Object expected, IonType type)
    {
        // This span includes the TID and length bytes
        Span span = seekableReader.currentSpan();
        // This gets just the value bytes
        byte[] expectedBytes = valueBytes((OffsetSpan)spanProvider.valueSpan());
        return new SpanTester(expected, type, expectedBytes, span);
    }

    private void generateSpans(List<SpanTester> output)
    {
        IonType type = null;
        while ((type = reader.next()) != null)
        {
            if (reader.isNullValue())
            {
                output.add(generateSpan(null, type));
                continue;
            }
            switch (type)
            {
                case BOOL:
                    output.add(generateSpan(reader.booleanValue(), type));
                    break;
                case INT:
                    output.add(generateSpan(reader.bigIntegerValue(), type));
                    break;
                case FLOAT:
                    output.add(generateSpan(reader.doubleValue(), type));
                    break;
                case DECIMAL:
                    output.add(generateSpan(reader.bigDecimalValue(), type));
                    break;
                case TIMESTAMP:
                    output.add(generateSpan(reader.timestampValue(), type));
                    break;
                case STRING:
                    output.add(generateSpan(reader.stringValue(), type));
                    break;
                case SYMBOL:
                    output.add(generateSpan(reader.symbolValue(), type));
                    break;
                case BLOB:
                case CLOB:
                    output.add(generateSpan(reader.newBytes(), type));
                    break;
                case LIST:
                case SEXP:
                case STRUCT:
                    // 'expected' not used for containers because there is no
                    // corresponding *Value() method on the reader. Instead,
                    // byte and position comparisons will be used, and the
                    // containers' values will be recursively checked.
                    output.add(generateSpan(null, type));
                    reader.stepIn();
                    generateSpans(output);
                    reader.stepOut();
                    break;
                default:
                    throw new IllegalStateException("unexpected type: " + type);
            }
        }
    }

    private void assertSpan(SpanTester tester) throws Exception
    {

        IonType type = tester.expectedType;
        seekableReader.hoist(tester.span); // seeks back to the given span
        assertEquals(type, reader.next());

        Object expected = tester.expected;
        if (reader.isNullValue())
        {
            assertEquals(expected, null);
        }
        else
        {
            OffsetSpan valueSpan = (OffsetSpan)spanProvider.valueSpan();
            switch(type)
            {
                case BOOL:
                    assertEquals(expected, reader.booleanValue());
                    break;
                case INT:
                    assertEquals(expected, reader.bigIntegerValue());
                    break;
                case FLOAT:
                    assertEquals(expected, reader.doubleValue());
                    break;
                case DECIMAL:
                    assertEquals(expected, reader.bigDecimalValue());
                    break;
                case TIMESTAMP:
                    assertEquals(expected, reader.timestampValue());
                    break;
                case STRING:
                    // A common use case will be to pass strings along without
                    // decoding. This tests that case.
                    assertArrayEquals(((String)expected).getBytes("UTF-8"), valueBytes(valueSpan));
                    assertEquals(expected, reader.stringValue());
                    break;
                case SYMBOL:
                    // SymbolTokenImpl does not override .equals
                    SymbolToken expectedToken = (SymbolToken)expected;
                    SymbolToken actualToken = reader.symbolValue();
                    assertEquals(expectedToken.getSid(), actualToken.getSid());
                    assertEquals(expectedToken.getText(), actualToken.getText());

                    break;
                case BLOB:
                case CLOB:
                    assertArrayEquals((byte[])expected, reader.newBytes());
                    break;
                case STRUCT:
                case LIST:
                case SEXP:
                    reader.stepIn();
                    if (reader.next() != null)
                    {
                        // The start position of the container's value span should
                        // be the same as the start position of its first element's
                        // seekable span.
                        long expectedValueStart = valueSpan.getStartOffset();
                        long expectedValueEnd = ((OffsetSpan) seekableReader.currentSpan()).getStartOffset();

                        // skips any nop pad to get at the actual value start
                        expectedValueStart += countNopPad((int) expectedValueStart);

                        if (reader.isInStruct())
                        {
                            // In structs, however, value spans will start before
                            // the first value's field name SID (VarUInt - 7
                            // bits per byte, hence division by 0x80).
                            expectedValueStart += (reader.getFieldNameSymbol().getSid() / 0x80) + 1;
                        }

                        assertEquals(expectedValueStart, expectedValueEnd);
                    }

                    reader.stepOut();

                    break;
                default:
                    throw new IllegalStateException("unexpected type: " + type);
            }
            assertArrayEquals(tester.expectedBytes, valueBytes(valueSpan));
            // All spans over the same value, no matter where they started, should finish at
            // the same position.
            assertEquals(((OffsetSpan)tester.span).getFinishOffset(), valueSpan.getFinishOffset());
        }
    }

    private int readVarUInt(int start) {
        int currentByte = 0;
        int result = 0;
        while ((currentByte & 0x80) == 0) {
            currentByte = inputBytes[start++];
            result = (result << 7) | (currentByte & 0x7F);
        }
        return result;
    }

    private int countNopPad(int start)
    {
        int index = start;
        int len = 0;

        if (reader.isInStruct())
        {
            // In structs nop pads have a sid before then
            index += (reader.getFieldNameSymbol().getSid() / 0x80) + 1;
            len += index - start;
        }

        int td = inputBytes[index++] & 0xFF;
        int tid = _Private_IonConstants.getTypeCode(td);
        int typeLen = _Private_IonConstants.getLowNibble(td);

        if(tid == _Private_IonConstants.tidNull && typeLen != _Private_IonConstants.lnIsNull){
            if(typeLen == _Private_IonConstants.lnIsVarLen) {
                len += readVarUInt(index);
            }
            else {
                len += 1;
            }

            return len;
        }

        return 0;
    }

    private static byte[] readFileAsBytes(File file) throws IOException
    {
        FileInputStream in = new FileInputStream(file);
        byte[] data = new byte[(int)file.length()];
        in.read(data);
        in.close();
        return data;
    }

    /**
     * Retrieve a span for each value. Seek back to them in any order and assert
     * that the values can be retrieved both through raw inspection of the
     * underlying buffer and through the high-level IonReader APIs.
     */
    @Test
    public void testSpans() throws Exception
    {
        // Seeking currently only works over byte-backed IonReaders -- not InputStreams
        inputBytes = readFileAsBytes(myTestFile);
        reader = IonReaderBuilder.standard().build(inputBytes);
        spanProvider = reader.asFacet(RawValueSpanProvider.class);
        seekableReader = reader.asFacet(SeekableReader.class);

        List<SpanTester> spans = new ArrayList<SpanTester>();
        generateSpans(spans);
        Collections.shuffle(spans); // spans can be revisited in any order
        for (SpanTester span : spans)
        {
            assertSpan(span);
        }
    }
}
