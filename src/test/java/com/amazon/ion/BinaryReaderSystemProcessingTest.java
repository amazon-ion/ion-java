// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion;


import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Iterator;

import static com.amazon.ion.SystemSymbols.ION_1_0;
import static com.amazon.ion.SystemSymbols.ION_1_0_SID;
import static com.amazon.ion.TestUtils.cleanCommentedHexBytes;
import static com.amazon.ion.TestUtils.hexStringToByteArray;

public class BinaryReaderSystemProcessingTest
    extends ReaderSystemProcessingTestCase
{
    protected byte[] myBytes;


    @Override
    protected void prepare(String text)
        throws Exception
    {
        myMissingSymbolTokensHaveText = false;

        IonLoader loader = loader();
        IonDatagram datagram = loader.load(text);
        myBytes = datagram.getBytes();
    }

    @Override
    public IonReader read() throws Exception
    {
        return getStreamingMode().newIonReader(system().getCatalog(), myBytes);
    }

    @Override
    public IonReader systemRead() throws Exception
    {
        return system().newSystemReader(myBytes);
    }

    private void prepare(int... binary) {
        myMissingSymbolTokensHaveText = false;
        myBytes = BitUtils.bytes(binary);
    }

    @Test
    public void testMultipleIvmsBetweenValues()
        throws Exception
    {
        prepare(
            0xE0, 0x01, 0x00, 0xEA, // IVM
            0xE0, 0x01, 0x00, 0xEA, // IVM
            0x21, 0x01, // 1
            0xE0, 0x01, 0x00, 0xEA, // IVM
            0x21, 0x02, // 2
            0xE0, 0x01, 0x00, 0xEA, // IVM
            0xE0, 0x01, 0x00, 0xEA, // IVM
            0xE0, 0x01, 0x00, 0xEA, // IVM
            0x21, 0x03, // 3
            0xE0, 0x01, 0x00, 0xEA, // IVM
            0xE0, 0x01, 0x00, 0xEA  // IVM
        );
        startSystemIteration();

        nextValue();
        checkSymbol(ION_1_0, ION_1_0_SID);
        nextValue();
        checkSymbol(ION_1_0, ION_1_0_SID);
        nextValue();
        checkInt(1);

        nextValue();
        checkSymbol(ION_1_0, ION_1_0_SID);
        nextValue();
        checkInt(2);

        nextValue();
        checkSymbol(ION_1_0, ION_1_0_SID);
        nextValue();
        checkSymbol(ION_1_0, ION_1_0_SID);
        nextValue();
        checkSymbol(ION_1_0, ION_1_0_SID);
        nextValue();
        checkInt(3);

        nextValue();
        checkSymbol(ION_1_0, ION_1_0_SID);
        nextValue();
        checkSymbol(ION_1_0, ION_1_0_SID);
    }

    @Test
    public void testSymbolTokensInSystemSymbolTable()
        throws Exception
    {
        prepare("{name: imports::max_id}");
        startSystemIteration();

        nextValue();
        checkSymbol(ION_1_0, ION_1_0_SID);
        nextValue();
        checkType(IonType.STRUCT);
        myReader.stepIn();
        nextValue();
        checkFieldName("name", SystemSymbols.NAME_SID);
        checkAnnotation("imports", SystemSymbols.IMPORTS_SID);
        checkSymbol("max_id", SystemSymbols.MAX_ID_SID);
        myReader.stepOut();
        checkTopEof();
    }

    @Test
    public void testSymbolTokensInLocalSymbolTable()
        throws Exception
    {
        prepare("{foo: bar::baz}");
        startSystemIteration();

        nextValue();
        checkSymbol(ION_1_0, ION_1_0_SID);

        nextValue();
        checkType(IonType.STRUCT);
        checkAnnotation(SystemSymbols.ION_SYMBOL_TABLE, SystemSymbols.ION_SYMBOL_TABLE_SID);
        myReader.stepIn();
        nextValue();
        checkType(IonType.LIST);
        checkFieldName(SystemSymbols.SYMBOLS, SystemSymbols.SYMBOLS_SID);
        myReader.stepIn();
        nextValue();
        checkString("baz");
        nextValue();
        checkString("bar");
        nextValue();
        checkString("foo");
        myReader.stepOut();
        myReader.stepOut();

        nextValue();
        checkType(IonType.STRUCT);
        myReader.stepIn();
        nextValue();
        checkFieldName(null, SystemSymbols.ION_1_0_MAX_ID + 3);
        checkAnnotation(null, SystemSymbols.ION_1_0_MAX_ID + 2);
        checkSymbol(null, SystemSymbols.ION_1_0_MAX_ID + 1);
        myReader.stepOut();
        checkTopEof();
    }

    @Test
    public void testSystemReaderReadsUserValues()
        throws Exception
    {
        prepare("true 123 1.23e0 1.23d0 2023T {{ YWJj }}");

        startSystemIteration();

        nextValue();
        checkSymbol(ION_1_0, ION_1_0_SID);

        nextValue();
        checkType(IonType.BOOL);
        assertTrue(myReader.booleanValue());

        nextValue();
        checkType(IonType.INT);
        assertEquals(IntegerSize.INT, myReader.getIntegerSize());
        assertEquals(123, myReader.intValue());
        assertEquals(123, myReader.longValue());
        assertEquals(BigInteger.valueOf(123), myReader.bigIntegerValue());

        nextValue();
        checkFloat(1.23e0);

        nextValue();
        checkType(IonType.DECIMAL);
        assertEquals(BigDecimal.valueOf(123, 2), myReader.bigDecimalValue());
        assertEquals(Decimal.valueOf(123, 2), myReader.decimalValue());

        nextValue();
        checkType(IonType.TIMESTAMP);
        assertEquals(Timestamp.valueOf("2023T"), myReader.timestampValue());
        assertEquals(Timestamp.valueOf("2023T").dateValue(), myReader.dateValue());

        nextValue();
        checkType(IonType.BLOB);
        assertEquals(3, myReader.byteSize());
        byte[] expected = new byte[]{'a', 'b', 'c'};
        assertArrayEquals(expected, myReader.newBytes());
        byte[] result = new byte[3];
        assertEquals(3, myReader.getBytes(result, 0, 3));
        assertArrayEquals(expected, result);

        checkTopEof();
    }

    private void prepareBinary(String commentedHexBytes) {
        myMissingSymbolTokensHaveText = false;
        myBytes = hexStringToByteArray(cleanCommentedHexBytes(commentedHexBytes));
    }

    @Test
    public void inlineFieldName() throws Exception {
        prepareBinary(
            "E0 01 01 EA | Ion 1.1 IVM \n" +
            "FD          | Variable-length struct \n" +
            "0F          | Length 7 \n" +
            "01          | Switch to FlexSym field names \n" +
            "FF          | Inline field name, length 1 \n" +
            "61          | UTF-8 byte 'a' \n" +
            "D3          | Struct length 3 \n" +
            "09          | Field name SID 4 ('name') \n" +
            "A1          | Inline symbol value, length 1 \n" +
            "62          | UTF-8 byte 'b' \n"
        );
        IonReader reader = systemRead();

        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("$ion_1_1", reader.symbolValue().getText());

        assertEquals(IonType.STRUCT, reader.next());
        reader.stepIn();
        assertEquals(IonType.STRUCT, reader.next());
        assertEquals("a", reader.getFieldName());
        SymbolToken aToken = reader.getFieldNameSymbol();
        assertEquals("a", aToken.getText());
        assertEquals(-1, aToken.getSid());
        reader.stepIn();
        assertNull(reader.getFieldNameSymbol());
        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("name", reader.getFieldName());
        assertEquals("b", reader.stringValue());
        SymbolToken bToken = reader.symbolValue();
        assertEquals("b", bToken.getText());
        assertEquals(-1, bToken.getSid());
        assertNull(reader.next());
        assertNull(reader.getFieldName());
        reader.stepOut();
        assertNull(reader.getFieldName());
        assertNull(reader.next());
        reader.stepOut();
        assertNull(reader.next());
        reader.close();
    }

    @Test
    public void inlineAnnotation() throws Exception {
        prepareBinary(
            "E0 01 01 EA | Ion 1.1 IVM \n" +
            "E8          | Two annotation FlexSyms follow \n" +
            "09          | Annotation SID 4 ('name') \n" +
            "FF          | Inline field name, length 1 \n" +
            "61          | UTF-8 byte 'a' \n" +
            "6F          | boolean false\n"
        );
        IonReader reader = systemRead();

        assertEquals(IonType.SYMBOL, reader.next());
        assertEquals("$ion_1_1", reader.symbolValue().getText());

        assertEquals(IonType.BOOL, reader.next());
        String[] annotations = reader.getTypeAnnotations();
        assertEquals(2, annotations.length);
        assertEquals("name", annotations[0]);
        assertEquals("a", annotations[1]);
        SymbolToken[] annotationTokens = reader.getTypeAnnotationSymbols();
        assertEquals(2, annotationTokens.length);
        assertEquals("name", annotationTokens[0].getText());
        assertEquals(4, annotationTokens[0].getSid());
        assertEquals("a", annotationTokens[1].getText());
        assertEquals(-1, annotationTokens[1].getSid());
        Iterator<String> annotationIterator = reader.iterateTypeAnnotations();
        assertTrue(annotationIterator.hasNext());
        assertEquals("name", annotationIterator.next());
        assertTrue(annotationIterator.hasNext());
        assertEquals("a", annotationIterator.next());
        assertFalse(reader.booleanValue());
        assertNull(reader.next());
        assertEquals(0, reader.getTypeAnnotations().length);
        reader.close();
    }
}
