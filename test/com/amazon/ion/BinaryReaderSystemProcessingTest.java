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

package com.amazon.ion;


import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

import static com.amazon.ion.SystemSymbols.ION_1_0;
import static com.amazon.ion.SystemSymbols.ION_1_0_SID;

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
}
