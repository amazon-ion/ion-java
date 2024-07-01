// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion;

import com.amazon.ion.system.IonReaderBuilder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class IonReaderValueCoercionTest {

    static Stream<InputStream> testData() {
        byte[] textData = "$ion_1_0 42 42e0 42.0".getBytes(StandardCharsets.UTF_8);
        byte[] binaryData = {
                // Version marker 1.0
                (byte) 0xE0, 0x01, 0x00, (byte) 0xEA,
                // 1-byte int
                0x21,
                // `42`
                0x2A,
                // 4-byte float
                0x44,
                // `42e0`
                0x42, 0x28, 0x00, 0x00,
                // 3-byte decimal
                0x53,
                // `42d0`
                (byte) 0xc1, 0x01, (byte) 0xa4,
                0x52,
        };
        return Stream.of(
                new ByteArrayInputStream(textData),
                new ByteArrayInputStream(binaryData)
        );
    }

    @ParameterizedTest
    @MethodSource("testData")
    void coerceNumberToDouble(InputStream inputStream) {
        IonReader reader = IonReaderBuilder.standard().build(inputStream);
        assertEquals(IonType.INT, reader.next());
        assertEquals(reader.doubleValue(), 42.0);
        assertEquals(IonType.FLOAT, reader.next());
        assertEquals(reader.doubleValue(), 42.0);
        assertEquals(IonType.DECIMAL, reader.next());
        assertEquals(reader.doubleValue(), 42.0);
    }

    @ParameterizedTest
    @MethodSource("testData")
    void coerceNumberToBigDecimal(InputStream inputStream) {
        IonReader reader = IonReaderBuilder.standard().build(inputStream);
        assertEquals(IonType.INT, reader.next());
        assertEquals(reader.bigDecimalValue(), BigDecimal.valueOf(42));
        assertEquals(IonType.FLOAT, reader.next());
        assertEquals(reader.bigDecimalValue(), BigDecimal.valueOf(42.0));
        assertEquals(IonType.DECIMAL, reader.next());
        assertEquals(reader.bigDecimalValue(), BigDecimal.valueOf(42.0));
    }

    @ParameterizedTest
    @MethodSource("testData")
    void coerceNumberToDecimal(InputStream inputStream) {
        IonReader reader = IonReaderBuilder.standard().build(inputStream);
        assertEquals(IonType.INT, reader.next());
        assertEquals(reader.decimalValue(), Decimal.valueOf(42));
        assertEquals(IonType.FLOAT, reader.next());
        assertEquals(reader.decimalValue(), Decimal.valueOf(42.0));
        assertEquals(IonType.DECIMAL, reader.next());
        assertEquals(reader.decimalValue(), Decimal.valueOf(42.0));
    }
}