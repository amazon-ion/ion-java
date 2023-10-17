package com.amazon.ion.impl.bin;

import com.amazon.ion.IonType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;

public class IonEncoder_1_1Test {

    private static BlockAllocator ALLOCATOR = BlockAllocatorProviders.basicProvider().vendAllocator(11);
    private WriteBuffer buf;
    private ByteArrayOutputStream out;

    @BeforeEach
    public void setup() {
        buf = new WriteBuffer(ALLOCATOR);
        out = new ByteArrayOutputStream();
    }

    @AfterEach
    public void teardown() {
        buf = null;
        out.reset();
    }

    private byte[] bytes() {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            buf.writeTo(out);
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
        return out.toByteArray();
    }

    @ParameterizedTest
    @CsvSource({
            "     NULL, EA",
            "     BOOL, EB 00",
            "      INT, EB 01",
            "    FLOAT, EB 02",
            "  DECIMAL, EB 03",
            "TIMESTAMP, EB 04",
            "   STRING, EB 05",
            "   SYMBOL, EB 06",
            "     BLOB, EB 07",
            "     CLOB, EB 08",
            "     LIST, EB 09",
            "     SEXP, EB 0A",
            "   STRUCT, EB 0B",
    })
    public void testWriteNullValue(IonType value, String expectedBytes) {
        int numBytes = IonEncoder_1_1.writeNullValue(buf, value);
        String actualBytes = byteArrayToHex(bytes());
        Assertions.assertEquals(expectedBytes, actualBytes);
        Assertions.assertEquals((expectedBytes.length() + 1)/3, numBytes);
    }

    @Test
    public void testWriteNullValueForDatagram() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> IonEncoder_1_1.writeNullValue(buf, IonType.DATAGRAM));
    }

    @ParameterizedTest
    @CsvSource({
            "true, 5E",
            "false, 5F",
    })
    public void testWriteBooleanValue(Boolean value, String expectedBytes) {
        int numBytes = IonEncoder_1_1.writeBoolValue(buf, value);
        String actualBytes = byteArrayToHex(bytes());
        Assertions.assertEquals(expectedBytes, actualBytes);
        Assertions.assertEquals((expectedBytes.length() + 1)/3, numBytes);
    }

    @ParameterizedTest
    @CsvSource({
            "                   0, 50",
            "                   1, 51 01",
            "                  17, 51 11",
            "                 127, 51 7F",
            "                 128, 52 80 00",
            "                5555, 52 B3 15",
            "               32767, 52 FF 7F",
            "               32768, 53 00 80 00",
            "              292037, 53 C5 74 04",
            "           321672342, 54 96 54 2C 13",
            "         64121672342, 55 96 12 F3 ED 0E",
            "       1274120283167, 56 1F A4 7C A7 28 01",
            "     851274120283167, 57 1F C4 8B B3 3A 06 03",
            "   72624976668147840, 58 80 40 20 10 08 04 02 01",
            " 9223372036854775807, 58 FF FF FF FF FF FF FF 7F", // Long.MAX_VALUE
            "                  -1, 51 FF",
            "                  -2, 51 FE",
            "                 -14, 51 F2",
            "                -128, 51 80",
            "                -129, 52 7F FF",
            "                -944, 52 50 FC",
            "              -32768, 52 00 80",
            "              -32769, 53 FF 7F FF",
            "            -8388608, 53 00 00 80",
            "            -8388609, 54 FF FF 7F FF",
            "  -72624976668147841, 58 7F BF DF EF F7 FB FD FE",
            "-9223372036854775808, 58 00 00 00 00 00 00 00 80", // Long.MIN_VALUE
    })
    public void testWriteIntegerValue(long value, String expectedBytes) {
        int numBytes = IonEncoder_1_1.writeIntValue(buf, value);
        String actualBytes = byteArrayToHex(bytes());
        Assertions.assertEquals(expectedBytes, actualBytes);
        Assertions.assertEquals((expectedBytes.length() + 1)/3, numBytes);
    }

    @ParameterizedTest
    @CsvSource({
            "                             0, 50",
            "                             1, 51 01",
            "                            17, 51 11",
            "                           127, 51 7F",
            "                           128, 52 80 00",
            "                          5555, 52 B3 15",
            "                         32767, 52 FF 7F",
            "                         32768, 53 00 80 00",
            "                        292037, 53 C5 74 04",
            "                     321672342, 54 96 54 2C 13",
            "                   64121672342, 55 96 12 F3 ED 0E",
            "                 1274120283167, 56 1F A4 7C A7 28 01",
            "               851274120283167, 57 1F C4 8B B3 3A 06 03",
            "             72624976668147840, 58 80 40 20 10 08 04 02 01",
            "           9223372036854775807, 58 FF FF FF FF FF FF FF 7F", // Long.MAX_VALUE
            "           9223372036854775808, F5 13 00 00 00 00 00 00 00 80 00",
            "999999999999999999999999999999, F5 1B FF FF FF 3F EA ED 74 46 D0 9C 2C 9F 0C",
            "                            -1, 51 FF",
            "                            -2, 51 FE",
            "                           -14, 51 F2",
            "                          -128, 51 80",
            "                          -129, 52 7F FF",
            "                          -944, 52 50 FC",
            "                        -32768, 52 00 80",
            "                        -32769, 53 FF 7F FF",
            "                      -8388608, 53 00 00 80",
            "                      -8388609, 54 FF FF 7F FF",
            "            -72624976668147841, 58 7F BF DF EF F7 FB FD FE",
            "          -9223372036854775808, 58 00 00 00 00 00 00 00 80", // Long.MIN_VALUE
            "          -9223372036854775809, F5 13 FF FF FF FF FF FF FF 7F FF",
            "-99999999999999999999999999999, F5 1B 01 00 00 60 35 E8 8D 92 51 F0 E1 BC FE",
    })
    public void testWriteIntegerValueForBigInteger(String value, String expectedBytes) {
        int numBytes = IonEncoder_1_1.writeIntValue(buf, new BigInteger(value));
        String actualBytes = byteArrayToHex(bytes());
        Assertions.assertEquals(expectedBytes, actualBytes);
        Assertions.assertEquals((expectedBytes.length() + 1)/3, numBytes);
    }

    @Test
    public void testWriteIntegerValueForNullBigInteger() {
        int numBytes = IonEncoder_1_1.writeIntValue(buf, null);
        String actualBytes = byteArrayToHex(bytes());
        Assertions.assertEquals("EB 01", actualBytes);
        Assertions.assertEquals(2, numBytes);
    }

    @ParameterizedTest
    @CsvSource({
            "            0.0, 5A",
            "            1.0, 5C 3F 80 00 00",
            "            1.5, 5C 3F C0 00 00",
            "      3.1415927, 5C 40 49 0F DB",
            "  4.00537109375, 5C 40 80 2C 00",
            "   423542.09375, 5C 48 CE CE C3",
            " 3.40282347E+38, 5C 7F 7F FF FF", // Float.MAX_VALUE
            "           -1.0, 5C BF 80 00 00",
            "           -1.5, 5C BF C0 00 00",
            "     -3.1415927, 5C C0 49 0F DB",
            " -4.00537109375, 5C C0 80 2C 00",
            "  -423542.09375, 5C C8 CE CE C3",
            "-3.40282347E+38, 5C FF 7F FF FF", // Float.MIN_VALUE
            "            NaN, 5C 7F C0 00 00",
            "       Infinity, 5C 7F 80 00 00",
            "      -Infinity, 5C FF 80 00 00",
    })
    public void testWriteFloatValue(float value, String expectedBytes) {
        int numBytes = IonEncoder_1_1.writeFloat(buf, value);
        String actualBytes = byteArrayToHex(bytes());
        Assertions.assertEquals(expectedBytes, actualBytes);
        Assertions.assertEquals((expectedBytes.length() + 1)/3, numBytes);
    }

    @ParameterizedTest
    @CsvSource({
            "                      0.0, 5A",
            "                      1.0, 5C 3F 80 00 00",
            "                      1.5, 5C 3F C0 00 00",
            "        3.141592653589793, 5D 40 09 21 FB 54 44 2D 18",
            "            4.00537109375, 5C 40 80 2C 00",
            "            4.11111111111, 5D 40 10 71 C7 1C 71 C2 39",
            "             423542.09375, 5C 48 CE CE C3",
            "         8236423542.09375, 5D 41 FE AE DD 97 61 80 00",
            " 1.79769313486231570e+308, 5D 7F EF FF FF FF FF FF FF", // Double.MAX_VALUE
            "                     -1.0, 5C BF 80 00 00",
            "                     -1.5, 5C BF C0 00 00",
            "       -3.141592653589793, 5D C0 09 21 FB 54 44 2D 18",
            "           -4.00537109375, 5C C0 80 2C 00",
            "           -4.11111111111, 5D C0 10 71 C7 1C 71 C2 39",
            "            -423542.09375, 5C C8 CE CE C3",
            "        -8236423542.09375, 5D C1 FE AE DD 97 61 80 00",
            "-1.79769313486231570e+308, 5D FF EF FF FF FF FF FF FF", // Double.MIN_VALUE
            "                      NaN, 5C 7F C0 00 00",
            "                 Infinity, 5C 7F 80 00 00",
            "                -Infinity, 5C FF 80 00 00",
    })
    public void testWriteFloatValueForDouble(double value, String expectedBytes) {
        int numBytes = IonEncoder_1_1.writeFloat(buf, value);
        String actualBytes = byteArrayToHex(bytes());
        Assertions.assertEquals(expectedBytes, actualBytes);
        Assertions.assertEquals((expectedBytes.length() + 1)/3, numBytes);
    }

    /**
     * Utility method to make it easier to write test cases that assert specific sequences of bytes.
     */
    private static String byteArrayToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X ", b));
        }
        return sb.toString().trim();
    }
}
