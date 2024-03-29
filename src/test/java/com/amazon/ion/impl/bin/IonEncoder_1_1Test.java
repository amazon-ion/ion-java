package com.amazon.ion.impl.bin;

import com.amazon.ion.BitUtils;
import com.amazon.ion.Decimal;
import com.amazon.ion.IonType;
import com.amazon.ion.Timestamp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.converter.ArgumentConversionException;
import org.junit.jupiter.params.converter.ConvertWith;
import org.junit.jupiter.params.converter.TypedArgumentConverter;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

public class IonEncoder_1_1Test {

    private static BlockAllocator ALLOCATOR = BlockAllocatorProviders.basicProvider().vendAllocator(11);
    private WriteBuffer buf;

    @BeforeEach
    public void setup() {
        buf = new WriteBuffer(ALLOCATOR, () -> {});
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

    /**
     * Checks that the function writes the expected bytes and returns the expected count of written bytes for the
     * given input value. The expected bytes should be a string of space-separated hexadecimal pairs.
     */
    private <T> void assertWritingValue(String expectedBytes, T value, BiFunction<WriteBuffer, T, Integer> writeOperation) {
        int numBytes = writeOperation.apply(buf, value);
        Assertions.assertEquals(expectedBytes, byteArrayToHex(bytes()));
        Assertions.assertEquals(byteLengthFromHexString(expectedBytes), numBytes);
    }

    /**
     * Checks that the function writes the expected bytes and returns the expected count of written bytes for the
     * given input value. The expected bytes should be a string of space-separated hexadecimal pairs.
     */
    private <T> void assertWritingValue(byte[] expectedBytes, T value, BiFunction<WriteBuffer, T, Integer> writeOperation) {
        int numBytes = writeOperation.apply(buf, value);
        Assertions.assertEquals(expectedBytes, bytes());
        Assertions.assertEquals(expectedBytes.length, numBytes);
    }

    /**
     * Checks that the function writes the expected bytes and returns the expected count of written bytes for the
     * given input value. The expectedBytes should be a string of space-separated binary octets.
     */
    private <T> void assertWritingValueWithBinary(String expectedBytes, T value, BiFunction<WriteBuffer, T, Integer> writeOperation) {
        int numBytes = writeOperation.apply(buf, value);
        Assertions.assertEquals(expectedBytes, byteArrayToBitString(bytes()));
        Assertions.assertEquals(byteLengthFromBitString(expectedBytes), numBytes);
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
        assertWritingValue(expectedBytes, value, IonEncoder_1_1::writeNullValue);
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
    public void testWriteBooleanValue(boolean value, String expectedBytes) {
        assertWritingValue(expectedBytes, value, IonEncoder_1_1::writeBoolValue);
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
        assertWritingValue(expectedBytes, value, IonEncoder_1_1::writeIntValue);
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
    public void testWriteIntegerValueForBigInteger(BigInteger value, String expectedBytes) {
        assertWritingValue(expectedBytes, value, IonEncoder_1_1::writeIntValue);
    }

    @Test
    public void testWriteIntegerValueForNullBigInteger() {
        int numBytes = IonEncoder_1_1.writeIntValue(buf, null);
        Assertions.assertEquals("EB 01", byteArrayToHex(bytes()));
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
        assertWritingValue(expectedBytes, value, IonEncoder_1_1::writeFloat);
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
        assertWritingValue(expectedBytes, value, IonEncoder_1_1::writeFloat);
    }

    @ParameterizedTest
    @CsvSource({
            "                           0., 60",
            "                          0e1, 6F 03",
            "                         0e63, 6F 7F",
            "                         0e99, 6F 8E 01",
            "                          0.0, 6F FF",
            "                         0.00, 6F FD",
            "                        0.000, 6F FB",
            "                        0e-64, 6F 81",
            "                        0e-99, 6F 76 FE",
            "                          -0., 61 01",
            "                         -0e1, 62 01 01",
            "                         -0e3, 62 01 03",
            "                       -0e127, 62 01 7F",
            "                       -0e199, 63 01 C7 00",
            "                        -0e-1, 62 01 FF",
            "                        -0e-2, 62 01 FE",
            "                        -0e-3, 62 01 FD",
            "                      -0e-127, 62 01 81",
            "                      -0e-199, 63 01 39 FF",
            "                         0.01, 62 03 FE",
            "                          0.1, 62 03 FF",
            "                            1, 61 03",
            "                          1e1, 62 03 01",
            "                          1e2, 62 03 02",
            "                        1e127, 62 03 7F",
            "                        1e128, 63 03 80 00",
            "                      1e65536, 64 03 00 00 01",
            "                            2, 61 05",
            "                            7, 61 0F",
            "                           14, 61 1D",
            "                          1.0, 62 15 FF",
            "                         1.00, 63 92 01 FE",
            "                         1.27, 63 FE 01 FE",
            "                        3.142, 63 1A 31 FD",
            "                      3.14159, 64 7C 59 26 FB",
            "                     3.141593, 65 98 FD FE 02 FA",
            "                  3.141592653, 66 B0 C9 1C 68 17 F7",
            "                3.14159265359, 67 E0 93 7D 56 49 12 F5",
            "           3.1415926535897932, 69 80 4C 43 76 65 9E 9C 6F F0",
            "  3.1415926535897932384626434, 6E 00 50 E0 DC F7 CC D6 08 48 99 92 3F 03 E7",
            "3.141592653589793238462643383, F6 1F 00 E0 2D 8F A4 21 D0 E7 46 C0 87 AA 89 02 E5",
    })
    public void testWriteDecimalValue(@ConvertWith(StringToDecimal.class) Decimal value, String expectedBytes) {
        assertWritingValue(expectedBytes, value, IonEncoder_1_1::writeDecimalValue);
    }

    @Test
    public void testWriteDecimalValueForNull() {
        int numBytes = IonEncoder_1_1.writeDecimalValue(buf, null);
        Assertions.assertEquals("EB 03", byteArrayToHex(bytes()));
        Assertions.assertEquals(2, numBytes);
    }

    // Because timestamp subfields are smeared across bytes, it's easier to reason about them in 1s and 0s
    // instead of hex digits
    @ParameterizedTest
    @CsvSource({
            //                               OpCode   MYYYYYYY DDDDDMMM mmmHHHHH ssssUmmm ffffffss ffffffff ffffffff ffffffff
            "2023-10-15T01:00Z,              01110011 00110101 01111101 00000001 00001000",
            "2023-10-15T01:59Z,              01110011 00110101 01111101 01100001 00001111",
            "2023-10-15T11:22Z,              01110011 00110101 01111101 11001011 00001010",
            "2023-10-15T23:00Z,              01110011 00110101 01111101 00010111 00001000",
            "2023-10-15T23:59Z,              01110011 00110101 01111101 01110111 00001111",
            "2023-10-15T11:22:00Z,           01110100 00110101 01111101 11001011 00001010 00000000",
            "2023-10-15T11:22:33Z,           01110100 00110101 01111101 11001011 00011010 00000010",
            "2023-10-15T11:22:59Z,           01110100 00110101 01111101 11001011 10111010 00000011",
            "2023-10-15T11:22:33.000Z,       01110101 00110101 01111101 11001011 00011010 00000010 00000000",
            "2023-10-15T11:22:33.444Z,       01110101 00110101 01111101 11001011 00011010 11110010 00000110",
            "2023-10-15T11:22:33.999Z,       01110101 00110101 01111101 11001011 00011010 10011110 00001111",
            "2023-10-15T11:22:33.000000Z,    01110110 00110101 01111101 11001011 00011010 00000010 00000000 00000000",
            "2023-10-15T11:22:33.444555Z,    01110110 00110101 01111101 11001011 00011010 00101110 00100010 00011011",
            "2023-10-15T11:22:33.999999Z,    01110110 00110101 01111101 11001011 00011010 11111110 00001000 00111101",
            "2023-10-15T11:22:33.000000000Z, 01110111 00110101 01111101 11001011 00011010 00000010 00000000 00000000 00000000",
            "2023-10-15T11:22:33.444555666Z, 01110111 00110101 01111101 11001011 00011010 01001010 10000110 11111101 01101001",
            "2023-10-15T11:22:33.999999999Z, 01110111 00110101 01111101 11001011 00011010 11111110 00100111 01101011 11101110",
    })
    public void testWriteTimestampValueWithUtcShortForm(@ConvertWith(StringToTimestamp.class) Timestamp value, String expectedBytes) {
        assertWritingValueWithBinary(expectedBytes, value, IonEncoder_1_1::writeTimestampValue);
    }


    @ParameterizedTest
    @CsvSource({
            //                                    OpCode   MYYYYYYY DDDDDMMM mmmHHHHH ssssUmmm ffffffss ffffffff ffffffff ffffffff
            "1970T,                               01110000 00000000",
            "2023T,                               01110000 00110101",
            "2097T,                               01110000 01111111",
            "2023-01T,                            01110001 10110101 00000000",
            "2023-10T,                            01110001 00110101 00000101",
            "2023-12T,                            01110001 00110101 00000110",
            "2023-10-01T,                         01110010 00110101 00001101",
            "2023-10-15T,                         01110010 00110101 01111101",
            "2023-10-31T,                         01110010 00110101 11111101",
            "2023-10-15T01:00-00:00,              01110011 00110101 01111101 00000001 00000000",
            "2023-10-15T01:59-00:00,              01110011 00110101 01111101 01100001 00000111",
            "2023-10-15T11:22-00:00,              01110011 00110101 01111101 11001011 00000010",
            "2023-10-15T23:00-00:00,              01110011 00110101 01111101 00010111 00000000",
            "2023-10-15T23:59-00:00,              01110011 00110101 01111101 01110111 00000111",
            "2023-10-15T11:22:00-00:00,           01110100 00110101 01111101 11001011 00000010 00000000",
            "2023-10-15T11:22:33-00:00,           01110100 00110101 01111101 11001011 00010010 00000010",
            "2023-10-15T11:22:59-00:00,           01110100 00110101 01111101 11001011 10110010 00000011",
            "2023-10-15T11:22:33.000-00:00,       01110101 00110101 01111101 11001011 00010010 00000010 00000000",
            "2023-10-15T11:22:33.444-00:00,       01110101 00110101 01111101 11001011 00010010 11110010 00000110",
            "2023-10-15T11:22:33.999-00:00,       01110101 00110101 01111101 11001011 00010010 10011110 00001111",
            "2023-10-15T11:22:33.000000-00:00,    01110110 00110101 01111101 11001011 00010010 00000010 00000000 00000000",
            "2023-10-15T11:22:33.444555-00:00,    01110110 00110101 01111101 11001011 00010010 00101110 00100010 00011011",
            "2023-10-15T11:22:33.999999-00:00,    01110110 00110101 01111101 11001011 00010010 11111110 00001000 00111101",
            "2023-10-15T11:22:33.000000000-00:00, 01110111 00110101 01111101 11001011 00010010 00000010 00000000 00000000 00000000",
            "2023-10-15T11:22:33.444555666-00:00, 01110111 00110101 01111101 11001011 00010010 01001010 10000110 11111101 01101001",
            "2023-10-15T11:22:33.999999999-00:00, 01110111 00110101 01111101 11001011 00010010 11111110 00100111 01101011 11101110",
    })
    public void testWriteTimestampValueWithUnknownOffsetShortForm(@ConvertWith(StringToTimestamp.class) Timestamp value, String expectedBytes) {
        assertWritingValueWithBinary(expectedBytes, value, IonEncoder_1_1::writeTimestampValue);
    }

    @ParameterizedTest
    @CsvSource({
            //                                    OpCode   MYYYYYYY DDDDDMMM mmmHHHHH ooooommm ssssssoo ffffffff ffffffff ffffffff ..ffffff
            "2023-10-15T01:00-14:00,              01111000 00110101 01111101 00000001 00000000 00000000",
            "2023-10-15T01:00+14:00,              01111000 00110101 01111101 00000001 10000000 00000011",
            "2023-10-15T01:00-01:15,              01111000 00110101 01111101 00000001 10011000 00000001",
            "2023-10-15T01:00+01:15,              01111000 00110101 01111101 00000001 11101000 00000001",
            "2023-10-15T01:59+01:15,              01111000 00110101 01111101 01100001 11101111 00000001",
            "2023-10-15T11:22+01:15,              01111000 00110101 01111101 11001011 11101010 00000001",
            "2023-10-15T23:00+01:15,              01111000 00110101 01111101 00010111 11101000 00000001",
            "2023-10-15T23:59+01:15,              01111000 00110101 01111101 01110111 11101111 00000001",
            "2023-10-15T11:22:00+01:15,           01111001 00110101 01111101 11001011 11101010 00000001",
            "2023-10-15T11:22:33+01:15,           01111001 00110101 01111101 11001011 11101010 10000101",
            "2023-10-15T11:22:59+01:15,           01111001 00110101 01111101 11001011 11101010 11101101",
            "2023-10-15T11:22:33.000+01:15,       01111010 00110101 01111101 11001011 11101010 10000101 00000000 00000000",
            "2023-10-15T11:22:33.444+01:15,       01111010 00110101 01111101 11001011 11101010 10000101 10111100 00000001",
            "2023-10-15T11:22:33.999+01:15,       01111010 00110101 01111101 11001011 11101010 10000101 11100111 00000011",
            "2023-10-15T11:22:33.000000+01:15,    01111011 00110101 01111101 11001011 11101010 10000101 00000000 00000000 00000000",
            "2023-10-15T11:22:33.444555+01:15,    01111011 00110101 01111101 11001011 11101010 10000101 10001011 11001000 00000110",
            "2023-10-15T11:22:33.999999+01:15,    01111011 00110101 01111101 11001011 11101010 10000101 00111111 01000010 00001111",
            "2023-10-15T11:22:33.000000000+01:15, 01111100 00110101 01111101 11001011 11101010 10000101 00000000 00000000 00000000 00000000",
            "2023-10-15T11:22:33.444555666+01:15, 01111100 00110101 01111101 11001011 11101010 10000101 10010010 01100001 01111111 00011010",
            "2023-10-15T11:22:33.999999999+01:15, 01111100 00110101 01111101 11001011 11101010 10000101 11111111 11001001 10011010 00111011",

    })
    public void testWriteTimestampValueWithKnownOffsetShortForm(@ConvertWith(StringToTimestamp.class) Timestamp value, String expectedBytes) {
        assertWritingValueWithBinary(expectedBytes, value, IonEncoder_1_1::writeTimestampValue);
    }

    @ParameterizedTest
    @CsvSource({
            //                                    OpCode   Length   YYYYYYYY MMYYYYYY HDDDDDMM mmmmHHHH oooooomm ssoooooo ....ssss Coefficient+ Scale
            "0001T,                               11110111 00000101 00000001 00000000",
            "1947T,                               11110111 00000101 10011011 00000111",
            "9999T,                               11110111 00000101 00001111 00100111",
            "1947-01T,                            11110111 00000111 10011011 01000111 00000000",
            "1947-12T,                            11110111 00000111 10011011 00000111 00000011",
            "1947-01-01T,                         11110111 00000111 10011011 01000111 00000100",
            "1947-12-23T,                         11110111 00000111 10011011 00000111 01011111",
            "1947-12-31T,                         11110111 00000111 10011011 00000111 01111111",
            "1947-12-23T00:00Z,                   11110111 00001101 10011011 00000111 01011111 00000000 10000000 00010110",
            "1947-12-23T23:59Z,                   11110111 00001101 10011011 00000111 11011111 10111011 10000011 00010110",
            "1947-12-23T23:59:00Z,                11110111 00001111 10011011 00000111 11011111 10111011 10000011 00010110 00000000",
            "1947-12-23T23:59:59Z,                11110111 00001111 10011011 00000111 11011111 10111011 10000011 11010110 00001110",
            "1947-12-23T23:59:00.0Z,              11110111 00010011 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00000001 00000001",
            "1947-12-23T23:59:00.00Z,             11110111 00010011 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00000001 00000010",
            "1947-12-23T23:59:00.000Z,            11110111 00010011 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00000001 00000011",
            "1947-12-23T23:59:00.0000Z,           11110111 00010011 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00000001 00000100",
            "1947-12-23T23:59:00.00000Z,          11110111 00010011 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00000001 00000101",
            "1947-12-23T23:59:00.000000Z,         11110111 00010011 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00000001 00000110",
            "1947-12-23T23:59:00.0000000Z,        11110111 00010011 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00000001 00000111",
            "1947-12-23T23:59:00.00000000Z,       11110111 00010011 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00000001 00001000",
            "1947-12-23T23:59:00.9Z,              11110111 00010011 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00010011 00000001",
            "1947-12-23T23:59:00.99Z,             11110111 00010011 10011011 00000111 11011111 10111011 10000011 00010110 00000000 11000111 00000010",
            "1947-12-23T23:59:00.999Z,            11110111 00010101 10011011 00000111 11011111 10111011 10000011 00010110 00000000 10011110 00001111 00000011",
            "1947-12-23T23:59:00.9999Z,           11110111 00010101 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00111110 10011100 00000100",
            "1947-12-23T23:59:00.99999Z,          11110111 00010111 10011011 00000111 11011111 10111011 10000011 00010110 00000000 11111100 00110100 00001100 00000101",
            "1947-12-23T23:59:00.999999Z,         11110111 00010111 10011011 00000111 11011111 10111011 10000011 00010110 00000000 11111100 00010001 01111010 00000110",
            "1947-12-23T23:59:00.9999999Z,        11110111 00011001 10011011 00000111 11011111 10111011 10000011 00010110 00000000 11111000 01100111 10001001 00001001 00000111",
            "1947-12-23T23:59:00.99999999Z,       11110111 00011001 10011011 00000111 11011111 10111011 10000011 00010110 00000000 11111000 00001111 01011110 01011111 00001000",

            "1947-12-23T23:59:00.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000Z, " +
                    "11110111 00010011 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00000001 10001101",

            "1947-12-23T23:59:00.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
                    "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
                    "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000Z, " +
                    "11110111 00010101 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00000001 01101000 00000001",

            "1947-12-23T23:59:00.999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999Z, " +
                    "11110111 10010111 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 " +
                    "11111100 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111 " +
                    "11111111 10010100 10001001 01111001 01101100 11001110 01111000 11110010 01000000 01111101 10100110 11000111 10101000 01000110 01011001 01110001 01001101 " +
                    "00100000 11110101 01101110 01111010 00001100 00001001 11101111 01111111 11110011 00011110 00010100 11010111 01101000 01110111 10101100 01101100 10001110 " +
                    "00110010 10110111 10000010 11110010 00110110 01101000 11110010 10100111 10001101",


            // Offsets
            "2048-01-01T01:01-23:59,              11110111 00001101 00000000 01001000 10000100 00010000 00000100 00000000",
            "2048-01-01T01:01-00:02,              11110111 00001101 00000000 01001000 10000100 00010000 01111000 00010110",
            "2048-01-01T01:01-00:01,              11110111 00001101 00000000 01001000 10000100 00010000 01111100 00010110",
            "2048-01-01T01:01-00:00,              11110111 00001101 00000000 01001000 10000100 00010000 11111100 00111111",
            "2048-01-01T01:01+00:00,              11110111 00001101 00000000 01001000 10000100 00010000 10000000 00010110",
            "2048-01-01T01:01+00:01,              11110111 00001101 00000000 01001000 10000100 00010000 10000100 00010110",
            "2048-01-01T01:01+00:02,              11110111 00001101 00000000 01001000 10000100 00010000 10001000 00010110",
            "2048-01-01T01:01+23:59,              11110111 00001101 00000000 01001000 10000100 00010000 11111100 00101100",
    })
    public void testWriteTimestampValueLongForm(@ConvertWith(StringToTimestamp.class) Timestamp value, String expectedBytes) {
        assertWritingValueWithBinary(expectedBytes, value, IonEncoder_1_1::writeLongFormTimestampValue);
    }

    @ParameterizedTest
    @CsvSource({
            // Long form because it's out of the year range
            "0001T,                               11110111 00000101 00000001 00000000",
            "9999T,                               11110111 00000101 00001111 00100111",

            // Long form because the offset is too high/low
            "2048-01-01T01:01+14:15,              11110111 00001101 00000000 01001000 10000100 00010000 11011100 00100011",
            "2048-01-01T01:01-14:15,              11110111 00001101 00000000 01001000 10000100 00010000 00100100 00001001",

            // Long form because the offset is not a multiple of 15
            "2048-01-01T01:01+00:01,              11110111 00001101 00000000 01001000 10000100 00010000 10000100 00010110",

            // Long form because the fractional seconds are millis, micros, or nanos
            "2023-12-31T23:59:00.0Z,              11110111 00010011 11100111 00000111 11111111 10111011 10000011 00010110 00000000 00000001 00000001",
    })
    public void testWriteTimestampDelegatesCorrectlyToLongForm(@ConvertWith(StringToTimestamp.class) Timestamp value, String expectedBytes) {
        assertWritingValueWithBinary(expectedBytes, value, IonEncoder_1_1::writeTimestampValue);
    }

    @Test
    public void testWriteTimestampValueForNullTimestamp() {
        int numBytes = IonEncoder_1_1.writeTimestampValue(buf, null);
        Assertions.assertEquals("EB 04", byteArrayToHex(bytes()));
        Assertions.assertEquals(2, numBytes);
    }

    @ParameterizedTest
    @CsvSource({
            "'', 80",
            "'a', 81 61",
            "'ab', 82 61 62",
            "'abc', 83 61 62 63",
            "'fourteen bytes', 8E 66 6F 75 72 74 65 65 6E 20 62 79 74 65 73",
            "'this has sixteen', F8 21 74 68 69 73 20 68 61 73 20 73 69 78 74 65 65 6E",
            "'variable length encoding', F8 31 76 61 72 69 61 62 6C 65 20 6C 65 6E 67 74 68 20 65 6E 63 6F 64 69 6E 67",
    })
    public void testWriteStringValue(String value, String expectedBytes) {
        assertWritingValue(expectedBytes, value, IonEncoder_1_1::writeStringValue);
    }

    @Test
    public void testWriteStringValueForNull() {
        int numBytes = IonEncoder_1_1.writeStringValue(buf, null);
        Assertions.assertEquals("EB 05", byteArrayToHex(bytes()));
        Assertions.assertEquals(2, numBytes);
    }

    @ParameterizedTest
    @CsvSource({
            "'', 90",
            "'a', 91 61",
            "'ab', 92 61 62",
            "'abc', 93 61 62 63",
            "'fourteen bytes', 9E 66 6F 75 72 74 65 65 6E 20 62 79 74 65 73",
            "'this has sixteen', F9 21 74 68 69 73 20 68 61 73 20 73 69 78 74 65 65 6E",
            "'variable length encoding', F9 31 76 61 72 69 61 62 6C 65 20 6C 65 6E 67 74 68 20 65 6E 63 6F 64 69 6E 67",
    })
    public void testWriteSymbolValue(String value, String expectedBytes) {
        assertWritingValue(expectedBytes, value, IonEncoder_1_1::writeSymbolValue);
    }

    @ParameterizedTest
    @CsvSource({
            "0,                   E1 00",
            "1,                   E1 01",
            "255,                 E1 FF",
            "256,                 E2 00 00",
            "257,                 E2 01 00",
            "512,                 E2 00 01",
            "513,                 E2 01 01",
            "65535,               E2 FF FE",
            "65791,               E2 FF FF",
            "65792,               E3 01",
            "65793,               E3 03",
            "65919,               E3 FF",
            "65920,               E3 02 02",
            "9223372036854775807, E3 00 FF FD FD FF FF FF FF FF"
    })
    public void testWriteSymbolValue(long value, String expectedBytes) {
        assertWritingValue(expectedBytes, value, IonEncoder_1_1::writeSymbolValue);
    }

    @Test
    public void testWriteSymbolValueForNull() {
        int numBytes = IonEncoder_1_1.writeSymbolValue(buf, null);
        Assertions.assertEquals("EB 06", byteArrayToHex(bytes()));
        Assertions.assertEquals(2, numBytes);
    }

    @ParameterizedTest
    @CsvSource({
            "'', FE 01", //
            "20, FE 03 20",
            "49 20 61 70 70 6C 61 75 64 20 79 6F 75 72 20 63 75 72 69 6F 73 69 74 79, " +
                    "FE 31 49 20 61 70 70 6C 61 75 64 20 79 6F 75 72 20 63 75 72 69 6F 73 69 74 79"
    })
    public void testWriteBlobValue(@ConvertWith(HexStringToByteArray.class) byte[] value, String expectedBytes) {
        assertWritingValue(expectedBytes, value, IonEncoder_1_1::writeBlobValue);
    }

    @Test
    public void testWriteBlobValueForNull() {
        int numBytes = IonEncoder_1_1.writeBlobValue(buf, null);
        Assertions.assertEquals("EB 07", byteArrayToHex(bytes()));
        Assertions.assertEquals(2, numBytes);
    }

    @ParameterizedTest
    @CsvSource({
            "'', FF 01",
            "20, FF 03 20",
            "49 20 61 70 70 6C 61 75 64 20 79 6F 75 72 20 63 75 72 69 6F 73 69 74 79, " +
                    "FF 31 49 20 61 70 70 6C 61 75 64 20 79 6F 75 72 20 63 75 72 69 6F 73 69 74 79"
    })
    public void testWriteClobValue(@ConvertWith(HexStringToByteArray.class) byte[] value, String expectedBytes) {
        assertWritingValue(expectedBytes, value, IonEncoder_1_1::writeClobValue);
    }

    @Test
    public void testWriteClobValueForNull() {
        int numBytes = IonEncoder_1_1.writeClobValue(buf, null);
        Assertions.assertEquals("EB 08", byteArrayToHex(bytes()));
        Assertions.assertEquals(2, numBytes);
    }

    @ParameterizedTest
    @CsvSource({
            "            '', ''", // Empty array of annotations
            "            $0, E4 01",
            "           $10, E4 15",
            "          $256, E4 02 04",
            "       $10 $11, E5 15 17",
            "     $256 $257, E5 02 04 06 04",
            "   $10 $11 $12, E6 07 15 17 19",
            "$256 $257 $258, E6 0D 02 04 06 04 0A 04",
    })
    public void testWriteAnnotations(@ConvertWith(SymbolIdsToLongArray.class) long[] value, String expectedBytes) {
        assertWritingValue(expectedBytes, value, IonEncoder_1_1::writeAnnotations);
    }

    @Test
    public void testWriteAnnotationsForNull() {
        int numBytes = IonEncoder_1_1.writeAnnotations(buf, null);
        Assertions.assertEquals("", byteArrayToHex(bytes()));
        Assertions.assertEquals(0, numBytes);
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

    /**
     * Determines the number of bytes needed to represent a series of hexadecimal digits.
     */
    private static int byteLengthFromHexString(String hexString) {
        return (hexString.replaceAll("[^\\dA-F]", "").length()) / 2;
    }

    /**
     * Converts a byte array to a string of bits, such as "00110110 10001001".
     * The purpose of this method is to make it easier to read and write test assertions.
     */
    private static String byteArrayToBitString(byte[] bytes) {
        StringBuilder s = new StringBuilder();
        for (byte aByte : bytes) {
            for (int bit = 7; bit >= 0; bit--) {
                if (((0x01 << bit) & aByte) != 0) {
                    s.append("1");
                } else {
                    s.append("0");
                }
            }
            s.append(" ");
        }
        return s.toString().trim();
    }

    /**
     * Determines the number of bytes needed to represent a series of hexadecimal digits.
     */
    private static int byteLengthFromBitString(String bitString) {
        return (bitString.replaceAll("[^01]", "").length()) / 8;
    }

    /**
     * Converts a String to a Timestamp for a @Parameterized test
     */
    static class StringToTimestamp extends TypedArgumentConverter<String, Timestamp> {
        protected StringToTimestamp() {
            super(String.class, Timestamp.class);
        }

        @Override
        protected Timestamp convert(String source) throws ArgumentConversionException {
            if (source == null) return null;
            return Timestamp.valueOf(source);
        }
    }

    /**
     * Converts a String to a Decimal for a @Parameterized test
     */
    static class StringToDecimal extends TypedArgumentConverter<String, Decimal> {
        protected StringToDecimal() {
            super(String.class, Decimal.class);
        }

        @Override
        protected Decimal convert(String source) throws ArgumentConversionException {
            if (source == null) return null;
            return Decimal.valueOf(source);
        }
    }

    /**
     * Converts a Hex String to a Byte Array for a @Parameterized test
     */
    static class HexStringToByteArray extends TypedArgumentConverter<String, byte[]> {

        private static final CharsetEncoder ASCII_ENCODER =  StandardCharsets.US_ASCII.newEncoder();

        protected HexStringToByteArray() {
            super(String.class, byte[].class);
        }

        @Override
        protected byte[] convert(String source) throws ArgumentConversionException {
            if (source == null) return null;
            if (source.trim().isEmpty()) return new byte[0];
            String[] octets = source.split(" ");
            byte[] result = new byte[octets.length];
            for (int i = 0; i < octets.length; i++) {
                if (octets[i].length() == 1) {
                    char c = octets[i].charAt(0);
                    if (!ASCII_ENCODER.canEncode(c)) {
                        throw new IllegalArgumentException("Cannot convert non-ascii character: " + c);
                    }
                    result[i] = (byte) c;
                } else {
                    result[i] = (byte) Integer.parseInt(octets[i], 16);
                }
            }
            return result;
        }
    }

    /**
     * Converts a String of symbol ids to a long[] for a @Parameterized test
     */
    static class SymbolIdsToLongArray extends TypedArgumentConverter<String, long[]> {
        protected SymbolIdsToLongArray() {
            super(String.class, long[].class);
        }

        @Override
        protected long[] convert(String source) throws ArgumentConversionException {
            if (source == null) return null;
            int size = (int) source.chars().filter(i -> i == '$').count();
            String[] sids = source.split("\\$");
            long[] result = new long[size];
            int i = 0;
            for (String sid : sids) {
                if (sid.isEmpty()) continue;
                result[i] = Long.parseLong(sid.trim());
                i++;
            }
            return result;
        }
    }
}
