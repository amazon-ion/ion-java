package com.amazon.ion.impl.bin;

import com.amazon.ion.Decimal;
import com.amazon.ion.IonType;
import com.amazon.ion.Timestamp;
import com.amazon.ion.impl.bin.utf8.Utf8StringEncoder;
import com.amazon.ion.impl.bin.utf8.Utf8StringEncoderPool;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.converter.ConvertWith;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.util.function.BiFunction;

import static com.amazon.ion.TestUtils.HexStringToByteArray;
import static com.amazon.ion.TestUtils.StringToDecimal;
import static com.amazon.ion.TestUtils.StringToTimestamp;
import static com.amazon.ion.TestUtils.SymbolIdsToLongArray;
import static com.amazon.ion.TestUtils.byteArrayToBitString;
import static com.amazon.ion.TestUtils.byteArrayToHex;
import static com.amazon.ion.TestUtils.byteLengthFromBitString;
import static com.amazon.ion.TestUtils.byteLengthFromHexString;

public class IonEncoder_1_1Test {

    private static BlockAllocator ALLOCATOR = BlockAllocatorProviders.basicProvider().vendAllocator(11);
    private WriteBuffer buf;

    @BeforeEach
    public void setup() {
        buf = new WriteBuffer(ALLOCATOR);
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
        assertWritingValue(expectedBytes, value, IonEncoder_1_1::writeFloatValue);
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
        assertWritingValue(expectedBytes, value, IonEncoder_1_1::writeFloatValue);
    }

    @ParameterizedTest
    @CsvSource({
            "                                 0., 60",
            "                                0e1, 61 03",
            "                               0e63, 61 7F",
            "                               0e64, 62 02 01",
            "                               0e99, 62 8E 01",
            "                                0.0, 61 FF",
            "                               0.00, 61 FD",
            "                              0.000, 61 FB",
            "                              0e-64, 61 81",
            "                              0e-99, 62 76 FE",
            "                                -0., 62 01 00",
            "                               -0e1, 62 03 00",
            "                               -0e3, 62 07 00",
            "                              -0e63, 62 7F 00",
            "                             -0e199, 63 1E 03 00",
            "                              -0e-1, 62 FF 00",
            "                              -0e-2, 62 FD 00",
            "                              -0e-3, 62 FB 00",
            "                             -0e-63, 62 83 00",
            "                             -0e-64, 62 81 00",
            "                             -0e-65, 63 FE FE 00",
            "                            -0e-199, 63 E6 FC 00",
            "                               0.01, 62 FD 01",
            "                                0.1, 62 FF 01",
            "                                  1, 62 01 01",
            "                                1e1, 62 03 01",
            "                                1e2, 62 05 01",
            "                               1e63, 62 7F 01",
            "                               1e64, 63 02 01 01",
            "                            1e65536, 64 04 00 08 01",
            "                                  2, 62 01 02",
            "                                  7, 62 01 07",
            "                                 14, 62 01 0E",
            "                                1.0, 62 FF 0A",
            "                               1.00, 62 FD 64",
            "                               1.27, 62 FD 7F",
            "                               1.28, 63 FD 80 00",
            "                              3.142, 63 FB 46 0C",
            "                            3.14159, 64 F7 2F CB 04",
            "                          3.1415927, 65 F3 77 5E DF 01",
            "                        3.141592653, 66 EF 4D E6 40 BB 00",
            "                     3.141592653590, 67 E9 16 9F 83 75 DB 02",
            "                3.14159265358979323, 69 DF FB A0 9E F6 2F 1E 5C 04",
            "           3.1415926535897932384626, 6B D5 72 49 64 CC AF EF 8F 0F A7 06",
            "      3.141592653589793238462643383, 6D CB B7 3C 92 86 40 9F 1B 01 1F AA 26 0A",
            " 3.14159265358979323846264338327950, 6F C1 8E 29 E5 E3 56 D5 DF C5 10 8F 55 3F 7D 0F",
            "3.141592653589793238462643383279503, F6 21 BF 8F 9F F3 E6 64 55 BE BA A7 96 57 79 E4 9A 00",
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
            //                                    OpCode   Length   YYYYYYYY MMYYYYYY HDDDDDMM mmmmHHHH oooooomm ssoooooo ....ssss Scale+ Coefficient
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
            "1947-12-23T23:59:00.0Z,              11110111 00010001 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00000011",
            "1947-12-23T23:59:00.00Z,             11110111 00010001 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00000101",
            "1947-12-23T23:59:00.000Z,            11110111 00010001 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00000111",
            "1947-12-23T23:59:00.0000Z,           11110111 00010001 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00001001",
            "1947-12-23T23:59:00.00000Z,          11110111 00010001 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00001011",
            "1947-12-23T23:59:00.000000Z,         11110111 00010001 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00001101",
            "1947-12-23T23:59:00.0000000Z,        11110111 00010001 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00001111",
            "1947-12-23T23:59:00.00000000Z,       11110111 00010001 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00010001",
            "1947-12-23T23:59:00.9Z,              11110111 00010011 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00000011 00001001",
            "1947-12-23T23:59:00.99Z,             11110111 00010011 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00000101 01100011",
            "1947-12-23T23:59:00.999Z,            11110111 00010101 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00000111 11100111 00000011",
            "1947-12-23T23:59:00.9999Z,           11110111 00010101 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00001001 00001111 00100111",
            "1947-12-23T23:59:00.99999Z,          11110111 00010111 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00001011 10011111 10000110 00000001",
            "1947-12-23T23:59:00.999999Z,         11110111 00010111 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00001101 00111111 01000010 00001111",
            "1947-12-23T23:59:00.9999999Z,        11110111 00011001 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00001111 01111111 10010110 10011000 00000000",
            "1947-12-23T23:59:00.99999999Z,       11110111 00011001 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00010001 11111111 11100000 11110101 00000101",

            "1947-12-23T23:59:00.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000Z, " +
                    "11110111 00010011 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00110110 00000010",

            "1947-12-23T23:59:00.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
                    "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
                    "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000Z, " +
                    "11110111 00010011 10011011 00000111 11011111 10111011 10000011 00010110 00000000 10100010 00000101",

            "1947-12-23T23:59:00.999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999Z, " +
                    "11110111 10001001 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00110110 00000010 11111111 11111111 11111111 11111111 11111111 11111111 " +
                    "11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111 10011111 00110010 00110001 10001111 11001101 00011001 " +
                    "01001111 00011110 10101000 11001111 11110100 00011000 11010101 00101000 00101011 10101110 00001001 10100100 11011110 01001101 10001111 00100001 11100001 " +
                    "11111101 01101111 11011110 10000011 11100010 00011010 11101101 10001110 10010101 11001101 01010001 11100110 01010110 01010000 11011110 00000110 01001101 " +
                    "11111110 00010100",

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

            // Long form because the fractional seconds are not millis, micros, or nanos
            "2023-12-31T23:59:00.0Z,              11110111 00010001 11100111 00000111 11111111 10111011 10000011 00010110 00000000 00000011",
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
        Utf8StringEncoder.Result result = Utf8StringEncoderPool.getInstance().getOrCreate().encode(value);
        assertWritingValue(expectedBytes, result, IonEncoder_1_1::writeStringValue);
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
        Utf8StringEncoder.Result result = Utf8StringEncoderPool.getInstance().getOrCreate().encode(value);
        assertWritingValue(expectedBytes, result, IonEncoder_1_1::writeSymbolValue);
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
            "2147483647         , E3 F0 DF DF FF 0F"
    })
    public void testWriteSymbolValue(int value, String expectedBytes) {
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
        assertWritingValue(expectedBytes, value, (buffer, bytes) -> IonEncoder_1_1.writeBlobValue(buffer, bytes, 0, bytes.length));
    }

    @Test
    public void testWriteBlobValueForNull() {
        int numBytes = IonEncoder_1_1.writeBlobValue(buf, null, 0, 0);
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
        assertWritingValue(expectedBytes, value, (buffer, bytes) -> IonEncoder_1_1.writeClobValue(buffer, bytes, 0, bytes.length));
    }

    @Test
    public void testWriteClobValueForNull() {
        int numBytes = IonEncoder_1_1.writeClobValue(buf, null, 0, 0);
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

}
