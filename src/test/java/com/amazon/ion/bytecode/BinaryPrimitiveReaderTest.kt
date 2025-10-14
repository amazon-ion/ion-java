package com.amazon.ion.bytecode

import com.amazon.ion.TextToBinaryUtils.hexStringToByteArray
import java.math.BigInteger
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

class BinaryPrimitiveReaderTest {

    @ParameterizedTest
    @CsvSource(
        " -8, 11110001",
        " -12, 11101001",
        "                  64, 00000010 00000001", // 02 01
        "                3257, 11100110 00110010", // E6 32
        "               -3257, 00011110 11001101",
        "                  78, 00111010 00000001", // 3A 01
        "               -6407, 11100110 10011011", // E6 9B
        "                   0, 00000001",
        "                   1, 00000011",
        "                   2, 00000101",
        "                   3, 00000111",
        "                   4, 00001001",
        "                   5, 00001011",
        "                  14, 00011101",
        "                  63, 01111111",
        "                  64, 00000010 00000001",
        "                 729, 01100110 00001011",
        "                8191, 11111110 01111111",
        "                8192, 00000100 00000000 00000001",
        "             1048575, 11111100 11111111 01111111",
        "             1048576, 00001000 00000000 00000000 00000001",
        "           134217727, 11111000 11111111 11111111 01111111",
        "           134217728, 00010000 00000000 00000000 00000000 00000001",
        "    ${Int.MAX_VALUE}, 11110000 11111111 11111111 11111111 00001111",
        "                  -1, 11111111",
        "                  -2, 11111101",
        "                  -3, 11111011",
        "                 -14, 11100101",
        "                 -64, 10000001",
        "                 -65, 11111110 11111110",
        "                -729, 10011110 11110100",
        "               -8192, 00000010 10000000",
        "               -8193, 11111100 11111111 11111110",
        "            -1048576, 00000100 00000000 10000000",
        "            -1048577, 11111000 11111111 11111111 11111110",
        "          -134217728, 00001000 00000000 00000000 10000000",
        "    ${Int.MIN_VALUE}, 00010000 00000000 00000000 00000000 11110000",
    )
    fun testReadFlexIntValueAndLengthAt(expectedValue: Int, input: String) {

        val data = ("0 0 0 0 $input").split(" ").map { it.toInt(2).toByte() }.toByteArray()

        val valueAndLength = BinaryPrimitiveReader.readFlexIntValueAndLengthAt(data, 4)
        val value = (valueAndLength shr 8).toInt()
        val length = (valueAndLength and 0xFF).toInt()

        Assertions.assertEquals(expectedValue, value)
        Assertions.assertEquals((input.length + 1) / 9, length)
    }


    @ParameterizedTest
    @CsvSource(
        " -8, 11110001",
        " -12, 11101001",
        "                  64, 00000010 00000001", // 02 01
        "                3257, 11100110 00110010", // E6 32
        "               -3257, 00011110 11001101",
        "                  78, 00111010 00000001", // 3A 01
        "               -6407, 11100110 10011011", // E6 9B
        "                   0, 00000001",
        "                   1, 00000011",
        "                   2, 00000101",
        "                   3, 00000111",
        "                   4, 00001001",
        "                   5, 00001011",
        "                  14, 00011101",
        "                  63, 01111111",
        "                  64, 00000010 00000001",
        "                 729, 01100110 00001011",
        "                8191, 11111110 01111111",
        "                8192, 00000100 00000000 00000001",
        "             1048575, 11111100 11111111 01111111",
        "             1048576, 00001000 00000000 00000000 00000001",
        "           134217727, 11111000 11111111 11111111 01111111",
        "           134217728, 00010000 00000000 00000000 00000000 00000001",
        "    ${Int.MAX_VALUE.shr(1)}, 11110000 11111111 11111111 11111111 00000111",
        "    ${Int.MAX_VALUE}, 11110000 11111111 11111111 11111111 00001111",
        "    ${Long.MAX_VALUE}, 00000000 11111110 11111111 11111111 11111111 11111111 11111111 11111111 11111111 00000001",
        "  123456789012345678901234567890, 00000000 10100000 10110100 11000010 10001111 10010011 00111011 11111000 11011100 10110000 11111101 01000011 10111010 01100011",
        "                  -1, 11111111",
        "                  -2, 11111101",
        "                  -3, 11111011",
        "                 -14, 11100101",
        "                 -64, 10000001",
        "                 -65, 11111110 11111110",
        "                -729, 10011110 11110100",
        "               -8192, 00000010 10000000",
        "               -8193, 11111100 11111111 11111110",
        "            -1048576, 00000100 00000000 10000000",
        "            -1048577, 11111000 11111111 11111111 11111110",
        "          -134217728, 00001000 00000000 00000000 10000000",
        "    ${Int.MIN_VALUE}, 00010000 00000000 00000000 00000000 11110000",
        "    ${Long.MIN_VALUE}, 00000000 00000010 00000000 00000000 00000000 00000000 00000000 00000000 00000000 11111110",
    )
    fun testReadFlexIntBigIntegerValue(expectedValue: String, input: String) {
        val data = ("0 0 0 0 $input").split(" ").map { it.toInt(2).toByte() }.toByteArray()
        val bigInteger = BinaryPrimitiveReader.readFlexIntBigIntegerValue(data, 4)
        assertEquals(BigInteger(expectedValue), bigInteger)
    }


    @ParameterizedTest
    @CsvSource(
        "                  64, 1, 40",
        "                3257, 2, B9 0C",
        "               -3257, 2, 47 F3",
        "                  78, 1, 4E",
        "               -6407, 2, F9 E6",
        "                   0, 1, 00",
        "                   1, 1, 01",
        "                   2, 1, 02",
        "                   3, 1, 03",
        "                   4, 1, 04",
        "                   5, 1, 05",
        "                  14, 1, 0E",
        "                 127, 1, 7F",
        "                 128, 2, 80 00", // length boundary
        "                 729, 2, D9 02",
        "               32767, 2, FF 7F",
        "               32768, 3, 00 80 00", // length boundary
        "             8388607, 3, FF FF 7F",
        "             8388608, 4, 00 00 80 00", // length boundary
        "    ${Int.MAX_VALUE}, 4, FF FF FF 7F",
        "          2147483648, 5, 00 00 00 80 00", // length boundary
        "        549755813887, 5, FF FF FF FF 7F",
        "        549755813888, 6, 00 00 00 00 80 00", // length boundary
        "     140737488355327, 6, FF FF FF FF FF 7F",
        "     140737488355328, 7, 00 00 00 00 00 80 00", // length boundary
        "   36028797018963967, 7, FF FF FF FF FF FF 7F",
        "   36028797018963968, 8, 00 00 00 00 00 00 80 00", // length boundary
        "   ${Long.MAX_VALUE}, 8, FF FF FF FF FF FF FF 7F",

        "                  -1, 1, FF",
        "                  -2, 1, FE",
        "                  -3, 1, FD",
        "                 -14, 1, F2",
        "                -128, 1, 80",
        "                -129, 2, 7F FF", // length boundary
        "                -729, 2, 27 FD",
        "              -32768, 2, 00 80",
        "              -32769, 3, FF 7F FF", // length boundary
        "            -8388608, 3, 00 00 80",
        "            -8388609, 4, FF FF 7F FF", // length boundary
        "    ${Int.MIN_VALUE}, 4, 00 00 00 80",
        "         -2147483649, 5, FF FF FF 7F FF", // length boundary
        "       -549755813888, 5, 00 00 00 00 80",
        "       -549755813889, 6, FF FF FF FF 7F FF", // length boundary
        "    -140737488355328, 6, 00 00 00 00 00 80",
        "    -140737488355329, 7, FF FF FF FF FF 7F FF", // length boundary
        "  -36028797018963968, 7, 00 00 00 00 00 00 80",
        "  -36028797018963969, 8, FF FF FF FF FF FF 7F FF", // length boundary
        "   ${Long.MIN_VALUE}, 8, 00 00 00 00 00 00 00 80",
    )
    fun testReadFixedIntAt(expectedValue: Long, length: Int, input: String) {
        val data = "00 00 00 00 $input".hexStringToByteArray()

        val value = BinaryPrimitiveReader.readFixedIntAt(data, 4, length)

        assertEquals(expectedValue, value)
    }

    // TODO: add tests for reading the rest of the binary encoding primitives
}
