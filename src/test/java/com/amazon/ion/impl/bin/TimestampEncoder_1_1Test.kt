// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.bin

import com.amazon.ion.Timestamp
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.converter.ArgumentConversionException
import org.junit.jupiter.params.converter.ConvertWith
import org.junit.jupiter.params.converter.TypedArgumentConverter
import org.junit.jupiter.params.provider.CsvSource
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.util.function.BiFunction

class TimestampEncoder_1_1Test {
    private val ALLOCATOR: BlockAllocator = BlockAllocatorProviders.basicProvider().vendAllocator(11)

    private val buf = WriteBuffer(ALLOCATOR) {}

    private fun bytes(): ByteArray {
        val out = ByteArrayOutputStream()
        try {
            buf.writeTo(out)
        } catch (e: IOException) {
            throw IllegalStateException(e)
        }
        return out.toByteArray()
    }

    /**
     * Checks that the function writes the expected bytes and returns the expected count of written bytes for the
     * given input value. The expected bytes should be a string of space-separated hexadecimal pairs.
     */
    private fun <T> assertWritingValue(
        expectedBytes: String,
        value: T,
        writeOperation: BiFunction<WriteBuffer?, T, Int>
    ) {
        val numBytes = writeOperation.apply(buf, value)
        Assertions.assertEquals(expectedBytes, byteArrayToHex(bytes()))
        Assertions.assertEquals(byteLengthFromHexString(expectedBytes), numBytes)
    }

    /**
     * Checks that the function writes the expected bytes and returns the expected count of written bytes for the
     * given input value. The expected bytes should be a string of space-separated hexadecimal pairs.
     */
    private fun <T> assertWritingValue(
        expectedBytes: ByteArray,
        value: T,
        writeOperation: BiFunction<WriteBuffer?, T, Int>
    ) {
        val numBytes = writeOperation.apply(buf, value)
        Assertions.assertEquals(expectedBytes, bytes())
        Assertions.assertEquals(expectedBytes.size, numBytes)
    }

    /**
     * Checks that the function writes the expected bytes and returns the expected count of written bytes for the
     * given input value. The expectedBytes should be a string of space-separated binary octets.
     */
    private fun assertWritingValueWithBinary(
        expectedBytes: String,
        writeOperation: () -> Int
    ) {
        val numBytes = writeOperation()
        Assertions.assertEquals(expectedBytes, byteArrayToBitString(bytes()))
        Assertions.assertEquals(byteLengthFromBitString(expectedBytes), numBytes)
    }

    // Because timestamp subfields are smeared across bytes, it's easier to reason about them in 1s and 0s
    // instead of hex digits
    @ParameterizedTest
    @CsvSource(
        //                               OpCode   MYYYYYYY DDDDDMMM mmmHHHHH ssssUmmm ffffffss ffffffff ffffffff ffffffff
        "2023-10-15T01:00Z,              10000011 00110101 01111101 00000001 00001000",
        "2023-10-15T01:59Z,              10000011 00110101 01111101 01100001 00001111",
        "2023-10-15T11:22Z,              10000011 00110101 01111101 11001011 00001010",
        "2023-10-15T23:00Z,              10000011 00110101 01111101 00010111 00001000",
        "2023-10-15T23:59Z,              10000011 00110101 01111101 01110111 00001111",
        "2023-10-15T11:22:00Z,           10000100 00110101 01111101 11001011 00001010 00000000",
        "2023-10-15T11:22:33Z,           10000100 00110101 01111101 11001011 00011010 00000010",
        "2023-10-15T11:22:59Z,           10000100 00110101 01111101 11001011 10111010 00000011",
        "2023-10-15T11:22:33.000Z,       10000101 00110101 01111101 11001011 00011010 00000010 00000000",
        "2023-10-15T11:22:33.444Z,       10000101 00110101 01111101 11001011 00011010 11110010 00000110",
        "2023-10-15T11:22:33.999Z,       10000101 00110101 01111101 11001011 00011010 10011110 00001111",
        "2023-10-15T11:22:33.000000Z,    10000110 00110101 01111101 11001011 00011010 00000010 00000000 00000000",
        "2023-10-15T11:22:33.444555Z,    10000110 00110101 01111101 11001011 00011010 00101110 00100010 00011011",
        "2023-10-15T11:22:33.999999Z,    10000110 00110101 01111101 11001011 00011010 11111110 00001000 00111101",
        "2023-10-15T11:22:33.000000000Z, 10000111 00110101 01111101 11001011 00011010 00000010 00000000 00000000 00000000",
        "2023-10-15T11:22:33.444555666Z, 10000111 00110101 01111101 11001011 00011010 01001010 10000110 11111101 01101001",
        "2023-10-15T11:22:33.999999999Z, 10000111 00110101 01111101 11001011 00011010 11111110 00100111 01101011 11101110"
    )
    fun testWriteTimestampValueWithUtcShortForm(
        @ConvertWith(StringToTimestamp::class) value: Timestamp,
        expectedBytes: String
    ) {
        assertWritingValueWithBinary(expectedBytes) {
            TimestampEncoder_1_1.writeTimestampValue(buf, value)
        }
    }

    @ParameterizedTest
    @CsvSource(
        //                                    OpCode   MYYYYYYY DDDDDMMM mmmHHHHH ssssUmmm ffffffss ffffffff ffffffff ffffffff
        "1970T,                               10000000 00000000",
        "2023T,                               10000000 00110101",
        "2097T,                               10000000 01111111",
        "2023-01T,                            10000001 10110101 00000000",
        "2023-10T,                            10000001 00110101 00000101",
        "2023-12T,                            10000001 00110101 00000110",
        "2023-10-01T,                         10000010 00110101 00001101",
        "2023-10-15T,                         10000010 00110101 01111101",
        "2023-10-31T,                         10000010 00110101 11111101",
        "2023-10-15T01:00-00:00,              10000011 00110101 01111101 00000001 00000000",
        "2023-10-15T01:59-00:00,              10000011 00110101 01111101 01100001 00000111",
        "2023-10-15T11:22-00:00,              10000011 00110101 01111101 11001011 00000010",
        "2023-10-15T23:00-00:00,              10000011 00110101 01111101 00010111 00000000",
        "2023-10-15T23:59-00:00,              10000011 00110101 01111101 01110111 00000111",
        "2023-10-15T11:22:00-00:00,           10000100 00110101 01111101 11001011 00000010 00000000",
        "2023-10-15T11:22:33-00:00,           10000100 00110101 01111101 11001011 00010010 00000010",
        "2023-10-15T11:22:59-00:00,           10000100 00110101 01111101 11001011 10110010 00000011",
        "2023-10-15T11:22:33.000-00:00,       10000101 00110101 01111101 11001011 00010010 00000010 00000000",
        "2023-10-15T11:22:33.444-00:00,       10000101 00110101 01111101 11001011 00010010 11110010 00000110",
        "2023-10-15T11:22:33.999-00:00,       10000101 00110101 01111101 11001011 00010010 10011110 00001111",
        "2023-10-15T11:22:33.000000-00:00,    10000110 00110101 01111101 11001011 00010010 00000010 00000000 00000000",
        "2023-10-15T11:22:33.444555-00:00,    10000110 00110101 01111101 11001011 00010010 00101110 00100010 00011011",
        "2023-10-15T11:22:33.999999-00:00,    10000110 00110101 01111101 11001011 00010010 11111110 00001000 00111101",
        "2023-10-15T11:22:33.000000000-00:00, 10000111 00110101 01111101 11001011 00010010 00000010 00000000 00000000 00000000",
        "2023-10-15T11:22:33.444555666-00:00, 10000111 00110101 01111101 11001011 00010010 01001010 10000110 11111101 01101001",
        "2023-10-15T11:22:33.999999999-00:00, 10000111 00110101 01111101 11001011 00010010 11111110 00100111 01101011 11101110"
    )
    fun testWriteTimestampValueWithUnknownOffsetShortForm(
        @ConvertWith(StringToTimestamp::class) value: Timestamp,
        expectedBytes: String
    ) {
        assertWritingValueWithBinary(expectedBytes) {
            TimestampEncoder_1_1.writeTimestampValue(buf, value)
        }
    }

    @ParameterizedTest
    @CsvSource(
        //                                    OpCode   MYYYYYYY DDDDDMMM mmmHHHHH ooooommm ssssssoo ffffffff ffffffff ffffffff ..ffffff
        "2023-10-15T01:00-14:00,              10001000 00110101 01111101 00000001 00000000 00000000",
        "2023-10-15T01:00+14:00,              10001000 00110101 01111101 00000001 10000000 00000011",
        "2023-10-15T01:00-01:15,              10001000 00110101 01111101 00000001 10011000 00000001",
        "2023-10-15T01:00+01:15,              10001000 00110101 01111101 00000001 11101000 00000001",
        "2023-10-15T01:59+01:15,              10001000 00110101 01111101 01100001 11101111 00000001",
        "2023-10-15T11:22+01:15,              10001000 00110101 01111101 11001011 11101010 00000001",
        "2023-10-15T23:00+01:15,              10001000 00110101 01111101 00010111 11101000 00000001",
        "2023-10-15T23:59+01:15,              10001000 00110101 01111101 01110111 11101111 00000001",
        "2023-10-15T11:22:00+01:15,           10001001 00110101 01111101 11001011 11101010 00000001",
        "2023-10-15T11:22:33+01:15,           10001001 00110101 01111101 11001011 11101010 10000101",
        "2023-10-15T11:22:59+01:15,           10001001 00110101 01111101 11001011 11101010 11101101",
        "2023-10-15T11:22:33.000+01:15,       10001010 00110101 01111101 11001011 11101010 10000101 00000000 00000000",
        "2023-10-15T11:22:33.444+01:15,       10001010 00110101 01111101 11001011 11101010 10000101 10111100 00000001",
        "2023-10-15T11:22:33.999+01:15,       10001010 00110101 01111101 11001011 11101010 10000101 11100111 00000011",
        "2023-10-15T11:22:33.000000+01:15,    10001011 00110101 01111101 11001011 11101010 10000101 00000000 00000000 00000000",
        "2023-10-15T11:22:33.444555+01:15,    10001011 00110101 01111101 11001011 11101010 10000101 10001011 11001000 00000110",
        "2023-10-15T11:22:33.999999+01:15,    10001011 00110101 01111101 11001011 11101010 10000101 00111111 01000010 00001111",
        "2023-10-15T11:22:33.000000000+01:15, 10001100 00110101 01111101 11001011 11101010 10000101 00000000 00000000 00000000 00000000",
        "2023-10-15T11:22:33.444555666+01:15, 10001100 00110101 01111101 11001011 11101010 10000101 10010010 01100001 01111111 00011010",
        "2023-10-15T11:22:33.999999999+01:15, 10001100 00110101 01111101 11001011 11101010 10000101 11111111 11001001 10011010 00111011"

    )
    fun testWriteTimestampValueWithKnownOffsetShortForm(
        @ConvertWith(StringToTimestamp::class) value: Timestamp,
        expectedBytes: String
    ) {
        assertWritingValueWithBinary(expectedBytes) {
            TimestampEncoder_1_1.writeTimestampValue(buf, value)
        }
    }

    @ParameterizedTest
    @CsvSource(
        //                                    Length   YYYYYYYY MMYYYYYY HDDDDDMM mmmmHHHH oooooomm ssoooooo ....ssss Coefficient+ Scale
        "0001T,                               00000101 00000001 00000000",
        "1947T,                               00000101 10011011 00000111",
        "9999T,                               00000101 00001111 00100111",
        "1947-01T,                            00000111 10011011 01000111 00000000",
        "1947-12T,                            00000111 10011011 00000111 00000011",
        "1947-01-01T,                         00000111 10011011 01000111 00000100",
        "1947-12-23T,                         00000111 10011011 00000111 01011111",
        "1947-12-31T,                         00000111 10011011 00000111 01111111",
        "1947-12-23T00:00Z,                   00001101 10011011 00000111 01011111 00000000 10000000 00010110",
        "1947-12-23T23:59Z,                   00001101 10011011 00000111 11011111 10111011 10000011 00010110",
        "1947-12-23T23:59:00Z,                00001111 10011011 00000111 11011111 10111011 10000011 00010110 00000000",
        "1947-12-23T23:59:59Z,                00001111 10011011 00000111 11011111 10111011 10000011 11010110 00001110",
        "1947-12-23T23:59:00.0Z,              00010011 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00000001 00000001",
        "1947-12-23T23:59:00.00Z,             00010011 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00000001 00000010",
        "1947-12-23T23:59:00.000Z,            00010011 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00000001 00000011",
        "1947-12-23T23:59:00.0000Z,           00010011 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00000001 00000100",
        "1947-12-23T23:59:00.00000Z,          00010011 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00000001 00000101",
        "1947-12-23T23:59:00.000000Z,         00010011 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00000001 00000110",
        "1947-12-23T23:59:00.0000000Z,        00010011 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00000001 00000111",
        "1947-12-23T23:59:00.00000000Z,       00010011 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00000001 00001000",
        "1947-12-23T23:59:00.9Z,              00010011 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00010011 00000001",
        "1947-12-23T23:59:00.99Z,             00010011 10011011 00000111 11011111 10111011 10000011 00010110 00000000 11000111 00000010",
        "1947-12-23T23:59:00.999Z,            00010101 10011011 00000111 11011111 10111011 10000011 00010110 00000000 10011110 00001111 00000011",
        "1947-12-23T23:59:00.9999Z,           00010101 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00111110 10011100 00000100",
        "1947-12-23T23:59:00.99999Z,          00010111 10011011 00000111 11011111 10111011 10000011 00010110 00000000 11111100 00110100 00001100 00000101",
        "1947-12-23T23:59:00.999999Z,         00010111 10011011 00000111 11011111 10111011 10000011 00010110 00000000 11111100 00010001 01111010 00000110",
        "1947-12-23T23:59:00.9999999Z,        00011001 10011011 00000111 11011111 10111011 10000011 00010110 00000000 11111000 01100111 10001001 00001001 00000111",
        "1947-12-23T23:59:00.99999999Z,       00011001 10011011 00000111 11011111 10111011 10000011 00010110 00000000 11111000 00001111 01011110 01011111 00001000",
        "1947-12-23T23:59:00.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000Z, " +
            "00010011 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00000001 10001101",
        (
            "1947-12-23T23:59:00.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
                "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
                "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000Z, " +
                "00010101 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00000001 01101000 00000001"
            ),
        (
            "1947-12-23T23:59:00.999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999Z, " +
                "10010111 10011011 00000111 11011111 10111011 10000011 00010110 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 " +
                "11111100 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111 " +
                "11111111 10010100 10001001 01111001 01101100 11001110 01111000 11110010 01000000 01111101 10100110 11000111 10101000 01000110 01011001 01110001 01001101 " +
                "00100000 11110101 01101110 01111010 00001100 00001001 11101111 01111111 11110011 00011110 00010100 11010111 01101000 01110111 10101100 01101100 10001110 " +
                "00110010 10110111 10000010 11110010 00110110 01101000 11110010 10100111 10001101"
            ), // Offsets

        "2048-01-01T01:01-23:59,              00001101 00000000 01001000 10000100 00010000 00000100 00000000",
        "2048-01-01T01:01-00:02,              00001101 00000000 01001000 10000100 00010000 01111000 00010110",
        "2048-01-01T01:01-00:01,              00001101 00000000 01001000 10000100 00010000 01111100 00010110",
        "2048-01-01T01:01-00:00,              00001101 00000000 01001000 10000100 00010000 11111100 00111111",
        "2048-01-01T01:01+00:00,              00001101 00000000 01001000 10000100 00010000 10000000 00010110",
        "2048-01-01T01:01+00:01,              00001101 00000000 01001000 10000100 00010000 10000100 00010110",
        "2048-01-01T01:01+00:02,              00001101 00000000 01001000 10000100 00010000 10001000 00010110",
        "2048-01-01T01:01+23:59,              00001101 00000000 01001000 10000100 00010000 11111100 00101100"
    )
    fun testWriteTimestampValueLongForm(
        @ConvertWith(StringToTimestamp::class) value: Timestamp,
        expectedBytes: String
    ) {
        assertWritingValueWithBinary(expectedBytes) {
            TimestampEncoder_1_1.writeLongFormTimestampBody(buf, value)
        }
    }

    @ParameterizedTest
    @CsvSource(
        // Long form because it's out of the year range
        "0001T,                               11110111 00000101 00000001 00000000",
        "9999T,                               11110111 00000101 00001111 00100111", // Long form because the offset is too high/low
        "2048-01-01T01:01+14:15,              11110111 00001101 00000000 01001000 10000100 00010000 11011100 00100011",
        "2048-01-01T01:01-14:15,              11110111 00001101 00000000 01001000 10000100 00010000 00100100 00001001", // Long form because the offset is not a multiple of 15

        "2048-01-01T01:01+00:01,              11110111 00001101 00000000 01001000 10000100 00010000 10000100 00010110", // Long form because the fractional seconds are millis, micros, or nanos

        "2023-12-31T23:59:00.0Z,              11110111 00010011 11100111 00000111 11111111 10111011 10000011 00010110 00000000 00000001 00000001"
    )
    fun testWriteTimestampDelegatesCorrectlyToLongForm(
        @ConvertWith(StringToTimestamp::class) value: Timestamp,
        expectedBytes: String
    ) {
        assertWritingValueWithBinary(expectedBytes) {
            TimestampEncoder_1_1.writeTimestampValue(buf, value)
        }
    }

    /**
     * Converts a String to a Timestamp for a @Parameterized test
     */
    internal class StringToTimestamp protected constructor() :
        TypedArgumentConverter<String, Timestamp>(String::class.java, Timestamp::class.java) {
        @Throws(ArgumentConversionException::class)
        override fun convert(source: String?): Timestamp? {
            if (source == null) return null
            return Timestamp.valueOf(source)
        }
    }

    /**
     * Utility method to make it easier to write test cases that assert specific sequences of bytes.
     */
    private fun byteArrayToHex(bytes: ByteArray): String {
        val sb = StringBuilder()
        for (b in bytes) {
            sb.append(String.format("%02X ", b))
        }
        return sb.toString().trim { it <= ' ' }
    }

    /**
     * Determines the number of bytes needed to represent a series of hexadecimal digits.
     */
    private fun byteLengthFromHexString(hexString: String): Int {
        return (hexString.replace("[^\\dA-F]".toRegex(), "").length) / 2
    }

    /**
     * Converts a byte array to a string of bits, such as "00110110 10001001".
     * The purpose of this method is to make it easier to read and write test assertions.
     */
    private fun byteArrayToBitString(bytes: ByteArray): String {
        val s = StringBuilder()
        for (aByte in bytes) {
            for (bit in 7 downTo 0) {
                if (((0x01 shl bit) and aByte.toInt()) != 0) {
                    s.append("1")
                } else {
                    s.append("0")
                }
            }
            s.append(" ")
        }
        return s.toString().trim { it <= ' ' }
    }

    /**
     * Determines the number of bytes needed to represent a series of hexadecimal digits.
     */
    private fun byteLengthFromBitString(bitString: String): Int {
        return (bitString.replace("[^01]".toRegex(), "").length) / 8
    }
}
