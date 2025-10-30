// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11

import com.amazon.ion.TextToBinaryUtils.hexStringToByteArray
import com.amazon.ion.Timestamp
import com.amazon.ion.bytecode.GeneratorTestUtil.shouldGenerate
import com.amazon.ion.bytecode.ir.Instructions
import com.amazon.ion.bytecode.ir.Instructions.packInstructionData
import com.amazon.ion.impl.bin.PrimitiveEncoder
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.ValueSource
import java.nio.charset.StandardCharsets

internal object ByteArrayBytecodeGenerator11Test {

    @ParameterizedTest
    @ValueSource(
        strings = [
            "64 4F 97 21 C5 " + // int32 -987654321
                "86 35 7D CB 12 2E 22 1B " + // short TS reference to 2023-10-15T11:22:33.444555-00:00
                "8F 0C " + // null struct
                "6A " + // float 0e0
                "6D 18 2D 44 54 FB 21 09 40 " + // float64 3.141592653589793
                "FE 31 49 20 61 70 70 6c 61 75 64 20 79 6f 75 72 20 63 75 72 69 6f 73 69 74 79 " + // 24-byte blob
                "6F " // false
        ]
    )
    fun `generator can compile input containing multiple simple opcodes`(inputBytesString: String) {
        val f64pi = 3.141592653589793
        val expectedBytecode = intArrayOf(
            Instructions.I_INT_I32, -987654321,
            Instructions.I_SHORT_TIMESTAMP_REF.packInstructionData(0x86), 6,
            Instructions.I_NULL_STRUCT,
            Instructions.I_FLOAT_F32, 0,
            Instructions.I_FLOAT_F64, f64pi.toRawBits().ushr(32).and(0xFFFFFFFF).toInt(), f64pi.toRawBits().toInt(),
            Instructions.I_BLOB_REF.packInstructionData(24), 27,
            Instructions.I_BOOL.packInstructionData(0),
            Instructions.I_END_OF_INPUT
        )

        val bytes = inputBytesString.hexStringToByteArray()
        val generator = ByteArrayBytecodeGenerator11(bytes, 0)
        generator.shouldGenerate(expectedBytecode)
    }

    // TODO: add tests cases for more complicated cases like nested containers, macro compilation, annots., etc.
    //  once those features are implemented

    @ParameterizedTest
    @CsvSource(
        "80 35,                         2023T",
        "81 35 05,                      2023-10T",
        "82 35 7D,                      2023-10-15T",
        "83 35 7D CB 0A,                2023-10-15T11:22Z",
        "84 35 7D CB 1A 02,             2023-10-15T11:22:33Z",
        "84 35 7D CB 12 02,             2023-10-15T11:22:33-00:00",
        "85 35 7D CB 12 F2 06,          2023-10-15T11:22:33.444-00:00",
        "86 35 7D CB 12 2E 22 1B,       2023-10-15T11:22:33.444555-00:00",
        "87 35 7D CB 12 4A 86 FD 69,    2023-10-15T11:22:33.444555666-00:00",
        "88 35 7D CB EA 01,             2023-10-15T11:22+01:15",
        "89 35 7D CB EA 85,             2023-10-15T11:22:33+01:15",
        "8A 35 7D CB EA 85 BC 01,       2023-10-15T11:22:33.444+01:15",
        "8B 35 7D CB EA 85 8B C8 06,    2023-10-15T11:22:33.444555+01:15",
        "8C 35 7D CB EA 85 92 61 7F 1A, 2023-10-15T11:22:33.444555666+01:15",
    )
    fun `generator can read short timestamp references`(inputBytesString: String, expectedTimestampString: String) {
        val bytes = inputBytesString.hexStringToByteArray()
        val generator = ByteArrayBytecodeGenerator11(bytes, 0)
        val opcode = bytes[0].toInt().and(0xFF)
        val expectedTimestamp = Timestamp.valueOf(expectedTimestampString)
        val readTimestamp = generator.readShortTimestampReference(1, opcode)
        assertEquals(expectedTimestamp, readTimestamp)
    }

    @ParameterizedTest
    @ValueSource(
        strings = [
            "Hello world",
            "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n<root>\n<elem>hello</elem>\n</root>\n",
            "Love it! \uD83D\uDE0D❤\uFE0F\uD83D\uDC95\uD83D\uDE3B\uD83D\uDC96",
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789`~!@#\$%^&*()-_=+[{]}\\|;:'\",<.>/?",
            // A line of the Odyssey, CC BY-SA 3.0 US, from https://www.perseus.tufts.edu/hopper/text?doc=Perseus:text:1999.01.0135:book=1:card=1
            "τῶν ἁμόθεν γε, θεά, θύγατερ Διός, εἰπὲ καὶ ἡμῖν.",
            "",
            "\u0000\u0001\u0002\u0003\u0004\u0005\u0006\u0007\u0008\u0009\u000a\u000b\u000c\u000d\u000e\u000f\u0010\u0011\u0012\u0013\u0014\u0015\u0016\u0017\u0018\u0019\u001a\u001b\u001c\u001d\u001e\u001f !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~\u007f",
            "   \tleading and trailing whitespace\u000c\r\n"
        ]
    )
    fun `generator can read string references`(expectedString: String) {
        val utf8Buffer = StandardCharsets.UTF_8.encode(expectedString)
        val utf8Bytes = ByteArray(utf8Buffer.remaining())
        utf8Buffer.get(utf8Bytes)
        val flexUIntBytes = generateFlexUIntBytes(utf8Bytes.size)
        val bytes = byteArrayOf(0xF8.toByte(), *flexUIntBytes, *utf8Bytes)

        val generator = ByteArrayBytecodeGenerator11(bytes, 0)
        // Size of input minus the opcode and FlexUInt length prefix
        val position = flexUIntBytes.size + 1
        val readString = generator.readTextReference(position, utf8Bytes.size)
        assertEquals(expectedString, readString)
    }

    @ParameterizedTest
    @ValueSource(
        strings = [
            "00 00 00 00 00 00 00 00 00 00",
            "FF FF FF FF FF FF FF FF FF FF",
            "00 01 02 03 04 05 06 07 08 09 0A 0B 0C 0D 0E 0F 10 11 12 13 14 15 16 17 18 19 1A 1B 1C 1D 1E 1F 20 21 22 23 24 25 26 27 28 29 2A 2B 2C 2D 2E 2F 30 31 32 33 34 35 36 37 38 39 3A 3B 3C 3D 3E 3F 40 41 42 43 44 45 46 47 48 49 4A 4B 4C 4D 4E 4F 50 51 52 53 54 55 56 57 58 59 5A 5B 5C 5D 5E 5F 60 61 62 63 64 65 66 67 68 69 6A 6B 6C 6D 6E 6F 70 71 72 73 74 75 76 77 78 79 7A 7B 7C 7D 7E 7F 80 81 82 83 84 85 86 87 88 89 8A 8B 8C 8D 8E 8F 90 91 92 93 94 95 96 97 98 99 9A 9B 9C 9D 9E 9F A0 A1 A2 A3 A4 A5 A6 A7 A8 A9 AA AB AC AD AE AF B0 B1 B2 B3 B4 B5 B6 B7 B8 B9 BA BB BC BD BE BF C0 C1 C2 C3 C4 C5 C6 C7 C8 C9 CA CB CC CD CE CF D0 D1 D2 D3 D4 D5 D6 D7 D8 D9 DA DB DC DD DE DF E0 E1 E2 E3 E4 E5 E6 E7 E8 E9 EA EB EC ED EE EF F0 F1 F2 F3 F4 F5 F6 F7 F8 F9 FA FB FC FD FE FF",
            "A5",
            ""
        ]
    )
    fun `generator can read lob references`(expectedLobBytes: String) {
        val lobBytes = expectedLobBytes.hexStringToByteArray()
        val flexUIntBytes = generateFlexUIntBytes(lobBytes.size)
        val bytes = byteArrayOf(0xFE.toByte(), *flexUIntBytes, *lobBytes)

        val generator = ByteArrayBytecodeGenerator11(bytes, 0)
        val position = flexUIntBytes.size + 1
        val readLob = generator.readBytesReference(position, lobBytes.size).newByteArray()
        assertArrayEquals(lobBytes, readLob)
    }

    /**
     * Helper function for generating FlexUInt bytes from an unsigned integer. Useful for test
     * cases that programmatically generate length-prefixed payloads.
     */
    private fun generateFlexUIntBytes(value: Int): ByteArray {
        val asLong = value.toLong()
        val length = PrimitiveEncoder.flexUIntLength(asLong)
        val bytes = ByteArray(length)
        PrimitiveEncoder.writeFlexIntOrUIntInto(bytes, 0, asLong, length)
        return bytes
    }
}
