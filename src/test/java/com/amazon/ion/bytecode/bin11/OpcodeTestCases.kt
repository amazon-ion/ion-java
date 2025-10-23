// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11

import com.amazon.ion.TextToBinaryUtils.byteArrayToHexString
import com.amazon.ion.TextToBinaryUtils.hexStringToByteArray
import com.amazon.ion.TextToBinaryUtils.toSingleHexByte
import com.amazon.ion.bytecode.ir.Instructions
import com.amazon.ion.bytecode.ir.Instructions.packInstructionData
import com.amazon.ion.impl.bin.PrimitiveEncoder
import org.junit.jupiter.params.provider.Arguments
import java.nio.charset.StandardCharsets

/**
 * Test cases for every binary 1.1 opcode supported by the bytecode generator. Test cases have the following components:
 * - Hex string of input bytes to test
 * - Decimal string of expected bytecode after compiling the input bytes
 * - String representation of the value encoded by these bytes. This is opcode-specific and up to individual opcode
 *   handlers to parse and understand. Not every test case supplies this.
 *
 * Bytecode can contain placeholders in the form `%pos:<number>%`, which should be replaced with `<number>` plus the
 * index of the first byte of the binary in the input. For example, if a bytecode string contains `%pos:30%` and the
 * test suite is writing the binary at index 0 of a byte array passed to a
 * [ByteArrayBytecodeGenerator11], then the placeholder should be replaced with `30`, and if the binary were written at
 * index 5, the placeholder should be replaced with `35`. This allows tests cases where the resulting bytecode is
 * sensitive to the opcode's position in the input (e.g. `OP_*_REF` codes) to be reused across test cases that use them
 * at different offsets. Pass the decimal string to [replacePositionTemplates] to parse these placeholders.
 */
object OpcodeTestCases {

    private const val THIS_NAME = "com.amazon.ion.bytecode.bin11.OpcodeTestCases"

    /**
     * Parse any placeholders in the form `%pos:<number>%` in [string] to `<number>` plus [position]. Reveals the
     * correct bytecode for opcodes that are sensitive to their position in the input.
     *
     * [position] should be the index in a BytecodeGenerator's input at which you are writing the corresponding
     * binary-encoded value.
     */
    @JvmStatic
    fun replacePositionTemplates(string: String, position: Int): String {
        return Regex("%pos:(\\d+)%").replace(string) { matchResult ->
            (matchResult.groups[1]?.value!!.toInt() + position).toString()
        }
    }

    const val BOOLEAN_OPCODE_CASES = "$THIS_NAME#booleanOpcodeCases"

    @JvmStatic
    fun booleanOpcodeCases() = listOf(
        "6E, ${Instructions.I_BOOL.packInstructionData(1)}, true",
        "6F, ${Instructions.I_BOOL.packInstructionData(0)}, false",
    ).toArguments()

    const val NULL_OPCODE_CASES = "$THIS_NAME#nullOpcodeCases"

    @JvmStatic
    fun nullOpcodeCases() = listOf(
        "8E, ${Instructions.I_NULL_NULL}",
    ).toArguments()

    const val TYPED_NULL_OPCODE_CASES = "$THIS_NAME#typedNullOpcodeCases"

    @JvmStatic
    fun typedNullOpcodeCases() = listOf(
        "8F 01, ${Instructions.I_NULL_BOOL}",
        "8F 02, ${Instructions.I_NULL_INT}",
        "8F 03, ${Instructions.I_NULL_FLOAT}",
        "8F 04, ${Instructions.I_NULL_DECIMAL}",
        "8F 05, ${Instructions.I_NULL_TIMESTAMP}",
        "8F 06, ${Instructions.I_NULL_STRING}",
        "8F 07, ${Instructions.I_NULL_SYMBOL}",
        "8F 08, ${Instructions.I_NULL_BLOB}",
        "8F 09, ${Instructions.I_NULL_CLOB}",
        "8F 0a, ${Instructions.I_NULL_LIST}",
        "8F 0b, ${Instructions.I_NULL_SEXP}",
        "8F 0c, ${Instructions.I_NULL_STRUCT}",
    ).toArguments()

    const val SHORT_TIMESTAMP_OPCODE_CASES = "$THIS_NAME#shortTimestampOpcodeCases"

    @JvmStatic
    fun shortTimestampOpcodeCases() = listOf(
        "80 35,                          ${Instructions.I_SHORT_TIMESTAMP_REF.packInstructionData(0x0)} %pos:1%, 2023T",
        "81 35 05,                       ${Instructions.I_SHORT_TIMESTAMP_REF.packInstructionData(0x1)} %pos:1%, 2023-10T",
        "82 35 7D,                       ${Instructions.I_SHORT_TIMESTAMP_REF.packInstructionData(0x2)} %pos:1%, 2023-10-15T",
        "83 35 7D CB 0A,                 ${Instructions.I_SHORT_TIMESTAMP_REF.packInstructionData(0x3)} %pos:1%, 2023-10-15T11:22Z",
        "84 35 7D CB 1A 02,              ${Instructions.I_SHORT_TIMESTAMP_REF.packInstructionData(0x4)} %pos:1%, 2023-10-15T11:22:33Z",
        "84 35 7D CB 12 02,              ${Instructions.I_SHORT_TIMESTAMP_REF.packInstructionData(0x4)} %pos:1%, 2023-10-15T11:22:33-00:00",
        "85 35 7D CB 12 F2 06,           ${Instructions.I_SHORT_TIMESTAMP_REF.packInstructionData(0x5)} %pos:1%, 2023-10-15T11:22:33.444-00:00",
        "86 35 7D CB 12 2E 22 1B,        ${Instructions.I_SHORT_TIMESTAMP_REF.packInstructionData(0x6)} %pos:1%, 2023-10-15T11:22:33.444555-00:00",
        "87 35 7D CB 12 4A 86 FD 69,     ${Instructions.I_SHORT_TIMESTAMP_REF.packInstructionData(0x7)} %pos:1%, 2023-10-15T11:22:33.444555666-00:00",
        "88 35 7D CB EA 01,              ${Instructions.I_SHORT_TIMESTAMP_REF.packInstructionData(0x8)} %pos:1%, 2023-10-15T11:22+01:15",
        "89 35 7D CB EA 85,              ${Instructions.I_SHORT_TIMESTAMP_REF.packInstructionData(0x9)} %pos:1%, 2023-10-15T11:22:33+01:15",
        "8A 35 7D CB EA 85 BC 01,        ${Instructions.I_SHORT_TIMESTAMP_REF.packInstructionData(0xA)} %pos:1%, 2023-10-15T11:22:33.444+01:15",
        "8B 35 7D CB EA 85 8B C8 06,     ${Instructions.I_SHORT_TIMESTAMP_REF.packInstructionData(0xB)} %pos:1%, 2023-10-15T11:22:33.444555+01:15",
        "8C 35 7D CB EA 85 92 61 7F 1A,  ${Instructions.I_SHORT_TIMESTAMP_REF.packInstructionData(0xC)} %pos:1%, 2023-10-15T11:22:33.444555666+01:15",
        // TODO: add tests for max/min values, other extremes
    ).toArguments()

    const val FLOAT0_OPCODE_CASES = "$THIS_NAME#float0OpcodeCases"

    @JvmStatic
    fun float0OpcodeCases() = listOf(
        "6A, ${Instructions.I_FLOAT_F32} 0, 0",
    ).toArguments()

    const val FLOAT16_OPCODE_CASES = "$THIS_NAME#float16OpcodeCases"

    @JvmStatic
    fun float16OpcodeCases() = listOf(
        "6B 01 00, ${Instructions.I_FLOAT_F32} 864026624,  0.000000059604645", // smallest positive subnormal number
        "6B FF 03, ${Instructions.I_FLOAT_F32} 947896320,  0.000060975552", // largest subnormal number
        "6B 00 04, ${Instructions.I_FLOAT_F32} 947912704,  0.00006103515625", // smallest positive normal number
        "6B FF 7B, ${Instructions.I_FLOAT_F32} 1199562752, 65504", // largest normal number
        "6B FF 3B, ${Instructions.I_FLOAT_F32} 1065345024, 0.99951172", // largest number less than one
        "6B 00 3C, ${Instructions.I_FLOAT_F32} 1065353216, 1",
        "6B 01 3C, ${Instructions.I_FLOAT_F32} 1065361408, 1.00097656", // smallest number larger than one

        // Same as above, but negative
        "6B 01 80, ${Instructions.I_FLOAT_F32} -1283457024, -0.000000059604645",
        "6B FF 83, ${Instructions.I_FLOAT_F32} -1199587328, -0.000060975552",
        "6B 00 84, ${Instructions.I_FLOAT_F32} -1199570944, -0.00006103515625",
        "6B FF FB, ${Instructions.I_FLOAT_F32} -947920896,  -65504",
        "6B FF BB, ${Instructions.I_FLOAT_F32} -1082138624, -0.99951172",
        "6B 00 BC, ${Instructions.I_FLOAT_F32} -1082130432, -1",
        "6B 01 BC, ${Instructions.I_FLOAT_F32} -1082122240, -1.00097656",

        "6B 00 00, ${Instructions.I_FLOAT_F32} 0,            0",
        "6B 00 80, ${Instructions.I_FLOAT_F32} -2147483648, -0",
        "6B 00 7C, ${Instructions.I_FLOAT_F32} 2139095040,   Infinity",
        "6B 00 FC, ${Instructions.I_FLOAT_F32} -8388608,    -Infinity",
        "6B 01 7E, ${Instructions.I_FLOAT_F32} 2143297536,   NaN", // quiet NaN
        "6B 01 7C, ${Instructions.I_FLOAT_F32} 2139103232,   NaN", // signaling NaN
        "6B 01 FE, ${Instructions.I_FLOAT_F32} -4186112,     NaN", // negative quiet NaN
        "6B 01 FC, ${Instructions.I_FLOAT_F32} -8380416,     NaN", // negative signaling NaN
        "6B 53 7F, ${Instructions.I_FLOAT_F32} 2146066432,   NaN", // another quiet NaN
        "6B 53 FF, ${Instructions.I_FLOAT_F32} -1417216,     NaN", // another negative quiet NaN

        "6B 00 C0, ${Instructions.I_FLOAT_F32} -1073741824, -2",
        "6B 55 35, ${Instructions.I_FLOAT_F32} 1051369472,   0.33325195",
        "6B 48 42, ${Instructions.I_FLOAT_F32} 1078525952,   3.140625"
    ).toArguments()

    const val FLOAT32_OPCODE_CASES = "$THIS_NAME#float32OpcodeCases"

    @JvmStatic
    fun float32OpcodeCases() = listOf(
        // TODO: cross-check all this stuff one more time
        "6C 01 00 00 00, ${Instructions.I_FLOAT_F32} 1,          1.4012984643e-45", // smallest positive subnormal number
        "6C FF FF 7F 00, ${Instructions.I_FLOAT_F32} 8388607,    1.1754942107e-38", // largest subnormal number
        "6C 00 00 80 00, ${Instructions.I_FLOAT_F32} 8388608,    1.1754943508e-38", // smallest positive normal number
        "6C FF FF 7F 7F, ${Instructions.I_FLOAT_F32} 2139095039, 3.4028234664e38", // largest normal number
        "6C FF FF 7F 3F, ${Instructions.I_FLOAT_F32} 1065353215, 0.999999940395355225", // largest number less than one
        "6C 00 00 80 3F, ${Instructions.I_FLOAT_F32} 1065353216, 1",
        "6C 01 00 80 3F, ${Instructions.I_FLOAT_F32} 1065353217, 1.00000011920928955", // smallest number larger than one

        // Same as above, but negative
        "6C 01 00 00 80, ${Instructions.I_FLOAT_F32} -2147483647, -1.4012984643e-45",
        "6C FF FF 7F 80, ${Instructions.I_FLOAT_F32} -2139095041, -1.1754942107e-38",
        "6C 00 00 80 80, ${Instructions.I_FLOAT_F32} -2139095040, -1.1754943508e-38",
        "6C FF FF 7F FF, ${Instructions.I_FLOAT_F32} -8388609,    -3.4028234664e38",
        "6C FF FF 7F BF, ${Instructions.I_FLOAT_F32} -1082130433, -0.999999940395355225",
        "6C 00 00 80 BF, ${Instructions.I_FLOAT_F32} -1082130432, -1",
        "6C 01 00 80 BF, ${Instructions.I_FLOAT_F32} -1082130431, -1.00000011920928955",

        "6C 00 00 00 00, ${Instructions.I_FLOAT_F32} 0,            0",
        "6C 00 00 00 80, ${Instructions.I_FLOAT_F32} -2147483648, -0",
        "6C 00 00 80 7F, ${Instructions.I_FLOAT_F32} 2139095040,   Infinity",
        "6C 00 00 80 FF, ${Instructions.I_FLOAT_F32} -8388608,    -Infinity",
        "6C 01 00 C0 7F, ${Instructions.I_FLOAT_F32} 2143289345,   NaN", // quiet NaN
        "6C 01 00 80 7F, ${Instructions.I_FLOAT_F32} 2139095041,   NaN", // signaling NaN
        "6C 01 00 C0 FF, ${Instructions.I_FLOAT_F32} -4194303,     NaN", // negative quiet NaN
        "6C 01 00 80 FF, ${Instructions.I_FLOAT_F32} -8388607,     NaN", // negative signaling NaN

        "6C 00 00 00 C0, ${Instructions.I_FLOAT_F32} -1073741824, -2",
        "6C AB AA AA 3E, ${Instructions.I_FLOAT_F32} 1051372203,   0.333333343267440796",
        "6C DB 0F 49 40, ${Instructions.I_FLOAT_F32} 1078530011,   3.14159274101257324"
    ).toArguments()

    const val FLOAT64_OPCODE_CASES = "$THIS_NAME#float64OpcodeCases"

    @JvmStatic
    fun float64OpcodeCases() = listOf(
        "6D 01 00 00 00 00 00 00 00, ${Instructions.I_FLOAT_F64} 0           1, 4.9406564584124654e-324", // smallest positive subnormal number
        "6D FF FF FF FF FF FF 0F 00, ${Instructions.I_FLOAT_F64} 1048575    -1, 2.2250738585072009e-308", // largest subnormal number
        "6D 00 00 00 00 00 00 10 00, ${Instructions.I_FLOAT_F64} 1048576     0, 2.2250738585072014e-308", // smallest positive normal number
        "6D FF FF FF FF FF FF EF 7F, ${Instructions.I_FLOAT_F64} 2146435071 -1, 1.7976931348623157e308", // largest normal number
        "6D FF FF FF FF FF FF EF 3F, ${Instructions.I_FLOAT_F64} 1072693247 -1, 0.99999999999999988898", // largest number less than one
        "6D 00 00 00 00 00 00 F0 3F, ${Instructions.I_FLOAT_F64} 1072693248  0, 1",
        "6D 01 00 00 00 00 00 F0 3F, ${Instructions.I_FLOAT_F64} 1072693248  1, 1.0000000000000002220", // smallest number larger than one
        "6D 02 00 00 00 00 00 F0 3F, ${Instructions.I_FLOAT_F64} 1072693248  2, 1.0000000000000004441", // the second smallest number greater than 1

        // Same as above, but negative
        "6D 01 00 00 00 00 00 00 80, ${Instructions.I_FLOAT_F64} -2147483648  1, -4.9406564584124654e-324",
        "6D FF FF FF FF FF FF 0F 80, ${Instructions.I_FLOAT_F64} -2146435073 -1, -2.2250738585072009e-308",
        "6D 00 00 00 00 00 00 10 80, ${Instructions.I_FLOAT_F64} -2146435072  0, -2.2250738585072014e-308",
        "6D FF FF FF FF FF FF EF FF, ${Instructions.I_FLOAT_F64} -1048577    -1, -1.7976931348623157e308",
        "6D FF FF FF FF FF FF EF BF, ${Instructions.I_FLOAT_F64} -1074790401 -1, -0.99999999999999988898",
        "6D 00 00 00 00 00 00 F0 BF, ${Instructions.I_FLOAT_F64} -1074790400  0, -1",
        "6D 01 00 00 00 00 00 F0 BF, ${Instructions.I_FLOAT_F64} -1074790400  1, -1.0000000000000002220",
        "6D 02 00 00 00 00 00 F0 BF, ${Instructions.I_FLOAT_F64} -1074790400  2, -1.0000000000000004441",

        "6D 00 00 00 00 00 00 00 00, ${Instructions.I_FLOAT_F64} 0           0,  0",
        "6D 00 00 00 00 00 00 00 80, ${Instructions.I_FLOAT_F64} -2147483648 0, -0",
        "6D 00 00 00 00 00 00 F0 7F, ${Instructions.I_FLOAT_F64} 2146435072  0,  Infinity",
        "6D 00 00 00 00 00 00 F0 FF, ${Instructions.I_FLOAT_F64} -1048576    0, -Infinity",
        "6D 01 00 00 00 00 00 F8 7F, ${Instructions.I_FLOAT_F64} 2146959360  1,  NaN", // quiet NaN
        "6D 01 00 00 00 00 00 F0 7F, ${Instructions.I_FLOAT_F64} 2146435072  1,  NaN", // signaling NaN
        "6D 01 00 00 00 00 00 F8 FF, ${Instructions.I_FLOAT_F64} -524288     1,  NaN", // negative quiet NaN
        "6D 01 00 00 00 00 00 F0 FF, ${Instructions.I_FLOAT_F64} -1048576    1,  NaN", // negative signaling NaN
        "6D FF FF FF FF FF FF FF 7F, ${Instructions.I_FLOAT_F64} 2147483647 -1,  NaN", // another quiet NaN
        "6D FF FF FF FF FF FF FF FF, ${Instructions.I_FLOAT_F64} -1         -1,  NaN", // another negative quiet NaN

        "6D 00 00 00 00 00 00 00 C0, ${Instructions.I_FLOAT_F64} -1073741824 0, -2",
        "6D 55 55 55 55 55 55 D5 3F, ${Instructions.I_FLOAT_F64} 1070945621 1431655765, 0.33333333333333331483",
        "6D 18 2D 44 54 FB 21 09 40, ${Instructions.I_FLOAT_F64} 1074340347 1413754136, 3.141592653589793116"
    ).toArguments()

    const val REFERENCE_OPCODE_CASES = "$THIS_NAME#referenceOpcodeCases"

    /**
     * Generates tests for handlers that emit similar *_REF bytecode (instructions packed with a UInt22 reference length
     * and followed by a UInt32 position of the data).
     */
    @JvmStatic
    fun referenceOpcodeCases(): List<Arguments> {
        val arguments = mutableListOf<Arguments>()

        val instructions = arrayOf(
            Pair(Instructions.I_ANNOTATION_REF, 0x59),
            Pair(Instructions.I_INT_REF, 0xF5),
            Pair(Instructions.I_DECIMAL_REF, 0xF6),
            Pair(Instructions.I_TIMESTAMP_REF, 0xF7),
            Pair(Instructions.I_STRING_REF, 0xF8),
            Pair(Instructions.I_SYMBOL_REF, 0xF9),
            Pair(Instructions.I_BLOB_REF, 0xFE),
            Pair(Instructions.I_CLOB_REF, 0xFF),
        )

        val testTemplates = listOf(
            /*
              FlexUInt length prefix for referenced payload
              |                 Decimal payload length
              |                 |   Expected payload start position
              |                 |   |
              |                 |   | */
            "03,                1,  2",
            "05,                2,  2",
            "07,                3,  2",
            "09,                4,  2",
            "0B,                5,  2",
            "1D,               14,  2",
            "7F,               63,  2",
            "81,               64,  2",
            "FF,              127,  2",
            "02 02,           128,  3",
            "FE FF,         16383,  3",
            "04 00 02,      16384,  4",
            "FC FF FF,    2097151,  4",
            "08 00 00 02, 2097152,  5",
            "F8 FF FF 03, 4194303,  5", // maximum length of a payload
            "01,                0,  2", // zero-length payload  TODO: is this legal?
            "00 18 00 00 00 00 00 00 00 00 00 00, 1, 13", // overlong encoding on the FlexUInt
        )

        instructions.forEach { (instruction, opcode) ->
            testTemplates.forEach {
                val (flexUIntStr, payloadLengthStr, expectedPayloadStartPosStr) = it.split(',')
                val payloadLength = payloadLengthStr.trim().toInt()
                val expectedPayloadStartPosition = expectedPayloadStartPosStr.trim().toInt()
                val expectedBytecodeString = "${instruction.packInstructionData(payloadLength)} %pos:$expectedPayloadStartPosition%"

                // Create a dummy payload for this value with all bytes set to zeros.
                // Not actually looked at by this test, but simulates an encoded value the handler would actually
                // encounter during parsing.
                val payload = "00 ".repeat(payloadLength)
                val inputBytes = "${opcode.toString(16).uppercase().padStart(2, '0')} $flexUIntStr $payload"
                arguments.add(Arguments.of(inputBytes, expectedBytecodeString))
            }
        }

        return arguments
    }

    const val STRING_REFERENCE_OPCODE_CASES = "$THIS_NAME#stringReferenceOpcodeCases"

    @JvmStatic
    fun stringReferenceOpcodeCases(): List<Arguments> {
        val arguments = mutableListOf<Arguments>()
        val testStrings = listOf(
            "Hello world",
            "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n<root>\n<elem>hello</elem>\n</root>\n",
            "Love it! \uD83D\uDE0D❤\uFE0F\uD83D\uDC95\uD83D\uDE3B\uD83D\uDC96",
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789`~!@#\$%^&*()-_=+[{]}\\|;:'\",<.>/?",
            "Ἀνέβην δέ με σῖτος εὐρυβίοιο Ἰλιάδης τε καὶ Ὀδυσσείας καὶ Φοινικίων",
            "",
            "\u0000\u0001\u0002\u0003\u0004\u0005\u0006\u0007\u0008\u0009\u000a\u000b\u000c\u000d\u000e\u000f\u0010\u0011\u0012\u0013\u0014\u0015\u0016\u0017\u0018\u0019\u001a\u001b\u001c\u001d\u001e\u001f !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~\u007f",
            "   \tleading and trailing whitespace\u000c\r\n"
        )

        testStrings.forEach {
            val utf8Buffer = StandardCharsets.UTF_8.encode(it)
            val utf8Bytes = ByteArray(utf8Buffer.remaining())
            utf8Buffer.get(utf8Bytes)
            val flexUIntStr = generateFlexUIntHexString(utf8Bytes.size)
            val payloadLength = utf8Bytes.size
            val expectedPayloadStartPosition = flexUIntStr.hexStringToByteArray().size + 1
            val expectedBytecodeString = "${Instructions.I_STRING_REF.packInstructionData(payloadLength)} %pos:$expectedPayloadStartPosition%"

            val inputBytes = "F8 $flexUIntStr ${utf8Bytes.byteArrayToHexString()}"
            arguments.add(Arguments.of(inputBytes, expectedBytecodeString, it))
        }

        return arguments
    }

    const val LOB_REFERENCE_OPCODE_CASES = "$THIS_NAME#lobReferenceOpcodeCases"

    @JvmStatic
    fun lobReferenceOpcodeCases(): List<Arguments> {
        val arguments = mutableListOf<Arguments>()
        val testLobBytes = listOf(
            "00 00 00 00 00 00 00 00 00 00",
            "FF FF FF FF FF FF FF FF FF FF",
            "00 01 02 03 04 05 06 07 08 09 0A 0B 0C 0D 0E 0F 10 11 12 13 14 15 16 17 18 19 1A 1B 1C 1D 1E 1F 20 21 22 23 24 25 26 27 28 29 2A 2B 2C 2D 2E 2F 30 31 32 33 34 35 36 37 38 39 3A 3B 3C 3D 3E 3F 40 41 42 43 44 45 46 47 48 49 4A 4B 4C 4D 4E 4F 50 51 52 53 54 55 56 57 58 59 5A 5B 5C 5D 5E 5F 60 61 62 63 64 65 66 67 68 69 6A 6B 6C 6D 6E 6F 70 71 72 73 74 75 76 77 78 79 7A 7B 7C 7D 7E 7F 80 81 82 83 84 85 86 87 88 89 8A 8B 8C 8D 8E 8F 90 91 92 93 94 95 96 97 98 99 9A 9B 9C 9D 9E 9F A0 A1 A2 A3 A4 A5 A6 A7 A8 A9 AA AB AC AD AE AF B0 B1 B2 B3 B4 B5 B6 B7 B8 B9 BA BB BC BD BE BF C0 C1 C2 C3 C4 C5 C6 C7 C8 C9 CA CB CC CD CE CF D0 D1 D2 D3 D4 D5 D6 D7 D8 D9 DA DB DC DD DE DF E0 E1 E2 E3 E4 E5 E6 E7 E8 E9 EA EB EC ED EE EF F0 F1 F2 F3 F4 F5 F6 F7 F8 F9 FA FB FC FD FE FF",
            "A5",
            ""
        )

        testLobBytes.forEach {
            val lobSize = it.hexStringToByteArray().size
            val flexUIntStr = generateFlexUIntHexString(lobSize)
            val expectedPayloadStartPosition = flexUIntStr.hexStringToByteArray().size + 1
            val expectedBytecodeString = "${Instructions.I_BLOB_REF.packInstructionData(lobSize)} %pos:$expectedPayloadStartPosition%"

            val inputBytes = "${OpCode.VARIABLE_LENGTH_BLOB.toSingleHexByte()} $flexUIntStr $it"
            arguments.add(Arguments.of(inputBytes, expectedBytecodeString, it))
        }

        return arguments
    }

    const val INT0_OPCODE_CASES = "$THIS_NAME#int0OpcodeCases"

    @JvmStatic
    fun int0OpcodeCases() = listOf(
        "60, ${Instructions.I_INT_I16.packInstructionData(0)}, 0", // 0-byte
    ).toArguments()

    const val INT8_OPCODE_CASES = "$THIS_NAME#int8OpcodeCases"

    @JvmStatic
    fun int8OpcodeCases() = listOf(
        "61 32, ${Instructions.I_INT_I16.packInstructionData(50)}, 50", // 1-byte positive
        "61 97, ${Instructions.I_INT_I16.packInstructionData(-105)}, -105", // 1-byte negative
        "61 7F, ${Instructions.I_INT_I16.packInstructionData(127)}, 127", // max value
        "61 80, ${Instructions.I_INT_I16.packInstructionData(-128)}, -128", // min value
    ).toArguments()

    const val INT16_OPCODE_CASES = "$THIS_NAME#int16OpcodeCases"

    @JvmStatic
    fun int16OpcodeCases() = listOf(
        "62 26 73, ${Instructions.I_INT_I16.packInstructionData(29478)}, 29478", // 2-byte positive
        "62 50 FC, ${Instructions.I_INT_I16.packInstructionData(-944)}, -944", // 2-byte negative
        "62 00 00, ${Instructions.I_INT_I16.packInstructionData(0)}, 0", // 2-byte overlong 0
        "62 FF FF, ${Instructions.I_INT_I16.packInstructionData(-1)}, -1", // 2-byte overlong -1
        "62 80 00, ${Instructions.I_INT_I16.packInstructionData(128)}, 128", // min positive
        "62 7F FF, ${Instructions.I_INT_I16.packInstructionData(-129)}, -129", // max negative
        "62 FF 7F, ${Instructions.I_INT_I16.packInstructionData(32767)}, 32767", // max value
        "62 00 80, ${Instructions.I_INT_I16.packInstructionData(-32768)}, -32768", // min value
    ).toArguments()

    const val INT24_OPCODE_CASES = "$THIS_NAME#int24OpcodeCases"

    @JvmStatic
    fun int24OpcodeCases() = listOf(
        "63 40 42 0F,    ${Instructions.I_INT_I32} 1000000, 1000000", // 3-byte positive
        "63 4F 34 8B,    ${Instructions.I_INT_I32} -7654321, -7654321", // 3-byte negative
        "63 00 80 00,    ${Instructions.I_INT_I32} 32768, 32768", // min positive, length boundary from i16
        "63 FF FF 7F,    ${Instructions.I_INT_I32} 8388607, 8388607", // max value
        "63 FF 7F FF,    ${Instructions.I_INT_I32} -32769, -32769", // max negative, length boundary from i16
        "63 00 00 80,    ${Instructions.I_INT_I32} -8388608, -8388608", // min value
    ).toArguments()

    const val INT32_OPCODE_CASES = "$THIS_NAME#int32OpcodeCases"

    @JvmStatic
    fun int32OpcodeCases() = listOf(
        "64 3B C4 42 7E, ${Instructions.I_INT_I32} 2118304827, 2118304827", // 4-byte positive
        "64 57 97 13 E9, ${Instructions.I_INT_I32} -384592041, -384592041", // 4-byte negative
        "64 00 00 00 00, ${Instructions.I_INT_I32} 0, 0", // 4-byte overlong 0
        "64 FF FF FF FF, ${Instructions.I_INT_I32} -1, -1", // 4-byte overlong -1
        "64 00 00 80 00, ${Instructions.I_INT_I32} 8388608, 8388608", // length boundary
        "64 FF FF FF 7F, ${Instructions.I_INT_I32} ${Int.MAX_VALUE}, ${Int.MAX_VALUE}", // max value
        "64 FF FF 7F FF, ${Instructions.I_INT_I32} -8388609, -8388609", // length boundary
        "64 00 00 00 80, ${Instructions.I_INT_I32} ${Int.MIN_VALUE}, ${Int.MIN_VALUE}", // min value
    ).toArguments()

    const val INT64_EMITTING_OPCODE_CASES = "$THIS_NAME#int64EmittingOpcodeCases"

    @JvmStatic
    fun int64EmittingOpcodeCases() = listOf(
        "65 6A 22 7C AB 5C,          ${Instructions.I_INT_I64} 92         -1417928086,         398014030442", // 5-byte positive
        "65 96 DD 83 54 A3,          ${Instructions.I_INT_I64} -93         1417928086,        -398014030442", // 5-byte negative
        "66 C4 87 8F 09 97 5D,       ${Instructions.I_INT_I64} 23959        160401348,      102903281846212", // 6-byte positive
        "66 3C 78 70 F6 68 A2,       ${Instructions.I_INT_I64} -23960      -160401348,     -102903281846212", // 6-byte negative
        "67 62 9A 42 56 83 77 10,    ${Instructions.I_INT_I64} 1079171     1447205474,     4635005598997090", // 7-byte positive
        "67 9E 65 BD A9 7C 88 EF,    ${Instructions.I_INT_I64} -1079172   -1447205474,    -4635005598997090", // 7-byte negative
        "68 A4 F7 64 69 16 27 BF 31, ${Instructions.I_INT_I64} 834610966   1768224676,  3584626805621192612", // 8-byte positive
        "68 5C 08 9B 96 E9 D8 40 CE, ${Instructions.I_INT_I64} -834610967 -1768224676, -3584626805621192612", // 8-byte negative
        "68 00 00 00 00 00 00 00 00, ${Instructions.I_INT_I64}  0  0,  0", // 8-byte overlong 0
        "68 FF FF FF FF FF FF FF FF, ${Instructions.I_INT_I64} -1 -1, -1", // 8-byte overlong -1

        "65 00 00 00 80 00,          ${Instructions.I_INT_I64} 0 -2147483648,        2147483648", // min positive, length boundary from i32
        "65 FF FF FF FF 7F,          ${Instructions.I_INT_I64} 127        -1,      549755813887",
        "66 00 00 00 00 80 00,       ${Instructions.I_INT_I64} 128         0,      549755813888", // length boundary
        "66 FF FF FF FF FF 7F,       ${Instructions.I_INT_I64} 32767      -1,   140737488355327",
        "67 00 00 00 00 00 80 00,    ${Instructions.I_INT_I64} 32768       0,   140737488355328", // length boundary
        "67 FF FF FF FF FF FF 7F,    ${Instructions.I_INT_I64} 8388607    -1, 36028797018963967",
        "68 00 00 00 00 00 00 80 00, ${Instructions.I_INT_I64} 8388608     0, 36028797018963968", // length boundary
        "68 FF FF FF FF FF FF FF 7F, ${Instructions.I_INT_I64} 2147483647 -1, ${Long.MAX_VALUE}", // max value

        "65 FF FF FF 7F FF,          ${Instructions.I_INT_I64} -1 2147483647,        -2147483649", // max negative, length boundary from i32
        "65 00 00 00 00 80,          ${Instructions.I_INT_I64} -128        0,      -549755813888",
        "66 FF FF FF FF 7F FF,       ${Instructions.I_INT_I64} -129       -1,      -549755813889", // length boundary
        "66 00 00 00 00 00 80,       ${Instructions.I_INT_I64} -32768      0,   -140737488355328",
        "67 FF FF FF FF FF 7F FF,    ${Instructions.I_INT_I64} -32769     -1,   -140737488355329", // length boundary
        "67 00 00 00 00 00 00 80,    ${Instructions.I_INT_I64} -8388608    0, -36028797018963968",
        "68 FF FF FF FF FF FF 7F FF, ${Instructions.I_INT_I64} -8388609   -1, -36028797018963969", // length boundary
        "68 00 00 00 00 00 00 00 80, ${Instructions.I_INT_I64} -2147483648 0,  ${Long.MIN_VALUE}", // min value
    ).toArguments()

    private fun List<String>.toArguments() = map {
        Arguments.of(*it.split(',').map { it.trim() }.toTypedArray())
    }

    /**
     * Helper function for generating FlexUInt hex strings from an unsigned integer. Useful for test
     * cases that programmatically generate length-prefixed payloads.
     */
    private fun generateFlexUIntHexString(value: Int): String {
        val asLong = value.toLong()
        val length = PrimitiveEncoder.flexUIntLength(asLong)
        val bytes = ByteArray(length)
        PrimitiveEncoder.writeFlexIntOrUIntInto(bytes, 0, asLong, length)
        return bytes.byteArrayToHexString()
    }
}
