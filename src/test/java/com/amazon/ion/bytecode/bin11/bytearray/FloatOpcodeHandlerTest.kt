// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11.bytearray

import com.amazon.ion.TextToBinaryUtils.hexStringToByteArray
import com.amazon.ion.bytecode.GeneratorTestUtil.assertEqualBytecode
import com.amazon.ion.bytecode.bin11.bytearray.float.DoubleOpcodeHandler
import com.amazon.ion.bytecode.bin11.bytearray.float.Float0OpcodeHandler
import com.amazon.ion.bytecode.bin11.bytearray.float.Float16OpcodeHandler
import com.amazon.ion.bytecode.bin11.bytearray.float.Float32OpcodeHandler
import com.amazon.ion.bytecode.ir.Instructions
import com.amazon.ion.bytecode.util.BytecodeBuffer
import com.amazon.ion.bytecode.util.ConstantPool
import com.amazon.ion.bytecode.util.unsignedToInt
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import kotlin.String

class FloatOpcodeHandlerTest {
    // In these tests, we check the float values themselves written to the bytecode buffer as well as the raw bits
    // on the floats to ensure that NaN semantics are preserved.

    @Test
    fun `float0 opcode handler emits correct bytecode`() {
        val inputByteArray: ByteArray = "6A".hexStringToByteArray()
        val buffer = BytecodeBuffer()

        var position = 0
        val opcode = inputByteArray[position++].unsignedToInt()
        position += Float0OpcodeHandler.convertOpcodeToBytecode(
            opcode,
            inputByteArray,
            position,
            buffer,
            ConstantPool(0),
            intArrayOf(),
            intArrayOf(),
            arrayOf()
        )

        val expectedBytecode = intArrayOf(Instructions.I_FLOAT_F32, 0)
        assertEqualBytecode(expectedBytecode, buffer.toArray())
        assertEquals(1, position)
    }

    @ParameterizedTest
    @CsvSource(
        "6B 01 00, 0 01100111 00000000000000000000000, 0.000000059604645", // smallest positive subnormal number
        "6B FF 03, 0 01110000 11111111100000000000000, 0.000060975552", // largest subnormal number
        "6B 00 04, 0 01110001 00000000000000000000000, 0.00006103515625", // smallest positive normal number
        "6B FF 7B, 0 10001110 11111111110000000000000, 65504", // largest normal number
        "6B FF 3B, 0 01111110 11111111110000000000000, 0.99951172", // largest number less than one
        "6B 00 3C, 0 01111111 00000000000000000000000, 1",
        "6B 01 3C, 0 01111111 00000000010000000000000, 1.00097656", // smallest number larger than one

        // Same as above, but negative
        "6B 01 80, 1 01100111 00000000000000000000000, -0.000000059604645",
        "6B FF 83, 1 01110000 11111111100000000000000, -0.000060975552",
        "6B 00 84, 1 01110001 00000000000000000000000, -0.00006103515625",
        "6B FF FB, 1 10001110 11111111110000000000000, -65504",
        "6B FF BB, 1 01111110 11111111110000000000000, -0.99951172",
        "6B 00 BC, 1 01111111 00000000000000000000000, -1",
        "6B 01 BC, 1 01111111 00000000010000000000000, -1.00097656",

        "6B 00 00, 0 00000000 00000000000000000000000, 0",
        "6B 00 80, 1 00000000 00000000000000000000000, -0",
        "6B 00 7C, 0 11111111 00000000000000000000000, Infinity",
        "6B 00 FC, 1 11111111 00000000000000000000000, -Infinity",
        "6B 01 7E, 0 11111111 10000000010000000000000, NaN", // quiet NaN
        "6B 01 7C, 0 11111111 00000000010000000000000, NaN", // signaling NaN
        "6B 01 FE, 1 11111111 10000000010000000000000, NaN", // negative quiet NaN
        "6B 01 FC, 1 11111111 00000000010000000000000, NaN", // negative signaling NaN
        "6B 53 7F, 0 11111111 11010100110000000000000, NaN", // another quiet NaN
        "6B 53 FF, 1 11111111 11010100110000000000000, NaN", // another negative quiet NaN

        "6B 00 C0, 1 10000000 00000000000000000000000, -2",
        "6B 55 35, 0 01111101 01010101010000000000000, 0.33325195",
        "6B 48 42, 0 10000000 10010010000000000000000, 3.140625"
    )
    fun `float16 opcode handler emits correct bytecode`(
        inputString: String,
        expectedRawBitsString: String,
        expectedFloat: Float
    ) {
        val inputByteArray: ByteArray = inputString.hexStringToByteArray()
        val buffer = BytecodeBuffer()

        var position = 0
        val opcode = inputByteArray[position++].unsignedToInt()
        position += Float16OpcodeHandler.convertOpcodeToBytecode(
            opcode,
            inputByteArray,
            position,
            buffer,
            ConstantPool(0),
            intArrayOf(),
            intArrayOf(),
            arrayOf()
        )

        val expectedRawBits = expectedRawBitsString.replace(" ", "").toUInt(2).toInt()
        val expectedBytecode = intArrayOf(Instructions.I_FLOAT_F32, expectedRawBits)
        assertEqualBytecode(expectedBytecode, buffer.toArray())
        assertEquals(3, position)

        val representedFloat = Float.fromBits(buffer.get(1))
        assertEquals(expectedFloat, representedFloat)
    }

    @ParameterizedTest
    @CsvSource(
        "6C 01 00 00 00, 0 00000000 00000000000000000000001, 1.4012984643e-45", // smallest positive subnormal number
        "6C FF FF 7F 00, 0 00000000 11111111111111111111111, 1.1754942107e-38", // largest subnormal number
        "6C 00 00 80 00, 0 00000001 00000000000000000000000, 1.1754943508e-38", // smallest positive normal number
        "6C FF FF 7F 7F, 0 11111110 11111111111111111111111, 3.4028234664e38", // largest normal number
        "6C FF FF 7F 3F, 0 01111110 11111111111111111111111, 0.999999940395355225", // largest number less than one
        "6C 00 00 80 3F, 0 01111111 00000000000000000000000, 1",
        "6C 01 00 80 3F, 0 01111111 00000000000000000000001, 1.00000011920928955", // smallest number larger than one

        // Same as above, but negative
        "6C 01 00 00 80, 1 00000000 00000000000000000000001, -1.4012984643e-45",
        "6C FF FF 7F 80, 1 00000000 11111111111111111111111, -1.1754942107e-38",
        "6C 00 00 80 80, 1 00000001 00000000000000000000000, -1.1754943508e-38",
        "6C FF FF 7F FF, 1 11111110 11111111111111111111111, -3.4028234664e38",
        "6C FF FF 7F BF, 1 01111110 11111111111111111111111, -0.999999940395355225",
        "6C 00 00 80 BF, 1 01111111 00000000000000000000000, -1",
        "6C 01 00 80 BF, 1 01111111 00000000000000000000001, -1.00000011920928955",

        "6C 00 00 00 00, 0 00000000 00000000000000000000000, 0",
        "6C 00 00 00 80, 1 00000000 00000000000000000000000, -0",
        "6C 00 00 80 7F, 0 11111111 00000000000000000000000, Infinity",
        "6C 00 00 80 FF, 1 11111111 00000000000000000000000, -Infinity",
        "6C 01 00 C0 7F, 0 11111111 10000000000000000000001, NaN", // quiet NaN
        "6C 01 00 80 7F, 0 11111111 00000000000000000000001, NaN", // signaling NaN
        "6C 01 00 C0 FF, 1 11111111 10000000000000000000001, NaN", // negative quiet NaN
        "6C 01 00 80 FF, 1 11111111 00000000000000000000001, NaN", // negative signaling NaN

        "6C 00 00 00 C0, 1 10000000 00000000000000000000000, -2",
        "6C AB AA AA 3E, 0 01111101 01010101010101010101011, 0.333333343267440796",
        "6C DB 0F 49 40, 0 10000000 10010010000111111011011, 3.14159274101257324"
    )
    fun `float32 opcode handler emits correct bytecode`(
        inputString: String,
        expectedRawBitsString: String,
        expectedFloat: Float
    ) {
        val inputByteArray: ByteArray = inputString.hexStringToByteArray()
        val buffer = BytecodeBuffer()

        var position = 0
        val opcode = inputByteArray[position++].unsignedToInt()
        position += Float32OpcodeHandler.convertOpcodeToBytecode(
            opcode,
            inputByteArray,
            position,
            buffer,
            ConstantPool(0),
            intArrayOf(),
            intArrayOf(),
            arrayOf()
        )

        val expectedRawBits = expectedRawBitsString.replace(" ", "").toUInt(2).toInt()
        val expectedBytecode = intArrayOf(Instructions.I_FLOAT_F32, expectedRawBits)
        assertEqualBytecode(expectedBytecode, buffer.toArray())
        assertEquals(5, position)

        val representedFloat = Float.fromBits(buffer.get(1))
        assertEquals(expectedFloat, representedFloat)
    }

    @ParameterizedTest
    @CsvSource(
        "6D 01 00 00 00 00 00 00 00, 0 00000000000 0000000000000000000000000000000000000000000000000001, 4.9406564584124654e-324", // smallest positive subnormal number
        "6D FF FF FF FF FF FF 0F 00, 0 00000000000 1111111111111111111111111111111111111111111111111111, 2.2250738585072009e-308", // largest subnormal number
        "6D 00 00 00 00 00 00 10 00, 0 00000000001 0000000000000000000000000000000000000000000000000000, 2.2250738585072014e-308", // smallest positive normal number
        "6D FF FF FF FF FF FF EF 7F, 0 11111111110 1111111111111111111111111111111111111111111111111111, 1.7976931348623157e308", // largest normal number
        "6D FF FF FF FF FF FF EF 3F, 0 01111111110 1111111111111111111111111111111111111111111111111111, 0.99999999999999988898", // largest number less than one
        "6D 00 00 00 00 00 00 F0 3F, 0 01111111111 0000000000000000000000000000000000000000000000000000, 1",
        "6D 01 00 00 00 00 00 F0 3F, 0 01111111111 0000000000000000000000000000000000000000000000000001, 1.0000000000000002220", // smallest number larger than one
        "6D 02 00 00 00 00 00 F0 3F, 0 01111111111 0000000000000000000000000000000000000000000000000010, 1.0000000000000004441", // the second smallest number greater than 1

        // Same as above, but negative
        "6D 01 00 00 00 00 00 00 80, 1 00000000000 0000000000000000000000000000000000000000000000000001, -4.9406564584124654e-324",
        "6D FF FF FF FF FF FF 0F 80, 1 00000000000 1111111111111111111111111111111111111111111111111111, -2.2250738585072009e-308",
        "6D 00 00 00 00 00 00 10 80, 1 00000000001 0000000000000000000000000000000000000000000000000000, -2.2250738585072014e-308",
        "6D FF FF FF FF FF FF EF FF, 1 11111111110 1111111111111111111111111111111111111111111111111111, -1.7976931348623157e308",
        "6D FF FF FF FF FF FF EF BF, 1 01111111110 1111111111111111111111111111111111111111111111111111, -0.99999999999999988898",
        "6D 00 00 00 00 00 00 F0 BF, 1 01111111111 0000000000000000000000000000000000000000000000000000, -1",
        "6D 01 00 00 00 00 00 F0 BF, 1 01111111111 0000000000000000000000000000000000000000000000000001, -1.0000000000000002220",
        "6D 02 00 00 00 00 00 F0 BF, 1 01111111111 0000000000000000000000000000000000000000000000000010, -1.0000000000000004441",

        "6D 00 00 00 00 00 00 00 00, 0 00000000000 0000000000000000000000000000000000000000000000000000, 0",
        "6D 00 00 00 00 00 00 00 80, 1 00000000000 0000000000000000000000000000000000000000000000000000, -0",
        "6D 00 00 00 00 00 00 F0 7F, 0 11111111111 0000000000000000000000000000000000000000000000000000, Infinity",
        "6D 00 00 00 00 00 00 F0 FF, 1 11111111111 0000000000000000000000000000000000000000000000000000, -Infinity",
        "6D 01 00 00 00 00 00 F8 7F, 0 11111111111 1000000000000000000000000000000000000000000000000001, NaN", // quiet NaN
        "6D 01 00 00 00 00 00 F0 7F, 0 11111111111 0000000000000000000000000000000000000000000000000001, NaN", // signaling NaN
        "6D 01 00 00 00 00 00 F8 FF, 1 11111111111 1000000000000000000000000000000000000000000000000001, NaN", // negative quiet NaN
        "6D 01 00 00 00 00 00 F0 FF, 1 11111111111 0000000000000000000000000000000000000000000000000001, NaN", // negative signaling NaN
        "6D FF FF FF FF FF FF FF 7F, 0 11111111111 1111111111111111111111111111111111111111111111111111, NaN", // another quiet NaN
        "6D FF FF FF FF FF FF FF FF, 1 11111111111 1111111111111111111111111111111111111111111111111111, NaN", // another negative quiet NaN

        "6D 00 00 00 00 00 00 00 C0, 1 10000000000 0000000000000000000000000000000000000000000000000000, -2",
        "6D 55 55 55 55 55 55 D5 3F, 0 01111111101 0101010101010101010101010101010101010101010101010101, 0.33333333333333331483",
        "6D 18 2D 44 54 FB 21 09 40, 0 10000000000 1001001000011111101101010100010001000010110100011000, 3.141592653589793116"
    )
    fun `float64 opcode handler emits correct bytecode`(
        inputString: String,
        expectedRawBitsString: String,
        expectedDouble: Double
    ) {
        val inputByteArray: ByteArray = inputString.hexStringToByteArray()
        val buffer = BytecodeBuffer()

        var position = 0
        val opcode = inputByteArray[position++].unsignedToInt()
        position += DoubleOpcodeHandler.convertOpcodeToBytecode(
            opcode,
            inputByteArray,
            position,
            buffer,
            ConstantPool(0),
            intArrayOf(),
            intArrayOf(),
            arrayOf()
        )

        val expectedRawBits = expectedRawBitsString.replace(" ", "").toULong(2).toLong()
        val expectedBytecode = intArrayOf(
            Instructions.I_FLOAT_F64,
            (expectedRawBits ushr 32).toInt(),
            expectedRawBits.toInt()
        )
        assertEqualBytecode(expectedBytecode, buffer.toArray())
        assertEquals(9, position)

        val representedFloat = Double.fromBits(
            buffer.get(1).toLong().shl(32)
                .or(buffer.get(2).toLong().and(0xFFFF_FFFF))
        )
        assertEquals(expectedDouble, representedFloat)
    }
}
