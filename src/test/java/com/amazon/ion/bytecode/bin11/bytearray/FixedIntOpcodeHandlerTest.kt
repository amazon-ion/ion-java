// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11.bytearray

import com.amazon.ion.TextToBinaryUtils.hexStringToByteArray
import com.amazon.ion.bytecode.GeneratorTestUtil.assertEqualBytecode
import com.amazon.ion.bytecode.ir.Instructions
import com.amazon.ion.bytecode.ir.Instructions.packInstructionData
import com.amazon.ion.bytecode.util.BytecodeBuffer
import com.amazon.ion.bytecode.util.ConstantPool
import com.amazon.ion.bytecode.util.unsignedToInt
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import kotlin.String

class FixedIntOpcodeHandlerTest {
    // These tests need to include the IVM in the test bytecode (or any 4 bytes before the FixedInt)
    // because the BinaryPrimitiveReader has logic that expects this to always be the case.

    @ParameterizedTest
    @CsvSource(
        "60,       0,     0", // 0-byte
        "61 32,    1,    50", // 1-byte positive
        "61 97,    1,  -105", // 1-byte negative
        "62 26 73, 2, 29478", // 2-byte positive
        "62 50 FC, 2,  -944", // 2-byte negative
        "62 00 00, 2,     0", // 2-byte overlong 0
        "62 FF FF, 2,    -1", // 2-byte overlong -1

        "61 7F,    1,   127",
        "62 80 00, 2,   128", // length boundary
        "62 FF 7F, 2, 32767", // max value

        "61 80,    1,   -128",
        "62 7F FF, 2,   -129", // length boundary
        "62 00 80, 2, -32768", // min value
    )
    fun testI16EmittingFixedInt(
        inputString: String,
        expectedBytesRead: Int,
        expectedInt16: Short
    ) {
        val inputByteArray: ByteArray = "E0 01 01 EA $inputString".hexStringToByteArray()
        val buffer = BytecodeBuffer()

        var position = 4 // skip the IVM
        val opcode = inputByteArray[position++].unsignedToInt()
        position += FixedIntOpcodeHandler.convertOpcodeToBytecode(
            opcode,
            inputByteArray,
            position,
            buffer,
            ConstantPool(0),
            intArrayOf(),
            intArrayOf(),
            arrayOf()
        )

        val expectedInstruction = Instructions.I_INT_I16.packInstructionData(expectedInt16.toInt())
        assertEqualBytecode(intArrayOf(expectedInstruction), buffer.toArray())
        assertEquals(5 + expectedBytesRead, position)

        val representedInteger = Instructions.getData(buffer.get(0)).toShort()
        assertEquals(expectedInt16, representedInteger)
    }

    @ParameterizedTest
    @CsvSource(
        "63 40 42 0F,    3,    1000000", // 3-byte positive
        "63 4F 34 8B,    3,   -7654321", // 3-byte negative
        "64 3B C4 42 7E, 4, 2118304827", // 4-byte positive
        "64 57 97 13 E9, 4, -384592041", // 4-byte negative
        "64 00 00 00 00, 4,          0", // 4-byte overlong 0
        "64 FF FF FF FF, 4,         -1", // 4-byte overlong -1

        "63 00 80 00,    3,            32768", // min positive, length boundary from i16
        "63 FF FF 7F,    3,          8388607",
        "64 00 00 80 00, 4,          8388608", // length boundary
        "64 FF FF FF 7F, 4, ${Int.MAX_VALUE}", // max value

        "63 FF 7F FF,    3,           -32769", // max negative, length boundary from i16
        "63 00 00 80,    3,         -8388608",
        "64 FF FF 7F FF, 4,         -8388609", // length boundary
        "64 00 00 00 80, 4, ${Int.MIN_VALUE}", // min value
    )
    fun testI32EmittingFixedInt(
        inputString: String,
        expectedBytesRead: Int,
        expectedInt32: Int
    ) {
        val inputByteArray: ByteArray = "E0 01 01 EA $inputString".hexStringToByteArray()
        val buffer = BytecodeBuffer()

        var position = 4 // skip the IVM
        val opcode = inputByteArray[position++].unsignedToInt()
        position += FixedIntOpcodeHandler.convertOpcodeToBytecode(
            opcode,
            inputByteArray,
            position,
            buffer,
            ConstantPool(0),
            intArrayOf(),
            intArrayOf(),
            arrayOf()
        )

        val expectedBytecode = intArrayOf(Instructions.I_INT_I32, expectedInt32)
        assertEqualBytecode(expectedBytecode, buffer.toArray())
        assertEquals(5 + expectedBytesRead, position)
    }

    @ParameterizedTest
    @CsvSource(
        "65 6A 22 7C AB 5C,          5,         398014030442", // 5-byte positive
        "65 96 DD 83 54 A3,          5,        -398014030442", // 5-byte negative
        "66 C4 87 8F 09 97 5D,       6,      102903281846212", // 6-byte positive
        "66 3C 78 70 F6 68 A2,       6,     -102903281846212", // 6-byte negative
        "67 62 9A 42 56 83 77 10,    7,     4635005598997090", // 7-byte positive
        "67 9E 65 BD A9 7C 88 EF,    7,    -4635005598997090", // 7-byte negative
        "68 A4 F7 64 69 16 27 BF 31, 8,  3584626805621192612", // 8-byte positive
        "68 5C 08 9B 96 E9 D8 40 CE, 8, -3584626805621192612", // 8-byte negative
        "68 00 00 00 00 00 00 00 00, 8,                    0", // 8-byte overlong 0
        "68 FF FF FF FF FF FF FF FF, 8,                   -1", // 8-byte overlong -1

        "65 00 00 00 80 00,          5,         2147483648", // min positive, length boundary from i32
        "65 FF FF FF FF 7F,          5,       549755813887",
        "66 00 00 00 00 80 00,       6,       549755813888", // length boundary
        "66 FF FF FF FF FF 7F,       6,    140737488355327",
        "67 00 00 00 00 00 80 00,    7,    140737488355328", // length boundary
        "67 FF FF FF FF FF FF 7F,    7,  36028797018963967",
        "68 00 00 00 00 00 00 80 00, 8,  36028797018963968", // length boundary
        "68 FF FF FF FF FF FF FF 7F, 8,  ${Long.MAX_VALUE}", // max value

        "65 FF FF FF 7F FF,          5,        -2147483649", // max negative, length boundary from i32
        "65 00 00 00 00 80,          5,      -549755813888",
        "66 FF FF FF FF 7F FF,       6,      -549755813889", // length boundary
        "66 00 00 00 00 00 80,       6,   -140737488355328",
        "67 FF FF FF FF FF 7F FF,    7,   -140737488355329", // length boundary
        "67 00 00 00 00 00 00 80,    7, -36028797018963968",
        "68 FF FF FF FF FF FF 7F FF, 8, -36028797018963969", // length boundary
        "68 00 00 00 00 00 00 00 80, 8,  ${Long.MIN_VALUE}", // min value
    )
    fun testI64EmittingFixedInt(
        inputString: String,
        expectedBytesRead: Int,
        expectedInt64: Long
    ) {
        val inputByteArray: ByteArray = "E0 01 01 EA $inputString".hexStringToByteArray()
        val buffer = BytecodeBuffer()

        var position = 4 // skip the IVM
        val opcode = inputByteArray[position++].unsignedToInt()
        position += FixedIntOpcodeHandler.convertOpcodeToBytecode(
            opcode,
            inputByteArray,
            position,
            buffer,
            ConstantPool(0),
            intArrayOf(),
            intArrayOf(),
            arrayOf()
        )

        val expectedBytecode = intArrayOf(
            Instructions.I_INT_I64,
            (expectedInt64 ushr 32).toInt(),
            expectedInt64.toInt()
        )
        assertEqualBytecode(expectedBytecode, buffer.toArray())
        assertEquals(5 + expectedBytesRead, position)

        val representedInteger = (buffer.get(1).toLong() shl 32) or (buffer.get(2).toLong() and 0xFFFF_FFFF)
        assertEquals(expectedInt64, representedInteger)
    }
}
