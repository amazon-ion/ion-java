// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11.bytearray

import com.amazon.ion.TextToBinaryUtils.cleanCommentedHexBytes
import com.amazon.ion.TextToBinaryUtils.hexStringToByteArray
import com.amazon.ion.bytecode.BytecodeUtils
import com.amazon.ion.bytecode.PrimitiveUtils.generateFlexUIntBytes
import com.amazon.ion.bytecode.bin11.OpCode
import com.amazon.ion.bytecode.bin11.bytearray.OpcodeHandlerTestUtil.shouldCompile
import com.amazon.ion.bytecode.ir.Instructions
import com.amazon.ion.bytecode.ir.Instructions.packInstructionData
import com.amazon.ion.bytecode.util.ConstantPool
import com.amazon.ion.bytecode.util.asHalfToFloat
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.math.BigInteger

class ListOpcodeHandlerTests {

    /*
     * ================================================
     * ==            SHORT PREFIXED LISTS            ==
     * ================================================
     */

    @Nested
    inner class `short length-prefixed lists` {
        @Test
        fun `handler compiles lists of simple elements`() {
            val bytes = "B6 61 01 61 02 61 03".hexStringToByteArray()
            val expectedBytecode = intArrayOf(
                Instructions.I_LIST_START.packInstructionData(4),
                Instructions.I_INT_I16.packInstructionData(1),
                Instructions.I_INT_I16.packInstructionData(2),
                Instructions.I_INT_I16.packInstructionData(3),
                Instructions.I_END_CONTAINER
            )
            ShortLengthPrefixedListOpcodeHandler.shouldCompile(bytes, expectedBytecode)
        }

        @Test
        fun `handler compiles lists of every supported length`() {
            for (length in 0 until 0xF) {
                // List of null.null of size `length`
                val bytes = byteArrayOf(
                    OpCode.LIST_LENGTH_0.or(length).toByte(),
                    *Array(length) { OpCode.NULL_NULL.toByte() }.toByteArray()
                )
                val expectedBytecode = intArrayOf(
                    Instructions.I_LIST_START.packInstructionData(length + 1),
                    *Array(length) { Instructions.I_NULL_NULL }.toIntArray(),
                    Instructions.I_END_CONTAINER
                )
                ShortLengthPrefixedListOpcodeHandler.shouldCompile(bytes, expectedBytecode)
            }
        }

        @Test
        fun `handler compiles nested lists`() {
            val bytes = """
                BF
                B6 8E B3 B2 8E 8E 8E | [null, [[null, null]], null]
                B3 61 01 6E          | [1, true]
                8E                   | null
                B0                   | empty list
                61 03                | int 3
            """.cleanCommentedHexBytes().hexStringToByteArray()
            val expectedBytecode = intArrayOf(
                Instructions.I_LIST_START.packInstructionData(19),

                Instructions.I_LIST_START.packInstructionData(9),
                Instructions.I_NULL_NULL,
                Instructions.I_LIST_START.packInstructionData(5),
                Instructions.I_LIST_START.packInstructionData(3),
                Instructions.I_NULL_NULL,
                Instructions.I_NULL_NULL,
                Instructions.I_END_CONTAINER,
                Instructions.I_END_CONTAINER,
                Instructions.I_NULL_NULL,
                Instructions.I_END_CONTAINER,

                Instructions.I_LIST_START.packInstructionData(3),
                Instructions.I_INT_I16.packInstructionData(1),
                Instructions.I_BOOL.packInstructionData(1),
                Instructions.I_END_CONTAINER,

                Instructions.I_NULL_NULL,

                Instructions.I_LIST_START.packInstructionData(1),
                Instructions.I_END_CONTAINER,

                Instructions.I_INT_I16.packInstructionData(3),

                Instructions.I_END_CONTAINER
            )
            ShortLengthPrefixedListOpcodeHandler.shouldCompile(bytes, expectedBytecode)
        }

        @Test
        fun `handler compiles deeply nested lists`() {
            val bytes = "BF BE BD BC BB BA B9 B8 B7 B6 B5 B4 B3 B2 B1 B0".hexStringToByteArray()
            val expectedBytecode = intArrayOf(
                Instructions.I_LIST_START.packInstructionData(31),
                Instructions.I_LIST_START.packInstructionData(29),
                Instructions.I_LIST_START.packInstructionData(27),
                Instructions.I_LIST_START.packInstructionData(25),
                Instructions.I_LIST_START.packInstructionData(23),
                Instructions.I_LIST_START.packInstructionData(21),
                Instructions.I_LIST_START.packInstructionData(19),
                Instructions.I_LIST_START.packInstructionData(17),
                Instructions.I_LIST_START.packInstructionData(15),
                Instructions.I_LIST_START.packInstructionData(13),
                Instructions.I_LIST_START.packInstructionData(11),
                Instructions.I_LIST_START.packInstructionData(9),
                Instructions.I_LIST_START.packInstructionData(7),
                Instructions.I_LIST_START.packInstructionData(5),
                Instructions.I_LIST_START.packInstructionData(3),
                Instructions.I_LIST_START.packInstructionData(1),
                *Array(16) { Instructions.I_END_CONTAINER }.toIntArray()
            )
            ShortLengthPrefixedListOpcodeHandler.shouldCompile(bytes, expectedBytecode)
        }
    }

    /*
     * ================================================
     * ==            LONG PREFIXED LISTS             ==
     * ================================================
     */

    @Nested
    inner class `long length-prefixed lists` {
        @Test
        fun `handler compiles lists of simple elements`() {
            val bytes = "FA 0D 61 01 61 02 61 03".hexStringToByteArray()
            val expectedBytecode = intArrayOf(
                Instructions.I_LIST_START.packInstructionData(4),
                Instructions.I_INT_I16.packInstructionData(1),
                Instructions.I_INT_I16.packInstructionData(2),
                Instructions.I_INT_I16.packInstructionData(3),
                Instructions.I_END_CONTAINER
            )
            LongLengthPrefixedListOpcodeHandler.shouldCompile(bytes, expectedBytecode)
        }

        @Test
        fun `handler compiles empty lists`() {
            val bytes = "FA 01".hexStringToByteArray()
            val expectedBytecode = intArrayOf(
                Instructions.I_LIST_START.packInstructionData(1),
                Instructions.I_END_CONTAINER
            )
            LongLengthPrefixedListOpcodeHandler.shouldCompile(bytes, expectedBytecode)
        }

        @Test
        fun `handler compiles large lists`() {
            val testLength = 10_000_000 // Much larger causes test crash
            val bytes = byteArrayOf(
                OpCode.VARIABLE_LENGTH_LIST.toByte(),
                *generateFlexUIntBytes(testLength),
                *Array<Byte>(testLength) { 0x6E /* true */ }.toByteArray()
            )
            val expectedBytecode = intArrayOf(
                Instructions.I_LIST_START.packInstructionData(testLength + 1),
                *Array(testLength) { Instructions.I_BOOL.packInstructionData(1) }.toIntArray(),
                Instructions.I_END_CONTAINER
            )
            LongLengthPrefixedListOpcodeHandler.shouldCompile(bytes, expectedBytecode)
        }

        @Test
        fun `handler compiles nested lists`() {
            val bytes = """
                FA 63
                FA 11                        | [
                8E                           |   null,
                FA 09 FA 05 8E 8E            |   [[null, null]],
                8E                           |   null,
                                             | ]
                FA 41                        | [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
                61 01 61 02 61 03 61 04
                61 05 61 06 61 07 61 08
                61 09 61 0A 61 0B 61 0C
                61 0D 61 0E 61 0F 61 10   
                8E                           | null
                FA 01                        | empty list
                61 03                        | int 3
            """.cleanCommentedHexBytes().hexStringToByteArray()
            val expectedBytecode = intArrayOf(
                Instructions.I_LIST_START.packInstructionData(33),

                Instructions.I_LIST_START.packInstructionData(9),
                Instructions.I_NULL_NULL,
                Instructions.I_LIST_START.packInstructionData(5),
                Instructions.I_LIST_START.packInstructionData(3),
                Instructions.I_NULL_NULL,
                Instructions.I_NULL_NULL,
                Instructions.I_END_CONTAINER,
                Instructions.I_END_CONTAINER,
                Instructions.I_NULL_NULL,
                Instructions.I_END_CONTAINER,

                Instructions.I_LIST_START.packInstructionData(17),
                *Array(16) { Instructions.I_INT_I16.packInstructionData(it + 1) }.toIntArray(),
                Instructions.I_END_CONTAINER,

                Instructions.I_NULL_NULL,

                Instructions.I_LIST_START.packInstructionData(1),
                Instructions.I_END_CONTAINER,

                Instructions.I_INT_I16.packInstructionData(3),

                Instructions.I_END_CONTAINER
            )
            LongLengthPrefixedListOpcodeHandler.shouldCompile(bytes, expectedBytecode)
        }

        // TODO: this does stack overflow on very deep lists
        @Test
        fun `handler compiles deeply nested lists`() {
            fun wrapBytesWithList(bytes: ByteArray): ByteArray {
                return byteArrayOf(
                    OpCode.VARIABLE_LENGTH_LIST.toByte(),
                    *generateFlexUIntBytes(bytes.size),
                    *bytes
                )
            }

            fun wrapBytecodeWithList(bytecode: IntArray): IntArray {
                return intArrayOf(
                    Instructions.I_LIST_START.packInstructionData(bytecode.size + 1),
                    *bytecode,
                    Instructions.I_END_CONTAINER
                )
            }

            val testDepth = 500
            var bytes = byteArrayOf()
            var expectedBytecode = intArrayOf()
            for (i in 0 until testDepth) {
                bytes = wrapBytesWithList(bytes)
                expectedBytecode = wrapBytecodeWithList(expectedBytecode)
            }
            LongLengthPrefixedListOpcodeHandler.shouldCompile(bytes, expectedBytecode)
        }
    }

    /*
     * ================================================
     * ==              DELIMITED LISTS               ==
     * ================================================
     */

    @Nested
    inner class `delimited lists` {
        @Test
        fun `handler compiles lists of simple elements`() {
            val bytes = "F0 61 01 61 02 61 03 EF".hexStringToByteArray()
            val expectedBytecode = intArrayOf(
                Instructions.I_LIST_START.packInstructionData(4),
                Instructions.I_INT_I16.packInstructionData(1),
                Instructions.I_INT_I16.packInstructionData(2),
                Instructions.I_INT_I16.packInstructionData(3),
                Instructions.I_END_CONTAINER
            )
            DelimitedListOpcodeHandler.shouldCompile(bytes, expectedBytecode)
        }

        @Test
        fun `handler compiles empty lists`() {
            val bytes = "F0 EF".hexStringToByteArray()
            val expectedBytecode = intArrayOf(
                Instructions.I_LIST_START.packInstructionData(1),
                Instructions.I_END_CONTAINER
            )
            DelimitedListOpcodeHandler.shouldCompile(bytes, expectedBytecode)
        }

        @Test
        fun `handler compiles large lists`() {
            val testLength = 10_000_000 // Much larger causes test crash
            val bytes = byteArrayOf(
                OpCode.DELIMITED_LIST.toByte(),
                *Array<Byte>(testLength) { 0x6E /* true */ }.toByteArray(),
                OpCode.DELIMITED_CONTAINER_END.toByte()
            )
            val expectedBytecode = intArrayOf(
                Instructions.I_LIST_START.packInstructionData(testLength + 1),
                *Array(testLength) { Instructions.I_BOOL.packInstructionData(1) }.toIntArray(),
                Instructions.I_END_CONTAINER
            )
            DelimitedListOpcodeHandler.shouldCompile(bytes, expectedBytecode)
        }

        @Test
        fun `handler compiles nested lists`() {
            val bytes = """
                F0
                F0 8E F0 8E F0 8E 8E EF EF 8E EF | [null, [null, [null, null]], null]
                F0 61 01 61 02 61 03 EF          | [1, 2, 3]
                8E                               | null
                F0 EF                            | empty list
                61 03                            | int 3
                EF
            """.cleanCommentedHexBytes().hexStringToByteArray()
            val expectedBytecode = intArrayOf(
                Instructions.I_LIST_START.packInstructionData(21),

                Instructions.I_LIST_START.packInstructionData(10),
                Instructions.I_NULL_NULL,
                Instructions.I_LIST_START.packInstructionData(6),
                Instructions.I_NULL_NULL,
                Instructions.I_LIST_START.packInstructionData(3),
                Instructions.I_NULL_NULL,
                Instructions.I_NULL_NULL,
                Instructions.I_END_CONTAINER,
                Instructions.I_END_CONTAINER,
                Instructions.I_NULL_NULL,
                Instructions.I_END_CONTAINER,

                Instructions.I_LIST_START.packInstructionData(4),
                Instructions.I_INT_I16.packInstructionData(1),
                Instructions.I_INT_I16.packInstructionData(2),
                Instructions.I_INT_I16.packInstructionData(3),
                Instructions.I_END_CONTAINER,

                Instructions.I_NULL_NULL,

                Instructions.I_LIST_START.packInstructionData(1),
                Instructions.I_END_CONTAINER,

                Instructions.I_INT_I16.packInstructionData(3),

                Instructions.I_END_CONTAINER
            )
            DelimitedListOpcodeHandler.shouldCompile(bytes, expectedBytecode)
        }

        // TODO: this does stack overflow on very deep lists
        @Test
        fun `handler compiles deeply nested lists`() {
            val testDepth = 500
            val bytes = mutableListOf<Byte>()
            val expectedBytecode = mutableListOf<Int>()
            for (i in 0 until testDepth) {
                val childCount = testDepth - i - 1
                bytes.add(OpCode.DELIMITED_LIST.toByte())
                expectedBytecode.add(Instructions.I_LIST_START.packInstructionData(childCount * 2 + 1))
            }
            for (i in 0 until testDepth) {
                bytes.add(OpCode.DELIMITED_CONTAINER_END.toByte())
                expectedBytecode.add(Instructions.I_END_CONTAINER)
            }
            DelimitedListOpcodeHandler.shouldCompile(bytes.toByteArray(), expectedBytecode.toIntArray())
        }
    }

    /*
     * ================================================
     * ==           TAGLESS ELEMENT LISTS            ==
     * ================================================
     */

    @Nested
    inner class `tagless element lists` {
        @Test
        fun `handler compiles simple int TE lists`() {
            val bytes = """
                5B 60 15
                01
                03
                FF
                02 01
                FE FE
                04 00 01
                08 00 00 01
                10 00 00 00 10
                00 FE FF FF FF FF FF FF FF 01
                00 02 00 00 00 00 00 00 00 02
            """.cleanCommentedHexBytes().hexStringToByteArray()
            val expectedBytecode = intArrayOf(
                Instructions.I_LIST_START.packInstructionData(20),
                Instructions.I_INT_I32, 0,
                Instructions.I_INT_I32, 1,
                Instructions.I_INT_I32, -1,
                Instructions.I_INT_I32, 64,
                Instructions.I_INT_I32, -65,
                Instructions.I_INT_I32, 8192,
                Instructions.I_INT_I32, 1048576,
                *BytecodeUtils.I64(Int.MAX_VALUE.toLong() + 1),
                // The tagless FlexInt handler will always compile 10-byte FlexInts to I_INT_CP even though some of them
                // can fit into a Long.
                Instructions.I_INT_CP.packInstructionData(0),
                Instructions.I_INT_CP.packInstructionData(1),
                Instructions.I_END_CONTAINER
            )
            val expectedConstantPool = ConstantPool().apply {
                add(BigInteger.valueOf(Long.MAX_VALUE))
                add(BigInteger.valueOf(Long.MAX_VALUE) + BigInteger.ONE)
            }
            TaglessElementListOpcodeHandler.shouldCompile(bytes, expectedBytecode, expectedConstantPool)
        }

        @Test
        fun `handler compiles simple int8 TE lists`() {
            val bytes = "5B 61 0B 00 01 FF 7F 80".hexStringToByteArray()
            val expectedBytecode = intArrayOf(
                Instructions.I_LIST_START.packInstructionData(6),
                Instructions.I_INT_I16.packInstructionData(0),
                Instructions.I_INT_I16.packInstructionData(1),
                Instructions.I_INT_I16.packInstructionData(-1),
                Instructions.I_INT_I16.packInstructionData(Byte.MAX_VALUE.toInt()),
                Instructions.I_INT_I16.packInstructionData(Byte.MIN_VALUE.toInt()),
                Instructions.I_END_CONTAINER
            )
            TaglessElementListOpcodeHandler.shouldCompile(bytes, expectedBytecode)
        }

        @Test
        fun `handler compiles simple int16 TE lists`() {
            val bytes = "5B 62 0B 00 00 01 00 FF FF FF 7F 00 80".hexStringToByteArray()
            val expectedBytecode = intArrayOf(
                Instructions.I_LIST_START.packInstructionData(6),
                Instructions.I_INT_I16.packInstructionData(0),
                Instructions.I_INT_I16.packInstructionData(1),
                Instructions.I_INT_I16.packInstructionData(-1),
                Instructions.I_INT_I16.packInstructionData(Short.MAX_VALUE.toInt()),
                Instructions.I_INT_I16.packInstructionData(Short.MIN_VALUE.toInt()),
                Instructions.I_END_CONTAINER
            )
            TaglessElementListOpcodeHandler.shouldCompile(bytes, expectedBytecode)
        }

        @Test
        fun `handler compiles simple int32 TE lists`() {
            val bytes = """
                5B 64 0B
                00 00 00 00
                01 00 00 00
                FF FF FF FF
                FF FF FF 7F
                00 00 00 80
            """.cleanCommentedHexBytes().hexStringToByteArray()
            val expectedBytecode = intArrayOf(
                Instructions.I_LIST_START.packInstructionData(11),
                Instructions.I_INT_I32, 0,
                Instructions.I_INT_I32, 1,
                Instructions.I_INT_I32, -1,
                Instructions.I_INT_I32, Int.MAX_VALUE,
                Instructions.I_INT_I32, Int.MIN_VALUE,
                Instructions.I_END_CONTAINER
            )
            TaglessElementListOpcodeHandler.shouldCompile(bytes, expectedBytecode)
        }

        @Test
        fun `handler compiles simple int64 TE lists`() {
            val bytes = """
                5B 68 0B
                00 00 00 00 00 00 00 00
                01 00 00 00 00 00 00 00
                FF FF FF FF FF FF FF FF
                FF FF FF FF FF FF FF 7F
                00 00 00 00 00 00 00 80
            """.cleanCommentedHexBytes().hexStringToByteArray()
            val expectedBytecode = intArrayOf(
                Instructions.I_LIST_START.packInstructionData(16),
                *BytecodeUtils.I64(0),
                *BytecodeUtils.I64(1),
                *BytecodeUtils.I64(-1),
                *BytecodeUtils.I64(Long.MAX_VALUE),
                *BytecodeUtils.I64(Long.MIN_VALUE),
                Instructions.I_END_CONTAINER
            )
            TaglessElementListOpcodeHandler.shouldCompile(bytes, expectedBytecode)
        }

        @Disabled("Test not yet implemented")
        @Test
        fun `handler compiles simple uint TE lists`() {
            TODO("Test not yet implemented")
        }

        @Test
        fun `handler compiles simple uint8 TE lists`() {
            val bytes = "5B E1 09 00 01 7F FF".hexStringToByteArray()
            val expectedBytecode = intArrayOf(
                Instructions.I_LIST_START.packInstructionData(5),
                Instructions.I_INT_I16.packInstructionData(0),
                Instructions.I_INT_I16.packInstructionData(1),
                Instructions.I_INT_I16.packInstructionData(Byte.MAX_VALUE.toInt()),
                Instructions.I_INT_I16.packInstructionData(0xFF),
                Instructions.I_END_CONTAINER
            )
            TaglessElementListOpcodeHandler.shouldCompile(bytes, expectedBytecode)
        }

        @Test
        fun `handler compiles simple uint16 TE lists`() {
            val bytes = "5B E2 09 00 00 01 00 FF 7F FF FF".hexStringToByteArray()
            val expectedBytecode = intArrayOf(
                Instructions.I_LIST_START.packInstructionData(9),
                Instructions.I_INT_I32, 0,
                Instructions.I_INT_I32, 1,
                Instructions.I_INT_I32, Short.MAX_VALUE.toInt(),
                Instructions.I_INT_I32, 0xFFFF,
                Instructions.I_END_CONTAINER
            )
            TaglessElementListOpcodeHandler.shouldCompile(bytes, expectedBytecode)
        }

        @Test
        fun `handler compiles simple uint32 TE lists`() {
            val bytes = """
                5B E4 09
                00 00 00 00
                01 00 00 00
                FF FF FF 7F
                FF FF FF FF
            """.cleanCommentedHexBytes().hexStringToByteArray()
            val expectedBytecode = intArrayOf(
                Instructions.I_LIST_START.packInstructionData(10),
                Instructions.I_INT_I32, 0,
                Instructions.I_INT_I32, 1,
                Instructions.I_INT_I32, Int.MAX_VALUE,
                *BytecodeUtils.I64(0xFFFFFFFFL),
                Instructions.I_END_CONTAINER
            )
            TaglessElementListOpcodeHandler.shouldCompile(bytes, expectedBytecode)
        }

        @Test
        fun `handler compiles simple uint64 TE lists`() {
            val bytes = """
                5B E8 09
                00 00 00 00 00 00 00 00
                01 00 00 00 00 00 00 00
                FF FF FF FF FF FF FF 7F
                FF FF FF FF FF FF FF FF
            """.cleanCommentedHexBytes().hexStringToByteArray()
            val expectedBytecode = intArrayOf(
                Instructions.I_LIST_START.packInstructionData(11),
                *BytecodeUtils.I64(0),
                *BytecodeUtils.I64(1),
                *BytecodeUtils.I64(Long.MAX_VALUE),
                Instructions.I_INT_CP.packInstructionData(0),
                Instructions.I_END_CONTAINER
            )
            TaglessElementListOpcodeHandler.shouldCompile(bytes, expectedBytecode)
        }

        @Test
        fun `handler compiles simple float16 TE lists`() {
            val bytes = """
                5B 6B 11
                00 00   |  0
                00 80   | -0
                00 3C   |  1
                00 BC   | -1
                FF 7B   |  65504
                00 04   |  0.00006103515625
                01 80   | -0.000000059604645
                01 7E   | NaN
            """.cleanCommentedHexBytes().hexStringToByteArray()
            // We need this because the NaN compiled by the handler will have a different bit layout than Float.NaN
            val expectedNaN = 0x7E01.toShort().asHalfToFloat()
            assert(expectedNaN.isNaN()) // Sanity check on the NaN
            val expectedBytecode = intArrayOf(
                Instructions.I_LIST_START.packInstructionData(17),
                Instructions.I_FLOAT_F32, 0f.toRawBits(),
                Instructions.I_FLOAT_F32, (-0f).toRawBits(),
                Instructions.I_FLOAT_F32, 1f.toRawBits(),
                Instructions.I_FLOAT_F32, (-1f).toRawBits(),
                Instructions.I_FLOAT_F32, 65504f.toRawBits(),
                Instructions.I_FLOAT_F32, 6.1035156E-5f.toRawBits(),
                Instructions.I_FLOAT_F32, (-0.000000059604645f).toRawBits(),
                Instructions.I_FLOAT_F32, expectedNaN.toRawBits(),
                Instructions.I_END_CONTAINER
            )
            TaglessElementListOpcodeHandler.shouldCompile(bytes, expectedBytecode)
        }

        @Test
        fun `handler compiles simple float32 TE lists`() {
            val bytes = """
                5B 6C 11
                00 00 00 00   |  0
                00 00 00 80   | -0
                00 00 80 3F   |  1
                00 00 80 BF   | -1
                FF FF 7F 7F   |  3.4028234664e38
                00 00 80 00   |  1.1754943508e-38
                01 00 00 80   | -1.4012984643e-45
                01 00 C0 7F   | NaN
            """.cleanCommentedHexBytes().hexStringToByteArray()
            // We need this because the NaN compiled by the handler will have a different bit layout than Float.NaN
            val expectedNaN = Float.fromBits(0x7FC00001)
            assert(expectedNaN.isNaN()) // Sanity check on the NaN
            val expectedBytecode = intArrayOf(
                Instructions.I_LIST_START.packInstructionData(17),
                Instructions.I_FLOAT_F32, 0f.toRawBits(),
                Instructions.I_FLOAT_F32, (-0f).toRawBits(),
                Instructions.I_FLOAT_F32, 1f.toRawBits(),
                Instructions.I_FLOAT_F32, (-1f).toRawBits(),
                Instructions.I_FLOAT_F32, 3.4028234664e38f.toRawBits(),
                Instructions.I_FLOAT_F32, 1.1754943508e-38f.toRawBits(),
                Instructions.I_FLOAT_F32, (-1.4012984643e-45f).toRawBits(),
                Instructions.I_FLOAT_F32, expectedNaN.toRawBits(),
                Instructions.I_END_CONTAINER
            )
            TaglessElementListOpcodeHandler.shouldCompile(bytes, expectedBytecode)
        }

        @Disabled("Test not yet implemented")
        @Test
        fun `handler compiles simple float64 TE lists`() {
            TODO("Test not yet implemented")
        }

        @Disabled("Test not yet implemented")
        @Test
        fun `handler compiles simple decimal TE lists`() {
            TODO("Test not yet implemented")
        }

        @Disabled("Test not yet implemented")
        @Test
        fun `handler compiles simple timestamp_day TE lists`() {
            TODO("Test not yet implemented")
        }

        @Disabled("Test not yet implemented")
        @Test
        fun `handler compiles simple timestamp_min TE lists`() {
            TODO("Test not yet implemented")
        }

        @Disabled("Test not yet implemented")
        @Test
        fun `handler compiles simple timestamp_s TE lists`() {
            TODO("Test not yet implemented")
        }

        @Disabled("Test not yet implemented")
        @Test
        fun `handler compiles simple timestamp_ms TE lists`() {
            TODO("Test not yet implemented")
        }

        @Disabled("Test not yet implemented")
        @Test
        fun `handler compiles simple timestamp_us TE lists`() {
            TODO("Test not yet implemented")
        }

        @Disabled("Test not yet implemented")
        @Test
        fun `handler compiles simple timestamp_ns TE lists`() {
            TODO("Test not yet implemented")
        }

        @Disabled("Test not yet implemented")
        @Test
        fun `handler compiles simple flexsym TE lists`() {
            TODO("Test not yet implemented")
        }

        @ParameterizedTest
        @ValueSource(
            strings = [
                "5B 60 01",
                "5B 61 01",
                "5B 62 01",
                "5B 64 01",
                "5B 68 01",
                "5B E0 01",
                "5B E1 01",
                "5B E2 01",
                "5B E4 01",
                "5B E8 01",
                "5B 6B 01",
                "5B 6C 01",
                "5B 6D 01",
                "5B 70 01",
                "5B 82 01",
                "5B 83 01",
                "5B 84 01",
                "5B 85 01",
                "5B 86 01",
                "5B 87 01",
                "5B EE 01",
            ]
        )
        fun `handler compiles empty lists`(bytes: String) {
            val bytes = bytes.hexStringToByteArray()
            val expectedBytecode = intArrayOf(
                Instructions.I_LIST_START.packInstructionData(1),
                Instructions.I_END_CONTAINER
            )
            TaglessElementListOpcodeHandler.shouldCompile(bytes, expectedBytecode)
        }

        @Test
        fun `handler compiles large lists`() {
            val testLength = 10_000_000 // Much larger causes test crash
            val bytes = byteArrayOf(
                OpCode.TAGLESS_ELEMENT_LIST.toByte(),
                OpCode.INT_8.toByte(),
                *generateFlexUIntBytes(testLength),
                *Array<Byte>(testLength) { 0x01 }.toByteArray()
            )
            val expectedBytecode = intArrayOf(
                Instructions.I_LIST_START.packInstructionData(testLength + 1),
                *Array(testLength) { Instructions.I_INT_I16.packInstructionData(1) }.toIntArray(),
                Instructions.I_END_CONTAINER
            )
            TaglessElementListOpcodeHandler.shouldCompile(bytes, expectedBytecode)
        }
    }
}
