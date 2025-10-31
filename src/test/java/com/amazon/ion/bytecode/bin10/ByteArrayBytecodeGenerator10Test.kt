// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin10

import com.amazon.ion.Decimal
import com.amazon.ion.TextToBinaryUtils.cleanCommentedHexBytes
import com.amazon.ion.TextToBinaryUtils.hexStringToByteArray
import com.amazon.ion.Timestamp
import com.amazon.ion.bytecode.GeneratorTestUtil.refillShouldThrowIonException
import com.amazon.ion.bytecode.GeneratorTestUtil.shouldGenerate
import com.amazon.ion.bytecode.ir.Instructions.I_ANNOTATION_SID
import com.amazon.ion.bytecode.ir.Instructions.I_BLOB_REF
import com.amazon.ion.bytecode.ir.Instructions.I_BOOL
import com.amazon.ion.bytecode.ir.Instructions.I_CLOB_REF
import com.amazon.ion.bytecode.ir.Instructions.I_DECIMAL_REF
import com.amazon.ion.bytecode.ir.Instructions.I_DIRECTIVE_SET_SYMBOLS
import com.amazon.ion.bytecode.ir.Instructions.I_END_CONTAINER
import com.amazon.ion.bytecode.ir.Instructions.I_END_OF_INPUT
import com.amazon.ion.bytecode.ir.Instructions.I_FIELD_NAME_SID
import com.amazon.ion.bytecode.ir.Instructions.I_FLOAT_F32
import com.amazon.ion.bytecode.ir.Instructions.I_FLOAT_F64
import com.amazon.ion.bytecode.ir.Instructions.I_INT_CP
import com.amazon.ion.bytecode.ir.Instructions.I_INT_I16
import com.amazon.ion.bytecode.ir.Instructions.I_INT_I32
import com.amazon.ion.bytecode.ir.Instructions.I_INT_I64
import com.amazon.ion.bytecode.ir.Instructions.I_IVM
import com.amazon.ion.bytecode.ir.Instructions.I_LIST_START
import com.amazon.ion.bytecode.ir.Instructions.I_NULL_BLOB
import com.amazon.ion.bytecode.ir.Instructions.I_NULL_BOOL
import com.amazon.ion.bytecode.ir.Instructions.I_NULL_CLOB
import com.amazon.ion.bytecode.ir.Instructions.I_NULL_DECIMAL
import com.amazon.ion.bytecode.ir.Instructions.I_NULL_FLOAT
import com.amazon.ion.bytecode.ir.Instructions.I_NULL_INT
import com.amazon.ion.bytecode.ir.Instructions.I_NULL_LIST
import com.amazon.ion.bytecode.ir.Instructions.I_NULL_NULL
import com.amazon.ion.bytecode.ir.Instructions.I_NULL_SEXP
import com.amazon.ion.bytecode.ir.Instructions.I_NULL_STRING
import com.amazon.ion.bytecode.ir.Instructions.I_NULL_STRUCT
import com.amazon.ion.bytecode.ir.Instructions.I_NULL_SYMBOL
import com.amazon.ion.bytecode.ir.Instructions.I_NULL_TIMESTAMP
import com.amazon.ion.bytecode.ir.Instructions.I_SEXP_START
import com.amazon.ion.bytecode.ir.Instructions.I_STRING_REF
import com.amazon.ion.bytecode.ir.Instructions.I_STRUCT_START
import com.amazon.ion.bytecode.ir.Instructions.I_SYMBOL_CP
import com.amazon.ion.bytecode.ir.Instructions.I_SYMBOL_SID
import com.amazon.ion.bytecode.ir.Instructions.I_TIMESTAMP_REF
import com.amazon.ion.bytecode.ir.Instructions.packInstructionData
import com.amazon.ion.bytecode.util.ByteSlice
import com.amazon.ion.bytecode.util.ConstantPool
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import java.math.BigInteger

class ByteArrayBytecodeGenerator10Test {

    @Test
    fun `getMinorVersion is 0`() {
        assertEquals(0, bytecodeGeneratorFor("").ionMinorVersion())
    }

    @Test
    fun `getGeneratorForMinorVersion 0 returns self`() {
        val generator = bytecodeGeneratorFor("")
        assertSame(generator, generator.getGeneratorForMinorVersion(0))
    }

    @Test
    fun `an IVM`() = assertIon10BinaryProducesBytecode(
        "E0 01 00 EA",
        intArrayOf(
            I_IVM withData 0x0100,
            I_END_OF_INPUT,
        )
    )

    @Test
    fun `stops refilling at an IVM, even if more data is available`() {

        val generator = bytecodeGeneratorFor("E0 01 00 EA 0F 0F")

        with(generator) {
            shouldGenerate(I_IVM.withData(0x0100))
            shouldGenerate(
                I_NULL_NULL,
                I_NULL_NULL,
                I_END_OF_INPUT,
            )
        }
    }

    @Test
    fun `a null`() = assertIon10BinaryProducesBytecode(
        "0F",
        intArrayOf(
            I_NULL_NULL,
            I_END_OF_INPUT,
        )
    )

    @Test
    fun `typed nulls`() = assertIon10BinaryProducesBytecode(
        // All the other valid typeIds for a null value.
        "1F 2F 3F 4F 5F 6F 7F 8F 9F AF BF CF DF",
        intArrayOf(
            I_NULL_BOOL,
            I_NULL_INT,
            I_NULL_INT,
            I_NULL_FLOAT,
            I_NULL_DECIMAL,
            I_NULL_TIMESTAMP,
            I_NULL_STRING,
            I_NULL_SYMBOL,
            I_NULL_CLOB,
            I_NULL_BLOB,
            I_NULL_LIST,
            I_NULL_SEXP,
            I_NULL_STRUCT,
            I_END_OF_INPUT,
        )
    )

    @ParameterizedTest
    @CsvSource(
        "00 0F",
        "01 FF 0F",
        "02 FF FF 0F",
        "03 FF FF FF 0F",
        "04 FF FF FF FF 0F",
        "05 FF FF FF FF FF 0F",
        "06 FF FF FF FF FF FF 0F",
        "07 FF FF FF FF FF FF FF 0F",
        "08 FF FF FF FF FF FF FF FF 0F",
        "09 FF FF FF FF FF FF FF FF FF 0F",
        "0A FF FF FF FF FF FF FF FF FF FF 0F",
        "0B FF FF FF FF FF FF FF FF FF FF FF 0F",
        "0C FF FF FF FF FF FF FF FF FF FF FF FF 0F",
        "0D FF FF FF FF FF FF FF FF FF FF FF FF FF 0F",
        "0E 81 FF 0F",
        "0E 87 FF FF FF FF FF FF FF  0F",
        "0E 8E FF FF FF FF FF FF FF FF FF FF FF FF FF FF 0F",
    )
    fun `generator can skip NOPs`(bytes: String) = assertIon10BinaryProducesBytecode(bytes, intArrayOf(I_NULL_NULL, I_END_OF_INPUT))

    @Test
    fun `boolean values`() = assertIon10BinaryProducesBytecode(
        "10 11",
        intArrayOf(
            I_BOOL withData 0,
            I_BOOL withData 1,
            I_END_OF_INPUT,
        )
    )

    @ParameterizedTest
    @CsvSource(
        // Positive integers
        "                    0, 20",
        "                    1, 21 01",
        "                  123, 21 7B",
        "                    2, 22 00 02",
        "                    3, 23 00 00 03",
        "                    4, 24 00 00 00 04",
        "                    5, 25 00 00 00 00 05",
        "                    6, 26 00 00 00 00 00 06",
        "                    7, 27 00 00 00 00 00 00 07",
        "                    8, 28 00 00 00 00 00 00 00 08",
        "                    9, 29 00 00 00 00 00 00 00 00 09",
        "                   10, 2A 00 00 00 00 00 00 00 00 00 0A",
        "                   11, 2B 00 00 00 00 00 00 00 00 00 00 0B",
        "                   12, 2C 00 00 00 00 00 00 00 00 00 00 00 0C",
        "                   13, 2D 00 00 00 00 00 00 00 00 00 00 00 00 0D",
        "                65538, 23 01 00 02",
        "                65539, 25 00 00 01 00 03",
        "                65540, 29 00 00 00 00 00 00 01 00 04",
        "           4294967298, 25 01 00 00 00 02",
        "           4294967299, 29 00 00 00 00 01 00 00 00 03",
        " 18446744073709551618, 29 01 00 00 00 00 00 00 00 02",
        // Var length positive integers
        "                    0, 2E 80",
        "                   17, 2E 81 11",
        "                   18, 2E 82 00 12",
        "                   19, 2E 84 00 00 00 13",
        "                   20, 2E 88 00 00 00 00 00 00 00 14",
        "                   21, 2E 8C 00 00 00 00 00 00 00 00 00 00 00 15",
        "                   22, 2E 90 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 16",
        // Negative Integers
        "                   -1, 31 01",
        "                   -2, 32 00 02",
        "                   -3, 33 00 00 03",
        "                   -4, 34 00 00 00 04",
        "                   -5, 35 00 00 00 00 05",
        "                   -6, 36 00 00 00 00 00 06",
        "                   -7, 37 00 00 00 00 00 00 07",
        "                   -8, 38 00 00 00 00 00 00 00 08",
        "                   -9, 39 00 00 00 00 00 00 00 00 09",
        "                  -10, 3A 00 00 00 00 00 00 00 00 00 0A",
        "                  -11, 3B 00 00 00 00 00 00 00 00 00 00 0B",
        "                  -12, 3C 00 00 00 00 00 00 00 00 00 00 00 0C",
        "                  -13, 3D 00 00 00 00 00 00 00 00 00 00 00 00 0D",
        "               -65538, 33 01 00 02",
        "               -65539, 35 00 00 01 00 03",
        "               -65540, 39 00 00 00 00 00 00 01 00 04",
        "          -4294967298, 35 01 00 00 00 02",
        "          -4294967299, 39 00 00 00 00 01 00 00 00 03",
        "-18446744073709551618, 39 01 00 00 00 00 00 00 00 02",
        // Var length negative integers
        "                  -17, 3E 81 11",
        "                  -18, 3E 82 00 12",
        "                  -19, 3E 84 00 00 00 13",
        "                  -20, 3E 88 00 00 00 00 00 00 00 14",
        "                  -21, 3E 8C 00 00 00 00 00 00 00 00 00 00 00 15",
        "                  -22, 3E 90 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 16",
    )
    fun `int values`(expectedValue: BigInteger, bytes: String) {
        val bits = expectedValue.bitLength()
        val expectedConstantPool = ConstantPool()
        val expectedBytecode = when {
            bits < 16 -> intArrayOf(I_INT_I16 withData expectedValue.toInt(), I_END_OF_INPUT)
            bits < 32 -> intArrayOf(I_INT_I32, expectedValue.toInt(), I_END_OF_INPUT)
            bits < 64 -> intArrayOf(I_INT_I64, expectedValue.toLong().shr(Int.SIZE_BITS).toInt(), expectedValue.toInt(), I_END_OF_INPUT)
            else -> {
                expectedConstantPool.add(expectedValue)
                intArrayOf(I_INT_CP withData 0, I_END_OF_INPUT)
            }
        }
        bytecodeGeneratorFor(bytes).shouldGenerate(expectedBytecode, expectedConstantPool)
    }

    @ParameterizedTest
    @CsvSource(
        "30",
        "31 00",
        "3E 80",
        "3E 81 00",
    )
    fun `int negative zero is illegal`(bytes: String) = bytecodeGeneratorFor(bytes).refillShouldThrowIonException()

    @ParameterizedTest
    @CsvSource(
        "40, 0.0",
        "44 00 00 00 01, 1.4012984643e-45",
        "44 00 7F FF FF, 1.1754942107e-38",
        "44 00 80 00 00, 1.1754943508e-38",
        "44 7F 7F FF FF, 3.4028234664e38",
        "44 3F 7F FF FF, 0.999999940395355225",
        "44 3F 80 00 00, 1",
        "44 3F 80 00 01, 1.00000011920928955",
        "44 80 00 00 01, -1.4012984643e-45",
        "44 80 7F FF FF, -1.1754942107e-38",
        "44 80 80 00 00, -1.1754943508e-38",
        "44 FF 7F FF FF, -3.4028234664e38",
        "44 BF 7F FF FF, -0.999999940395355225",
        "44 BF 80 00 00, -1",
        "44 BF 80 00 01, -1.00000011920928955",
        "44 00 00 00 00, 0",
        "44 80 00 00 00, -0",
        "44 7F 80 00 00, Infinity",
        "44 FF 80 00 00, -Infinity",
        "44 7F C0 00 00, NaN",
        "44 C0 00 00 00, -2",
        "44 3E AA AA AB, 0.333333343267440796",
        "44 40 49 0F DB, 3.14159274101257324",
        "48 00 00 00 00 00 00 00 01, 4.9406564584124654e-324",
        "48 00 0F FF FF FF FF FF FF, 2.2250738585072009e-308",
        "48 00 10 00 00 00 00 00 00, 2.2250738585072014e-308",
        "48 7F EF FF FF FF FF FF FF, 1.7976931348623157e308",
        "48 3F EF FF FF FF FF FF FF, 0.99999999999999988898",
        "48 3F F0 00 00 00 00 00 00, 1",
        "48 3F F0 00 00 00 00 00 01, 1.0000000000000002220",
        "48 3F F0 00 00 00 00 00 02, 1.0000000000000004441",
        "48 80 00 00 00 00 00 00 01, -4.9406564584124654e-324",
        "48 80 0F FF FF FF FF FF FF, -2.2250738585072009e-308",
        "48 80 10 00 00 00 00 00 00, -2.2250738585072014e-308",
        "48 FF EF FF FF FF FF FF FF, -1.7976931348623157e308",
        "48 BF EF FF FF FF FF FF FF, -0.99999999999999988898",
        "48 BF F0 00 00 00 00 00 00, -1",
        "48 BF F0 00 00 00 00 00 01, -1.0000000000000002220",
        "48 BF F0 00 00 00 00 00 02, -1.0000000000000004441",
        "48 00 00 00 00 00 00 00 00, 0",
        "48 80 00 00 00 00 00 00 00, -0",
        "48 7F F0 00 00 00 00 00 00, Infinity",
        "48 FF F0 00 00 00 00 00 00, -Infinity",
        "48 7F F8 00 00 00 00 00 00, NaN",
        "48 C0 00 00 00 00 00 00 00, -2",
        "48 3F D5 55 55 55 55 55 55, 0.33333333333333331483",
        "48 40 09 21 FB 54 44 2D 18, 3.141592653589793116"
    )
    fun `float values`(bytes: String, expectedValue: Double) {
        val byteArray = bytes.hexStringToByteArray()
        val expectedBytecode = if (byteArray.size == 9) {
            intArrayOf(
                I_FLOAT_F64,
                expectedValue.toRawBits().shr(Int.SIZE_BITS).toInt(),
                expectedValue.toRawBits().toInt(),
                I_END_OF_INPUT,
            )
        } else {
            intArrayOf(
                I_FLOAT_F32,
                expectedValue.toFloat().toRawBits(),
                I_END_OF_INPUT,
            )
        }
        bytecodeGeneratorFor(bytes).shouldGenerate(expectedBytecode)
    }

    @ParameterizedTest
    @CsvSource(
        "41 00",
        "42 00 00",
        "43 00 00 00 00",
        "45 00 00 00 00 00",
        "46 00 00 00 00 00 00",
        "47 00 00 00 00 00 00 00",
        "49 00 00 00 00 00 00 00 00 00",
        "4A 00 00 00 00 00 00 00 00 00 00",
        "4B 00 00 00 00 00 00 00 00 00 00 00",
        "4C 00 00 00 00 00 00 00 00 00 00 00 00",
        "4D 00 00 00 00 00 00 00 00 00 00 00 00 00",
        "4E 84 3F 80 00 00",
    )
    fun `other float sizes are illegal`(bytes: String) = bytecodeGeneratorFor(bytes).refillShouldThrowIonException()

    @ParameterizedTest
    @CsvSource(
        // Actual content bytes do not matter in this test because it should always produce a reference without having to inspect the value content.
        // pos, len, bytes
        "1,  0, 50 0F",
        "3,  0, 01 FF 50 01 FF 0F",
        "4,  0, 02 FF FF 50 01 FF 0F",
        "4,  1, 02 FF FF 51 FF 01 FF 0F",
        "1,  2, 52 FF FF 0F",
        "1,  3, 53 FF FF FF 0F",
        "1,  4, 54 FF FF FF FF 0F",
        "1,  5, 55 FF FF FF FF FF 0F",
        "1,  6, 56 FF FF FF FF FF FF 0F",
        "1,  7, 57 FF FF FF FF FF FF FF 0F",
        "1,  8, 58 FF FF FF FF FF FF FF FF 0F",
        "1,  9, 59 FF FF FF FF FF FF FF FF FF 0F",
        "1, 10, 5A FF FF FF FF FF FF FF FF FF FF 0F",
        "1, 11, 5B FF FF FF FF FF FF FF FF FF FF FF 0F",
        "1, 12, 5C FF FF FF FF FF FF FF FF FF FF FF FF 0F",
        "1, 13, 5D FF FF FF FF FF FF FF FF FF FF FF FF FF 0F",
        "2,  1, 5E 81 FF 0F",
        "2,  7, 5E 87 FF FF FF FF FF FF FF  0F",
        "2, 14, 5E 8E FF FF FF FF FF FF FF FF FF FF FF FF FF FF 0F",
    )
    fun `decimal values`(position: Int, length: Int, bytes: String) = assertIon10BinaryProducesBytecode(
        bytes,
        intArrayOf(
            I_DECIMAL_REF withData length, position,
            I_NULL_NULL,
            I_END_OF_INPUT,
        )
    )

    @ParameterizedTest
    @CsvSource(
        // Expected, pos, len, bytes
        "        0.,   1,   0,   50",
        "       0.0,   1,   1,   51 C1",
        "      0.00,   1,   1,   51 C2",
        "      -0.0,   1,   2,   52 C1 80",
        "       1.8,   1,   2,   52 C1 12",
        "       180,   1,   2,   52 81 12",
        "    18e129,   1,   3,   53 01 81 12",
        "      2.58,   0,   3,   C2 01 02",
        "      2.58,   1,   3,   53 C2 01 02",
        "      2.58,   2,   3,   FF 53 C2 01 02",
        "      2.58,   3,   3,   FF FF 53 C2 01 02",
    )
    fun `read decimal references`(expectedValue: String, position: Int, length: Int, bytes: String) {
        val expected = Decimal.valueOf(expectedValue)
        val generator = bytecodeGeneratorFor(bytes)
        // We're comparing the string value because that will give us Ion equivalence rather than mathematical equivalence.
        assertEquals(expected.toEngineeringString(), generator.readDecimalReference(position, length).toEngineeringString())
    }

    @ParameterizedTest
    @CsvSource(
        // Actual content bytes do not matter in this test because it should always produce a reference without having to inspect the value content.
        // pos, len, bytes
        "1,  2, 62 FF FF 0F",
        "1,  3, 63 FF FF FF 0F",
        "1,  4, 64 FF FF FF FF 0F",
        "1,  5, 65 FF FF FF FF FF 0F",
        "1,  6, 66 FF FF FF FF FF FF 0F",
        "1,  7, 67 FF FF FF FF FF FF FF 0F",
        "1,  8, 68 FF FF FF FF FF FF FF FF 0F",
        "1,  9, 69 FF FF FF FF FF FF FF FF FF 0F",
        "1, 10, 6A FF FF FF FF FF FF FF FF FF FF 0F",
        "1, 11, 6B FF FF FF FF FF FF FF FF FF FF FF 0F",
        "1, 12, 6C FF FF FF FF FF FF FF FF FF FF FF FF 0F",
        "1, 13, 6D FF FF FF FF FF FF FF FF FF FF FF FF FF 0F",
        "2,  1, 6E 81 FF 0F",
        "2,  7, 6E 87 FF FF FF FF FF FF FF  0F",
        "2, 14, 6E 8E FF FF FF FF FF FF FF FF FF FF FF FF FF FF 0F",
    )
    fun `timestamp values`(position: Int, length: Int, bytes: String) = assertIon10BinaryProducesBytecode(
        bytes,
        intArrayOf(
            I_TIMESTAMP_REF withData length, position,
            I_NULL_NULL,
            I_END_OF_INPUT,
        )
    )

    @ParameterizedTest
    @CsvSource(
        // Expected, pos, len, bytes
        "2000T,                         0,  3, 80 0F D0",
        "2000T,                         1,  3, 63 80 0F D0",
        "2000T,                         2,  3, 00 63 80 0F D0",
        "2001-02T,                      0,  4, 80 0F D1 82",
        "2002-03-01T,                   0,  5, 80 0F D2 83 81",
        "2003-04-02T03:04Z,             0,  7, 80 0F D3 84 82 83 84",
        "2004-05-03T04:05:01Z,          0,  8, 80 0F D4 85 83 84 85 81",
        "2005-06-04T05:06:02Z,          0,  9, 80 0F D5 86 84 85 86 82 80",
        "2006-07-05T06:07:03Z,          0, 10, 80 0F D6 87 85 86 87 83 80 00",
        "2007-08-06T07:08:04Z,          0,  9, 80 0F D7 88 86 87 88 84 C0",
        "2008-09-07T08:09:05Z,          0,  9, 80 0F D8 89 87 88 89 85 81",
        "2009-10-08T09:10:06.00Z,       0,  9, 80 0F D9 8A 88 89 8A 86 C2",
        "2010-11-09T10:11:07.114Z,      0, 10, 80 0F DA 8B 89 8A 8B 87 C3 72",
        "2010-01-01T00:15:00+00:15,     0,  8, 8F 0F DA 81 81 80 80 80",
        "2010-01-01T00:00:00-00:00,     0,  8, C0 0F DA 81 81 80 80 80",
        "2010-01-01T00:45:00-00:15,     0,  8, CF 0F DA 81 81 81 80 80",
    )
    fun `read timestamp references`(expectedValue: String, position: Int, length: Int, bytes: String) {
        val expected = Timestamp.valueOf(expectedValue)
        val generator = bytecodeGeneratorFor(bytes)
        assertEquals(expected, generator.readTimestampReference(position, length))
    }

    @ParameterizedTest
    @CsvSource("60", "61 FF")
    fun `illegal timestamp type ids`(bytes: String) = bytecodeGeneratorFor(bytes).refillShouldThrowIonException()

    @Test
    fun `readShortTimestamp throws IllegalStateException`() {
        val generator = bytecodeGeneratorFor("")
        assertThrows<IllegalStateException> { generator.readShortTimestampReference(0, 1) }
    }

    @ParameterizedTest
    @CsvSource(
        "    0, 70 0F",
        "    1, 01 FF 71 01 0F",
        "    1, 02 FF FF 71 01 0F",
        "    4, 71 04 0F",
        "  255, 71 FF 0F",
        "  258, 72 01 02 0F",
        "  515, 73 00 02 03 0F",
        "    4, 7E 81 04 0F",
        "  259, 7E 82 01 03 0F",
        "66051, 7E 83 01 02 03 0F",
    )
    fun `symbol values`(sid: Int, bytes: String) = assertIon10BinaryProducesBytecode(
        bytes,
        intArrayOf(
            I_SYMBOL_SID withData sid,
            I_NULL_NULL,
            I_END_OF_INPUT,
        )
    )

    @ParameterizedTest
    @CsvSource(
        // Actual content bytes do not matter in this test because it should always produce a reference without having to inspect the value content.
        // pos, len, bytes
        "1,  0, 80 0F",
        "3,  0, 01 FF 80 01 FF 0F",
        "4,  0, 02 FF FF 80 01 FF 0F",
        "4,  1, 02 FF FF 81 FF 01 FF 0F",
        "1,  2, 82 FF FF 0F",
        "1,  3, 83 FF FF FF 0F",
        "1,  4, 84 FF FF FF FF 0F",
        "1,  5, 85 FF FF FF FF FF 0F",
        "1,  6, 86 FF FF FF FF FF FF 0F",
        "1,  7, 87 FF FF FF FF FF FF FF 0F",
        "1,  8, 88 FF FF FF FF FF FF FF FF 0F",
        "1,  9, 89 FF FF FF FF FF FF FF FF FF 0F",
        "1, 10, 8A FF FF FF FF FF FF FF FF FF FF 0F",
        "1, 11, 8B FF FF FF FF FF FF FF FF FF FF FF 0F",
        "1, 12, 8C FF FF FF FF FF FF FF FF FF FF FF FF 0F",
        "1, 13, 8D FF FF FF FF FF FF FF FF FF FF FF FF FF 0F",
        "2,  1, 8E 81 FF 0F",
        "2,  7, 8E 87 FF FF FF FF FF FF FF  0F",
        "2, 14, 8E 8E FF FF FF FF FF FF FF FF FF FF FF FF FF FF 0F",
    )
    fun `string values`(position: Int, length: Int, bytes: String) = assertIon10BinaryProducesBytecode(
        bytes,
        intArrayOf(
            I_STRING_REF withData length, position,
            I_NULL_NULL,
            I_END_OF_INPUT,
        )
    )

    @ParameterizedTest
    @CsvSource(
        // Expected, pos, len, bytes
        "'',          0,  0, 99",
        "a,           0,  1, 61",
        "ab,          0,  2, 61 62",
        "abcd,        0,  4, 61 62 63 64",
        "abcdefgh,    0,  8, 61 62 63 64 65 66 67 68",
        "abcd,        2,  4, 99 99 61 62 63 64 65 66 99 99",
    )
    fun `read string references`(expectedValue: String, position: Int, length: Int, bytes: String) {
        val generator = bytecodeGeneratorFor(bytes)
        assertEquals(expectedValue, generator.readTextReference(position, length))
    }

    @ParameterizedTest
    @CsvSource(
        // Actual content bytes do not matter in this test because it should always produce a reference without having to inspect the value content.
        // pos, len, bytes
        "1,  0, 90 0F",
        "3,  0, 01 FF 90 01 FF 0F",
        "4,  0, 02 FF FF 90 01 FF 0F",
        "4,  1, 02 FF FF 91 FF 01 FF 0F",
        "1,  2, 92 FF FF 0F",
        "1,  3, 93 FF FF FF 0F",
        "1,  4, 94 FF FF FF FF 0F",
        "1,  5, 95 FF FF FF FF FF 0F",
        "1,  6, 96 FF FF FF FF FF FF 0F",
        "1,  7, 97 FF FF FF FF FF FF FF 0F",
        "1,  8, 98 FF FF FF FF FF FF FF FF 0F",
        "1,  9, 99 FF FF FF FF FF FF FF FF FF 0F",
        "1, 10, 9A FF FF FF FF FF FF FF FF FF FF 0F",
        "1, 11, 9B FF FF FF FF FF FF FF FF FF FF FF 0F",
        "1, 12, 9C FF FF FF FF FF FF FF FF FF FF FF FF 0F",
        "1, 13, 9D FF FF FF FF FF FF FF FF FF FF FF FF FF 0F",
        "2,  1, 9E 81 FF 0F",
        "2,  7, 9E 87 FF FF FF FF FF FF FF  0F",
        "2, 14, 9E 8E FF FF FF FF FF FF FF FF FF FF FF FF FF FF 0F",
    )
    fun `clob values`(position: Int, length: Int, bytes: String) = assertIon10BinaryProducesBytecode(
        bytes,
        intArrayOf(
            I_CLOB_REF withData length, position,
            I_NULL_NULL,
            I_END_OF_INPUT,
        )
    )

    @ParameterizedTest
    @CsvSource(
        // Actual content bytes do not matter in this test because it should always produce a reference without having to inspect the value content.
        // pos, len, bytes
        "1,  0, A0 0F",
        "3,  0, 01 FF A0 01 FF 0F",
        "4,  0, 02 FF FF A0 01 FF 0F",
        "4,  1, 02 FF FF A1 FF 01 FF 0F",
        "1,  2, A2 FF FF 0F",
        "1,  3, A3 FF FF FF 0F",
        "1,  4, A4 FF FF FF FF 0F",
        "1,  5, A5 FF FF FF FF FF 0F",
        "1,  6, A6 FF FF FF FF FF FF 0F",
        "1,  7, A7 FF FF FF FF FF FF FF 0F",
        "1,  8, A8 FF FF FF FF FF FF FF FF 0F",
        "1,  9, A9 FF FF FF FF FF FF FF FF FF 0F",
        "1, 10, AA FF FF FF FF FF FF FF FF FF FF 0F",
        "1, 11, AB FF FF FF FF FF FF FF FF FF FF FF 0F",
        "1, 12, AC FF FF FF FF FF FF FF FF FF FF FF FF 0F",
        "1, 13, AD FF FF FF FF FF FF FF FF FF FF FF FF FF 0F",
        "2,  1, AE 81 FF 0F",
        "2,  7, AE 87 FF FF FF FF FF FF FF  0F",
        "2, 14, AE 8E FF FF FF FF FF FF FF FF FF FF FF FF FF FF 0F",
    )
    fun `blob values`(position: Int, length: Int, bytes: String) = assertIon10BinaryProducesBytecode(
        bytes,
        intArrayOf(
            I_BLOB_REF withData length, position,
            I_NULL_NULL,
            I_END_OF_INPUT,
        )
    )

    @ParameterizedTest
    @CsvSource(
        // Expected, pos, len, bytes
        "0,  0, 99",
        "0,  1, 61",
        "0,  2, 61 62",
        "0,  4, 61 62 63 64",
        "0,  8, 61 62 63 64 65 66 67 68",
        "2,  4, 99 99 61 62 63 64 65 66 99 99",
    )
    fun `read bytes references`(position: Int, length: Int, bytes: String) {
        val expected = ByteSlice(bytes.hexStringToByteArray(), position, position + length)
        val generator = bytecodeGeneratorFor(bytes)
        assertArrayEquals(expected.newByteArray(), generator.readBytesReference(position, length).newByteArray())
    }

    @Test
    fun `an empty list`() = assertIon10BinaryProducesBytecode(
        """
            B0
            BE 80
        """.trimIndent(),
        intArrayOf(
            I_LIST_START withData 1,
            I_END_CONTAINER,
            I_LIST_START withData 1,
            I_END_CONTAINER,
            I_END_OF_INPUT,
        )
    )

    @Test
    fun `a list with one value`() = assertIon10BinaryProducesBytecode(
        """
            B1 0F
        """.trimIndent(),
        intArrayOf(
            I_LIST_START withData 2,
            I_NULL_NULL,
            I_END_CONTAINER,
            I_END_OF_INPUT,
        )
    )

    @Test
    fun `a list with multiple values`() = assertIon10BinaryProducesBytecode(
        """
            B3 0F 10 11
        """.trimIndent(),
        intArrayOf(
            I_LIST_START withData 4,
            I_NULL_NULL,
            I_BOOL withData 0,
            I_BOOL withData 1,
            I_END_CONTAINER,
            I_END_OF_INPUT,
        )
    )

    @Test
    fun `a list with a nop value`() = assertIon10BinaryProducesBytecode(
        """
            B3 0F 00 11
        """.trimIndent(),
        intArrayOf(
            I_LIST_START withData 3,
            I_NULL_NULL,
            I_BOOL withData 1,
            I_END_CONTAINER,
            I_END_OF_INPUT,
        )
    )

    @Test
    fun `a var length list`() = assertIon10BinaryProducesBytecode(
        """
            BE 83 0F 10 11
        """.trimIndent(),
        intArrayOf(
            I_LIST_START withData 4,
            I_NULL_NULL,
            I_BOOL withData 0,
            I_BOOL withData 1,
            I_END_CONTAINER,
            I_END_OF_INPUT,
        )
    )

    @Test
    fun `an empty sexp`() = assertIon10BinaryProducesBytecode(
        """
            C0
            CE 80
        """.trimIndent(),
        intArrayOf(
            I_SEXP_START withData 1,
            I_END_CONTAINER,
            I_SEXP_START withData 1,
            I_END_CONTAINER,
            I_END_OF_INPUT,
        )
    )

    @Test
    fun `a sexp with one value`() = assertIon10BinaryProducesBytecode(
        """
            C1 0F
        """.trimIndent(),
        intArrayOf(
            I_SEXP_START withData 2,
            I_NULL_NULL,
            I_END_CONTAINER,
            I_END_OF_INPUT,
        )
    )

    @Test
    fun `a sexp with multiple values`() = assertIon10BinaryProducesBytecode(
        """
            C3 0F 10 11
        """.trimIndent(),
        intArrayOf(
            I_SEXP_START withData 4,
            I_NULL_NULL,
            I_BOOL withData 0,
            I_BOOL withData 1,
            I_END_CONTAINER,
            I_END_OF_INPUT,
        )
    )

    @Test
    fun `a sexp with a nop value`() = assertIon10BinaryProducesBytecode(
        """
            C3 0F 00 11
        """.trimIndent(),
        intArrayOf(
            I_SEXP_START withData 3,
            I_NULL_NULL,
            I_BOOL withData 1,
            I_END_CONTAINER,
            I_END_OF_INPUT,
        )
    )

    @Test
    fun `a var length sexp`() = assertIon10BinaryProducesBytecode(
        """
            CE 83 0F 10 11
        """.trimIndent(),
        intArrayOf(
            I_SEXP_START withData 4,
            I_NULL_NULL,
            I_BOOL withData 0,
            I_BOOL withData 1,
            I_END_CONTAINER,
            I_END_OF_INPUT,
        )
    )

    @Test
    fun `empty structs`() = assertIon10BinaryProducesBytecode(
        """
            D0
            D1 80
            DE 80
        """.trimIndent(),
        intArrayOf(
            I_STRUCT_START withData 1,
            I_END_CONTAINER,
            I_STRUCT_START withData 1,
            I_END_CONTAINER,
            I_STRUCT_START withData 1,
            I_END_CONTAINER,
            I_END_OF_INPUT,
        )
    )

    @Test
    fun `a struct with one field`() = assertIon10BinaryProducesBytecode(
        """
            D2 84 0F
        """.trimIndent(),
        intArrayOf(
            I_STRUCT_START withData 3,
            I_FIELD_NAME_SID withData 4,
            I_NULL_NULL,
            I_END_CONTAINER,
            I_END_OF_INPUT,
        )
    )

    @Test
    fun `a struct with multiple fields`() = assertIon10BinaryProducesBytecode(
        """
            D6 84 0F 85 10 86 11
        """.trimIndent(),
        intArrayOf(
            I_STRUCT_START withData 7,
            I_FIELD_NAME_SID withData 4,
            I_NULL_NULL,
            I_FIELD_NAME_SID withData 5,
            I_BOOL withData 0,
            I_FIELD_NAME_SID withData 6,
            I_BOOL withData 1,
            I_END_CONTAINER,
            I_END_OF_INPUT,
        )
    )

    @Test
    fun `a struct with a nop field`() = assertIon10BinaryProducesBytecode(
        """
            D6 84 0F 85 00 86 11
        """.trimIndent(),
        intArrayOf(
            I_STRUCT_START withData 6,
            I_FIELD_NAME_SID withData 4,
            I_NULL_NULL,
            I_FIELD_NAME_SID withData 5,
            I_FIELD_NAME_SID withData 6,
            I_BOOL withData 1,
            I_END_CONTAINER,
            I_END_OF_INPUT,
        )
    )

    @Test
    fun `a var length struct`() = assertIon10BinaryProducesBytecode(
        """
            DE 86 84 0F 85 10 86 11
        """.trimIndent(),
        intArrayOf(
            I_STRUCT_START withData 7,
            I_FIELD_NAME_SID withData 4,
            I_NULL_NULL,
            I_FIELD_NAME_SID withData 5,
            I_BOOL withData 0,
            I_FIELD_NAME_SID withData 6,
            I_BOOL withData 1,
            I_END_CONTAINER,
            I_END_OF_INPUT,
        )
    )

    @Test
    fun `a var length struct using opcode D1`() = assertIon10BinaryProducesBytecode(
        """
            D1 86 84 0F 85 10 86 11
        """.trimIndent(),
        intArrayOf(
            I_STRUCT_START withData 7,
            I_FIELD_NAME_SID withData 4,
            I_NULL_NULL,
            I_FIELD_NAME_SID withData 5,
            I_BOOL withData 0,
            I_FIELD_NAME_SID withData 6,
            I_BOOL withData 1,
            I_END_CONTAINER,
            I_END_OF_INPUT,
        )
    )

    @Test
    fun `one annotation`() = assertIon10BinaryProducesBytecode(
        """
            E3   | Annotations L=3
            81   | Inner annotation length = 1
            84   | $4::
            0F   |     null
        """.trimIndent(),
        intArrayOf(
            I_ANNOTATION_SID withData 4,
            I_NULL_NULL,
            I_END_OF_INPUT,
        )
    )

    @Test
    fun `multiple annotations`() = assertIon10BinaryProducesBytecode(
        """
            E5   | Annotations L=5
            83   | Inner annotation length = 3
            84   | $4::
            85   | $5::
            86   | $6::
            0F   |     null
        """.trimIndent(),
        intArrayOf(
            I_ANNOTATION_SID withData 4,
            I_ANNOTATION_SID withData 5,
            I_ANNOTATION_SID withData 6,
            I_NULL_NULL,
            I_END_OF_INPUT,
        )
    )

    @Test
    fun `var length annotations`() = assertIon10BinaryProducesBytecode(
        """
            EE   | Annotations L=5
            85   | Outer annotation length = 5
            83   | Inner annotation length = 3
            84   | $4::
            85   | $5::
            86   | $6::
            0F   |     null
        """.trimIndent(),
        intArrayOf(
            I_ANNOTATION_SID withData 4,
            I_ANNOTATION_SID withData 5,
            I_ANNOTATION_SID withData 6,
            I_NULL_NULL,
            I_END_OF_INPUT,
        )
    )

    @ParameterizedTest
    @CsvSource(
        "E3 81 84 00",
        "B4 E3 81 84 00",
        "C4 E3 81 84 00",
        "D5 84 E3 81 84 00",
    )
    fun `nop after annotations is illegal`(bytes: String) = bytecodeGeneratorFor(bytes).refillShouldThrowIonException()

    @ParameterizedTest
    @CsvSource(
        "E6 81 84 E0 01 10 EA",
        "B7 E6 81 84 E0 01 10 EA",
        "C7 E6 81 84 E0 01 10 EA",
        "D8 83 E6 81 84 E0 01 10 EA",
    )
    fun `ivm after annotations is illegal`(bytes: String) = bytecodeGeneratorFor(bytes).refillShouldThrowIonException()

    @ParameterizedTest
    @CsvSource(
        "E6 81 84 E3 81 84 0F",
        "B7 E6 81 84 E3 81 84 0F",
        "C7 E6 81 84 E3 81 84 0F",
        "D8 84 E6 81 84 E3 81 84 0F",
    )
    fun `annotations inside annotations wrapper is illegal`(bytes: String) = bytecodeGeneratorFor(bytes).refillShouldThrowIonException()

    @ParameterizedTest
    @CsvSource(
        "B4 E0 01 10 EA",
        "C4 E0 01 10 EA",
        "D5 83 E0 01 10 EA",
    )
    fun `ivm in container is illegal`(bytes: String) = bytecodeGeneratorFor(bytes).refillShouldThrowIonException()

    @ParameterizedTest
    @CsvSource(
        "12", "13", "14", "15", "16", "17", "18", "19", "1A", "1B", "1C", "1D", "1E",
        "E1", "E2", "EF",
        "F0", "F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8", "F9", "FA", "FB", "FC", "FD", "FE", "FF"
    )
    /** Illegal TypeIds not covered by other tests: 12..1E, EF, F0..FF, illegal annotations sizes */
    fun `other illegal type ids`(bytes: String) = bytecodeGeneratorFor(bytes).refillShouldThrowIonException()

    @Test
    fun `all values types inside a container`() = assertIon10BinaryProducesBytecode(
        """
            BE 94                 | [
               00 0F 10 20 40 50  |   <NOP> null, false, 0, 0e0, 0d0,
               62 80 80           |   0000T,
               70 80 90 A0        |   $0, "", {{""}}, {{}},
               B0 C0 D0           |   [], (), {},
               E3 81 84 0F        |   $4::null,
                                  | ]
        """,
        intArrayOf(
            I_LIST_START withData 25,
            I_NULL_NULL,
            I_BOOL,
            I_INT_I16,
            I_FLOAT_F32, 0.0f.toRawBits(),
            I_DECIMAL_REF, 8,
            I_TIMESTAMP_REF withData 2, 9,
            I_SYMBOL_SID,
            I_STRING_REF, 13,
            I_CLOB_REF, 14,
            I_BLOB_REF, 15,
            I_LIST_START withData 1, I_END_CONTAINER,
            I_SEXP_START withData 1, I_END_CONTAINER,
            I_STRUCT_START withData 1, I_END_CONTAINER,
            I_ANNOTATION_SID withData 4,
            I_NULL_NULL,
            I_END_CONTAINER,
            I_END_OF_INPUT,
        ),
    )

    @Test
    fun `complex data with symbol table`() {
        /*
          {
            name: "Fido",
            age: years::4,
            birthday: 2012-03-01T,
            toys: [ ball, rope ],
            weight: pounds::41.2,
          }
         */
        val generator = bytecodeGeneratorFor(
            """
            E0 01 00 EA EE B7 81 83  DE B3 87 BE B0 83 61 67  65 85 79 65 61 72 73 88  62 69 72 74 68 64 61 79 
            84 74 6F 79 73 84 62 61  6C 6C 84 72 6F 70 65 86  77 65 69 67 68 74 86 70  6F 75 6E 64 73 DE A1 84 
            84 46 69 64 6F 8A E4 81  8B 21 04 8C 65 C0 0F DC  83 81 8D B4 71 0E 71 0F  90 E6 81 91 53 C1 01 9C 
            """.trimIndent()
        )

        with(generator) {
            shouldGenerate(
                I_IVM.withData(0x0100)
            )
            shouldGenerate(
                I_DIRECTIVE_SET_SYMBOLS,
                I_SYMBOL_CP withData 0,
                I_SYMBOL_CP withData 1,
                I_SYMBOL_CP withData 2,
                I_SYMBOL_CP withData 3,
                I_SYMBOL_CP withData 4,
                I_SYMBOL_CP withData 5,
                I_SYMBOL_CP withData 6,
                I_SYMBOL_CP withData 7,
                I_END_CONTAINER,
            )
            shouldGenerate(
                *buildStruct(
                    I_FIELD_NAME_SID withData 4,
                    I_STRING_REF withData 4, 65,
                    I_FIELD_NAME_SID withData 10,
                    I_ANNOTATION_SID withData 11,
                    I_INT_I16 withData 4,
                    I_FIELD_NAME_SID withData 12,
                    I_TIMESTAMP_REF withData 5, 77,
                    I_FIELD_NAME_SID withData 13,
                    *buildList(
                        I_SYMBOL_SID withData 14,
                        I_SYMBOL_SID withData 15,
                    ),
                    I_FIELD_NAME_SID withData 16,
                    I_ANNOTATION_SID withData 17,
                    I_DECIMAL_REF withData 3, 93,
                ),
                I_END_OF_INPUT,
            )
        }
    }

    private fun assertIon10BinaryProducesBytecode(commentedHexBytes: String, expectedBytecode: IntArray) {
        bytecodeGeneratorFor(commentedHexBytes).shouldGenerate(expectedBytecode)
    }

    private fun bytecodeGeneratorFor(commentedHexBytes: String) = ByteArrayBytecodeGenerator10(commentedHexBytes.cleanCommentedHexBytes().hexStringToByteArray(), 0)

    // Helper functions to build bytecode a little more easily
    private fun buildStruct(vararg instructions: Int): IntArray = intArrayOf(I_STRUCT_START.withData(instructions.size + 1), *instructions, I_END_CONTAINER)
    private fun buildList(vararg instructions: Int): IntArray = intArrayOf(I_LIST_START.withData(instructions.size + 1), *instructions, I_END_CONTAINER)
    private infix fun Int.withData(data: Int): Int = this.packInstructionData(data)
}
