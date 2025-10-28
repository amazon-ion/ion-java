// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode

import com.amazon.ion.Decimal
import com.amazon.ion.IntegerSize
import com.amazon.ion.IonException
import com.amazon.ion.IonReader
import com.amazon.ion.IonType
import com.amazon.ion.SymbolToken
import com.amazon.ion.Timestamp
import com.amazon.ion.bytecode.ir.Instructions.I_ANNOTATION_CP
import com.amazon.ion.bytecode.ir.Instructions.I_ANNOTATION_REF
import com.amazon.ion.bytecode.ir.Instructions.I_ANNOTATION_SID
import com.amazon.ion.bytecode.ir.Instructions.I_ARGUMENT_NONE
import com.amazon.ion.bytecode.ir.Instructions.I_BLOB_CP
import com.amazon.ion.bytecode.ir.Instructions.I_BLOB_REF
import com.amazon.ion.bytecode.ir.Instructions.I_BOOL
import com.amazon.ion.bytecode.ir.Instructions.I_CLOB_CP
import com.amazon.ion.bytecode.ir.Instructions.I_CLOB_REF
import com.amazon.ion.bytecode.ir.Instructions.I_DECIMAL_CP
import com.amazon.ion.bytecode.ir.Instructions.I_DECIMAL_REF
import com.amazon.ion.bytecode.ir.Instructions.I_END_CONTAINER
import com.amazon.ion.bytecode.ir.Instructions.I_END_OF_INPUT
import com.amazon.ion.bytecode.ir.Instructions.I_FIELD_NAME_CP
import com.amazon.ion.bytecode.ir.Instructions.I_FIELD_NAME_REF
import com.amazon.ion.bytecode.ir.Instructions.I_FIELD_NAME_SID
import com.amazon.ion.bytecode.ir.Instructions.I_FLOAT_F32
import com.amazon.ion.bytecode.ir.Instructions.I_FLOAT_F64
import com.amazon.ion.bytecode.ir.Instructions.I_INT_CP
import com.amazon.ion.bytecode.ir.Instructions.I_INT_I16
import com.amazon.ion.bytecode.ir.Instructions.I_INT_I32
import com.amazon.ion.bytecode.ir.Instructions.I_INT_I64
import com.amazon.ion.bytecode.ir.Instructions.I_INT_REF
import com.amazon.ion.bytecode.ir.Instructions.I_IVM
import com.amazon.ion.bytecode.ir.Instructions.I_LIST_START
import com.amazon.ion.bytecode.ir.Instructions.I_SEXP_START
import com.amazon.ion.bytecode.ir.Instructions.I_STRING_CP
import com.amazon.ion.bytecode.ir.Instructions.I_STRING_REF
import com.amazon.ion.bytecode.ir.Instructions.I_STRUCT_START
import com.amazon.ion.bytecode.ir.Instructions.I_SYMBOL_CHAR
import com.amazon.ion.bytecode.ir.Instructions.I_SYMBOL_CP
import com.amazon.ion.bytecode.ir.Instructions.I_SYMBOL_REF
import com.amazon.ion.bytecode.ir.Instructions.I_SYMBOL_SID
import com.amazon.ion.bytecode.ir.Instructions.I_TIMESTAMP_CP
import com.amazon.ion.bytecode.ir.Instructions.I_TIMESTAMP_REF
import com.amazon.ion.bytecode.ir.Instructions.packInstructionData
import com.amazon.ion.bytecode.util.AppendableConstantPoolView
import com.amazon.ion.bytecode.util.ByteSlice
import com.amazon.ion.bytecode.util.BytecodeBuffer
import com.amazon.ion.bytecode.util.ConstantPool
import com.amazon.ion.impl._Private_Utils
import com.amazon.ion.system.IonSystemBuilder
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import java.math.BigDecimal
import java.math.BigInteger

class BytecodeIonReaderTest {

    private val ION = IonSystemBuilder.standard().build()

    @Test
    fun `read I_BOOL`() {
        val generator = MockGenerator(
            I_BOOL.packInstructionData(0),
            I_BOOL.packInstructionData(1),
            I_END_OF_INPUT,
        )
        with(BytecodeIonReader(generator)) {
            next() shouldBe IonType.BOOL
            type shouldBe IonType.BOOL
            booleanValue() shouldBe false

            next() shouldBe IonType.BOOL
            booleanValue() shouldBe true

            next() shouldBe null
        }
        generator.assertAllRefillsUsed()
    }

    @Nested
    inner class `INT operations` {

        @Test
        fun `read I_INT_16`() {
            val generator = MockGenerator(
                I_INT_I16.packInstructionData(123),
                I_END_OF_INPUT
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.INT
                isNullValue shouldBe false

                integerSize shouldBe IntegerSize.INT

                intValue() shouldBe 123
                longValue() shouldBe 123L
                bigIntegerValue() shouldBe 123.toBigInteger()
                doubleValue() shouldBe 123.toDouble()
                decimalValue() shouldBe Decimal.valueOf(123)
                bigDecimalValue() shouldBe 123.toBigDecimal()

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }

        @Test
        fun `read I_INT_32`() {
            val generator = MockGenerator(
                I_INT_I32,
                234,
                I_END_OF_INPUT,
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.INT
                isNullValue shouldBe false

                integerSize shouldBe IntegerSize.INT

                intValue() shouldBe 234
                longValue() shouldBe 234L
                bigIntegerValue() shouldBe 234.toBigInteger()
                doubleValue() shouldBe 234.toDouble()
                decimalValue() shouldBe Decimal.valueOf(234)
                bigDecimalValue() shouldBe 234.toBigDecimal()

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }

        @Test
        fun `read I_INT_64`() {
            val generator = MockGenerator(
                I_INT_I64,
                0x12345678,
                0x13579ACE,
                I_END_OF_INPUT,
            )

            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.INT
                isNullValue shouldBe false

                integerSize shouldBe IntegerSize.LONG

                // Because it's too large for an Int
                shouldThrow<IonException> { intValue() }

                longValue() shouldBe 0x12345678_13579ACEL
                bigIntegerValue() shouldBe 0x12345678_13579ACE.toBigInteger()
                doubleValue() shouldBe 0x12345678_13579ACE.toDouble()
                decimalValue() shouldBe Decimal.valueOf(0x12345678_13579ACE)
                bigDecimalValue() shouldBe 0x12345678_13579ACE.toBigDecimal()

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }

        @Test
        fun `read I_INT_CP`() {
            val expectedBigInteger = BigInteger("999999999999999999999999999999")

            val generator = MockGenerator(
                I_INT_CP.packInstructionData(4),
                I_END_OF_INPUT,
                constants = ConstantPool().apply {
                    repeat(4) { add(null) }
                    add(expectedBigInteger)
                }
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.INT
                isNullValue shouldBe false

                integerSize shouldBe IntegerSize.BIG_INTEGER

                shouldThrow<IonException> { intValue() }
                shouldThrow<IonException> { longValue() }
                bigIntegerValue() shouldBe expectedBigInteger
                doubleValue() shouldBe expectedBigInteger.toDouble()
                decimalValue() shouldBe Decimal.valueOf(expectedBigInteger)
                bigDecimalValue() shouldBe expectedBigInteger.toBigDecimal()

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }

        @Test
        fun `read I_INT_REF`() {
            val generator = MockGenerator(
                I_INT_REF.packInstructionData(10),
                123,
                I_END_OF_INPUT,
                references = mapOf(123 to BigInteger.TEN)
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.INT
                isNullValue shouldBe false

                integerSize shouldBe IntegerSize.BIG_INTEGER

                intValue() shouldBe 10
                longValue() shouldBe 10L
                bigIntegerValue() shouldBe 10.toBigInteger()
                doubleValue() shouldBe 10.toDouble()
                decimalValue() shouldBe Decimal.valueOf(10)
                bigDecimalValue() shouldBe 10.toBigDecimal()

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }
    }

    @Nested
    inner class `FLOAT operations` {

        @Test
        fun `read I_FLOAT_F32`() {
            val generator = MockGenerator(
                I_FLOAT_F32,
                0x40040000,
                I_END_OF_INPUT,
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.FLOAT
                isNullValue shouldBe false
                intValue() shouldBe 2
                longValue() shouldBe 2L
                bigIntegerValue() shouldBe 2.toBigInteger()
                doubleValue() shouldBe 2.0625
                decimalValue() shouldBe Decimal.valueOf(2.0625)
                bigDecimalValue() shouldBe 2.0625.toBigDecimal()

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }

        @Test
        fun `read I_FLOAT_F64`() {
            val generator = MockGenerator(
                I_FLOAT_F64,
                0x40000800,
                0x00000000,
                I_END_OF_INPUT,
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.FLOAT
                isNullValue shouldBe false
                intValue() shouldBe 2
                longValue() shouldBe 2L
                bigIntegerValue() shouldBe 2.toBigInteger()
                doubleValue() shouldBe 2.00390625
                decimalValue() shouldBe Decimal.valueOf(2.00390625)
                bigDecimalValue() shouldBe 2.00390625.toBigDecimal()

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }
    }

    @Nested
    inner class `DECIMAL operations` {

        @Test
        fun `read I_DECIMAL_CP`() {
            val generator = MockGenerator(
                I_DECIMAL_CP.packInstructionData(4),
                I_END_OF_INPUT,
                constants = ConstantPool().apply {
                    repeat(4) { add(null) }
                    add(Decimal.TEN)
                }
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.DECIMAL
                isNullValue shouldBe false
                intValue() shouldBe 10
                longValue() shouldBe 10L
                bigIntegerValue() shouldBe 10.toBigInteger()
                doubleValue() shouldBe 10.toDouble()
                decimalValue() shouldBe Decimal.valueOf(10)
                bigDecimalValue() shouldBe 10.toBigDecimal()

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }

        @Test
        fun `read I_DECIMAL_REF`() {
            val generator = MockGenerator(
                I_DECIMAL_REF.packInstructionData(4),
                123,
                I_END_OF_INPUT,
                references = mapOf(123 to Decimal.TEN)
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.DECIMAL
                isNullValue shouldBe false
                intValue() shouldBe 10
                longValue() shouldBe 10L
                bigIntegerValue() shouldBe 10.toBigInteger()
                doubleValue() shouldBe 10.toDouble()
                decimalValue() shouldBe Decimal.valueOf(10)
                bigDecimalValue() shouldBe 10.toBigDecimal()

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }
    }

    @Nested
    inner class `TIMESTAMP operations` {
        val THE_TIMESTAMP = Timestamp.valueOf("2025-10-27T")

        @Test
        fun `read I_TIMESTAMP_CP`() {
            val generator = MockGenerator(
                I_TIMESTAMP_CP.packInstructionData(4),
                I_END_OF_INPUT,
                constants = ConstantPool().apply {
                    repeat(4) { add(null) }
                    add(THE_TIMESTAMP)
                }
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.TIMESTAMP
                isNullValue shouldBe false
                timestampValue() shouldBe THE_TIMESTAMP
                dateValue() shouldBe THE_TIMESTAMP.dateValue()

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }

        @Test
        fun `read I_TIMESTAMP_REF`() {
            val generator = MockGenerator(
                I_TIMESTAMP_REF.packInstructionData(4),
                123,
                I_END_OF_INPUT,
                references = mapOf(123 to THE_TIMESTAMP)
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.TIMESTAMP
                isNullValue shouldBe false
                timestampValue() shouldBe THE_TIMESTAMP
                dateValue() shouldBe THE_TIMESTAMP.dateValue()

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }

        @Test
        fun `read I_SHORT_TIMESTAMP_REF`() {
            val generator = MockGenerator(
                I_TIMESTAMP_REF.packInstructionData(4),
                123,
                I_END_OF_INPUT,
                references = mapOf(123 to THE_TIMESTAMP)
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.TIMESTAMP
                isNullValue shouldBe false
                timestampValue() shouldBe THE_TIMESTAMP
                dateValue() shouldBe THE_TIMESTAMP.dateValue()

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }
    }

    @Nested
    inner class `STRING operations` {
        @Test
        fun `read I_STRING_CP`() {
            val generator = MockGenerator(
                I_STRING_CP.packInstructionData(4),
                I_END_OF_INPUT,
                constants = ConstantPool().apply {
                    repeat(4) { add(null) }
                    add("hello world")
                }
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.STRING
                isNullValue shouldBe false
                stringValue() shouldBe "hello world"

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }

        @Test
        fun `read I_STRING_REF`() {
            val generator = MockGenerator(
                I_STRING_REF.packInstructionData(4),
                123,
                I_END_OF_INPUT,
                references = mapOf(123 to "hello world")
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.STRING
                isNullValue shouldBe false
                stringValue() shouldBe "hello world"

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }
    }

    @Nested
    inner class `SYMBOL operations` {

        @Test
        fun `read I_SYMBOL_CP`() {
            val generator = MockGenerator(
                I_SYMBOL_CP.packInstructionData(4),
                I_END_OF_INPUT,
                constants = ConstantPool().apply {
                    repeat(4) { add(null) }
                    add("hello world")
                }
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.SYMBOL
                isNullValue shouldBe false
                stringValue() shouldBe "hello world"
                symbolValue() shouldBe symbolToken("hello world", -1)

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }

        @Test
        fun `read I_SYMBOL_REF`() {
            val generator = MockGenerator(
                I_SYMBOL_REF.packInstructionData(4),
                123,
                I_END_OF_INPUT,
                references = mapOf(123 to "hello world")
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.SYMBOL
                isNullValue shouldBe false
                stringValue() shouldBe "hello world"
                symbolValue() shouldBe symbolToken("hello world", -1)

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }

        @Test
        fun `read I_SYMBOL_SID`() {
            val generator = MockGenerator(
                I_SYMBOL_SID.packInstructionData(4),
                I_END_OF_INPUT,
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.SYMBOL
                isNullValue shouldBe false
                stringValue() shouldBe "name"
                symbolValue() shouldBe symbolToken("name", 4)

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }

        @Test
        fun `read I_SYMBOL_SID for $0`() {
            val generator = MockGenerator(
                I_SYMBOL_SID.packInstructionData(0),
                I_END_OF_INPUT,
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.SYMBOL
                isNullValue shouldBe false
                stringValue() shouldBe null
                symbolValue() shouldBe symbolToken(null, 0)

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }

        @Test
        fun `read I_SYMBOL_CHAR`() {
            val generator = MockGenerator(
                I_SYMBOL_CHAR.packInstructionData('a'.code),
                I_END_OF_INPUT,
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.SYMBOL
                isNullValue shouldBe false
                stringValue() shouldBe "a"
                symbolValue() shouldBe symbolToken("a", -1)

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }
    }

    @Nested
    inner class `LOB operations` {

        private val THE_BYTES = byteArrayOf(1, 2, 3, 4, 5, 6, 7, 8)
        private val THE_BYTES_COPIED_INTO_ARRAY = byteArrayOf(0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 0, 0, 0, 0)

        @Test
        fun `read I_CLOB_CP`() {
            val generator = MockGenerator(
                I_CLOB_CP.packInstructionData(4),
                I_END_OF_INPUT,
                constants = ConstantPool().apply {
                    repeat(4) { add(null) }
                    add(ByteSlice(THE_BYTES, 0, 8))
                }
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.CLOB
                isNullValue shouldBe false
                byteSize() shouldBe THE_BYTES.size
                assertArrayEquals(THE_BYTES, newBytes())
                val bytes = ByteArray(16)
                val numBytesCopied = getBytes(bytes, 4, bytes.size)
                numBytesCopied shouldBe THE_BYTES.size
                assertArrayEquals(THE_BYTES_COPIED_INTO_ARRAY, bytes)

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }

        @Test
        fun `read I_CLOB_REF`() {
            val generator = MockGenerator(
                I_CLOB_REF.packInstructionData(8),
                123,
                I_END_OF_INPUT,
                references = mapOf(123 to ByteSlice(THE_BYTES, 0, 8))
            )

            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.CLOB
                isNullValue shouldBe false
                byteSize() shouldBe THE_BYTES.size
                assertArrayEquals(THE_BYTES, newBytes())
                val bytes = ByteArray(16)
                val numBytesCopied = getBytes(bytes, 4, bytes.size)
                numBytesCopied shouldBe THE_BYTES.size
                assertArrayEquals(THE_BYTES_COPIED_INTO_ARRAY, bytes)

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }

        @Test
        fun `read I_BLOB_CP`() {
            val generator = MockGenerator(
                I_BLOB_CP.packInstructionData(4),
                I_END_OF_INPUT,
                constants = ConstantPool().apply {
                    repeat(4) { add(null) }
                    add(ByteSlice(THE_BYTES, 0, 8))
                }
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.BLOB
                isNullValue shouldBe false
                byteSize() shouldBe THE_BYTES.size
                assertArrayEquals(THE_BYTES, newBytes())
                val bytes = ByteArray(16)
                val numBytesCopied = getBytes(bytes, 4, bytes.size)
                numBytesCopied shouldBe THE_BYTES.size
                assertArrayEquals(THE_BYTES_COPIED_INTO_ARRAY, bytes)

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }

        @Test
        fun `read I_BLOB_REF`() {
            val generator = MockGenerator(
                I_BLOB_REF.packInstructionData(8),
                123,
                I_END_OF_INPUT,
                references = mapOf(123 to ByteSlice(THE_BYTES, 0, 8))
            )

            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.BLOB
                isNullValue shouldBe false
                byteSize() shouldBe THE_BYTES.size
                assertArrayEquals(THE_BYTES, newBytes())
                val bytes = ByteArray(16)
                val numBytesCopied = getBytes(bytes, 4, bytes.size)
                numBytesCopied shouldBe THE_BYTES.size
                assertArrayEquals(THE_BYTES_COPIED_INTO_ARRAY, bytes)

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }
    }

    @Nested
    inner class `LIST operations` {

        @Test
        fun `read a list`() {
            val generator = MockGenerator(
                I_LIST_START.packInstructionData(3),
                I_INT_I16.packInstructionData(0),
                I_INT_I16.packInstructionData(1),
                I_END_CONTAINER,
                I_INT_I16.packInstructionData(2),
                I_END_OF_INPUT,
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.LIST
                isNullValue shouldBe false

                depth shouldBe 0
                stepIn()
                depth shouldBe 1

                next() shouldBe IonType.INT
                intValue() shouldBe 0
                next() shouldBe IonType.INT
                intValue() shouldBe 1

                // End of list
                next() shouldBe null
                // Even if we try again, it should still be the end of the list.
                next() shouldBe null

                depth shouldBe 1
                stepOut()
                depth shouldBe 0

                next() shouldBe IonType.INT
                intValue() shouldBe 2

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }

        @Test
        fun `step out early from a list`() {
            val generator = MockGenerator(
                I_LIST_START.packInstructionData(3),
                I_INT_I16.packInstructionData(0),
                I_INT_I16.packInstructionData(1),
                I_END_CONTAINER,
                I_INT_I16.packInstructionData(2),
                I_END_OF_INPUT,
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.LIST
                isNullValue shouldBe false

                depth shouldBe 0
                stepIn()
                depth shouldBe 1

                next() shouldBe IonType.INT
                intValue() shouldBe 0

                depth shouldBe 1
                stepOut()
                depth shouldBe 0

                next() shouldBe IonType.INT
                intValue() shouldBe 2

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }

        @Test
        fun `skip a list`() {
            val generator = MockGenerator(
                I_LIST_START.packInstructionData(3),
                I_INT_I16.packInstructionData(0),
                I_INT_I16.packInstructionData(1),
                I_END_CONTAINER,
                I_INT_I16.packInstructionData(2),
                I_END_OF_INPUT,
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.LIST
                isNullValue shouldBe false
                depth shouldBe 0
                next() shouldBe IonType.INT
                depth shouldBe 0
                intValue() shouldBe 2

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }

        @Test
        fun `read an empty list`() {
            val generator = MockGenerator(
                I_LIST_START.packInstructionData(1),
                I_END_CONTAINER,
                // value here helps test that we are properly respecting the container boundaries.
                I_INT_I16.packInstructionData(2),
                I_END_OF_INPUT,
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.LIST
                isNullValue shouldBe false

                stepIn()
                // End of list
                next() shouldBe null
                // Even if we try again, it should still be the end of the list.
                next() shouldBe null
                stepOut()

                next() shouldBe IonType.INT

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }

        @Test
        fun `step out early from an empty list`() {
            val generator = MockGenerator(
                I_LIST_START.packInstructionData(1),
                I_END_CONTAINER,
                // value here helps test that we are properly respecting the container boundaries.
                I_INT_I16.packInstructionData(2),
                I_END_OF_INPUT,
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.LIST
                isNullValue shouldBe false

                stepIn()
                // Stepping out early in that we didn't even look to see if there is a value in the list
                stepOut()

                next() shouldBe IonType.INT

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }

        @Test
        fun `skip an empty list`() {
            val generator = MockGenerator(
                I_LIST_START.packInstructionData(1),
                I_END_CONTAINER,
                I_INT_I16.packInstructionData(2),
                I_END_OF_INPUT,
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.LIST
                isNullValue shouldBe false

                next() shouldBe IonType.INT
                intValue() shouldBe 2

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }
    }

    @Nested
    inner class `SEXP operations` {

        @Test
        fun `read a sexp`() {
            val generator = MockGenerator(
                I_SEXP_START.packInstructionData(3),
                I_INT_I16.packInstructionData(0),
                I_INT_I16.packInstructionData(1),
                I_END_CONTAINER,
                I_INT_I16.packInstructionData(2),
                I_END_OF_INPUT,
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.SEXP
                isNullValue shouldBe false

                depth shouldBe 0
                stepIn()
                depth shouldBe 1

                next() shouldBe IonType.INT
                intValue() shouldBe 0
                next() shouldBe IonType.INT
                intValue() shouldBe 1

                // End of sexp
                next() shouldBe null
                // Even if we try again, it should still be the end of the sexp.
                next() shouldBe null

                depth shouldBe 1
                stepOut()
                depth shouldBe 0

                next() shouldBe IonType.INT
                intValue() shouldBe 2

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }

        @Test
        fun `step out early from a sexp`() {
            val generator = MockGenerator(
                I_SEXP_START.packInstructionData(3),
                I_INT_I16.packInstructionData(0),
                I_INT_I16.packInstructionData(1),
                I_END_CONTAINER,
                I_INT_I16.packInstructionData(2),
                I_END_OF_INPUT,
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.SEXP
                isNullValue shouldBe false

                depth shouldBe 0
                stepIn()
                depth shouldBe 1

                next() shouldBe IonType.INT
                intValue() shouldBe 0

                depth shouldBe 1
                stepOut()
                depth shouldBe 0

                next() shouldBe IonType.INT
                intValue() shouldBe 2

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }

        @Test
        fun `skip a sexp`() {
            val generator = MockGenerator(
                I_SEXP_START.packInstructionData(3),
                I_INT_I16.packInstructionData(0),
                I_INT_I16.packInstructionData(1),
                I_END_CONTAINER,
                I_INT_I16.packInstructionData(2),
                I_END_OF_INPUT,
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.SEXP
                isNullValue shouldBe false

                next() shouldBe IonType.INT
                intValue() shouldBe 2

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }

        @Test
        fun `read an empty sexp`() {
            val generator = MockGenerator(
                I_SEXP_START.packInstructionData(1),
                I_END_CONTAINER,
                // value here helps test that we are properly respecting the container boundaries.
                I_INT_I16.packInstructionData(2),
                I_END_OF_INPUT,
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.SEXP
                isNullValue shouldBe false

                stepIn()
                // End of sexp
                next() shouldBe null
                // Even if we try again, it should still be the end of the sexp.
                next() shouldBe null
                stepOut()

                next() shouldBe IonType.INT

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }

        @Test
        fun `step out early from an empty sexp`() {
            val generator = MockGenerator(
                I_SEXP_START.packInstructionData(1),
                I_END_CONTAINER,
                // value here helps test that we are properly respecting the container boundaries.
                I_INT_I16.packInstructionData(2),
                I_END_OF_INPUT,
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.SEXP
                isNullValue shouldBe false

                stepIn()
                // Stepping out early in that we didn't even look to see if there is a value in the list
                stepOut()

                next() shouldBe IonType.INT

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }

        @Test
        fun `skip an empty sexp`() {
            val generator = MockGenerator(
                I_SEXP_START.packInstructionData(1),
                I_END_CONTAINER,
                I_INT_I16.packInstructionData(2),
                I_END_OF_INPUT,
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.SEXP
                isNullValue shouldBe false

                next() shouldBe IonType.INT
                intValue() shouldBe 2

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }
    }

    @Nested
    inner class `STRUCT operations` {

        private fun IonReader.checkFieldName(text: String?, sid: Int) {
            fieldName shouldBe text
            fieldId shouldBe sid
            fieldNameSymbol shouldBe symbolToken(text, sid)
        }

        // Some of these tests do not check the field name. That is intentional so that different field name instructions
        // can be tested somewhat independently of the struct traversal logic.

        @Test
        fun `read a struct`() {
            val generator = MockGenerator(
                I_STRUCT_START.packInstructionData(5),
                I_FIELD_NAME_SID.packInstructionData(4),
                I_INT_I16.packInstructionData(0),
                I_FIELD_NAME_SID.packInstructionData(5),
                I_INT_I16.packInstructionData(1),
                I_END_CONTAINER,
                I_INT_I16.packInstructionData(2),
                I_END_OF_INPUT,
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.STRUCT
                isNullValue shouldBe false
                isInStruct shouldBe false
                depth shouldBe 0

                stepIn()

                isInStruct shouldBe true
                depth shouldBe 1

                next() shouldBe IonType.INT
                intValue() shouldBe 0
                next() shouldBe IonType.INT
                intValue() shouldBe 1

                // End of struct
                next() shouldBe null
                // Even if we try again, it should still be the end of the struct.
                next() shouldBe null
                stepOut()

                isInStruct shouldBe false
                depth shouldBe 0

                next() shouldBe IonType.INT
                intValue() shouldBe 2

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }

        @Test
        fun `step out early from a struct`() {
            val generator = MockGenerator(
                I_STRUCT_START.packInstructionData(5),
                I_FIELD_NAME_SID.packInstructionData(4),
                I_INT_I16.packInstructionData(0),
                I_FIELD_NAME_SID.packInstructionData(5),
                I_INT_I16.packInstructionData(1),
                I_END_CONTAINER,
                I_INT_I16.packInstructionData(2),
                I_END_OF_INPUT,
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.STRUCT
                isNullValue shouldBe false
                isInStruct shouldBe false
                depth shouldBe 0

                stepIn()

                isInStruct shouldBe true
                depth shouldBe 1

                next() shouldBe IonType.INT
                intValue() shouldBe 0
                stepOut()

                isInStruct shouldBe false
                depth shouldBe 0

                next() shouldBe IonType.INT
                intValue() shouldBe 2

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }

        @Test
        fun `skip a struct`() {
            val generator = MockGenerator(
                I_STRUCT_START.packInstructionData(5),
                I_FIELD_NAME_SID.packInstructionData(4),
                I_INT_I16.packInstructionData(0),
                I_FIELD_NAME_SID.packInstructionData(5),
                I_INT_I16.packInstructionData(1),
                I_END_CONTAINER,
                I_INT_I16.packInstructionData(2),
                I_END_OF_INPUT,
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.STRUCT
                isNullValue shouldBe false

                isInStruct shouldBe false
                depth shouldBe 0

                next() shouldBe IonType.INT

                isInStruct shouldBe false
                depth shouldBe 0

                intValue() shouldBe 2

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }

        @Test
        fun `read an empty struct`() {
            val generator = MockGenerator(
                I_STRUCT_START.packInstructionData(1),
                I_END_CONTAINER,
                // value here helps test that we are properly respecting the container boundaries.
                I_INT_I16.packInstructionData(2),
                I_END_OF_INPUT,
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.STRUCT
                isNullValue shouldBe false

                stepIn()
                // End of struct
                next() shouldBe null
                // Even if we try again, it should still be the end of the struct.
                next() shouldBe null
                stepOut()

                next() shouldBe IonType.INT

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }

        @Test
        fun `step out early from an empty struct`() {
            val generator = MockGenerator(
                I_STRUCT_START.packInstructionData(1),
                I_END_CONTAINER,
                // value here helps test that we are properly respecting the container boundaries.
                I_INT_I16.packInstructionData(2),
                I_END_OF_INPUT,
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.STRUCT
                isNullValue shouldBe false

                stepIn()
                // Stepping out early in that we didn't even look to see if there is a value in the list
                stepOut()

                next() shouldBe IonType.INT

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }

        @Test
        fun `skip an empty struct`() {
            val generator = MockGenerator(
                I_STRUCT_START.packInstructionData(1),
                I_END_CONTAINER,
                I_INT_I16.packInstructionData(2),
                I_END_OF_INPUT,
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.STRUCT
                isNullValue shouldBe false

                next() shouldBe IonType.INT
                intValue() shouldBe 2

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }

        @Test
        fun `read I_FIELD_NAME_SID`() {
            val generator = MockGenerator(
                I_STRUCT_START.packInstructionData(7),
                I_FIELD_NAME_SID.packInstructionData(4),
                I_INT_I16.packInstructionData(0),
                I_FIELD_NAME_SID.packInstructionData(5),
                I_INT_I16.packInstructionData(1),
                I_FIELD_NAME_SID.packInstructionData(0),
                I_INT_I16.packInstructionData(2),
                I_END_CONTAINER,
                I_END_OF_INPUT,
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.STRUCT
                isNullValue shouldBe false

                stepIn()

                next() shouldBe IonType.INT
                checkFieldName("name", 4)
                intValue() shouldBe 0

                next() shouldBe IonType.INT
                checkFieldName("version", 5)
                intValue() shouldBe 1

                next() shouldBe IonType.INT
                checkFieldName(null, 0)
                intValue() shouldBe 2

                next() shouldBe null
                stepOut()
                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }

        @Test
        fun `read I_FIELD_NAME_CP`() {
            val generator = MockGenerator(
                I_STRUCT_START.packInstructionData(5),
                I_FIELD_NAME_CP.packInstructionData(3),
                I_INT_I16.packInstructionData(0),
                I_FIELD_NAME_CP.packInstructionData(4),
                I_INT_I16.packInstructionData(1),
                I_END_CONTAINER,
                I_END_OF_INPUT,
                constants = ConstantPool().apply {
                    repeat(3) { add(null) }
                    add("foo")
                    add("bar")
                }
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.STRUCT
                isNullValue shouldBe false

                stepIn()

                next() shouldBe IonType.INT
                checkFieldName("foo", -1)
                intValue() shouldBe 0
                next() shouldBe IonType.INT
                checkFieldName("bar", -1)
                intValue() shouldBe 1

                next() shouldBe null
                stepOut()
                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }

        @Test
        fun `read I_FIELD_NAME_REF`() {
            val generator = MockGenerator(
                I_STRUCT_START.packInstructionData(7),
                I_FIELD_NAME_REF.packInstructionData(3),
                10,
                I_INT_I16.packInstructionData(0),
                I_FIELD_NAME_REF.packInstructionData(3),
                15,
                I_INT_I16.packInstructionData(1),
                I_END_CONTAINER,
                I_END_OF_INPUT,
                references = mapOf(
                    10 to "foo",
                    15 to "bar",
                )
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.STRUCT
                isNullValue shouldBe false

                stepIn()

                next() shouldBe IonType.INT
                checkFieldName("foo", -1)
                intValue() shouldBe 0
                next() shouldBe IonType.INT
                checkFieldName("bar", -1)
                intValue() shouldBe 1

                next() shouldBe null
                stepOut()
                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }
    }

    @Nested
    inner class `ANNOTATION operations` {

        @Test
        fun `read I_ANNOTATION_SID`() {
            val generator = MockGenerator(
                I_ANNOTATION_SID.packInstructionData(5),
                I_INT_I16.packInstructionData(0),
                I_INT_I16.packInstructionData(1),
                I_END_OF_INPUT,
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.INT
                intValue() shouldBe 0
                typeAnnotations shouldBe arrayOf("version")
                typeAnnotationSymbols shouldBe arrayOf(symbolToken("version", 5))
                iterateTypeAnnotations().asSequence().toList() shouldBe listOf("version")

                next() shouldBe IonType.INT
                intValue() shouldBe 1
                // No annotations should leak into this value.
                typeAnnotations shouldBe emptyArray()
                typeAnnotationSymbols shouldBe emptyArray()

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }

        @Test
        fun `read I_ANNOTATION_SID for $0`() {
            val generator = MockGenerator(
                I_ANNOTATION_SID.packInstructionData(0),
                I_INT_I16.packInstructionData(0),
                I_INT_I16.packInstructionData(1),
                I_END_OF_INPUT,
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.INT
                intValue() shouldBe 0
                typeAnnotations shouldBe arrayOf(null)
                typeAnnotationSymbols shouldBe arrayOf(symbolToken(null, 0))
                iterateTypeAnnotations().asSequence().toList() shouldBe listOf(null)

                next() shouldBe IonType.INT
                intValue() shouldBe 1
                // No annotations should leak into this value.
                typeAnnotations shouldBe emptyArray()
                typeAnnotationSymbols shouldBe emptyArray()

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }

        @Test
        fun `read I_ANNOTATION_CP`() {
            val generator = MockGenerator(
                I_ANNOTATION_CP.packInstructionData(0),
                I_INT_I16.packInstructionData(0),
                I_INT_I16.packInstructionData(1),
                I_END_OF_INPUT,
                constants = ConstantPool().apply { add("foo") }
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.INT
                intValue() shouldBe 0
                typeAnnotations shouldBe arrayOf("foo")
                typeAnnotationSymbols shouldBe arrayOf(symbolToken("foo", -1))
                iterateTypeAnnotations().asSequence().toList() shouldBe listOf("foo")

                next() shouldBe IonType.INT
                intValue() shouldBe 1
                // No annotations should leak into this value.
                typeAnnotations shouldBe emptyArray()
                typeAnnotationSymbols shouldBe emptyArray()

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }

        @Test
        fun `read I_ANNOTATION_REF`() {
            val generator = MockGenerator(
                I_ANNOTATION_REF.packInstructionData(3),
                5,
                I_INT_I16.packInstructionData(0),
                I_INT_I16.packInstructionData(1),
                I_END_OF_INPUT,
                references = mapOf(5 to "foo"),
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.INT
                intValue() shouldBe 0
                typeAnnotations shouldBe arrayOf("foo")
                typeAnnotationSymbols shouldBe arrayOf(symbolToken("foo", -1))
                iterateTypeAnnotations().asSequence().toList() shouldBe listOf("foo")

                next() shouldBe IonType.INT
                intValue() shouldBe 1
                // No annotations should leak into this value.
                typeAnnotations shouldBe emptyArray()
                typeAnnotationSymbols shouldBe emptyArray()

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }

        @Test
        fun `read multiple annotations on a single value`() {
            val generator = MockGenerator(
                I_ANNOTATION_SID.packInstructionData(4),
                I_ANNOTATION_SID.packInstructionData(5),
                I_INT_I16.packInstructionData(0),
                I_INT_I16.packInstructionData(1),
                I_END_OF_INPUT,
                references = mapOf(5 to "foo"),
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.INT
                intValue() shouldBe 0
                typeAnnotations shouldBe arrayOf("name", "version")
                typeAnnotationSymbols shouldBe arrayOf(symbolToken("name", 4), symbolToken("version", 5))
                iterateTypeAnnotations().asSequence().toList() shouldBe listOf("name", "version")

                next() shouldBe IonType.INT
                intValue() shouldBe 1
                // No annotations should leak into this value.
                typeAnnotations shouldBe emptyArray()
                typeAnnotationSymbols shouldBe emptyArray()

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }

        @Test
        fun `read multiple types of annotations on a single value`() {
            val generator = MockGenerator(
                I_ANNOTATION_SID.packInstructionData(4),
                I_ANNOTATION_CP.packInstructionData(1),
                I_ANNOTATION_REF.packInstructionData(3),
                10,
                I_INT_I16.packInstructionData(0),
                I_INT_I16.packInstructionData(1),
                I_END_OF_INPUT,
                constants = ConstantPool().apply { add(null); add("foo") },
                references = mapOf(10 to "bar"),
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.INT
                intValue() shouldBe 0
                typeAnnotations shouldBe arrayOf("name", "foo", "bar")
                typeAnnotationSymbols shouldBe arrayOf(symbolToken("name", 4), symbolToken("foo", -1), symbolToken("bar", -1))
                iterateTypeAnnotations().asSequence().toList() shouldBe listOf("name", "foo", "bar")

                next() shouldBe IonType.INT
                intValue() shouldBe 1
                // No annotations should leak into this value.
                typeAnnotations shouldBe emptyArray()
                typeAnnotationSymbols shouldBe emptyArray()

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }
    }

    @Nested
    inner class `I_REFILL and I_END_OF_INPUT cases` {
        @Test
        fun `handle I_REFILL`() {
            val generator = MockGenerator(
                MockGenerator.Refill(I_INT_I16.packInstructionData(1)),
                // REFILL is automatically appended by the BytecodeIonReader.
                MockGenerator.Refill(
                    I_INT_I16.packInstructionData(2),
                    I_END_OF_INPUT,
                )
            )
            BytecodeIonReader(generator) shouldProduce "1 2"
            generator.assertAllRefillsUsed()
        }

        @Test
        fun `reading I_END_OF_INPUT indicates no value`() {
            val generator = MockGenerator(I_END_OF_INPUT)
            with(BytecodeIonReader(generator)) {
                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }

        @Test
        fun `after reading I_END_OF_INPUT, it should be possible to check for more input`() {
            val generator = MockGenerator(
                MockGenerator.Refill(I_END_OF_INPUT),
                MockGenerator.Refill(
                    I_INT_I16.packInstructionData(1),
                    I_END_OF_INPUT,
                )
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe null
                next() shouldBe IonType.INT
                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }
    }

    @Test
    fun `handle I_IVM`() {
        val generator = MockGenerator(
            I_INT_I16.packInstructionData(1),
            I_IVM.packInstructionData(0x0101),
            I_INT_I16.packInstructionData(2),
            I_IVM.packInstructionData(0x0100),
            I_INT_I16.packInstructionData(3),
            I_END_OF_INPUT,
        )
        with(BytecodeIonReader(generator)) {
            next() shouldBe IonType.INT
            minorVersion shouldBe 0
            next() shouldBe IonType.INT
            minorVersion shouldBe 1
            next() shouldBe IonType.INT
            minorVersion shouldBe 0
            next() shouldBe null
        }
        generator.assertAllRefillsUsed()
    }

    @Nested
    inner class `I_ARGUMENT_NONE cases` {

        @Test
        fun `discard I_ARGUMENT_NONE`() {
            val generator = MockGenerator(
                I_INT_I16.packInstructionData(1),
                I_ARGUMENT_NONE,
                I_INT_I16.packInstructionData(2),
                I_END_OF_INPUT,
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.INT
                intValue() shouldBe 1
                next() shouldBe IonType.INT
                intValue() shouldBe 2
                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }

        @Test
        fun `discard any annotations preceding I_ARGUMENT_NONE`() {
            val generator = MockGenerator(
                I_INT_I16.packInstructionData(1),
                I_ANNOTATION_SID.packInstructionData(4),
                I_ARGUMENT_NONE,
                I_INT_I16.packInstructionData(2),
                I_END_OF_INPUT,
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.INT
                typeAnnotations shouldBe emptyArray()
                intValue() shouldBe 1

                next() shouldBe IonType.INT
                typeAnnotations shouldBe emptyArray()
                intValue() shouldBe 2

                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }

        @Test
        fun `discard I_ARGUMENT_NONE in a list`() {
            val generator = MockGenerator(
                I_LIST_START.packInstructionData(4),
                I_INT_I16.packInstructionData(1),
                I_ARGUMENT_NONE,
                I_INT_I16.packInstructionData(2),
                I_END_CONTAINER,
                I_END_OF_INPUT,
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.LIST
                stepIn()

                next() shouldBe IonType.INT
                intValue() shouldBe 1
                next() shouldBe IonType.INT
                intValue() shouldBe 2
                next() shouldBe null

                stepOut()
                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }

        @Test
        fun `discard a field name preceding I_ARGUMENT_NONE`() {
            val generator = MockGenerator(
                I_STRUCT_START.packInstructionData(7),
                I_FIELD_NAME_SID.packInstructionData(4),
                I_INT_I16.packInstructionData(1),
                I_FIELD_NAME_SID.packInstructionData(5),
                I_ARGUMENT_NONE,
                I_FIELD_NAME_SID.packInstructionData(6),
                I_INT_I16.packInstructionData(2),
                I_END_CONTAINER,
                I_END_OF_INPUT,
            )
            with(BytecodeIonReader(generator)) {
                next() shouldBe IonType.STRUCT
                stepIn()

                next() shouldBe IonType.INT
                fieldName shouldBe "name"
                intValue() shouldBe 1

                next() shouldBe IonType.INT
                fieldName shouldBe "imports"
                intValue() shouldBe 2

                next() shouldBe null

                stepOut()
                next() shouldBe null
            }
            generator.assertAllRefillsUsed()
        }
    }

    /*
    TODO: Test cases for
        I_DIRECTIVE_SET_SYMBOLS
        I_DIRECTIVE_ADD_SYMBOLS
        I_DIRECTIVE_SET_MACROS
        I_DIRECTIVE_ADD_MACROS
        I_DIRECTIVE_USE
        I_DIRECTIVE_MODULE
        I_DIRECTIVE_ENCODING
        I_INVOKE -- if we are exposing macro invocations
        I_META_OFFSET
        I_META_ROWCOL
        I_META_COMMENT
     */

    /** Helper function that creates a [SymbolToken] instance. */
    private fun symbolToken(text: String?, id: Int): SymbolToken = _Private_Utils.newSymbolToken(text, id)

    /** Helper function that asserts an action throws a particular exception. */
    private inline fun <reified T : Throwable> shouldThrow(action: Executable) = assertThrows(T::class.java, action)

    /** Asserts that [this] is equal to [expected]. */
    private infix fun <T> T.shouldBe(expected: T) = assertEquals(expected, this)

    /** Asserts that [this] array's contents are equal to the [expected] array's content. */
    private infix fun <T> Array<T>.shouldBe(expected: Array<T>) = assertArrayEquals(expected, this)

    /** Asserts that this [BytecodeIonReader] produces the [expected] Ion data. */
    private infix fun BytecodeIonReader.shouldProduce(expected: String) {
        val iter10 = ION.iterate(ION.newReader(expected))
        val iter11 = ION.iterate(this)
        while (iter10.hasNext() && iter11.hasNext()) {
            assertEquals(iter10.next(), iter11.next())
        }
        assertEquals(iter10.hasNext(), iter11.hasNext())
    }

    /**
     * A mock [BytecodeGenerator]. Supplied by one or more [Refill]s.
     */
    private class MockGenerator(vararg refills: Refill) : BytecodeGenerator {

        class Refill(
            vararg val bytecode: Int,
            val constants: ConstantPool = ConstantPool(),
            val references: Map<Int, Any?> = emptyMap(),
            val minorVersion: Int = 1,
        )

        /** Convenience constructor that constructs a [MockGenerator] with exactly one [Refill] */
        constructor(
            vararg bytecode: Int,
            constants: ConstantPool = ConstantPool(),
            references: Map<Int, Any?> = emptyMap(),
            minorVersion: Int = 1,
        ) : this(Refill(*bytecode, constants = constants, references = references, minorVersion = minorVersion))

        /** Asserts that all refills for this [MockGenerator] have been used */
        fun assertAllRefillsUsed() = assertTrue(refills.isEmpty())

        private val refills = ArrayDeque<Refill>().apply { addAll(refills) }
        private lateinit var current: Refill

        override fun refill(
            destination: BytecodeBuffer,
            constantPool: AppendableConstantPoolView,
            macroSrc: IntArray,
            macroIndices: IntArray,
            symTab: Array<String?>
        ) {
            val current = refills.removeFirst()
            this.current = current
            val bytecode = current.bytecode
            destination.addSlice(bytecode, 0, bytecode.size)
            current.constants.toArray().forEach { c -> constantPool.add(c) }
        }

        override fun readBigIntegerReference(position: Int, length: Int): BigInteger = current.references[position] as BigInteger
        override fun readDecimalReference(position: Int, length: Int): Decimal = Decimal.valueOf(current.references[position] as BigDecimal)
        override fun readShortTimestampReference(position: Int, opcode: Int): Timestamp = current.references[position] as Timestamp
        override fun readTimestampReference(position: Int, length: Int): Timestamp = current.references[position] as Timestamp
        override fun readTextReference(position: Int, length: Int): String = current.references[position] as String
        override fun readBytesReference(position: Int, length: Int): ByteSlice = current.references[position] as ByteSlice
        override fun ionMinorVersion(): Int = current.minorVersion
        override fun getGeneratorForMinorVersion(minorVersion: Int): BytecodeGenerator = this
    }
}
