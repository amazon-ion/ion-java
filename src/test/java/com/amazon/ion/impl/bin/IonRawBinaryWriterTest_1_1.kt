// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.bin

import com.amazon.ion.*
import com.amazon.ion.TestUtils.*
import com.amazon.ion.impl.*
import java.io.ByteArrayOutputStream
import java.math.BigDecimal
import java.math.BigInteger
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

class IonRawBinaryWriterTest_1_1 {

    private inline fun writeAsHexString(autoClose: Boolean = true, block: IonRawBinaryWriter_1_1.() -> Unit): String {
        val baos = ByteArrayOutputStream()
        val rawWriter = IonRawBinaryWriter_1_1(
            out = baos,
            buffer = WriteBuffer(BlockAllocatorProviders.basicProvider().vendAllocator(32)) {},
            lengthPrefixPreallocation = 1,
        )
        block.invoke(rawWriter)
        if (autoClose) rawWriter.close()
        @OptIn(ExperimentalStdlibApi::class)
        return baos.toByteArray().joinToString(" ") { it.toHexString(HexFormat.UpperCase) }
    }

    /**
     * @param hexBytes a string containing white-space delimited pairs of hex digits representing the expected output.
     *                 The string may contain multiple lines. Anything after a `|` character on a line is ignored, so
     *                 you can use `|` to add comments.
     */
    private inline fun assertWriterOutputEquals(hexBytes: String, autoClose: Boolean = true, block: IonRawBinaryWriter_1_1.() -> Unit) {
        assertEquals(cleanCommentedHexBytes(hexBytes), writeAsHexString(autoClose, block))
    }

    private inline fun assertWriterThrows(block: IonRawBinaryWriter_1_1.() -> Unit) {
        val baos = ByteArrayOutputStream()
        val rawWriter = IonRawBinaryWriter_1_1(
            out = baos,
            buffer = WriteBuffer(BlockAllocatorProviders.basicProvider().vendAllocator(32)) {},
            lengthPrefixPreallocation = 1,
        )
        assertThrows<IonException> {
            block.invoke(rawWriter)
        }
    }

    @Test
    fun `calling close while in a container should throw IonException`() {
        assertWriterThrows {
            stepInList(false)
            close()
        }
    }

    @Test
    fun `calling finish while in a container should throw IonException`() {
        assertWriterThrows {
            stepInList(true)
            finish()
        }
    }

    @Test
    fun `calling finish with a dangling annotation should throw IonException`() {
        assertWriterThrows {
            writeAnnotations(10)
            finish()
        }
    }

    @Test
    fun `calling stepOut while not in a container should throw IonException`() {
        assertWriterThrows {
            stepOut()
        }
    }

    @Test
    fun `calling stepOut with a dangling annotation should throw IonException`() {
        assertWriterThrows {
            stepInList(true)
            writeAnnotations(10)
            stepOut()
        }
    }

    @Test
    fun `calling writeIVM when in a container should throw IonException`() {
        assertWriterThrows {
            stepInList(false)
            writeIVM()
        }
    }

    @Test
    fun `calling writeIVM with a dangling annotation should throw IonException`() {
        assertWriterThrows {
            writeAnnotations(10)
            writeIVM()
        }
    }

    @Test
    fun `calling finish should cause the buffered data to be written to the output stream`() {
        val actual = writeAsHexString(autoClose = false) {
            writeIVM()
            finish()
        }
        // Just checking that data is written, not asserting the content.
        assertTrue(actual.isNotBlank())
    }

    @Test
    fun `after calling finish, it should still be possible to write more data`() {
        val actual = writeAsHexString {
            finish()
            writeIVM()
        }
        // Just checking that data is written, not asserting the content.
        assertTrue(actual.isNotBlank())
    }

    @Test
    fun `calling close should cause the buffered data to be written to the output stream`() {
        val actual = writeAsHexString(autoClose = false) {
            writeIVM()
            close()
        }
        // Just checking that data is written, not asserting the content.
        assertTrue(actual.isNotBlank())
    }

    @Test
    fun `calling close or finish multiple times should not throw any exceptions`() {
        val actual = writeAsHexString {
            writeIVM()
            finish()
            close()
            finish()
            close()
            finish()
        }
        // Just checking that data is written, not asserting the content.
        assertTrue(actual.isNotBlank())
    }

    @Test
    fun `write the IVM`() {
        assertWriterOutputEquals("E0 01 01 EA") {
            writeIVM()
        }
    }

    @Test
    fun `write nothing`() {
        assertWriterOutputEquals("") {
        }
    }

    @Test
    fun `write a null`() {
        assertWriterOutputEquals("EA") {
            writeNull()
        }
    }

    @Test
    fun `write a null with a specific type`() {
        // Just checking one type. The full range of types are checked in IonEncoder_1_1Test
        assertWriterOutputEquals("EB 00") {
            writeNull(IonType.BOOL)
        }
    }

    @ParameterizedTest
    @CsvSource("true, 5E", "false, 5F")
    fun `write a boolean`(value: Boolean, hexBytes: String) {
        assertWriterOutputEquals(hexBytes) {
            writeBool(value)
        }
    }

    @Test
    fun `write a delimited list`() {
        assertWriterOutputEquals("F1 5E 5F F0") {
            stepInList(true)
            writeBool(true)
            writeBool(false)
            stepOut()
        }
    }

    @Test
    fun `write a prefixed list`() {
        assertWriterOutputEquals("A2 5E 5F") {
            stepInList(false)
            writeBool(true)
            writeBool(false)
            stepOut()
        }
    }

    @Test
    fun `write a variable-length prefixed list`() {
        assertWriterOutputEquals("FA 21 ${" 5E".repeat(16)}") {
            stepInList(false)
            repeat(16) { writeBool(true) }
            stepOut()
            finish()
        }
    }

    @Test
    fun `write a prefixed list that is so long it requires patch points`() {
        assertWriterOutputEquals("FA 02 02 ${" 5E".repeat(128)}") {
            stepInList(false)
            repeat(128) { writeBool(true) }
            stepOut()
        }
    }

    @Test
    fun `write multiple nested prefixed lists`() {
        assertWriterOutputEquals("A4 A3 A2 A1 A0") {
            repeat(5) { stepInList(false) }
            repeat(5) { stepOut() }
        }
    }

    @Test
    fun `write multiple nested delimited lists`() {
        assertWriterOutputEquals("F1 F1 F1 F1 F0 F0 F0 F0") {
            repeat(4) { stepInList(true) }
            repeat(4) { stepOut() }
        }
    }

    @Test
    fun `write multiple nested delimited and prefixed lists`() {
        assertWriterOutputEquals("F1 A9 F1 A6 F1 A3 F1 A0 F0 F0 F0 F0") {
            repeat(4) {
                stepInList(true)
                stepInList(false)
            }
            repeat(8) { stepOut() }
        }
    }

    @Test
    fun `write a delimited sexp`() {
        assertWriterOutputEquals("F2 5E 5F F0") {
            stepInSExp(true)
            writeBool(true)
            writeBool(false)
            stepOut()
        }
    }

    @Test
    fun `write a prefixed sexp`() {
        assertWriterOutputEquals("B2 5E 5F") {
            stepInSExp(false)
            writeBool(true)
            writeBool(false)
            stepOut()
        }
    }

    @Test
    fun `write a variable-length prefixed sexp`() {
        assertWriterOutputEquals("FB 21 ${" 5E".repeat(16)}") {
            stepInSExp(false)
            repeat(16) { writeBool(true) }
            stepOut()
            finish()
        }
    }

    @Test
    fun `write a prefixed sexp that is so long it requires patch points`() {
        assertWriterOutputEquals("FB 02 02 ${" 5E".repeat(128)}") {
            stepInSExp(false)
            repeat(128) { writeBool(true) }
            stepOut()
        }
    }

    @Test
    fun `write multiple nested prefixed sexps`() {
        assertWriterOutputEquals("B4 B3 B2 B1 B0") {
            repeat(5) { stepInSExp(false) }
            repeat(5) { stepOut() }
        }
    }

    @Test
    fun `write multiple nested delimited sexps`() {
        assertWriterOutputEquals("F2 F2 F2 F2 F0 F0 F0 F0") {
            repeat(4) { stepInSExp(true) }
            repeat(4) { stepOut() }
        }
    }

    @Test
    fun `write multiple nested delimited and prefixed sexps`() {
        assertWriterOutputEquals("F2 B9 F2 B6 F2 B3 F2 B0 F0 F0 F0 F0") {
            repeat(4) {
                stepInSExp(true)
                stepInSExp(false)
            }
            repeat(8) { stepOut() }
        }
    }

    @Test
    fun `write a prefixed struct`() {
        assertWriterOutputEquals(
            """
            C4  | Struct Length = 4
            17  | SID 11
            5E  | true
            19  | SID 12
            5F  | false
            """
        ) {
            stepInStruct(false)
            writeFieldName(11)
            writeBool(true)
            writeFieldName(12)
            writeBool(false)
            stepOut()
        }
    }

    @Test
    fun `write a variable length prefixed struct`() {
        assertWriterOutputEquals(
            """
            FC      | Variable Length SID Struct
            21      | Length = 16
            ${"17 5E ".repeat(8)}
            """
        ) {
            stepInStruct(false)
            repeat(8) {
                writeFieldName(11)
                writeBool(true)
            }
            stepOut()
        }
    }

    @Test
    fun `write a struct so long it requires patch points`() {
        assertWriterOutputEquals(
            """
            FC      | Variable Length SID Struct
            02 02   | Length = 128
            ${"17 5E ".repeat(64)}
            """
        ) {
            stepInStruct(false)
            repeat(64) {
                writeFieldName(11)
                writeBool(true)
            }
            stepOut()
        }
    }

    @Test
    fun `write multiple nested prefixed structs`() {
        assertWriterOutputEquals(
            """
            C8  | Struct Length = 8
            17  | SID 11
            C6  | Struct Length = 6
            17  | SID 11
            C4  | Struct Length = 4
            17  | SID 11
            C2  | Struct Length = 2
            17  | SID 11
            C0  | Struct Length = 0
            """
        ) {
            stepInStruct(false)
            repeat(4) {
                writeFieldName(11)
                stepInStruct(false)
            }
            repeat(5) {
                stepOut()
            }
        }
    }

    @Test
    fun `write multiple nested delimited structs`() {
        assertWriterOutputEquals(
            """
            F3                      | Begin delimited struct
            17                      | FlexSym SID 11
            F3                      | Begin delimited struct
            17 F3 17 F3 17 F3       | etc.
            01 F0                   | End delimited struct
            01 F0 01 F0 01 F0 01 F0 | etc.
            """
        ) {
            stepInStruct(true)
            repeat(4) {
                writeFieldName(11)
                stepInStruct(true)
            }
            repeat(5) {
                stepOut()
            }
        }
    }

    @Test
    fun `write empty prefixed struct`() {
        assertWriterOutputEquals("C0") {
            stepInStruct(false)
            stepOut()
        }
    }

    @Test
    fun `write delimited struct`() {
        assertWriterOutputEquals(
            """
            F3          | Begin delimited struct
            17          | SID 11
            5E          | true
            FB 66 6F 6F | FlexSym 'foo'
            5E          | true
            02 01       | FlexSym SID 64
            5E          | true
            01 F0       | End delimited struct
            """
        ) {
            stepInStruct(true)
            writeFieldName(11)
            writeBool(true)
            writeFieldName("foo")
            writeBool(true)
            writeFieldName(64)
            writeBool(true)
            stepOut()
        }
    }

    @Test
    fun `write empty delimited struct`() {
        assertWriterOutputEquals(
            """
            F3          | Begin delimited struct
            01 F0       | End delimited struct
            """
        ) {
            stepInStruct(true)
            stepOut()
        }
    }

    @Test
    fun `write prefixed struct with a single flex sym field`() {
        assertWriterOutputEquals(
            """
            FD          | Variable length FlexSym Struct
            0B          | Length = 5
            FB 66 6F 6F | FlexSym 'foo'
            5E          | true
            """
        ) {
            stepInStruct(false)
            writeFieldName("foo")
            writeBool(true)
            stepOut()
        }
    }

    @Test
    fun `write prefixed struct with multiple fields and flex syms`() {
        assertWriterOutputEquals(
            """
            FD           | Variable length FlexSym Struct
            1F           | Length = 15
            FB 66 6F 6F  | FlexSym 'foo'
            5E           | true
            FB 62 61 72  | FlexSym 'bar'
            5E           | true
            FB 62 61 7A  | FlexSym 'baz'
            5E           | true
            """
        ) {
            stepInStruct(false)
            writeFieldName("foo")
            writeBool(true)
            writeFieldName("bar")
            writeBool(true)
            writeFieldName("baz")
            writeBool(true)
            stepOut()
        }
    }

    @Test
    fun `write prefixed struct that starts with sids and switches partway through to use flex syms`() {
        assertWriterOutputEquals(
            """
            FC             | Variable length SID Struct
            17             | Length = 11
            81             | SID 64
            5E             | true
            01             | switch to FlexSym encoding
            FB 66 6F 6F    | FlexSym 'foo'
            5E             | true
            02 01          | FlexSym SID 64
            5E             | true
            """
        ) {
            stepInStruct(false)
            writeFieldName(64)
            writeBool(true)
            writeFieldName("foo")
            writeBool(true)
            writeFieldName(64)
            writeBool(true)
            stepOut()
        }
    }

    @Test
    fun `write prefixed struct with sid 0`() {
        assertWriterOutputEquals(
            """
            FD     | Variable length FlexSym Struct
            07     | Length = 3
            01 90  | FlexSym SID 0
            5E     | true
            """
        ) {
            stepInStruct(false)
            writeFieldName(0)
            writeBool(true)
            stepOut()
        }
    }

    @Test
    fun `write prefixed struct with sid 0 after another value`() {
        assertWriterOutputEquals(
            """
            FC      | Variable length SID struct
            17      | Length = FlexUInt 11
            03      | SID 1
            5E      | true
            01      | switch to FlexSym encoding
            01 90   | FlexSym SID 0
            5E      | true
            05      | FlexSym SID 2
            5E      | true
            01 90   | FlexSym SID 0
            5E      | true
            """
        ) {
            stepInStruct(false)
            writeFieldName(1)
            writeBool(true)
            writeFieldName(0)
            writeBool(true)
            writeFieldName(2)
            writeBool(true)
            writeFieldName(0)
            writeBool(true)
            stepOut()
        }
    }

    @Test
    fun `writing a value in a struct with no field name should throw an exception`() {
        assertWriterThrows {
            stepInStruct(true)
            writeBool(true)
        }
        assertWriterThrows {
            stepInStruct(false)
            writeBool(true)
        }
    }

    @Test
    fun `calling writeFieldName outside of a struct should throw an exception`() {
        assertWriterThrows {
            writeFieldName(12)
        }
        assertWriterThrows {
            writeFieldName("foo")
        }
    }

    @Test
    fun `calling stepOut with a dangling field name should throw an exception`() {
        assertWriterThrows {
            stepInStruct(false)
            writeFieldName(12)
            stepOut()
        }
        assertWriterThrows {
            stepInStruct(true)
            writeFieldName("foo")
            stepOut()
        }
    }

    @Test
    fun `writeAnnotations with empty int array should write no annotations`() {
        assertWriterOutputEquals("5E") {
            writeAnnotations(intArrayOf())
            writeBool(true)
        }
    }

    @Test
    fun `write one sid annotation`() {
        val expectedBytes = "E4 07 5E"
        assertWriterOutputEquals(expectedBytes) {
            writeAnnotations(3)
            writeBool(true)
        }
        assertWriterOutputEquals(expectedBytes) {
            writeAnnotations(3)
            writeAnnotations(intArrayOf())
            writeAnnotations(arrayOf<CharSequence>())
            writeBool(true)
        }
    }

    @Test
    fun `write two sid annotations`() {
        val expectedBytes = "E5 07 09 5E"
        assertWriterOutputEquals(expectedBytes) {
            writeAnnotations(3)
            writeAnnotations(4)
            writeBool(true)
        }
        assertWriterOutputEquals(expectedBytes) {
            writeAnnotations(3, 4)
            writeBool(true)
        }
        assertWriterOutputEquals(expectedBytes) {
            writeAnnotations(intArrayOf(3, 4))
            writeBool(true)
        }
    }

    @Test
    fun `write three sid annotations`() {
        val expectedBytes = "E6 09 07 09 02 04 5E"
        assertWriterOutputEquals(expectedBytes) {
            writeAnnotations(3)
            writeAnnotations(4)
            writeAnnotations(256)
            writeBool(true)
        }
        assertWriterOutputEquals(expectedBytes) {
            writeAnnotations(3)
            writeAnnotations(4, 256)
            writeBool(true)
        }
        assertWriterOutputEquals(expectedBytes) {
            writeAnnotations(intArrayOf(3, 4))
            writeAnnotations(256)
            writeBool(true)
        }
        assertWriterOutputEquals(expectedBytes) {
            writeAnnotations(intArrayOf(3, 4, 256))
            writeBool(true)
        }
    }

    @Test
    fun `write sid 0 annotation`() {
        assertWriterOutputEquals("E4 01 5E") {
            writeAnnotations(0)
            writeBool(true)
        }
    }

    @Test
    fun `write one inline annotation`() {
        val expectedBytes = "E7 FB 66 6F 6F 5F"
        assertWriterOutputEquals(expectedBytes) {
            writeAnnotations("foo")
            writeBool(false)
        }
        assertWriterOutputEquals(expectedBytes) {
            writeAnnotations("foo")
            writeAnnotations(intArrayOf())
            writeBool(false)
        }
    }

    @Test
    fun `write two inline annotations`() {
        val expectedBytes = "E8 FB 66 6F 6F FB 62 61 72 5F"
        assertWriterOutputEquals(expectedBytes) {
            writeAnnotations("foo")
            writeAnnotations("bar")
            writeBool(false)
        }
        assertWriterOutputEquals(expectedBytes) {
            writeAnnotations(arrayOf("foo", "bar"))
            writeBool(false)
        }
    }

    @Test
    fun `write three inline annotations`() {
        val expectedBytes = "E9 19 FB 66 6F 6F FB 62 61 72 FB 62 61 7A 5F"
        assertWriterOutputEquals(expectedBytes) {
            writeAnnotations("foo")
            writeAnnotations("bar")
            writeAnnotations("baz")
            writeBool(false)
        }
        assertWriterOutputEquals(expectedBytes) {
            writeAnnotations("foo")
            writeAnnotations("bar", "baz")
            writeBool(false)
        }
        assertWriterOutputEquals(expectedBytes) {
            writeAnnotations(arrayOf("foo", "bar"))
            writeAnnotations("baz")
            writeBool(false)
        }
        assertWriterOutputEquals(expectedBytes) {
            writeAnnotations(arrayOf("foo", "bar", "baz"))
            writeBool(false)
        }
    }

    @Test
    fun `write empty text and sid 0 annotations`() {
        assertWriterOutputEquals("E8 01 90 01 80 5E") {
            writeAnnotations(0)
            writeAnnotations("")
            writeBool(true)
        }
    }

    @Test
    fun `write two mixed sid and inline annotations`() {
        val expectedBytes = "E8 15 FB 66 6F 6F 5F"
        assertWriterOutputEquals(expectedBytes) {
            writeAnnotations(10)
            writeAnnotations("foo")
            writeBool(false)
        }
    }

    @Test
    fun `write three mixed sid and inline annotations`() {
        val expectedBytes = "E9 13 15 FB 66 6F 6F FB 62 61 72 5F"
        assertWriterOutputEquals(expectedBytes) {
            writeAnnotations(10)
            writeAnnotations("foo")
            writeAnnotations("bar")
            writeBool(false)
        }
        assertWriterOutputEquals(expectedBytes) {
            writeAnnotations(10)
            writeAnnotations(arrayOf("foo", "bar"))
            writeBool(false)
        }
    }

    @Test
    fun `write annotations that are long enough to need a patch point`() {
        val opCode = "E7"
        val length = "C6 FD"
        val text = "41 6D 61 7A 6F 6E 20 49 6F 6E 20 69 73 20 61 20 72 69 63 68 6C 79 2D 74 79 70 65 64 2C 20 73 65 " +
            "6C 66 2D 64 65 73 63 72 69 62 69 6E 67 2C 20 68 69 65 72 61 72 63 68 69 63 61 6C 20 64 61 74 61 20 " +
            "73 65 72 69 61 6C 69 7A 61 74 69 6F 6E 20 66 6F 72 6D 61 74 20 6F 66 66 65 72 69 6E 67 20 69 6E 74 " +
            "65 72 63 68 61 6E 67 65 61 62 6C 65 20 62 69 6E 61 72 79 20 61 6E 64 20 74 65 78 74 20 72 65 70 72 " +
            "65 73 65 6E 74 61 74 69 6F 6E 73 2E 5F"
        assertWriterOutputEquals("$opCode $length $text") {
            writeAnnotations(
                "Amazon Ion is a richly-typed, self-describing, hierarchical data serialization " +
                    "format offering interchangeable binary and text representations."
            )
            writeBool(false)
        }
    }

    @Test
    fun `write int`() {
        assertWriterOutputEquals(
            """
            51 01
            51 0A
            """
        ) {
            writeInt(1)
            writeInt(BigInteger.TEN)
        }
    }

    @Test
    fun `write float`() {
        assertWriterOutputEquals(
            """
            5A
            5C 40 48 F5 C3
            5D 40 09 1E B8 51 EB 85 1F
            """
        ) {
            writeFloat(0.0)
            writeFloat(3.14f)
            writeFloat(3.14)
        }
    }

    @Test
    fun `write decimal`() {
        assertWriterOutputEquals(
            """
            60
            62 01 00
            """
        ) {
            writeDecimal(BigDecimal.ZERO)
            writeDecimal(Decimal.NEGATIVE_ZERO)
        }
    }

    @Test
    fun `write timestamp`() {
        assertWriterOutputEquals(
            """
            77 35 46 AF 7C 55 47 70 2D
            F7 05 4B 08
            """
        ) {
            writeTimestamp(Timestamp.valueOf("2023-12-08T15:37:23.190583253Z"))
            writeTimestamp(Timestamp.valueOf("2123T"))
        }
    }

    @Test
    fun `write symbol`() {
        assertWriterOutputEquals(
            """
            E1 00
            E1 01
            E2 39 2F
            93 66 6F 6F
            """
        ) {
            writeSymbol(0)
            writeSymbol(1)
            writeSymbol(12345)
            writeSymbol("foo")
        }
    }

    @Test
    fun `write string`() {
        assertWriterOutputEquals("83 66 6F 6F") {
            writeString("foo")
        }
    }

    @Test
    fun `write blob`() {
        assertWriterOutputEquals("FE 07 01 02 03") {
            writeBlob(byteArrayOf(1, 2, 3), 0, 3)
        }
    }

    @Test
    fun `write clob`() {
        assertWriterOutputEquals("FF 07 04 05 06") {
            writeClob(byteArrayOf(4, 5, 6), 0, 3)
        }
    }

    @Test
    fun `write an e-expression`() {
        assertWriterOutputEquals("00") {
            stepInEExp(0)
            stepOut()
        }
        assertWriterOutputEquals("3F") {
            stepInEExp(63)
            stepOut()
        }
    }

    @ParameterizedTest
    @CsvSource(
        "      64, 40 01",
        "      65, 41 01",
        "      80, 40 03",
        "    1211, 4B 8F",
        "    2111, 4F FF",
        "    2112, 40 02 02",
        "   71376, 40 A6 45",
        "  262207, 4F FE FF",
        "  262208, 40 04 00 02",
        "33554495, 4F FC FF FF",
    )
    fun `write an e-expression with a multi-byte biased id`(id: Int, expectedBytes: String) {
        assertWriterOutputEquals(expectedBytes) {
            stepInEExp(id)
            stepOut()
        }
    }

    @Test
    fun `write nested e-expressions`() {
        // E-Expressions don't have length prefixes, so we're putting them inside lists
        // so that we can check that the length gets propagated correctly to the parent
        assertWriterOutputEquals(
            """
            AB         | List Length 11
            1F         | Macro 31
            A9         | List Length 9
            40 01      | Macro 64
            A6         | List Length 6
            43 03      | Macro 83
            A3         | List Length 3
            44 5A 02   | Macro 2468
            """
        ) {
            stepInList(false)
            stepInEExp(31)
            stepInList(false)
            stepInEExp(64)
            stepInList(false)
            stepInEExp(83)
            stepInList(false)
            stepInEExp(2468)
            repeat(8) { stepOut() }
        }
    }

    @Test
    fun `write an e-expression in the field name position of a sid struct`() {
        assertWriterOutputEquals(
            """
            FC      | Variable Length SID Struct
            11      | Length = 8
            15      | SID 10
            5E      | true
            01      | switch to FlexSym encoding
            01      | FlexSym Escape Byte
            1F      | Macro 31 (0x1F)
            01      | FlexSym Escape Byte
            40 01   | Macro 64
            """
        ) {
            stepInStruct(false)
            writeFieldName(10)
            writeBool(true)
            stepInEExp(31)
            stepOut()
            stepInEExp(64)
            stepOut()
            stepOut()
        }
    }

    @Test
    fun `write an e-expression in the field name position of a delimited struct`() {
        assertWriterOutputEquals(
            """
            F3      | Begin Delimited Struct
            01      | FlexSym Escape Byte
            1F      | Macro 31 (0x1F)
            01      | FlexSym Escape Byte
            F0      | End Delimiter
            """
        ) {
            stepInStruct(true)
            stepInEExp(31)
            stepOut()
            stepOut()
        }
    }

    @Test
    fun `write an e-expression in the field name position of a variable length flex-sym struct`() {
        assertWriterOutputEquals(
            """
            FD      | Variable Length FlexSym Struct
            05      | Length = 2
            01      | FlexSym Escape Byte
            1F      | Macro 31 (0x1F)
            """
        ) {
            stepInStruct(false)
            stepInEExp(31)
            stepOut()
            stepOut()
        }
    }

    @Test
    fun `write an e-expression in the value position of a struct`() {
        assertWriterOutputEquals(
            """
            C2      | Struct length 2
            03      | SID 1
            01      | Macro 1
            """
        ) {
            stepInStruct(false)
            writeFieldName(1)
            stepInEExp(1)
            stepOut()
            stepOut()
        }
    }

    @Test
    fun `calling stepInEExp(String) should throw NotImplementedError`() {
        assertThrows<UnsupportedOperationException> {
            writeAsHexString {
                stepInEExp("foo")
            }
        }
    }

    @Test
    fun `calling stepInEExp with an annotation should throw IonException`() {
        assertWriterThrows {
            writeAnnotations("foo")
            stepInEExp(1)
        }
    }

    @Test
    fun `write a delimited expression group`() {
        assertWriterOutputEquals(
            """
            00      | Macro 0
            01      | FlexUInt 0 (delimited expression group)
            5E      | true
            F0      | End of Expression Group
            """
        ) {
            stepInEExp(0)
            stepInExpressionGroup(true)
            writeBool(true)
            stepOut()
            stepOut()
        }
    }

    @Test
    fun `write a prefixed expression group`() {
        assertWriterOutputEquals(
            """
            00      | Macro 0
            03      | Expression Group, Length = 1
            5E      | true
            """
        ) {
            stepInEExp(0)
            stepInExpressionGroup(false)
            writeBool(true)
            stepOut()
            stepOut()
        }
    }

    @Test
    fun `write a prefixed expression group so long that it requires a patch point`() {
        assertWriterOutputEquals(
            """
            00      | Macro 0
            FE 03   | Expression Group, Length = 255
            ${"5E ".repeat(255)}
            """
        ) {
            stepInEExp(0)
            stepInExpressionGroup(false)
            repeat(255) { writeBool(true) }
            stepOut()
            stepOut()
        }
    }

    @Test
    fun `write an empty prefixed expression group`() {
        // Regardless of whether we step in to a delimited or prefixed expression group, the empty expression group
        // is always represented as a delimited expression group.
        assertWriterOutputEquals("00 01 F0") {
            stepInEExp(0)
            stepInExpressionGroup(false)
            stepOut()
            stepOut()
        }
    }

    @Test
    fun `write an empty delimited expression group`() {
        assertWriterOutputEquals("00 01 F0") {
            stepInEExp(0)
            stepInExpressionGroup(true)
            stepOut()
            stepOut()
        }
    }

    @Test
    fun `calling stepInExpressionGroup with an annotation should throw IonException`() {
        assertWriterThrows {
            stepInEExp(1)
            writeAnnotations("foo")
            stepInExpressionGroup(false)
        }
    }

    @Test
    fun `calling stepInExpressionGroup while not directly in a Macro container should throw IonException`() {
        assertWriterThrows {
            stepInExpressionGroup(false)
        }
        assertWriterThrows {
            stepInList(false)
            stepInExpressionGroup(false)
        }
        assertWriterThrows {
            stepInSExp(false)
            stepInExpressionGroup(false)
        }
        assertWriterThrows {
            stepInStruct(false)
            stepInExpressionGroup(false)
        }
        assertWriterThrows {
            stepInEExp(123)
            stepInExpressionGroup(false)
            stepInExpressionGroup(false)
        }
    }

    /**
     * Writes this Ion, taken from https://amazon-ion.github.io/ion-docs/
     * ```
     * {
     *   name: "Fido",
     *   age: years::4,
     *   birthday: 2012-03-01T,
     *   toys: [ball, rope],
     *   weight: pounds::41.2,
     *   buzz: {{VG8gaW5maW5pdHkuLi4gYW5kIGJleW9uZCE=}},
     * }
     * ```
     */
    @Test
    fun `write something complex`() {
        assertWriterOutputEquals(
            """
            E0 01 01 EA                 | IVM
            E4 07 FC 63                 | $3::{             // length=49
            0F FA 5D                    |   $7: [           // length=46
            84 6E 61 6D 65              |     "name",
            83 61 67 65                 |     "age",
            85 79 65 61 72 73           |     "years",
            88 62 69 72 74 68 64 61 79  |     "birthday",
            84 74 6F 79 73              |     "toys",
            84 62 61 6C 6C              |     "ball",
            86 77 65 69 67 68 74        |     "weight",
            84 62 75 7A 7A              |     "buzz",
                                        |   ]
                                        | }
            FC 85                       | {                 // length=66
            15 84 46 69 64 6F           |   $10: "Fido",
            17 E4 19 51 04              |   $11: $12::4,
            1B 72 AA 09                 |   $13: 2012-03-01T
            1D A7                       |   $14: [          // length=7
            E1 0F                       |     $15,
            94 72 6F 70 65              |     rope
                                        |   ],
            21                          |   $16:
            E7 F5 70 6F 75 6E 64 73     |       pounds::
            63 FF 9C 01                 |       41.2
            23 FE 35                    |   $17: {{         // length=26
            54 6F 20 69 6E 66 69 6E 69  |     VG8gaW5maW5p
            74 79 2E 2E 2E 20 61 6E 64  |     dHkuLi4gYW5k
            20 62 65 79 6F 6E 64 21     |     IGJleW9uZCE=
                                        |   }}
                                        | }
            """
        ) {
            writeIVM()
            writeAnnotations(3)
            writeStruct {
                writeFieldName(7)
                writeList {
                    writeString("name")
                    writeString("age")
                    writeString("years")
                    writeString("birthday")
                    writeString("toys")
                    writeString("ball")
                    writeString("weight")
                    writeString("buzz")
                }
            }
            writeStruct {
                writeFieldName(10)
                writeString("Fido")
                writeFieldName(11)
                writeAnnotations(12)
                writeInt(4)
                writeFieldName(13)
                writeTimestamp(Timestamp.valueOf("2012-03-01T"))
                writeFieldName(14)
                writeList {
                    writeSymbol(15)
                    writeSymbol("rope")
                }
                writeFieldName(16)
                writeAnnotations("pounds")
                writeDecimal(BigDecimal.valueOf(41.2))
                writeFieldName(17)
                writeBlob(
                    byteArrayOf(
                        84, 111, 32, 105, 110, 102, 105, 110, 105,
                        116, 121, 46, 46, 46, 32, 97, 110, 100,
                        32, 98, 101, 121, 111, 110, 100, 33
                    )
                )
            }
        }
    }

    /**
     * Helper function that steps into a struct, applies the contents of [block] to
     * the writer, and then steps out of the struct.
     * Using this function makes it easy for the indentation of the writer code to
     * match the indentation of the equivalent pretty-printed Ion.
     */
    private inline fun IonRawWriter_1_1.writeStruct(block: IonRawWriter_1_1.() -> Unit) {
        stepInStruct(false)
        block()
        stepOut()
    }

    /**
     * Helper function that steps into a list, applies the contents of [block] to
     * the writer, and then steps out of the list.
     * Using this function makes it easy for the indentation of the writer code to
     * match the indentation of the equivalent pretty-printed Ion.
     */
    private inline fun IonRawWriter_1_1.writeList(block: IonRawWriter_1_1.() -> Unit) {
        stepInList(false)
        block()
        stepOut()
    }
}
