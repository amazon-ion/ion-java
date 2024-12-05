// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.bin

import com.amazon.ion.*
import com.amazon.ion.TestUtils.*
import com.amazon.ion.impl.*
import com.amazon.ion.impl.macro.*
import com.amazon.ion.impl.macro.Macro.*
import java.io.ByteArrayOutputStream
import java.math.BigDecimal
import java.math.BigInteger
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.EnumSource

class IonRawBinaryWriterTest_1_1 {

    private fun ionWriter(
        baos: ByteArrayOutputStream = ByteArrayOutputStream()
    ) = IonRawBinaryWriter_1_1(
        out = baos,
        buffer = WriteBuffer(BlockAllocatorProviders.basicProvider().vendAllocator(32)) {},
        lengthPrefixPreallocation = 1,
    )

    private inline fun writeAsHexString(autoClose: Boolean = true, block: IonRawBinaryWriter_1_1.() -> Unit): String {
        val baos = ByteArrayOutputStream()
        val rawWriter = ionWriter(baos)
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
    @OptIn(ExperimentalStdlibApi::class)
    private inline fun assertWriterOutputEquals(hexBytes: String, autoClose: Boolean = true, block: IonRawBinaryWriter_1_1.() -> Unit) {
        val cleanedHexBytes = cleanCommentedHexBytes(hexBytes)
        assertEquals(cleanCommentedHexBytes(hexBytes), writeAsHexString(autoClose, block))

        // Also check to see that the correct number of bytes are being reported to an enclosing container
        val expectedLength = if (cleanedHexBytes.isBlank()) 0 else cleanedHexBytes.split(' ').size
        val actualByteString = writeAsHexString(autoClose) {
            try {
                stepInList(usingLengthPrefix = true)
                block()
                stepOut()
            } catch (t: Throwable) {
                // It's illegal to wrap `block()` in a list, so we'll just skip this check.
                return
            }
        }
        if (expectedLength > 0xF) {
            // Rather than try to parse the flexuint in the output, we'll just compare them as flexuint hex strings
            // If this fails, it could be confusing. It's possible that if the length is underreported as being less
            // than 16, then the "actualLengthBytes" could be an empty string.
            val flexUIntLen = FlexInt.flexUIntLength(expectedLength.toLong())
            val flexUIntBytes = ByteArray(flexUIntLen)
            FlexInt.writeFlexIntOrUIntInto(flexUIntBytes, 0, expectedLength.toLong(), flexUIntLen)
            val byteString = flexUIntBytes.joinToString(" ") { it.toHexString(HexFormat.UpperCase) }
            val actualLengthBytes = actualByteString.drop(3).dropLast(expectedLength * 3)
            assertEquals(byteString, actualLengthBytes)
        } else {
            // Take the length from the opcode and compare with the length we calculated
            val actualLen = "${actualByteString[1]}".toInt(radix = 0x10) // Fun fact! Every radix is 10 unless you write it in another base.
            assertEquals(expectedLength, actualLen)
        }
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
            stepInList(usingLengthPrefix = true)
            close()
        }
    }

    @Test
    fun `calling finish while in a container should throw IonException`() {
        assertWriterThrows {
            stepInList(usingLengthPrefix = false)
            flush()
        }
    }

    @Test
    fun `calling finish with a dangling annotation should throw IonException`() {
        assertWriterThrows {
            writeAnnotations(10)
            flush()
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
            stepInList(usingLengthPrefix = false)
            writeAnnotations(10)
            stepOut()
        }
    }

    @Test
    fun `calling writeIVM when in a container should throw IonException`() {
        assertWriterThrows {
            stepInList(usingLengthPrefix = true)
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
            flush()
        }
        // Just checking that data is written, not asserting the content.
        assertTrue(actual.isNotBlank())
    }

    @Test
    fun `after calling finish, it should still be possible to write more data`() {
        val actual = writeAsHexString {
            flush()
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
            flush()
            close()
            flush()
            close()
            flush()
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
    @CsvSource("true, 6E", "false, 6F")
    fun `write a boolean`(value: Boolean, hexBytes: String) {
        assertWriterOutputEquals(hexBytes) {
            writeBool(value)
        }
    }

    @Test
    fun `write a delimited list`() {
        assertWriterOutputEquals("F1 6E 6F F0") {
            stepInList(usingLengthPrefix = false)
            writeBool(true)
            writeBool(false)
            stepOut()
        }
    }

    @Test
    fun `write a prefixed list`() {
        assertWriterOutputEquals("B2 6E 6F") {
            stepInList(usingLengthPrefix = true)
            writeBool(true)
            writeBool(false)
            stepOut()
        }
    }

    @Test
    fun `write a variable-length prefixed list`() {
        assertWriterOutputEquals("FB 21 ${" 6E".repeat(16)}") {
            stepInList(usingLengthPrefix = true)
            repeat(16) { writeBool(true) }
            stepOut()
            flush()
        }
    }

    @Test
    fun `write a prefixed list that is so long it requires patch points`() {
        assertWriterOutputEquals("FB 02 02 ${" 6E".repeat(128)}") {
            stepInList(usingLengthPrefix = true)
            repeat(128) { writeBool(true) }
            stepOut()
        }
    }

    @Test
    fun `write multiple nested prefixed lists`() {
        assertWriterOutputEquals("B4 B3 B2 B1 B0") {
            repeat(5) { stepInList(usingLengthPrefix = true) }
            repeat(5) { stepOut() }
        }
    }

    @Test
    fun `write multiple nested delimited lists`() {
        assertWriterOutputEquals("F1 F1 F1 F1 F0 F0 F0 F0") {
            repeat(4) { stepInList(usingLengthPrefix = false) }
            repeat(4) { stepOut() }
        }
    }

    @Test
    fun `write multiple nested delimited and prefixed lists`() {
        assertWriterOutputEquals("F1 B9 F1 B6 F1 B3 F1 B0 F0 F0 F0 F0") {
            repeat(4) {
                stepInList(usingLengthPrefix = false)
                stepInList(usingLengthPrefix = true)
            }
            repeat(8) { stepOut() }
        }
    }

    @Test
    fun `write a delimited sexp`() {
        assertWriterOutputEquals("F2 6E 6F F0") {
            stepInSExp(usingLengthPrefix = false)
            writeBool(true)
            writeBool(false)
            stepOut()
        }
    }

    @Test
    fun `write a prefixed sexp`() {
        assertWriterOutputEquals("C2 6E 6F") {
            stepInSExp(usingLengthPrefix = true)
            writeBool(true)
            writeBool(false)
            stepOut()
        }
    }

    @Test
    fun `write a variable-length prefixed sexp`() {
        assertWriterOutputEquals("FC 21 ${" 6E".repeat(16)}") {
            stepInSExp(usingLengthPrefix = true)
            repeat(16) { writeBool(true) }
            stepOut()
            flush()
        }
    }

    @Test
    fun `write a prefixed sexp that is so long it requires patch points`() {
        assertWriterOutputEquals("FC 02 02 ${" 6E".repeat(128)}") {
            stepInSExp(usingLengthPrefix = true)
            repeat(128) { writeBool(true) }
            stepOut()
        }
    }

    @Test
    fun `write multiple nested prefixed sexps`() {
        assertWriterOutputEquals("C4 C3 C2 C1 C0") {
            repeat(5) { stepInSExp(usingLengthPrefix = true) }
            repeat(5) { stepOut() }
        }
    }

    @Test
    fun `write multiple nested delimited sexps`() {
        assertWriterOutputEquals("F2 F2 F2 F2 F0 F0 F0 F0") {
            repeat(4) { stepInSExp(usingLengthPrefix = false) }
            repeat(4) { stepOut() }
        }
    }

    @Test
    fun `write multiple nested delimited and prefixed sexps`() {
        assertWriterOutputEquals("F2 C9 F2 C6 F2 C3 F2 C0 F0 F0 F0 F0") {
            repeat(4) {
                stepInSExp(usingLengthPrefix = false)
                stepInSExp(usingLengthPrefix = true)
            }
            repeat(8) { stepOut() }
        }
    }

    @Test
    fun `write a prefixed struct`() {
        assertWriterOutputEquals(
            """
            D4  | Struct Length = 4
            17  | SID 11
            6E  | true
            19  | SID 12
            6F  | false
            """
        ) {
            stepInStruct(usingLengthPrefix = true)
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
            FD      | Variable Length SID Struct
            21      | Length = 16
            ${"17 6E ".repeat(8)}
            """
        ) {
            stepInStruct(usingLengthPrefix = true)
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
            FD      | Variable Length SID Struct
            02 02   | Length = 128
            ${"17 6E ".repeat(64)}
            """
        ) {
            stepInStruct(usingLengthPrefix = true)
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
            D8  | Struct Length = 8
            17  | SID 11
            D6  | Struct Length = 6
            17  | SID 11
            D4  | Struct Length = 4
            17  | SID 11
            D2  | Struct Length = 2
            17  | SID 11
            D0  | Struct Length = 0
            """
        ) {
            stepInStruct(usingLengthPrefix = true)
            repeat(4) {
                writeFieldName(11)
                stepInStruct(usingLengthPrefix = true)
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
            stepInStruct(usingLengthPrefix = false)
            repeat(4) {
                writeFieldName(11)
                stepInStruct(usingLengthPrefix = false)
            }
            repeat(5) {
                stepOut()
            }
        }
    }

    @Test
    fun `write empty prefixed struct`() {
        assertWriterOutputEquals("D0") {
            stepInStruct(usingLengthPrefix = true)
            stepOut()
        }
    }

    @Test
    fun `write delimited struct`() {
        assertWriterOutputEquals(
            """
            F3          | Begin delimited struct
            17          | SID 11
            6E          | true
            FB 66 6F 6F | FlexSym 'foo'
            6E          | true
            02 01       | FlexSym SID 64
            6E          | true
            01 6F       | System Symbol symbol_table
            6E          | true
            01 F0       | End delimited struct
            """
        ) {
            stepInStruct(usingLengthPrefix = false)
            writeFieldName(11)
            writeBool(true)
            writeFieldName("foo")
            writeBool(true)
            writeFieldName(64)
            writeBool(true)
            writeFieldName(SystemSymbols_1_1.SYMBOL_TABLE)
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
            stepInStruct(usingLengthPrefix = false)
            stepOut()
        }
    }

    @Test
    fun `write prefixed struct with a single flex sym field`() {
        assertWriterOutputEquals(
            """
            FD          | Variable length Struct
            0D          | Length = 6
            01          | switch to FlexSym encoding
            FB 66 6F 6F | FlexSym 'foo'
            6E          | true
            """
        ) {
            stepInStruct(usingLengthPrefix = true)
            writeFieldName("foo")
            writeBool(true)
            stepOut()
        }
    }

    @Test
    fun `write prefixed struct with multiple fields and flex syms`() {
        assertWriterOutputEquals(
            """
            FD           | Variable length Struct
            21           | Length = 16
            01             | switch to FlexSym encoding
            FB 66 6F 6F  | FlexSym 'foo'
            6E           | true
            FB 62 61 72  | FlexSym 'bar'
            6E           | true
            FB 62 61 7A  | FlexSym 'baz'
            6E           | true
            """
        ) {
            stepInStruct(usingLengthPrefix = true)
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
            FD             | Variable length Struct
            17             | Length = 11
            81             | SID 64
            6E             | true
            01             | switch to FlexSym encoding
            FB 66 6F 6F    | FlexSym 'foo'
            6E             | true
            02 01          | FlexSym SID 64
            6E             | true
            """
        ) {
            stepInStruct(usingLengthPrefix = true)
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
            FD     | Variable length Struct
            09     | Length = 4
            01     | switch to FlexSym encoding
            01 60  | FlexSym SID 0
            6E     | true
            """
        ) {
            stepInStruct(usingLengthPrefix = true)
            writeFieldName(0)
            writeBool(true)
            stepOut()
        }
    }

    @Test
    fun `write prefixed struct with sid 0 after another value`() {
        assertWriterOutputEquals(
            """
            FD      | Variable length struct
            17      | Length = FlexUInt 11
            03      | SID 1
            6E      | true
            01      | switch to FlexSym encoding
            01 60   | FlexSym SID 0
            6E      | true
            05      | FlexSym SID 2
            6E      | true
            01 60   | FlexSym SID 0
            6E      | true
            """
        ) {
            stepInStruct(usingLengthPrefix = true)
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
    fun `write prefixed struct with a system symbol as a field name`() {
        assertWriterOutputEquals(
            """
            FD     | Variable length Struct
            09     | Length = 4
            01     | switch to FlexSym encoding
            01 6F  | FlexSym System Symbol 'symbol_table'
            6E     | true
            """
        ) {
            stepInStruct(usingLengthPrefix = true)
            writeFieldName(SystemSymbols_1_1.SYMBOL_TABLE)
            writeBool(true)
            stepOut()
        }
    }

    @Test
    fun `writing a value in a struct with no field name should throw an exception`() {
        assertWriterThrows {
            stepInStruct(usingLengthPrefix = false)
            writeBool(true)
        }
        assertWriterThrows {
            stepInStruct(usingLengthPrefix = true)
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
            stepInStruct(usingLengthPrefix = true)
            writeFieldName(12)
            stepOut()
        }
        assertWriterThrows {
            stepInStruct(usingLengthPrefix = false)
            writeFieldName("foo")
            stepOut()
        }
    }

    @Test
    fun `writeAnnotations with empty int array should write no annotations`() {
        assertWriterOutputEquals("6E") {
            writeAnnotations(intArrayOf())
            writeBool(true)
        }
    }

    @Test
    fun `write one sid annotation`() {
        val expectedBytes = "E4 07 6E"
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
        val expectedBytes = "E5 07 09 6E"
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
        val expectedBytes = "E6 09 07 09 02 04 6E"
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
        assertWriterOutputEquals("E4 01 6E") {
            writeAnnotations(0)
            writeBool(true)
        }
    }

    @Test
    fun `attempting to write negative SID annotations should throw exception`() {
        assertWriterThrows { writeAnnotations(-1) }
        assertWriterThrows { writeAnnotations(-1, 2) }
        assertWriterThrows { writeAnnotations(1, -2) }
        assertWriterThrows { writeAnnotations(intArrayOf(-1, 2, 3)) }
        assertWriterThrows { writeAnnotations(intArrayOf(1, -2, 3)) }
        assertWriterThrows { writeAnnotations(intArrayOf(1, 2, -3)) }
    }

    @Test
    fun `write one inline annotation`() {
        val expectedBytes = "E7 FB 66 6F 6F 6F"
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
        val expectedBytes = "E8 FB 66 6F 6F FB 62 61 72 6F"
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
        val expectedBytes = "E9 19 FB 66 6F 6F FB 62 61 72 FB 62 61 7A 6F"
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
        // Empty text is a system symbol
        assertWriterOutputEquals("E8 01 60 01 75 6E") {
            writeAnnotations(0)
            writeAnnotations("")
            writeBool(true)
        }
    }

    @Test
    fun `write two mixed sid and inline annotations`() {
        val expectedBytes = "E8 15 FB 66 6F 6F 6F"
        assertWriterOutputEquals(expectedBytes) {
            writeAnnotations(10)
            writeAnnotations("foo")
            writeBool(false)
        }
    }

    @Test
    fun `write three mixed sid and inline annotations`() {
        val expectedBytes = "E9 13 15 FB 66 6F 6F FB 62 61 72 6F"
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
    fun `write one system symbol annotation`() {
        val expectedBytes = "E7 01 64 6E"
        assertWriterOutputEquals(expectedBytes) {
            writeAnnotations(SystemSymbols_1_1.NAME)
            writeBool(true)
        }
        assertWriterOutputEquals(expectedBytes) {
            writeAnnotations(SystemSymbols_1_1.NAME)
            writeAnnotations(intArrayOf())
            writeAnnotations(arrayOf<CharSequence>())
            writeBool(true)
        }
    }

    @Test
    fun `write two mixed sid and system annotations`() {
        val expectedBytes = "E8 15 01 6A 6F"
        assertWriterOutputEquals(expectedBytes) {
            writeAnnotations(10)
            writeAnnotations(SystemSymbols_1_1.ENCODING)
            writeBool(false)
        }
    }

    @Test
    fun `write two mixed inline and system annotations`() {
        val expectedBytes = "E8 FB 66 6F 6F 01 6A 6F"
        assertWriterOutputEquals(expectedBytes) {
            writeAnnotations("foo")
            writeAnnotations(SystemSymbols_1_1.ENCODING)
            writeBool(false)
        }
    }

    @Test
    fun `write three mixed sid, inline, and system annotations`() {
        val expectedBytes = "E9 0F 15 FB 66 6F 6F 01 6A 6F"
        assertWriterOutputEquals(expectedBytes) {
            writeAnnotations(10)
            writeAnnotations("foo")
            writeAnnotations(SystemSymbols_1_1.ENCODING)
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
            "65 73 65 6E 74 61 74 69 6F 6E 73 2E"
        val falseOpCode = "6F"
        assertWriterOutputEquals("$opCode $length $text $falseOpCode") {
            writeAnnotations(
                "Amazon Ion is a richly-typed, self-describing, hierarchical data serialization " +
                    "format offering interchangeable binary and text representations."
            )
            writeBool(false)
        }
    }

    @Test
    fun `write enough annotations for one value to require resizing the internal annotation buffers`() {
        val expectedBytes = """
            E9
            3D
            07 07 07 07 07 07 07 07 07 07 | 10x SID 3
            FF 20 FF 20 FF 20 FF 20 FF 20 |  5x " "
            FF 20 FF 20 FF 20 FF 20 FF 20 |  5x " "
            6E
            """
        assertWriterOutputEquals(expectedBytes) {
            repeat(10) { writeAnnotations(3) }
            repeat(10) { writeAnnotations(" ") }
            writeBool(true)
        }
    }

    @Test
    fun `_private_hasFirstAnnotation() should return false when there are no annotations`() {
        val rawWriter = ionWriter()
        assertFalse(rawWriter._private_hasFirstAnnotation(SystemSymbols.ION_SID, SystemSymbols.ION))
    }

    @Test
    fun `_private_hasFirstAnnotation() should return true if only the sid matches`() {
        val rawWriter = ionWriter()
        rawWriter.writeAnnotations(SystemSymbols.ION_SID)
        assertTrue(rawWriter._private_hasFirstAnnotation(SystemSymbols.ION_SID, null))
    }

    @Test
    fun `_private_hasFirstAnnotation() should return true if only the text matches`() {
        val rawWriter = ionWriter()
        rawWriter.writeAnnotations(SystemSymbols.ION)
        assertTrue(rawWriter._private_hasFirstAnnotation(-1, SystemSymbols.ION))
    }

    @Test
    fun `_private_hasFirstAnnotation() should return false if the first annotation does not match the sid or text`() {
        val rawWriter = ionWriter()
        rawWriter.writeAnnotations(SystemSymbols.IMPORTS_SID)
        rawWriter.writeAnnotations(SystemSymbols.ION)
        rawWriter.writeAnnotations(SystemSymbols.ION_SID)
        // Matches the second and third annotations, but not the first one.
        assertFalse(rawWriter._private_hasFirstAnnotation(SystemSymbols.ION_SID, SystemSymbols.ION))
    }

    @Test
    fun `_private_clearAnnotations() should clear text annotations`() {
        assertWriterOutputEquals(""" 6E """) {
            repeat(5) { writeAnnotations(" ") }
            _private_clearAnnotations()
            assertFalse(_private_hasFirstAnnotation(-1, " "))
            writeBool(true)
        }
        assertWriterOutputEquals(""" E4 07 6E """) {
            repeat(5) { writeAnnotations(" ") }
            _private_clearAnnotations()
            writeAnnotations(3)
            assertFalse(_private_hasFirstAnnotation(-1, " "))
            writeBool(true)
        }
        assertWriterOutputEquals(""" E5 07 09 6E """) {
            repeat(5) { writeAnnotations("a") }
            _private_clearAnnotations()
            writeAnnotations(3)
            writeAnnotations(4)
            writeBool(true)
        }
    }

    @Test
    fun `_private_clearAnnotations() should clear sid annotations`() {
        assertWriterOutputEquals(""" 6E """) {
            repeat(5) { writeAnnotations(3) }
            _private_clearAnnotations()
            assertFalse(_private_hasFirstAnnotation(3, null))
            writeBool(true)
        }
        assertWriterOutputEquals(""" E7 FF 20 6E """) {
            repeat(5) { writeAnnotations(3) }
            _private_clearAnnotations()
            writeAnnotations(" ")
            assertFalse(_private_hasFirstAnnotation(3, null))
            writeBool(true)
        }
        assertWriterOutputEquals(""" E8 FF 61 FF 62 6E """) {
            repeat(5) { writeAnnotations(3) }
            _private_clearAnnotations()
            writeAnnotations("a")
            writeAnnotations("b")
            writeBool(true)
        }
    }

    @Test
    fun `write int`() {
        assertWriterOutputEquals(
            """
            61 01
            61 0A
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
            6A
            6C C3 F5 48 40
            6D 1F 85 EB 51 B8 1E 09 40
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
            70
            72 01 00
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
            87 35 46 AF 7C 55 47 70 2D
            F8 05 4B 08
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
            A3 66 6F 6F
            EE 0B
            EE 15
            """
        ) {
            writeSymbol(0)
            writeSymbol(1)
            writeSymbol(12345)
            writeSymbol("foo")
            writeSymbol(SystemSymbols_1_1.ION_LITERAL)
            writeSymbol(SystemSymbols_1_1.THE_EMPTY_SYMBOL)
        }
    }

    @Test
    fun `attempting to write a negative SID should throw exception`() {
        assertWriterThrows {
            writeSymbol(-1)
        }
    }

    @Test
    fun `write string`() {
        assertWriterOutputEquals("93 66 6F 6F") {
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

    @ParameterizedTest
    @EnumSource(SystemMacro::class)
    fun `write a system macro e-expression`(systemMacro: SystemMacro) {
        val numVariadicParameters = systemMacro.signature.count { it.cardinality != ParameterCardinality.ExactlyOne }
        val signatureBytes = when (numVariadicParameters) {
            0 -> ""
            1, 2, 3, 4 -> "00"
            5, 6, 7, 8 -> "00 00"
            else -> TODO("There are definitely no system macros with more than 8 variadic parameters")
        }
        assertWriterOutputEquals(String.format("EF %02X $signatureBytes", systemMacro.id)) {
            stepInEExp(systemMacro)
            stepOut()
        }
    }

    @Test
    fun `write a delimited e-expression`() {
        assertWriterOutputEquals("00") {
            stepInEExp(0, false, dummyMacro(nArgs = 0))
            stepOut()
        }
        assertWriterOutputEquals("3F") {
            stepInEExp(63, false, dummyMacro(nArgs = 0))
            stepOut()
        }
    }

    @ParameterizedTest
    @CsvSource(
        "              64, 40 00",
        "              65, 40 01",
        "             319, 40 FF",
        "             320, 41 00",
        "            1211, 44 7B",
        "            4159, 4F FF",
        "            4160, 50 00 00",
        "            4161, 50 01 00",
        "            4415, 50 FF 00",
        "            4416, 50 00 01",
        "           69695, 50 FF FF",
        "           69696, 51 00 00",
        "         1052735, 5F FF FF",
        "         1052736, F4 04 82 80",
        "${Int.MAX_VALUE}, F4 F0 FF FF FF 0F"
    )
    fun `write a delimited e-expression with a multi-byte biased id`(id: Int, expectedBytes: String) {
        assertWriterOutputEquals(expectedBytes) {
            stepInEExp(id, usingLengthPrefix = false, dummyMacro(nArgs = 0))
            stepOut()
        }
    }

    @Test
    fun `write a delimited e-expression that requires a presence bitmap`() {
        assertWriterOutputEquals(
            """
            3F     | Opcode/MacroID 63
            55     | PresenceBitmap (4 single expressions)
            61 01  | Int 1
            61 02  | Int 2
            61 03  | Int 3
            61 04  | Int 4
        """
        ) {
            stepInEExp(63, usingLengthPrefix = false, dummyMacro(nArgs = 4, variadicParam(ParameterEncoding.Tagged)))
            writeInt(1)
            writeInt(2)
            writeInt(3)
            writeInt(4)
            stepOut()
        }
    }

    @Test
    fun `write a delimited e-expression with presence bitmap where many args are implicitly void`() {
        assertWriterOutputEquals(
            """
            3F           | MacroID 63
            00 00 00 00  | PresenceBitmap (16 void)
        """
        ) {
            stepInEExp(63, usingLengthPrefix = false, dummyMacro(nArgs = 16, variadicParam(ParameterEncoding.Tagged)))
            // Don't write any trailing void args (which is all of them in this case)
            stepOut()
        }
    }

    @ParameterizedTest
    @CsvSource(
        //       Macro Id; Op Address  Length=0
        "               0, F5 01             01",
        "              64, F5 81             01",
        "              65, F5 83             01",
        "             127, F5 FF             01",
        "             128, F5 02 02          01",
        "             729, F5 66 0B          01",
        "           16383, F5 FE FF          01",
        "           16384, F5 04 00 02       01",
        "         1052736, F5 04 82 80       01",
        "         2097151, F5 FC FF FF       01",
        "         2097152, F5 08 00 00 02    01",
        "${Int.MAX_VALUE}, F5 F0 FF FF FF 0F 01",
    )
    fun `write a length-prefixed e-expression with no args`(id: Int, expectedBytes: String) {
        // This test ensures that the macro address is written correctly
        assertWriterOutputEquals(expectedBytes) {
            stepInEExp(id, usingLengthPrefix = true, dummyMacro(nArgs = 0))
            stepOut()
        }
    }

    @Test
    fun `write a length-prefixed e-expression with many args`() {
        // This test ensures that the macro length is written correctly
        assertWriterOutputEquals("F5 03 15 60 60 60 60 60 60 60 60 60 60") {
            stepInEExp(1, usingLengthPrefix = true, dummyMacro(nArgs = 10))
            repeat(10) { writeInt(0L) }
            stepOut()
        }
    }

    @Test
    fun `write a length-prefixed e-expression that requires a presence bitmap`() {
        assertWriterOutputEquals(
            """
            F5     | Length-prefixed macro
            81     | MacroID 64
            13     | Length = 9
            55     | PresenceBitmap (4 single expressions)
            61 01  | Int 1
            61 02  | Int 2
            61 03  | Int 3
            61 04  | Int 4
        """
        ) {
            stepInEExp(64, usingLengthPrefix = true, dummyMacro(nArgs = 4, variadicParam(ParameterEncoding.Tagged)))
            writeInt(1)
            writeInt(2)
            writeInt(3)
            writeInt(4)
            stepOut()
        }
    }

    @Test
    fun `write a length-prefixed e-expression that requires a multi-byte presence bitmap`() {
        assertWriterOutputEquals(
            """
            F5     | Length-prefixed macro
            81     | MacroID 64
            1D     | Length = 14
            55 05  | PresenceBitmap (6 single expressions)
            61 01  | Int 1
            61 02  | Int 2
            61 03  | Int 3
            61 04  | Int 4
            61 05  | Int 5
            61 06  | Int 6
        """
        ) {
            stepInEExp(64, usingLengthPrefix = true, dummyMacro(nArgs = 6, variadicParam(ParameterEncoding.Tagged)))
            writeInt(1)
            writeInt(2)
            writeInt(3)
            writeInt(4)
            writeInt(5)
            writeInt(6)
            stepOut()
        }
    }

    @Test
    fun `write a length-prefixed e-expression with presence bitmap where many args are implicitly void`() {
        assertWriterOutputEquals(
            """
            F5           | Length-prefixed macro
            81           | MacroID 64
            09           | Length = 4
            00 00 00 00  | PresenceBitmap (16 void)
        """
        ) {
            stepInEExp(64, usingLengthPrefix = true, dummyMacro(nArgs = 16, variadicParam(ParameterEncoding.Tagged)))
            // Don't write any trailing void args (which is all of them in this case)
            stepOut()
        }
    }

    @Test
    fun `write nested e-expressions`() {
        // E-Expressions don't have length prefixes, so we're putting them inside lists
        // so that we can check that the length gets propagated correctly to the parent
        assertWriterOutputEquals(
            """
            BB         | List Length 11
            1F         | Macro 31
            B9         | List Length 9
            40 00      | Macro 64
            B6         | List Length 6
            40 13      | Macro 83
            B3         | List Length 3
            50 00 00   | Macro 4160
            """
        ) {
            stepInList(usingLengthPrefix = true)
            stepInEExp(31, usingLengthPrefix = false, dummyMacro(nArgs = 1))
            stepInList(usingLengthPrefix = true)
            stepInEExp(64, usingLengthPrefix = false, dummyMacro(nArgs = 1))
            stepInList(usingLengthPrefix = true)
            stepInEExp(83, usingLengthPrefix = false, dummyMacro(nArgs = 1))
            stepInList(usingLengthPrefix = true)
            stepInEExp(4160, usingLengthPrefix = false, dummyMacro(nArgs = 0))
            repeat(8) { stepOut() }
        }
    }

    @Test
    fun `write an e-expression in the field name position of a variable-length struct`() {
        assertWriterOutputEquals(
            """
            FD      | Variable Length Struct
            11      | Length = 8
            15      | SID 10
            6E      | true
            01      | switch to FlexSym encoding
            01      | FlexSym Escape Byte
            1F      | Macro 31 (0x1F)
            01      | FlexSym Escape Byte
            40 00   | Macro 64
            """
        ) {
            stepInStruct(usingLengthPrefix = true)
            writeFieldName(10)
            writeBool(true)
            stepInEExp(31, usingLengthPrefix = false, dummyMacro(nArgs = 0))
            stepOut()
            stepInEExp(64, usingLengthPrefix = false, dummyMacro(nArgs = 0))
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
            stepInStruct(usingLengthPrefix = false)
            stepInEExp(31, usingLengthPrefix = false, dummyMacro(nArgs = 0))
            stepOut()
            stepOut()
        }
    }

    @Test
    fun `write an e-expression in the value position of a struct`() {
        assertWriterOutputEquals(
            """
            D2      | Struct length 2
            03      | SID 1
            01      | Macro 1
            """
        ) {
            stepInStruct(usingLengthPrefix = true)
            writeFieldName(1)
            stepInEExp(1, usingLengthPrefix = false, dummyMacro(nArgs = 0))
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
            stepInEExp(1, usingLengthPrefix = false, dummyMacro(nArgs = 0))
        }
    }

    @Test
    fun `write a prefixed, tagged expression group with zero values`() {
        assertWriterOutputEquals(""" 3D 01 """) {
            stepInEExp(0x3D, usingLengthPrefix = false, dummyMacro(nArgs = 1))
            stepInExpressionGroup(usingLengthPrefix = true)
            stepOut()
            stepOut()
        }
    }

    @Test
    fun `write a prefixed, tagged expression group with one value`() {
        assertWriterOutputEquals(
            """
            3D      | Macro 61
            03      | Expression Group, Length = 1
            6E      | true
            """
        ) {
            stepInEExp(0x3D, usingLengthPrefix = false, dummyMacro(nArgs = 1))
            stepInExpressionGroup(usingLengthPrefix = true)
            writeBool(true)
            stepOut()
            stepOut()
        }
    }

    @Test
    fun `write a prefixed, tagged expression group with multiple values`() {
        assertWriterOutputEquals(
            """
            3D              | Macro 77
            0B              | Expression Group, Length = 5
            60 61 01 61 02  | Ints 0, 1, 2
            """
        ) {
            stepInEExp(0x3D, usingLengthPrefix = false, dummyMacro(nArgs = 1))
            stepInExpressionGroup(usingLengthPrefix = true)
            writeInt(0)
            writeInt(1)
            continueExpressionGroup() // Should have no effect
            writeInt(2)
            stepOut()
            stepOut()
        }
    }

    @Test
    fun `write a prefixed, tagged expression group so long that it requires a patch point`() {
        assertWriterOutputEquals(
            """
            3D      | Macro 0
            FE 03   | Expression Group, Length = 255
            ${"6E ".repeat(255)}
            """
        ) {
            stepInEExp(0x3D, usingLengthPrefix = false, dummyMacro(nArgs = 1))
            stepInExpressionGroup(usingLengthPrefix = true)
            repeat(255) { writeBool(true) }
            stepOut()
            stepOut()
        }
    }

    @Test
    fun `write a delimited, tagged expression group with zero values`() {
        assertWriterOutputEquals("3D 01 F0") {
            stepInEExp(0x3D, usingLengthPrefix = false, dummyMacro(nArgs = 1))
            stepInExpressionGroup(usingLengthPrefix = false)
            stepOut()
            stepOut()
        }
    }

    @Test
    fun `write a delimited, tagged expression group with one value`() {
        assertWriterOutputEquals("3D 01 60 F0") {
            stepInEExp(0x3D, usingLengthPrefix = false, dummyMacro(nArgs = 1))
            stepInExpressionGroup(usingLengthPrefix = false)
            writeInt(0)
            stepOut()
            stepOut()
        }
    }

    @Test
    fun `write a delimited, tagged expression group with multiple values`() {
        assertWriterOutputEquals("3D 01 60 61 01 61 02 F0") {
            stepInEExp(0x3D, usingLengthPrefix = false, dummyMacro(nArgs = 1))
            stepInExpressionGroup(usingLengthPrefix = false)
            writeInt(0)
            writeInt(1)
            continueExpressionGroup() // Should have no effect
            writeInt(2)
            stepOut()
            stepOut()
        }
    }

    @Test
    fun `write a tagless expression group with zero values`() {
        // Empty expression group is elided to be void, so we just have `00` presence bitmap
        assertWriterOutputEquals("3D 00") {
            stepInEExp(0x3D, usingLengthPrefix = false, dummyMacro(nArgs = 1, variadicParam(ParameterEncoding.Uint8)))
            stepInExpressionGroup(usingLengthPrefix = false)
            stepOut()
            stepOut()
        }
    }

    @Test
    fun `write a tagless expression group with one value`() {
        assertWriterOutputEquals("3D 02 03 1A 01") {
            stepInEExp(0x3D, usingLengthPrefix = false, dummyMacro(nArgs = 1, variadicParam(ParameterEncoding.Uint8)))
            stepInExpressionGroup(usingLengthPrefix = false)
            writeInt(0x1A)
            stepOut()
            stepOut()
        }
    }

    @Test
    fun `write a tagless expression group with multiple values`() {
        assertWriterOutputEquals("3D 02 07 1A 2B 3C 01") {
            stepInEExp(0x3D, usingLengthPrefix = false, dummyMacro(nArgs = 1, variadicParam(ParameterEncoding.Uint8)))
            stepInExpressionGroup(usingLengthPrefix = false)
            writeInt(0x1A)
            writeInt(0x2B)
            writeInt(0x3C)
            stepOut()
            stepOut()
        }
    }

    @Test
    fun `write a tagless expression group with multiple segments using continueExpressionGroup()`() {
        assertWriterOutputEquals(
            """
            3D 02
            07 1A 2B 3C  | 3 ints
            05 4D 5E     | 2 more ints
            01           | End of expression group
            """
        ) {
            stepInEExp(0x3D, usingLengthPrefix = false, dummyMacro(nArgs = 1, variadicParam(ParameterEncoding.Uint8)))
            stepInExpressionGroup(usingLengthPrefix = false)
            writeInt(0x1A)
            writeInt(0x2B)
            writeInt(0x3C)
            continueExpressionGroup()
            writeInt(0x4D)
            writeInt(0x5E)
            stepOut()
            stepOut()
        }
    }

    @Test
    fun `calling stepOut() immediately after continueExpressionGroup() should be handled correctly`() {
        assertWriterOutputEquals(
            """
            3D 02
            07 1A 2B 3C  | 3 ints
            01           | End of expression group
            """
        ) {
            stepInEExp(0x3D, usingLengthPrefix = false, dummyMacro(nArgs = 1, variadicParam(ParameterEncoding.Uint8)))
            stepInExpressionGroup(usingLengthPrefix = false)
            writeInt(0x1A)
            writeInt(0x2B)
            writeInt(0x3C)
            continueExpressionGroup()
            stepOut()
            stepOut()
        }
    }

    @Test
    fun `calling stepOut() with too many parameters in a length-prefixed e-expression throws IllegalArgumentException`() {
        val rawWriter = ionWriter()

        assertThrows<IllegalArgumentException> {
            rawWriter.stepInEExp(64, usingLengthPrefix = true, dummyMacro(nArgs = 0))
            rawWriter.stepInEExp(SystemMacro.None)
            rawWriter.stepOut()
            rawWriter.stepOut()
        }
    }

    @Test
    fun `calling continueExpressionGroup() has no effect when there are no expressions in the current segment`() {
        assertWriterOutputEquals(
            """
            3D 02
            05 1A 2B  | 2 ints
            03 3C     | 1 int
            01        | End of expression group
            """
        ) {
            stepInEExp(0x3D, usingLengthPrefix = false, dummyMacro(nArgs = 1, variadicParam(ParameterEncoding.Uint8)))
            stepInExpressionGroup(usingLengthPrefix = false)
            repeat(10) { continueExpressionGroup() }
            writeInt(0x1A)
            writeInt(0x2B)
            repeat(10) { continueExpressionGroup() }
            writeInt(0x3C)
            stepOut()
            stepOut()
        }
    }

    @Test
    fun `calling continueExpressionGroup() throws an exception if not in an expression group`() {
        assertWriterThrows { continueExpressionGroup() }
        assertWriterThrows { writeList { continueExpressionGroup() } }
        assertWriterThrows { writeSExp { continueExpressionGroup() } }
        assertWriterThrows { writeStruct { continueExpressionGroup() } }
        assertWriterThrows {
            stepInEExp(0x3D, usingLengthPrefix = false, dummyMacro(nArgs = 1, variadicParam(ParameterEncoding.Uint8)))
            continueExpressionGroup()
        }
    }

    @Test
    fun `calling stepInExpressionGroup with an annotation should throw IonException`() {
        assertWriterThrows {
            stepInEExp(1, usingLengthPrefix = false, dummyMacro(nArgs = 1))
            writeAnnotations("foo")
            stepInExpressionGroup(usingLengthPrefix = true)
        }
    }

    @Test
    fun `calling stepInExpressionGroup while not directly in a Macro container should throw IonException`() {
        assertWriterThrows {
            stepInExpressionGroup(usingLengthPrefix = true)
        }
        assertWriterThrows {
            stepInList(usingLengthPrefix = true)
            stepInExpressionGroup(usingLengthPrefix = true)
        }
        assertWriterThrows {
            stepInSExp(usingLengthPrefix = true)
            stepInExpressionGroup(usingLengthPrefix = true)
        }
        assertWriterThrows {
            stepInStruct(usingLengthPrefix = true)
            stepInExpressionGroup(usingLengthPrefix = true)
        }
        assertWriterThrows {
            stepInEExp(123, usingLengthPrefix = false, dummyMacro(nArgs = 1))
            stepInExpressionGroup(usingLengthPrefix = true)
            stepInExpressionGroup(usingLengthPrefix = true)
        }
    }

    @ParameterizedTest
    @CsvSource(
        // These tests are intentionally limited. Full testing of int logic is in `IonEncoder_1_1Test` and `WriteBufferTest`
        "      Uint8,   0, 00",
        "      Uint8,   1, 01",
        "     Uint16,   0, 00 00",
        "     Uint16,   1, 01 00",
        "     Uint32,   0, 00 00 00 00",
        "     Uint32,   1, 01 00 00 00",
        "     Uint64,   0, 00 00 00 00 00 00 00 00",
        "     Uint64,   1, 01 00 00 00 00 00 00 00",
        "   FlexUint,   0, 01",
        "   FlexUint,   1, 03",
        "       Int8,   0, 00",
        "       Int8,   1, 01",
        "       Int8,  -1, FF",
        "      Int16,   0, 00 00",
        "      Int16,   1, 01 00",
        "      Int16,  -1, FF FF",
        "      Int32,   0, 00 00 00 00",
        "      Int32,   1, 01 00 00 00",
        "      Int32,  -1, FF FF FF FF",
        "      Int64,   0, 00 00 00 00 00 00 00 00",
        "      Int64,   1, 01 00 00 00 00 00 00 00",
        "      Int64,  -1, FF FF FF FF FF FF FF FF",
        "    FlexInt,   0, 01",
        "    FlexInt,   1, 03",
        "    FlexInt,  -1, FF",
    )
    fun `write a tagless int`(encoding: ParameterEncoding, value: Long, expectedBytes: String) {
        val macro = dummyMacro(nArgs = 1, variadicParam(encoding))
        // Write the value as single expression
        assertWriterOutputEquals("3D 01 $expectedBytes") {
            stepInEExp(0x3D, usingLengthPrefix = false, macro)
            writeInt(value)
            stepOut()
        }
        // ...and again using writeInt(BigInteger)
        assertWriterOutputEquals("3D 01 $expectedBytes") {
            stepInEExp(0x3D, usingLengthPrefix = false, macro)
            writeInt(value.toBigInteger())
            stepOut()
        }
    }

    @ParameterizedTest
    @CsvSource(
        // These tests are intentionally limited. Full testing of int logic is in `IonEncoder_1_1Test` and `WriteBufferTest`
        // Primitive, Ints to write, expression group bytes
        "      Uint8, 0 1,        05 00 01 01",
        "     Uint16, 0 1,        09 00 00 01 00 01",
        "     Uint32, 0 1,        11 00 00 00 00 01 00 00 00 01",
        "     Uint64, 0 1,        21 00 00 00 00 00 00 00 00 01 00 00 00 00 00 00 00 01",
        "   FlexUint, 0 1 256,    09 01 03 02 04 01",
        "       Int8, -1 0 1,     07 FF 00 01 01",
        "      Int16, -1 0 1,     0D FF FF 00 00 01 00 01",
        "      Int32, -1 0 1,     19 FF FF FF FF 00 00 00 00 01 00 00 00 01",
        "      Int64, -1 0 1,     31 FF FF FF FF FF FF FF FF 00 00 00 00 00 00 00 00 01 00 00 00 00 00 00 00 01",
        "    FlexInt, -1 0 1 256, 0B FF 01 03 02 04 01",
    )
    fun `write a tagless int in an expression group`(encoding: ParameterEncoding, values: String, expressionGroupBytes: String) {
        val longValues = values.split(" ").map { it.toLong() }
        val macro = dummyMacro(nArgs = 1, variadicParam(encoding))
        // Write the value in expression group
        assertWriterOutputEquals("3D 02 $expressionGroupBytes") {
            stepInEExp(0x3D, usingLengthPrefix = false, macro)
            stepInExpressionGroup(usingLengthPrefix = false)
            longValues.forEach { writeInt(it) }
            stepOut()
            stepOut()
        }
        // ...and again using writeInt(BigInteger)
        assertWriterOutputEquals("3D 02 $expressionGroupBytes") {
            stepInEExp(0x3D, usingLengthPrefix = false, macro)
            stepInExpressionGroup(usingLengthPrefix = false)
            longValues.forEach { writeInt(it.toBigInteger()) }
            stepOut()
            stepOut()
        }
    }

    @ParameterizedTest
    @CsvSource(
        "      Uint8,  ${UByte.MIN_VALUE}",
        "      Uint8,  ${UByte.MAX_VALUE}",
        "     Uint16, ${UShort.MIN_VALUE}",
        "     Uint16, ${UShort.MAX_VALUE}",
        "     Uint32,   ${UInt.MIN_VALUE}",
        "     Uint32,   ${UInt.MAX_VALUE}",
        "     Uint64,  ${ULong.MIN_VALUE}",
        "     Uint64,  ${ULong.MAX_VALUE}",
        "       Int8,   ${Byte.MIN_VALUE}",
        "       Int8,   ${Byte.MAX_VALUE}",
        "      Int16,  ${Short.MIN_VALUE}",
        "      Int16,  ${Short.MAX_VALUE}",
        "      Int32,    ${Int.MIN_VALUE}",
        "      Int32,    ${Int.MAX_VALUE}",
        "      Int64,   ${Long.MIN_VALUE}",
        "      Int64,   ${Long.MAX_VALUE}",
        "   FlexUint,                   0",
        // There is no upper bound for FlexUInt, and no bounds at all for FlexInt
    )
    fun `attempting to write a tagless int that is out of bounds for its encoding primitive should throw exception`(
        encoding: ParameterEncoding,
        // The min or max value of that particular parameter encoding.
        goodValue: BigInteger,
    ) {
        val badValue = if (goodValue > BigInteger.ZERO) goodValue + BigInteger.ONE else goodValue - BigInteger.ONE
        val macro = dummyMacro(nArgs = 2, variadicParam(encoding))
        assertWriterThrows {
            stepInEExp(0x3D, usingLengthPrefix = false, macro)
            writeInt(badValue)
            stepOut()
        }

        if (badValue.bitLength() < Long.SIZE_BITS) {
            // If this bad value fits in a long, test it on the long API as well.
            assertWriterThrows {
                stepInEExp(0x3D, usingLengthPrefix = false, macro)
                writeInt(badValue.longValueExact())
                stepOut()
            }
        }
    }

    @Test
    fun `attempting to write an int when another tagless type is expected should throw exception`() {
        val macro = dummyMacro(nArgs = 1, variadicParam(ParameterEncoding.Float64))
        assertWriterThrows {
            stepInEExp(0x3D, usingLengthPrefix = false, macro)
            writeInt(0L)
            stepOut()
        }
        assertWriterThrows {
            stepInEExp(0x3D, usingLengthPrefix = false, macro)
            writeInt(0.toBigInteger())
            stepOut()
        }
    }

    @ParameterizedTest
    @CsvSource(
        // These tests are intentionally limited. Full testing of float logic is in `IonEncoder_1_1Test`
        // TODO: Float16 cases, once Float16 is supported
        "Float32,       0.0, 00 00 00 00",
        "Float32,       1.0, 00 00 80 3F",
        "Float32,       NaN, 00 00 C0 7F",
        "Float32,  Infinity, 00 00 80 7F",
        "Float32, -Infinity, 00 00 80 FF",
        "Float64,       0.0, 00 00 00 00 00 00 00 00",
        "Float64,       1.0, 00 00 00 00 00 00 F0 3F",
        "Float64,       NaN, 00 00 00 00 00 00 F8 7F",
        "Float64,  Infinity, 00 00 00 00 00 00 F0 7F",
        "Float64, -Infinity, 00 00 00 00 00 00 F0 FF",
    )
    fun `write a tagless float`(encoding: ParameterEncoding, value: Float, expectedBytes: String) {
        val macro = dummyMacro(nArgs = 1, variadicParam(encoding))
        // Write the value as single expression
        assertWriterOutputEquals("3D 01 $expectedBytes") {
            stepInEExp(0x3D, usingLengthPrefix = false, macro)
            writeFloat(value)
            stepOut()
        }
        // ...and again using writeFloat(Double)
        assertWriterOutputEquals("3D 01 $expectedBytes") {
            stepInEExp(0x3D, usingLengthPrefix = false, macro)
            writeFloat(value.toDouble())
            stepOut()
        }
    }

    @OptIn(ExperimentalStdlibApi::class)
    @ParameterizedTest
    @CsvSource(
        // These tests are intentionally limited. Full testing of float logic is in `IonEncoder_1_1Test`
        // TODO: Float16 cases, once Float16 is supported
        "Float32,       0.0, 00 00 00 00",
        "Float32,       1.0, 00 00 80 3F",
        "Float32,       NaN, 00 00 C0 7F",
        "Float32,  Infinity, 00 00 80 7F",
        "Float32, -Infinity, 00 00 80 FF",
        "Float64,       0.0, 00 00 00 00 00 00 00 00",
        "Float64,       1.0, 00 00 00 00 00 00 F0 3F",
        "Float64,       NaN, 00 00 00 00 00 00 F8 7F",
        "Float64,  Infinity, 00 00 00 00 00 00 F0 7F",
        "Float64, -Infinity, 00 00 00 00 00 00 F0 FF",
    )
    fun `write a tagless float in an expression group`(encoding: ParameterEncoding, value: Float, expectedBytes: String) {
        val taglessTypeByteSize = when (encoding) {
            ParameterEncoding.Float16 -> 2
            ParameterEncoding.Float32 -> 4
            ParameterEncoding.Float64 -> 8
            else -> TODO("Other types not supported in this test.")
        }
        val macro = dummyMacro(nArgs = 1, variadicParam(encoding))
        // For small numbers, we can use x*2+1 to calculate the FlexUInt encoding
        val lengthByte = ((taglessTypeByteSize + taglessTypeByteSize) * 2 + 1).toByte().toHexString(HexFormat.UpperCase)
        // Write the value twice in expression group
        assertWriterOutputEquals("3D 02 $lengthByte $expectedBytes $expectedBytes 01") {
            stepInEExp(0x3D, usingLengthPrefix = false, macro)
            stepInExpressionGroup(usingLengthPrefix = false)
            writeFloat(value)
            writeFloat(value)
            stepOut()
            stepOut()
        }
        // ...and again using writeFloat(Double)
        assertWriterOutputEquals("3D 02 $lengthByte $expectedBytes $expectedBytes 01") {
            stepInEExp(0x3D, usingLengthPrefix = false, macro)
            stepInExpressionGroup(usingLengthPrefix = false)
            writeFloat(value.toDouble())
            writeFloat(value.toDouble())
            stepOut()
            stepOut()
        }
    }

    @Test
    fun `attempting to write a float when another tagless type is expected should throw exception`() {
        val macro = dummyMacro(nArgs = 1, variadicParam(ParameterEncoding.Uint8))
        assertWriterThrows {
            stepInEExp(0x3D, usingLengthPrefix = false, macro)
            writeFloat(0.0) // double
            stepOut()
        }
        assertWriterThrows {
            stepInEExp(0x3D, usingLengthPrefix = false, macro)
            writeFloat(0.0f) // float
            stepOut()
        }
    }

    @OptIn(ExperimentalStdlibApi::class)
    @ParameterizedTest
    @CsvSource(
        // SID
        "    0, 01 60",
        "    4, 09",
        "  246, DA 03",
        // Text
        "    a, FF 61",
        "  abc, FB 61 62 63",
        "   '', 01 75",
    )
    fun `write a tagless symbol`(value: String, expectedBytes: String) {
        val macro = dummyMacro(nArgs = 1, variadicParam(ParameterEncoding.FlexSym))
        // If it's an int, write as SID, else write as text
        val writeTheValue: IonRawBinaryWriter_1_1.() -> Unit = value.toIntOrNull()
            ?.let { { writeSymbol(it) } }
            ?: { writeSymbol(value) }
        // Write the value as single expression
        assertWriterOutputEquals("3D 01 $expectedBytes") {
            stepInEExp(0x3D, usingLengthPrefix = false, macro)
            writeTheValue()
            stepOut()
        }
        // Write the value twice in expression group
        // For small numbers, we can use x*2+1 to calculate the FlexUInt encoding
        // Also, it conveniently happens that once the white-space is removed, the number of characters is
        // equal to the number of bytes to write the values twice.
        val lengthByte = ((expectedBytes.replace(" ", "").length) * 2 + 1).toByte().toHexString(HexFormat.UpperCase)
        assertWriterOutputEquals("3D 02 $lengthByte $expectedBytes $expectedBytes 01") {
            stepInEExp(0x3D, usingLengthPrefix = false, macro)
            stepInExpressionGroup(usingLengthPrefix = false)
            writeTheValue()
            writeTheValue()
            stepOut()
            stepOut()
        }
    }

    @Test
    fun `attempting to write a symbol when another tagless type is expected should throw exception`() {
        val macro = dummyMacro(nArgs = 1, variadicParam(ParameterEncoding.Uint8))
        assertWriterThrows {
            stepInEExp(0x3D, usingLengthPrefix = false, macro)
            writeSymbol(4)
            stepOut()
        }
        assertWriterThrows {
            stepInEExp(0x3D, usingLengthPrefix = false, macro)
            writeSymbol("foo")
            stepOut()
        }
    }

    @Test
    fun `attempting to write a tagless value with annotations should throw exception`() {
        assertWriterThrows {
            stepInEExp(0x3D, usingLengthPrefix = false, dummyMacro(nArgs = 1, variadicParam(ParameterEncoding.Uint8)))
            writeAnnotations("foo")
            writeInt(0)
            stepOut()
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
            E4 07 FD 63                 | $3::{             // length=49
            0F FB 5D                    |   $7: [           // length=46
            94 6E 61 6D 65              |     "name",
            93 61 67 65                 |     "age",
            95 79 65 61 72 73           |     "years",
            98 62 69 72 74 68 64 61 79  |     "birthday",
            94 74 6F 79 73              |     "toys",
            94 62 61 6C 6C              |     "ball",
            96 77 65 69 67 68 74        |     "weight",
            94 62 75 7A 7A              |     "buzz",
                                        |   ]
                                        | }
            FD 85                       | {                 // length=66
            15 94 46 69 64 6F           |   $10: "Fido",
            17 E4 19 61 04              |   $11: $12::4,
            1B 82 AA 09                 |   $13: 2012-03-01T
            1D B7                       |   $14: [          // length=7
            E1 0F                       |     $15,
            A4 72 6F 70 65              |     rope
                                        |   ],
            21                          |   $16:
            E7 F5 70 6F 75 6E 64 73     |       pounds::
            73 FF 9C 01                 |       41.2
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
        stepInStruct(usingLengthPrefix = true)
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
        stepInList(usingLengthPrefix = true)
        block()
        stepOut()
    }

    /**
     * Helper function that steps into a sexp, applies the contents of [block] to
     * the writer, and then steps out of the sexp.
     * Using this function makes it easy for the indentation of the writer code to
     * match the indentation of the equivalent pretty-printed Ion.
     */
    private inline fun IonRawWriter_1_1.writeSExp(block: IonRawWriter_1_1.() -> Unit) {
        stepInSExp(usingLengthPrefix = true)
        block()
        stepOut()
    }

    /**
     * Helper function that creates a dummy macro with the given number of arguments.
     */
    private fun dummyMacro(nArgs: Int, param: Parameter = Parameter("arg", ParameterEncoding.Tagged, ParameterCardinality.ExactlyOne)) =
        TemplateMacro(List(nArgs) { param.copy("arg_$it") }, listOf())

    private fun variadicParam(encoding: ParameterEncoding) = Parameter("arg", encoding, ParameterCardinality.ZeroOrMore)
}
