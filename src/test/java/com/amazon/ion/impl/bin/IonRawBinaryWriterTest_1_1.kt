// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.bin

import com.amazon.ion.IonException
import com.amazon.ion.IonType
import java.io.ByteArrayOutputStream
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
            buffer = WriteBuffer(BlockAllocatorProviders.basicProvider().vendAllocator(32)),
            lengthPrefixPreallocation = 1,
        )
        block.invoke(rawWriter)
        if (autoClose) rawWriter.close()
        @OptIn(ExperimentalStdlibApi::class)
        return baos.toByteArray().joinToString(" ") { it.toHexString(HexFormat.UpperCase) }
    }

    private inline fun assertWriterOutputEquals(hexBytes: String, autoClose: Boolean = true, block: IonRawBinaryWriter_1_1.() -> Unit) {
        assertEquals(hexBytes, writeAsHexString(autoClose, block))
    }

    private inline fun assertWriterThrows(block: IonRawBinaryWriter_1_1.() -> Unit) {
        val baos = ByteArrayOutputStream()
        val rawWriter = IonRawBinaryWriter_1_1(
            out = baos,
            buffer = WriteBuffer(BlockAllocatorProviders.basicProvider().vendAllocator(32)),
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
        assertWriterOutputEquals("FA 21${" 5E".repeat(16)}") {
            stepInList(false)
            repeat(16) { writeBool(true) }
            stepOut()
            finish()
        }
    }

    @Test
    fun `write a prefixed list that is so long it requires patch points`() {
        assertWriterOutputEquals("FA 02 02${" 5E".repeat(128)}") {
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
            writeAnnotations("Amazon Ion is a richly-typed, self-describing, hierarchical data serialization " +
                    "format offering interchangeable binary and text representations.")
            writeBool(false)
        }
    }
}
