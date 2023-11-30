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

    private inline fun writeAsHexString(block: IonRawBinaryWriter_1_1.() -> Unit): String {
        val baos = ByteArrayOutputStream()
        val rawWriter = IonRawBinaryWriter_1_1(
            out = baos,
            buffer = WriteBuffer(BlockAllocatorProviders.basicProvider().vendAllocator(32)),
            lengthPrefixPreallocation = 1,
        )
        block.invoke(rawWriter)
        @OptIn(ExperimentalStdlibApi::class)
        return baos.toByteArray().joinToString(" ") { it.toHexString(HexFormat.UpperCase) }
    }

    private inline fun assertWriterOutputEquals(hexBytes: String, block: IonRawBinaryWriter_1_1.() -> Unit) {
        assertEquals(hexBytes, writeAsHexString(block))
    }

    @Test
    fun `calling close while in a container should throw IonException`() {
        assertThrows<IonException> {
            writeAsHexString {
                stepInList(false)
                close()
            }
        }
    }

    @Test
    fun `calling finish while in a container should throw IonException`() {
        assertThrows<IonException> {
            writeAsHexString {
                stepInList(true)
                finish()
            }
        }
    }

    @Test
    fun `calling stepOut while not in a container should throw IonException`() {
        assertThrows<IonException> {
            writeAsHexString {
                stepOut()
            }
        }
    }

    @Test
    fun `calling writeIVM when in a container should throw IonException`() {
        assertThrows<IonException> {
            writeAsHexString {
                stepInList(false)
                writeIVM()
            }
        }
    }

    @Test
    fun `calling finish should cause the buffered data to be written to the output stream`() {
        val actual = writeAsHexString {
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
            close()
        }
        // Just checking that data is written, not asserting the content.
        assertTrue(actual.isNotBlank())
    }

    @Test
    fun `calling close should cause the buffered data to be written to the output stream`() {
        val actual = writeAsHexString {
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
            close()
        }
    }

    @Test
    fun `write nothing`() {
        assertWriterOutputEquals("") {
            close()
        }
    }

    @Test
    fun `write a null`() {
        assertWriterOutputEquals("EA") {
            writeNull()
            close()
        }
    }

    @Test
    fun `write a null with a specific type`() {
        // Just checking one type. The full range of types are checked in IonEncoder_1_1Test
        assertWriterOutputEquals("EB 00") {
            writeNull(IonType.BOOL)
            close()
        }
    }

    @ParameterizedTest
    @CsvSource("true, 5E", "false, 5F")
    fun `write a boolean`(value: Boolean, hexBytes: String) {
        assertWriterOutputEquals(hexBytes) {
            writeBool(value)
            close()
        }
    }

    @Test
    fun `write a delimited list`() {
        assertWriterOutputEquals("F1 5E 5F F0") {
            stepInList(true)
            writeBool(true)
            writeBool(false)
            stepOut()
            close()
        }
    }

    @Test
    fun `write a prefixed list`() {
        assertWriterOutputEquals("A2 5E 5F") {
            stepInList(false)
            writeBool(true)
            writeBool(false)
            stepOut()
            close()
        }
    }

    @Test
    fun `write a variable-length prefixed list`() {
        assertWriterOutputEquals("FA 21${" 5E".repeat(16)}") {
            stepInList(false)
            repeat(16) { writeBool(true) }
            stepOut()
            finish()
            close()
        }
    }

    @Test
    fun `write a prefixed list that is so long it requires patch points`() {
        assertWriterOutputEquals("FA 02 02${" 5E".repeat(128)}") {
            stepInList(false)
            repeat(128) { writeBool(true) }
            stepOut()
            close()
        }
    }

    @Test
    fun `write multiple nested prefixed lists`() {
        assertWriterOutputEquals("A4 A3 A2 A1 A0") {
            repeat(5) { stepInList(false) }
            repeat(5) { stepOut() }
            close()
        }
    }

    @Test
    fun `write multiple nested delimited lists`() {
        assertWriterOutputEquals("F1 F1 F1 F1 F0 F0 F0 F0") {
            repeat(4) { stepInList(true) }
            repeat(4) { stepOut() }
            close()
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
            close()
        }
    }
}
