// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.bin

import com.amazon.ion.Decimal
import com.amazon.ion.IonException
import com.amazon.ion.IonType
import com.amazon.ion.TextToBinaryUtils.cleanCommentedHexBytes
import com.amazon.ion.Timestamp
import com.amazon.ion.bytecode.bin11.OpCode
import com.amazon.ion.ion_1_1.IonRawWriter_1_1
import com.amazon.ion.ion_1_1.TaglessScalarType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.ValueSource
import java.io.ByteArrayOutputStream
import java.math.BigDecimal
import java.math.BigInteger

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
        val cleanedHexBytes = hexBytes.cleanCommentedHexBytes()
        assertEquals(hexBytes.cleanCommentedHexBytes(), writeAsHexString(autoClose, block))

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
            val flexUIntLen = PrimitiveEncoder.flexUIntLength(expectedLength.toLong())
            val flexUIntBytes = ByteArray(flexUIntLen)
            PrimitiveEncoder.writeFlexIntOrUIntInto(flexUIntBytes, 0, expectedLength.toLong(), flexUIntLen)
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
    fun `calling stepOut while not in a container should throw IonException`() {
        assertWriterThrows {
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
        assertWriterOutputEquals("8E 8E") {
            writeNull()
            writeNull(IonType.NULL)
        }
    }

    @ParameterizedTest
    @CsvSource(
        "BOOL,      8F 01",
        "INT,       8F 02",
        "FLOAT,     8F 03",
        "DECIMAL,   8F 04",
        "TIMESTAMP, 8F 05",
        "SYMBOL,    8F 06",
        "STRING,    8F 07",
        "CLOB,      8F 08",
        "BLOB,      8F 09",
        "LIST,      8F 0A",
        "SEXP,      8F 0B",
        "STRUCT,    8F 0C",
    )
    fun `write a null with a specific type`(ionType: IonType, expectedBytes: String) {
        assertWriterOutputEquals(expectedBytes) { writeNull(ionType) }
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
        assertWriterOutputEquals("F0 6E 6F EF") {
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
        assertWriterOutputEquals("FA 21 ${" 6E".repeat(16)}") {
            stepInList(usingLengthPrefix = true)
            repeat(16) { writeBool(true) }
            stepOut()
            flush()
        }
    }

    @Test
    fun `write a prefixed list that is so long it requires patch points`() {
        assertWriterOutputEquals("FA 02 02 ${" 6E".repeat(128)}") {
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
        assertWriterOutputEquals("F0 F0 F0 B0 EF EF EF") {
            repeat(4) { stepInList(usingLengthPrefix = false) }
            repeat(4) { stepOut() }
        }
    }

    @Test
    fun `write multiple nested delimited and prefixed lists`() {
        assertWriterOutputEquals("F0 B9 F0 B6 F0 B3 F0 B0 EF EF EF EF") {
            repeat(4) {
                stepInList(usingLengthPrefix = false)
                stepInList(usingLengthPrefix = true)
            }
            repeat(8) { stepOut() }
        }
    }

    @Test
    fun `write a delimited sexp`() {
        assertWriterOutputEquals("F1 6E 6F EF") {
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
        assertWriterOutputEquals("FB 21 ${" 6E".repeat(16)}") {
            stepInSExp(usingLengthPrefix = true)
            repeat(16) { writeBool(true) }
            stepOut()
            flush()
        }
    }

    @Test
    fun `write a prefixed sexp that is so long it requires patch points`() {
        assertWriterOutputEquals("FB 02 02 ${" 6E".repeat(128)}") {
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
        assertWriterOutputEquals("F1 F1 F1 C0 EF EF EF") {
            repeat(4) { stepInSExp(usingLengthPrefix = false) }
            repeat(4) { stepOut() }
        }
    }

    @Test
    fun `write multiple nested delimited and prefixed sexps`() {
        assertWriterOutputEquals("F1 C9 F1 C6 F1 C3 F1 C0 EF EF EF EF") {
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
            FC      | Variable Length SID Struct
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
            FC      | Variable Length SID Struct
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
            F2                      | Begin delimited struct
            17                      | FlexSym SID 11
            F2                      | Begin delimited struct
            17 F2 17 F2 17          | etc.
            D0                      | empty struct
            01 EF                   | End delimited struct
            01 EF 01 EF 01 EF       | etc.
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
    fun `write delimited struct`() {
        assertWriterOutputEquals(
            """
            F2          | Begin delimited struct
            17          | SID 11
            6E          | true
            01 EE       | switch to flex sym mode
            F9 66 6F 6F | FlexSym 'foo'
            6E          | true
            02 01       | FlexSym SID 64
            6E          | true
            01 EF       | End delimited struct
            """
        ) {
            stepInStruct(usingLengthPrefix = false)
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
    fun `write empty struct`() {
        assertWriterOutputEquals("D0 D0") {
            stepInStruct(usingLengthPrefix = false)
            stepOut()
            stepInStruct(usingLengthPrefix = true)
            stepOut()
        }
    }

    @Test
    fun `write prefixed struct with a single flex sym field`() {
        assertWriterOutputEquals(
            """
            D7          | Variable length Struct, L=7
            01 EE       | switch to FlexSym encoding
            F9 66 6F 6F | FlexSym 'foo'
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
            FC           | Variable length Struct
            23           | Length = 16
            01 EE        | switch to FlexSym encoding
            F9 66 6F 6F  | FlexSym 'foo'
            6E           | true
            F9 62 61 72  | FlexSym 'bar'
            6E           | true
            F9 62 61 7A  | FlexSym 'baz'
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
            DC             | Struct, Length = 11
            81             | SID 64
            6E             | true
            01 EE          | switch to FlexSym encoding
            F9 66 6F 6F    | FlexSym 'foo'
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
            D2     | Variable length Struct
            01     | FlexSym SID 0
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
            D8      | Variable length struct (length=8)
            03      | SID 1
            6E      | true
            01      | SID 0
            6E      | true
            05      | SID 2
            6E      | true
            01      | SID 0
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
    fun `calling writeFieldName outside of a struct should throw an exception`() {
        assertWriterThrows { writeFieldName(12) }
        assertWriterThrows { writeFieldName("foo") }
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
        val expectedBytes = "58 07 6E"
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
    fun `write multiple sid annotations`() {
        val expectedBytes = "58 07 58 09 58 02 04 6E"
        assertWriterOutputEquals(expectedBytes) {
            writeAnnotations(3)
            writeAnnotations(4)
            writeAnnotations(256)
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
        assertWriterOutputEquals("58 01 6E") {
            writeAnnotations(0)
            writeBool(true)
        }
    }

    @Test
    fun `write one inline annotation`() {
        val expectedBytes = "59 07 66 6F 6F 6F"
        assertWriterOutputEquals(expectedBytes) {
            writeAnnotations("foo")
            writeBool(false)
        }
    }

    @Test
    fun `write two inline annotations`() {
        val expectedBytes = "59 07 66 6F 6F 59 07 62 61 72 6F"
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
        val expectedBytes = "59 07 66 6F 6F 59 07 62 61 72 59 07 62 61 7A 6F"
        assertWriterOutputEquals(expectedBytes) {
            writeAnnotations("foo")
            writeAnnotations("bar")
            writeAnnotations("baz")
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
    fun `write two mixed sid and inline annotations`() {
        val expectedBytes = "58 15 59 07 66 6F 6F 6E"
        assertWriterOutputEquals(expectedBytes) {
            writeAnnotations(10)
            writeAnnotations("foo")
            writeBool(true)
        }
    }

    @Test
    fun `write annotations that are long enough to need a patch point`() {
        val opCode = "59"
        val length = "3E 02"
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
    fun `write int`() {
        assertWriterOutputEquals(
            """
            61 01
            61 0A
            62 C7 CF
            F5 13 D2 0A 1F EB 8C A9 54 AB 00
            """
        ) {
            writeInt(1)
            writeInt(BigInteger.TEN)
            writeInt(-12345)
            writeInt(BigInteger("12345678901234567890"))
        }
    }

    @Test
    fun `write float`() {
        assertWriterOutputEquals(
            """
            6A
            6A
            6C C3 F5 48 40
            6D 1F 85 EB 51 B8 1E 09 40
            """
        ) {
            writeFloat(0.0f)
            writeFloat(0.0)
            writeFloat(3.14f)
            writeFloat(3.14)
        }
    }

    @ParameterizedTest
    @CsvSource(
        "                                         0., 70",
        "                                        0e1, 71 03",
        "                                       0e63, 71 7F",
        "                                       0e64, 72 02 01",
        "                                       0e99, 72 8E 01",
        "                                        0.0, 71 FF",
        "                                       0.00, 71 FD",
        "                                      0.000, 71 FB",
        "                                      0e-64, 71 81",
        "                                      0e-65, 72 FE FE",
        "                                      0e-99, 72 76 FE",
        "                                        -0., 72 01 00",
        "                                       -0e1, 72 03 00",
        "                                       -0e3, 72 07 00",
        "                                      -0e63, 72 7F 00",
        "                                      -0e64, 73 02 01 00",
        "                                     -0e127, 73 FE 01 00",
        "                                     -0e199, 73 1E 03 00",
        "                                      -0e-1, 72 FF 00",
        "                                      -0e-2, 72 FD 00",
        "                                      -0e-3, 72 FB 00",
        "                                     -0e-64, 72 81 00",
        "                                     -0e-65, 73 FE FE 00",
        "                                    -0e-199, 73 E6 FC 00",
        "                                       0.01, 72 FD 01",
        "                                        0.1, 72 FF 01",
        "                                          1, 72 01 01",
        "                                        1e1, 72 03 01",
        "                                        1e2, 72 05 01",
        "                                       1e63, 72 7F 01",
        "                                       1e64, 73 02 01 01",
        "                                    1e65536, 74 04 00 08 01",
        "                                          2, 72 01 02",
        "                                          7, 72 01 07",
        "                                         14, 72 01 0E",
        "                                        1.0, 72 FF 0A",
        "                                       1.00, 72 FD 64",
        "                                       1.27, 72 FD 7F",
        "                                      3.142, 73 FB 46 0C",
        "                                    3.14159, 74 F7 2F CB 04",
        "                                   3.141593, 74 F5 D9 EF 2F",
        "                                3.141592653, 76 EF 4D E6 40 BB 00",
        "                              3.14159265359, 76 EB 4F F6 59 25 49",
        "                         3.1415926535897932, 78 E1 4C 43 76 65 9E 9C 6F",
        "                       3.141592653589793238, 79 DD D6 49 32 A2 DF 2D 99 2B",
        "                     3.14159265358979323846, 7A D9 C6 D7 A4 5B 5B EB D5 07 11",
        "                    3.141592653589793238462, 7B D7 BE 6D 70 94 91 31 5B 4E AA 00",
        "                 3.141592653589793238462643, 7C D1 B3 B0 2C D7 AB A0 39 14 42 99 02",
        "               3.14159265358979323846264343, 7D CD 17 06 75 0D 20 C3 82 E6 CF DD 03 01",
        "            3.14159265358979323846264338328, 7E C7 98 B7 1F 91 34 35 CA 6E 1C 74 1A F7 03",
        "         3.14159265358979323846264338327950, 7F C1 8E 29 E5 E3 56 D5 DF C5 10 8F 55 3F 7D 0F",
        "      3.14159265358979323846264338327950288, F6 21 BB D0 53 2A 37 6A 5B 59 F2 84 D9 36 66 3F 81 3C",
        " 3.1415926535897932384626433832795028841971, F6 25 B1 F3 E5 23 F6 6C F2 1C 99 4B 7C A8 71 57 5D B7 52 5C",
    )
    fun `write decimal`(decimalValue: String, expectedBytes: String) {
        assertWriterOutputEquals(expectedBytes) {
            writeDecimal(Decimal.valueOf(decimalValue))
        }
    }

    @Test
    fun `write timestamp`() {
        assertWriterOutputEquals(
            """
            87 35 46 AF 7C 55 47 70 2D
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
            50 01
            51 01
            52 01
            55 01
            57 01
            50 03
            51 1E 18
            A3 66 6F 6F
            """
        ) {
            writeSymbol(0)
            writeSymbol(1)
            writeSymbol(2)
            writeSymbol(5)
            writeSymbol(7)
            writeSymbol(8)
            writeSymbol(12345)
            writeSymbol("foo")
        }
        // Longer symbol text
        assertWriterOutputEquals(
            """
            F9                                   | Var-length Symbol text
            3F                                   | Length = 31
            69 6E 74 65 72 63 68 61 6E 67 65 61
            62 6C 65 20 62 69 6E 61 72 79 20 61
            6E 64 20 74 65 78 74
        """
        ) {
            writeSymbol("interchangeable binary and text")
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
        assertWriterOutputEquals(
            """
            F8                                   | Var-length String
            3F                                   | Length = 31
            69 6E 74 65 72 63 68 61 6E 67 65 61
            62 6C 65 20 62 69 6E 61 72 79 20 61
            6E 64 20 74 65 78 74
        """
        ) {
            writeString("interchangeable binary and text")
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
    @CsvSource(
        // one-byte macro address opcodes
        "               0, 00",
        "               1, 01",
        "              64, 40",
        "              71, 47",
        // Extended macro addresses
        "              72, 48 01",
        "              73, 49 01",
        "              79, 4F 01",
        "              80, 48 03",
        "              87, 4F 03",
        "              88, 48 05",
        "             319, 4F 3D",
        "             320, 48 3F",
        "            1095, 4F FF ",
        "            1096, 48 02 02",
        "            1211, 4B 3A 02",
        "            4159, 4F FA 07",
        "            4160, 48 FE 07",
        "            4161, 49 FE 07",
        "           69695, 4F FA 87",
        "           69696, 48 FE 87",
        "          131143, 4F FE FF",
        "          131144, 48 04 00 02",
        "         1052735, 4F F4 0F 10",
        "        16777287, 4F FC FF FF",
        "        16777288, 48 08 00 00 02",
        "${Int.MAX_VALUE}, 4F 68 FF FF FF"
    )
    fun `write an e-expression with no args`(id: Int, expectedBytes: String) {
        assertWriterOutputEquals(expectedBytes) {
            stepInEExp(id, usingLengthPrefix = false)
            stepOut()
        }
    }

    @ParameterizedTest
    @CsvSource(
        //       Macro Id; Op Address   Length=0
        "               0, F4 01             01",
        "              64, F4 81             01",
        "              65, F4 83             01",
        "             127, F4 FF             01",
        "             128, F4 02 02          01",
        "             729, F4 66 0B          01",
        "           16383, F4 FE FF          01",
        "           16384, F4 04 00 02       01",
        "         1052736, F4 04 82 80       01",
        "         2097151, F4 FC FF FF       01",
        "         2097152, F4 08 00 00 02    01",
        "${Int.MAX_VALUE}, F4 F0 FF FF FF 0F 01",
    )
    fun `write a length-prefixed e-expression with no args`(id: Int, expectedBytes: String) {
        // This test ensures that the macro address is written correctly
        assertWriterOutputEquals(expectedBytes) {
            stepInEExp(id, usingLengthPrefix = true)
            stepOut()
        }
    }

    @Test
    fun `write a length-prefixed e-expression with many args`() {
        // This test ensures that the macro length is written correctly
        assertWriterOutputEquals("F4 03 15 60 60 60 60 60 60 60 60 60 60") {
            stepInEExp(1, usingLengthPrefix = true)
            repeat(10) { writeInt(0L) }
            stepOut()
        }
    }

    @Test
    fun `write nested e-expressions`() {
        // E-Expressions don't have length prefixes, so we're putting them inside lists
        // so that we can check that the length gets propagated correctly to the parent
        assertWriterOutputEquals(
            """
            BA         | List Length 10
            1F         | Macro 31
            B8         | List Length 8
            40         | Macro 64
            B6         | List Length 6
            4B 03      | Macro 83
            B3         | List Length 3
            48 FE 07   | Macro 4160
            """
        ) {
            stepInList(usingLengthPrefix = true)
            stepInEExp(31, usingLengthPrefix = false)
            stepInList(usingLengthPrefix = true)
            stepInEExp(64, usingLengthPrefix = false)
            stepInList(usingLengthPrefix = true)
            stepInEExp(83, usingLengthPrefix = false)
            stepInList(usingLengthPrefix = true)
            stepInEExp(4160, usingLengthPrefix = false)
            repeat(8) { stepOut() }
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
            stepInEExp(1, usingLengthPrefix = false)
            stepOut()
            stepOut()
        }
    }

    @Test
    fun `write an e-expression with an absent arg`() {
        assertWriterOutputEquals("01 E0") {
            stepInEExp(1, usingLengthPrefix = false)
            writeAbsentArgument()
            stepOut()
        }
        // This test ensures that the "absent arg" length is accounted correctly
        assertWriterOutputEquals("F4 03 03 E0") {
            stepInEExp(1, usingLengthPrefix = true)
            writeAbsentArgument()
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

    @ParameterizedTest
    @CsvSource(
        // SID
        "    0, 01",
        "    4, 09",
        "  246, DA 03",
        // Text
        "    a, FD 61",
        "  abc, F9 61 62 63",
        "   '', FF",
    )
    fun `write a tagless symbol`(value: String, expectedBytes: String) {
        // If it's an int, write as SID, else write as text
        val writeTheValue: IonRawBinaryWriter_1_1.() -> Unit = value.toIntOrNull()
            ?.let { { writeTaglessSymbol(TaglessScalarType.SYMBOL.getOpcode(), it) } }
            ?: { writeTaglessSymbol(TaglessScalarType.SYMBOL.getOpcode(), value) }
        // Write the value as single expression
        assertWriterOutputEquals("02 $expectedBytes") {
            stepInEExp(0x02, usingLengthPrefix = false)
            writeTheValue()
            stepOut()
        }
    }

    @Test
    fun `write a tagless symbol in a length-prefixed e-expression`() {
        assertWriterOutputEquals(
            """
            F4           | Length prefixed e-expression
            05           | Macro Address 2
            0B           | Length = 5
            09           | FlexSym $4
            F9 61 62 63  | FlexSym 'abc'
        """
        ) {
            stepInEExp(0x02, usingLengthPrefix = true)
            writeTaglessSymbol(TaglessScalarType.SYMBOL.getOpcode(), 4)
            writeTaglessSymbol(TaglessScalarType.SYMBOL.getOpcode(), "abc")
            stepOut()
        }
    }

    @ParameterizedTest
    @ValueSource(
        ints = [
            OpCode.DIRECTIVE_SET_SYMBOLS,
            OpCode.DIRECTIVE_ADD_SYMBOLS,
            OpCode.DIRECTIVE_SET_MACROS,
            OpCode.DIRECTIVE_ADD_MACROS,
            OpCode.DIRECTIVE_USE,
            OpCode.DIRECTIVE_MODULE,
            OpCode.DIRECTIVE_IMPORT,
            OpCode.DIRECTIVE_ENCODING,
        ]
    )
    fun `write a directive`(directiveOpcode: Int) {
        val dir = directiveOpcode.toString(radix = 16)
        assertWriterOutputEquals("$dir 60 61 01 EF $dir EF") {
            stepInDirective(directiveOpcode)
            writeInt(0)
            writeInt(1)
            stepOut()
            stepInDirective(directiveOpcode)
            stepOut()
        }
    }

    @Test
    fun `write a tagged placeholder`() {
        assertWriterOutputEquals("E9") {
            writeTaggedPlaceholder()
        }
    }

    @Test
    fun `write a tagged placeholder with default value`() {
        assertWriterOutputEquals("EA 60") {
            writeTaggedPlaceholderWithDefault { it.writeInt(0) }
        }
        assertWriterOutputEquals("EA 58 01 60") {
            writeTaggedPlaceholderWithDefault {
                it.writeAnnotations(0)
                it.writeInt(0)
            }
        }
    }

    @ParameterizedTest
    @CsvSource(
        "INT,           EB 60",
        "INT_8,         EB 61",
        "INT_16,        EB 62",
        "INT_32,        EB 64",
        "INT_64,        EB 68",
        "UINT,          EB E0",
        "UINT_8,        EB E1",
        "UINT_16,       EB E2",
        "UINT_32,       EB E4",
        "UINT_64,       EB E8",
        "FLOAT_16,      EB 6B",
        "FLOAT_32,      EB 6C",
        "FLOAT_64,      EB 6D",
        "SMALL_DECIMAL, EB 70",
        "TIMESTAMP_DAY, EB 82",
        "TIMESTAMP_MIN, EB 83",
        "TIMESTAMP_S,   EB 84",
        "TIMESTAMP_MS,  EB 85",
        "TIMESTAMP_US,  EB 86",
        "TIMESTAMP_NS,  EB 87",
        "SYMBOL,        EB EE",
    )
    fun `write a tagless placeholder`(type: TaglessScalarType, expectedBytes: String) {
        assertWriterOutputEquals(expectedBytes) { writeTaglessPlaceholder(type.getOpcode()) }
    }

    @ParameterizedTest
    @CsvSource(
        // Value,    Type, Expected Bytes
        "      1,   INT_8, 01",
        "      2,  INT_16, 02 00",
        "      3,  INT_32, 03 00 00 00",
        "      4,  INT_64, 04 00 00 00 00 00 00 00",
        "      5,     INT, 0B",
        "     50,     INT, 65",
        "    500,     INT, D2 07",
        "   5000,     INT, 22 4E",
        "  50000,     INT, 84 1A 06",
        "      6,  UINT_8, 06",
        "      7, UINT_16, 07 00",
        "      8, UINT_32, 08 00 00 00",
        "      9, UINT_64, 09 00 00 00 00 00 00 00",
        "     10,    UINT, 15",
        "    100,    UINT, C9",
        "   1000,    UINT, A2 0F",
        "  10000,    UINT, 42 9C",
        " 100000,    UINT, 04 35 0C",
    )
    fun `write tagless integers`(value: BigInteger, encoding: TaglessScalarType, expectedBytes: String) {
        // This writes the ints completely out of context so that we can test them apart from a macro or container.
        assertWriterOutputEquals(expectedBytes) { writeTaglessInt(encoding.getOpcode(), value) }
        assertWriterOutputEquals(expectedBytes) { writeTaglessInt(encoding.getOpcode(), value.toLong()) }
    }

    @ParameterizedTest
    @CsvSource(
        // Value,                        Type,          Expected Bytes
        "2025-01-01T,                    TIMESTAMP_DAY, B7 08",
        "2025-01-01T01:02Z,              TIMESTAMP_MIN, B7 08 41 08",
        "2025-01-01T01:02:03Z,           TIMESTAMP_S,   B7 08 41 38 00",
        "2025-01-01T01:02:03.004Z,       TIMESTAMP_MS,  B7 08 41 38 10 00",
        "2025-01-01T01:02:03.004005Z,    TIMESTAMP_US,  B7 08 41 38 94 3E 00",
        "2025-01-01T01:02:03.004005006Z, TIMESTAMP_NS,  B7 08 41 38 38 72 F4 00",
    )
    fun `write tagless timestamps`(tsText: String, encoding: TaglessScalarType, expectedBytes: String) {
        assertWriterOutputEquals(expectedBytes) {
            writeTaglessTimestamp(encoding.getOpcode(), Timestamp.valueOf(tsText))
        }
    }

    @ParameterizedTest
    @CsvSource(
        // TODO: Implement writing Float 16
        // "1.0, 00 3C",
        // "2.0, 00 40",
        "3.0, 00 00 40 40",
        "4.0, 00 00 80 40",
        "5.0, 00 00 00 00 00 00 14 40",
        "6.0, 00 00 00 00 00 00 18 40",
    )
    fun `write tagless float`(value: Double, expectedBytes: String) {
        val opcode = when (expectedBytes.length) {
            5 -> OpCode.FLOAT_16
            11 -> OpCode.FLOAT_32
            23 -> OpCode.FLOAT_64
            else -> TODO("Unreachable: ${expectedBytes.length}")
        }
        assertWriterOutputEquals(expectedBytes) { writeTaglessFloat(opcode, value) }
        assertWriterOutputEquals(expectedBytes) { writeTaglessFloat(opcode, value.toFloat()) }
    }

    @ParameterizedTest
    @CsvSource(
        "   1.5, 1F FF",
        "  7.29, 66 0B FE",
        "1.2345, CC 81 01 FC",
    )
    fun `write tagless small decimal`(value: BigDecimal, expectedBytes: String) {
        assertWriterOutputEquals(expectedBytes) {
            writeTaglessDecimal(OpCode.TE_SMALL_DECIMAL, value)
        }
    }

    @Test
    fun `write tagless element list with scalar`() {
        assertWriterOutputEquals(
            """
            5B 62       | List<Int16>
            07          | Length = 3 (children)
            01 00
            02 00
            03 00
        """
        ) {
            val taglessOp = TaglessScalarType.INT_16.getOpcode()
            stepInTaglessElementList(taglessOp)
            writeTaglessInt(taglessOp, 1)
            writeTaglessInt(taglessOp, 2)
            writeTaglessInt(taglessOp, 3)
            stepOut()
        }
    }

    @Test
    fun `write tagless element list with macro-shape`() {
        assertWriterOutputEquals(
            """
            5B 01       | List, macro 1
            05          | Length = 2 (children)
            E0
            61 05
            E0
            61 06
        """
        ) {
            stepInTaglessElementList(1, "foo", false)
            stepInTaglessEExp()
            writeAbsentArgument()
            writeInt(5)
            stepOut()
            stepInTaglessEExp()
            writeAbsentArgument()
            writeInt(6)
            stepOut()
            stepOut()
        }
    }

    @Test
    fun `write tagless element list with length-prefixed macro-shape`() {
        assertWriterOutputEquals(
            """
            5B F4 03    | List, length-prefixed macro 1
            05          | Length = 2 (children)
            07
            E0
            61 05
            07
            E0
            61 06
        """
        ) {
            stepInTaglessElementList(1, "foo", true)
            stepInTaglessEExp()
            writeAbsentArgument()
            writeInt(5)
            stepOut()
            stepInTaglessEExp()
            writeAbsentArgument()
            writeInt(6)
            stepOut()
            stepOut()
        }
    }

    @Test
    fun `write empty tagless element list`() {
        assertWriterOutputEquals("B0 B0") {
            val taglessOp = TaglessScalarType.INT_16.getOpcode()
            stepInTaglessElementList(taglessOp)
            stepOut()
            stepInTaglessElementList(1, "foo", false)
            stepOut()
        }
    }

    @Test
    fun `write tagless element sexp with scalar`() {
        assertWriterOutputEquals(
            """
            5C 62       | Sexp<Int16>
            07          | Length = 3 (children)
            01 00
            02 00
            03 00
        """
        ) {
            val taglessOp = TaglessScalarType.INT_16.getOpcode()
            stepInTaglessElementSExp(taglessOp)
            writeTaglessInt(taglessOp, 1)
            writeTaglessInt(taglessOp, 2)
            writeTaglessInt(taglessOp, 3)
            stepOut()
        }
    }

    @Test
    fun `write tagless element sexp with macro-shape`() {
        assertWriterOutputEquals(
            """
            5C 01       | Sexp, macro 1
            05          | Length = 2 (children)
            E0
            61 05
            E0
            61 06
        """
        ) {
            stepInTaglessElementSExp(1, "foo", false)
            stepInTaglessEExp()
            writeAbsentArgument()
            writeInt(5)
            stepOut()
            stepInTaglessEExp()
            writeAbsentArgument()
            writeInt(6)
            stepOut()
            stepOut()
        }
    }

    @Test
    fun `write tagless element sexp with length-prefixed macro-shape`() {
        assertWriterOutputEquals(
            """
            5C F4 03    | Sexp, length-prefixed macro 1
            05          | Length = 2 (children)
            07
            E0
            61 05
            07
            E0
            61 06
        """
        ) {
            stepInTaglessElementSExp(1, "foo", true)
            stepInTaglessEExp()
            writeAbsentArgument()
            writeInt(5)
            stepOut()
            stepInTaglessEExp()
            writeAbsentArgument()
            writeInt(6)
            stepOut()
            stepOut()
        }
    }

    @Test
    fun `write empty tagless element sexp`() {
        assertWriterOutputEquals("C0 C0") {
            val taglessOp = TaglessScalarType.INT_16.getOpcode()
            stepInTaglessElementSExp(taglessOp)
            stepOut()
            stepInTaglessElementSExp(1, "foo", false)
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
            E1                          | (:$ ion set_symbols
            94 6E 61 6D 65              |   "name",
            93 61 67 65                 |   "age",
            95 79 65 61 72 73           |   "years",
            98 62 69 72 74 68 64 61 79  |   "birthday",
            94 74 6F 79 73              |   "toys",
            94 62 61 6C 6C              |   "ball",
            96 77 65 69 67 68 74        |   "weight",
            94 62 75 7A 7A              |   "buzz",
            EF                          | )
            FC 85                       | {                 // length=66
            15 94 46 69 64 6F           |   $10: "Fido",
            17 58 19 61 04              |   $11: $12::4,
            1B 82 AA 09                 |   $13: 2012-03-01T
            1D B7                       |   $14: [          // length=7
            57 03                       |     $15,
            A4 72 6F 70 65              |     rope
                                        |   ],
            21                          |   $16:
            59 0D 70 6F 75 6E 64 73     |       pounds::
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
            stepInDirective(OpCode.DIRECTIVE_SET_SYMBOLS)
            writeString("name")
            writeString("age")
            writeString("years")
            writeString("birthday")
            writeString("toys")
            writeString("ball")
            writeString("weight")
            writeString("buzz")
            stepOut()
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
}
