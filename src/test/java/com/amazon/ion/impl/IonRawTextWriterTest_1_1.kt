// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl

import com.amazon.ion.Decimal
import com.amazon.ion.IonException
import com.amazon.ion.IonType
import com.amazon.ion.Timestamp
import com.amazon.ion.bytecode.bin11.OpCode
import com.amazon.ion.ion_1_1.IonRawWriter_1_1
import com.amazon.ion.ion_1_1.TaglessScalarType
import com.amazon.ion.system.IonTextWriterBuilder
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.EnumSource
import java.math.BigDecimal
import java.math.BigInteger

class IonRawTextWriterTest_1_1 {

    private fun IonRawTextWriter_1_1.stepInEExp(id: Int) = stepInEExp(id, false)

    private fun standardBuilder(): _Private_IonTextWriterBuilder {
        return _Private_IonTextWriterBuilder.standard()
    }

    private inline fun ionWriter(
        out: StringBuilder = StringBuilder(),
        builderConfigurator: IonTextWriterBuilder.() -> Unit = { /* noop */ },
        block: IonRawTextWriter_1_1.() -> Unit = {},
    ): IonRawTextWriter_1_1 {
        val b = standardBuilder()
            .withMaximumTimestampPrecisionDigits(20)
            .apply(builderConfigurator)
            // Always use LF because the tests' expected data uses LF.
            .withNewLineType(IonTextWriterBuilder.NewLineType.LF)

        val rawWriter = IonRawTextWriter_1_1(
            options = b as _Private_IonTextWriterBuilder,
            output = _Private_IonTextAppender.forAppendable(out)
        )
        block.invoke(rawWriter)
        return rawWriter
    }

    private inline fun writeAsString(
        builderConfigurator: IonTextWriterBuilder.() -> Unit = { /* noop */ },
        autoClose: Boolean = true,
        block: IonRawTextWriter_1_1.() -> Unit,
    ): String {
        val out = StringBuilder()
        val rawWriter = ionWriter(out, builderConfigurator, block)
        if (autoClose) rawWriter.close()
        return out.toString()
    }

    private inline fun assertWriterOutputEquals(
        text: String,
        builderConfigurator: IonTextWriterBuilder.() -> Unit = { /* noop */ },
        autoClose: Boolean = true,
        block: IonRawTextWriter_1_1.() -> Unit,
    ) {
        // Trim whitespace since the IonRawTextWriter_1_1 eagerly writes top-level separators.
        assertEquals(text, writeAsString(builderConfigurator, autoClose, block).trim())
    }

    @Test
    fun `calling close while in a container should throw IonException`() {
        ionWriter {
            stepInList(false)
            assertThrows<IonException> { close() }
        }
    }

    @Test
    fun `calling finish while in a container should throw IonException`() {
        ionWriter {
            stepInList(true)
            assertThrows<IonException> { flush() }
        }
    }

    @Test
    fun `calling finish with a dangling annotation should throw IonException`() {
        ionWriter {
            writeAnnotations(10)
            assertThrows<IonException> { flush() }
        }
    }

    @Test
    fun `calling stepOut while not in a container should throw IonException`() {
        ionWriter {
            assertThrows<IonException> { stepOut() }
        }
    }

    @Test
    fun `calling stepOut with a dangling annotation should throw IonException`() {
        ionWriter {
            stepInList(true)
            writeAnnotations(10)
            assertThrows<IonException> { stepOut() }
        }
    }

    @Test
    fun `calling writeIVM when in a container should throw IonException`() {
        ionWriter {
            stepInList(false)
            assertThrows<IonException> { writeIVM() }
        }
    }

    @Test
    fun `calling writeIVM with a dangling annotation should throw IonException`() {
        ionWriter {
            writeAnnotations(10)
            assertThrows<IonException> { writeIVM() }
        }
    }

    @Test
    fun `calling finish should cause the buffered data to be written to the output stream`() {
        val actual = writeAsString(autoClose = false) {
            writeIVM()
            flush()
        }
        // Just checking that data is written, not asserting the content.
        assertTrue(actual.isNotBlank())
    }

    @Test
    fun `after calling finish, it should still be possible to write more data`() {
        val actual = writeAsString {
            flush()
            writeIVM()
        }
        // Just checking that data is written, not asserting the content.
        assertTrue(actual.isNotBlank())
    }

    @Test
    fun `calling close should cause the buffered data to be written to the output stream`() {
        val actual = writeAsString(autoClose = false) {
            writeIVM()
            close()
        }
        // Just checking that data is written, not asserting the content.
        assertTrue(actual.isNotBlank())
    }

    @Test
    fun `calling close or finish multiple times should not throw any exceptions`() {
        val actual = writeAsString {
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
        assertWriterOutputEquals("\$ion_1_1") {
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
        assertWriterOutputEquals("null") {
            writeNull()
        }
        assertWriterOutputEquals("null") {
            writeNull(IonType.NULL)
        }
    }

    @ParameterizedTest
    @EnumSource(IonType::class, mode = EnumSource.Mode.EXCLUDE, names = ["DATAGRAM", "NULL"])
    fun `write a null with a specific type`(type: IonType) {
        assertWriterOutputEquals("null.${type.name.lowercase()}") {
            writeNull(type)
        }
    }

    @ParameterizedTest
    @CsvSource("true, true", "false, false")
    fun `write a boolean`(value: Boolean, expected: String) {
        assertWriterOutputEquals(expected) {
            writeBool(value)
        }
    }

    @Test
    fun `write a delimited list`() {
        assertWriterOutputEquals("[true,false]") {
            stepInList(true)
            writeBool(true)
            writeBool(false)
            stepOut()
        }
    }

    @Test
    fun `write a prefixed list`() {
        assertWriterOutputEquals("[true,false]") {
            stepInList(false)
            writeBool(true)
            writeBool(false)
            stepOut()
        }
    }

    @Test
    fun `write multiple nested prefixed lists`() {
        assertWriterOutputEquals("[[[[[]]]]]") {
            repeat(5) { stepInList(false) }
            repeat(5) { stepOut() }
        }
    }

    @Test
    fun `write multiple nested delimited lists`() {
        assertWriterOutputEquals("[[[[]]]]") {
            repeat(4) { stepInList(true) }
            repeat(4) { stepOut() }
        }
    }

    @Test
    fun `write multiple nested delimited and prefixed lists`() {
        assertWriterOutputEquals("[[[[[[[[]]]]]]]]") {
            repeat(4) {
                stepInList(true)
                stepInList(false)
            }
            repeat(8) { stepOut() }
        }
    }

    @Test
    fun `write a sexp`() {
        assertWriterOutputEquals("(true false)") {
            stepInSExp(usingLengthPrefix = false)
            writeBool(true)
            writeBool(false)
            stepOut()
        }
        assertWriterOutputEquals("(true false)") {
            stepInSExp(usingLengthPrefix = true)
            writeBool(true)
            writeBool(false)
            stepOut()
        }
    }

    @Test
    fun `write multiple nested sexps`() {
        assertWriterOutputEquals("(((((((())))))))") {
            repeat(4) {
                stepInSExp(usingLengthPrefix = false)
                stepInSExp(usingLengthPrefix = true)
            }
            repeat(8) { stepOut() }
        }
    }

    @Test
    fun `write a struct`() {
        assertWriterOutputEquals(
            """{$11:true,$12:false}"""
        ) {
            stepInStruct(usingLengthPrefix = true)
            assertTrue(isInStruct())
            writeFieldName(11)
            writeBool(true)
            writeFieldName(12)
            writeBool(false)
            stepOut()
            assertFalse(isInStruct())
        }
        assertWriterOutputEquals(
            """{$11:true,$12:false}"""
        ) {
            stepInStruct(usingLengthPrefix = false)
            writeFieldName(11)
            writeBool(true)
            writeFieldName(12)
            writeBool(false)
            stepOut()
        }
    }

    @Test
    fun `write multiple nested structs`() {
        assertWriterOutputEquals(
            "{a:{b:{a:{b:{a:{b:{a:{b:{}}}}}}}}}"
        ) {
            stepInStruct(usingLengthPrefix = true)
            repeat(4) {
                writeFieldName("a")
                stepInStruct(usingLengthPrefix = false)
                writeFieldName("b")
                stepInStruct(usingLengthPrefix = true)
            }
            repeat(9) {
                stepOut()
            }
        }
    }

    @Test
    fun `write empty struct`() {
        assertWriterOutputEquals("{}") {
            stepInStruct(usingLengthPrefix = true)
            stepOut()
        }
        assertWriterOutputEquals("{}") {
            stepInStruct(usingLengthPrefix = false)
            stepOut()
        }
    }

    @Test
    fun `write prefixed struct with a single text field name`() {
        assertWriterOutputEquals(
            """{foo:true}"""
        ) {
            stepInStruct(false)
            writeFieldName("foo")
            writeBool(true)
            stepOut()
        }
    }

    @Test
    fun `write a struct with sid 0`() {
        assertWriterOutputEquals(
            "{\$0:true}"
        ) {
            stepInStruct(usingLengthPrefix = true)
            writeFieldName(0)
            writeBool(true)
            stepOut()
        }
        assertWriterOutputEquals(
            "{\$0:true}"
        ) {
            stepInStruct(usingLengthPrefix = false)
            writeFieldName(0)
            writeBool(true)
            stepOut()
        }
    }

    @Test
    fun `writing a value in a struct with no field name should throw an exception`() {
        ionWriter {
            stepInStruct(true)
            assertThrows<IonException> { writeBool(true) }
        }
        ionWriter {
            stepInStruct(false)
            assertThrows<IonException> { writeBool(true) }
        }
    }

    @Test
    fun `calling writeFieldName outside of a struct should throw an exception`() {
        ionWriter {
            assertThrows<IonException> { writeFieldName(12) }
        }
        ionWriter {
            assertThrows<IonException> { writeFieldName("foo") }
        }
    }

    @Test
    fun `calling stepOut with a dangling field name should throw an exception`() {
        ionWriter {
            stepInStruct(false)
            writeFieldName(12)
            assertThrows<IonException> { stepOut() }
        }
        ionWriter {
            stepInStruct(true)
            writeFieldName("foo")
            assertThrows<IonException> { stepOut() }
        }
    }

    @Test
    fun `writeAnnotations with empty int array should write no annotations`() {
        assertWriterOutputEquals("true") {
            writeAnnotations(intArrayOf())
            writeBool(true)
        }
    }

    @Test
    fun `write one sid annotation`() {
        val expectedText = "\$3::true"
        assertWriterOutputEquals(expectedText) {
            writeAnnotations(3)
            writeBool(true)
        }
        assertWriterOutputEquals(expectedText) {
            writeAnnotations(3)
            writeAnnotations(intArrayOf())
            writeAnnotations(arrayOf<CharSequence>())
            writeBool(true)
        }
    }

    @Test
    fun `write two sid annotations`() {
        val expectedText = "\$3::\$4::true"
        assertWriterOutputEquals(expectedText) {
            writeAnnotations(3)
            writeAnnotations(4)
            writeBool(true)
        }
        assertWriterOutputEquals(expectedText) {
            writeAnnotations(3, 4)
            writeBool(true)
        }
        assertWriterOutputEquals(expectedText) {
            writeAnnotations(intArrayOf(3, 4))
            writeBool(true)
        }
    }

    @Test
    fun `write three sid annotations`() {
        val expectedText = "\$3::\$4::\$256::true"
        assertWriterOutputEquals(expectedText) {
            writeAnnotations(3)
            writeAnnotations(4)
            writeAnnotations(256)
            writeBool(true)
        }
        assertWriterOutputEquals(expectedText) {
            writeAnnotations(3)
            writeAnnotations(4, 256)
            writeBool(true)
        }
        assertWriterOutputEquals(expectedText) {
            writeAnnotations(intArrayOf(3, 4))
            writeAnnotations(256)
            writeBool(true)
        }
        assertWriterOutputEquals(expectedText) {
            writeAnnotations(intArrayOf(3, 4, 256))
            writeBool(true)
        }
    }

    @Test
    fun `write sid 0 annotation`() {
        assertWriterOutputEquals("\$0::true") {
            writeAnnotations(0)
            writeBool(true)
        }
    }

    @Test
    fun `write one text annotation`() {
        val expectedText = "foo::false"
        assertWriterOutputEquals(expectedText) {
            writeAnnotations("foo")
            writeBool(false)
        }
        assertWriterOutputEquals(expectedText) {
            writeAnnotations("foo")
            writeAnnotations(intArrayOf())
            writeBool(false)
        }
    }

    @Test
    fun `write two text annotations`() {
        val expectedText = "foo::bar::false"
        assertWriterOutputEquals(expectedText) {
            writeAnnotations("foo")
            writeAnnotations("bar")
            writeBool(false)
        }
        assertWriterOutputEquals(expectedText) {
            writeAnnotations(arrayOf("foo", "bar"))
            writeBool(false)
        }
    }

    @Test
    fun `write three text annotations`() {
        val expectedText = "foo::bar::baz::false"
        assertWriterOutputEquals(expectedText) {
            writeAnnotations("foo")
            writeAnnotations("bar")
            writeAnnotations("baz")
            writeBool(false)
        }
        assertWriterOutputEquals(expectedText) {
            writeAnnotations("foo")
            writeAnnotations("bar", "baz")
            writeBool(false)
        }
        assertWriterOutputEquals(expectedText) {
            writeAnnotations(arrayOf("foo", "bar"))
            writeAnnotations("baz")
            writeBool(false)
        }
        assertWriterOutputEquals(expectedText) {
            writeAnnotations(arrayOf("foo", "bar", "baz"))
            writeBool(false)
        }
    }

    @Test
    fun `write empty text and sid 0 annotations`() {
        assertWriterOutputEquals("\$0::''::true") {
            writeAnnotations(0)
            writeAnnotations("")
            writeBool(true)
        }
    }

    @Test
    fun `write two mixed sid and text annotations`() {
        val expectedText = "\$10::foo::false"
        assertWriterOutputEquals(expectedText) {
            writeAnnotations(10)
            writeAnnotations("foo")
            writeBool(false)
        }
    }

    @Test
    fun `write three mixed sid and inline annotations`() {
        val expectedText = "\$10::foo::bar::false"
        assertWriterOutputEquals(expectedText) {
            writeAnnotations(10)
            writeAnnotations("foo")
            writeAnnotations("bar")
            writeBool(false)
        }
        assertWriterOutputEquals(expectedText) {
            writeAnnotations(10)
            writeAnnotations(arrayOf("foo", "bar"))
            writeBool(false)
        }
    }

    @Test
    fun `write many annotations`() {
        val expectedText = "$0::$1::$2::$3::$4::$5::$6::$7::$8::$9::$10::$11::true"
        assertWriterOutputEquals(expectedText) {
            repeat(12) { writeAnnotations(it) }
            writeBool(true)
        }
    }

    @Test
    fun `write int`() {
        assertWriterOutputEquals(
            """1 10"""
        ) {
            writeInt(1)
            writeInt(BigInteger.TEN)
        }
    }

    @Test
    fun `write float`() {
        assertWriterOutputEquals(
            """0e0 3.140000104904175e0 3.14e0"""
        ) {
            writeFloat(0.0)
            writeFloat(3.14f)
            writeFloat(3.14)
        }
    }

    @Test
    fun `write decimal`() {
        assertWriterOutputEquals(
            """0. -0."""
        ) {
            writeDecimal(BigDecimal.ZERO)
            writeDecimal(Decimal.NEGATIVE_ZERO)
        }
    }

    @Test
    fun `write timestamp`() {
        assertWriterOutputEquals(
            """2000-01-02 2023-12-08T15:37:23.190583253Z 2123T"""
        ) {
            writeTimestamp(Timestamp.valueOf("2000-01-02"))
            writeTimestamp(Timestamp.valueOf("2023-12-08T15:37:23.190583253Z"))
            writeTimestamp(Timestamp.valueOf("2123T"))
        }
    }

    @Test
    fun `write symbol`() {
        assertWriterOutputEquals(
            "\$0 \$1 \$12345 foo 'null' 'null.int' 'bat\\'leth' '$99' 'true' 'false' 'nan' \$ion_1_1 '+' '==' '.'"
        ) {
            writeSymbol(0)
            writeSymbol(1)
            writeSymbol(12345)
            writeSymbol("foo")
            writeSymbol("null")
            writeSymbol("null.int")
            writeSymbol("bat'leth")
            writeSymbol("$99")
            writeSymbol("true")
            writeSymbol("false")
            writeSymbol("nan")
            writeSymbol("\$ion_1_1")
            writeSymbol("+")
            writeSymbol("==")
            writeSymbol(".")
        }
    }

    @Test
    fun `write symbols in a sexp`() {
        assertWriterOutputEquals(
            "(\$0 \$1 \$12345 foo 'null' 'null.int' 'bat\\'leth' '$99' 'true' 'false' 'nan' \$ion_1_1 + == .)"
        ) {
            writeSexp {
                writeSymbol(0)
                writeSymbol(1)
                writeSymbol(12345)
                writeSymbol("foo")
                writeSymbol("null")
                writeSymbol("null.int")
                writeSymbol("bat'leth")
                writeSymbol("$99")
                writeSymbol("true")
                writeSymbol("false")
                writeSymbol("nan")
                writeSymbol("\$ion_1_1")
                writeSymbol("+")
                writeSymbol("==")
                writeSymbol(".")
            }
        }
    }

    @Test
    fun `write string`() {
        assertWriterOutputEquals("\"foo\"") {
            writeString("foo")
        }
    }

    @Test
    fun `write blob`() {
        assertWriterOutputEquals("{{AQID}}") {
            writeBlob(byteArrayOf(1, 2, 3), 0, 3)
        }
    }

    @Test
    fun `write clob`() {
        assertWriterOutputEquals("{{\"abc\"}}") {
            writeClob(byteArrayOf(0x61, 0x62, 0x63), 0, 3)
        }
    }

    @Test
    fun `write E-expression by name`() {
        assertWriterOutputEquals("(:foo)") {
            stepInEExp("foo")
            stepOut()
        }
        assertWriterOutputEquals("(:'1A')") {
            stepInEExp("1A")
            stepOut()
        }
    }

    @Test
    fun `write E-expression by id`() {
        assertWriterOutputEquals("(:1)") {
            stepInEExp(1)
            stepOut()
        }
    }

    @Test
    fun `write E-Expression with one arg`() {
        assertWriterOutputEquals("(:foo true)") {
            stepInEExp("foo")
            writeBool(true)
            stepOut()
        }
    }

    @Test
    fun `write an absent argument`() {
        assertWriterOutputEquals("(:foo (:))") {
            writeEExp("foo") {
                writeAbsentArgument()
            }
        }
    }

    @ParameterizedTest
    @CsvSource(
        "0xE1, (:\$ion set_symbols)",
        "0xE2, (:\$ion add_symbols)",
        "0xE3, (:\$ion set_macros)",
        "0xE4, (:\$ion add_macros)",
        "0xE5, (:\$ion use)",
        "0xE6, (:\$ion module)",
        "0xE7, (:\$ion import)",
        "0xE8, (:\$ion encoding)",
    )
    fun `write a directive`(directiveOpcode: Int, expectedOutput: String) {
        assertWriterOutputEquals(expectedOutput) {
            stepInDirective(directiveOpcode)
            stepOut()
        }
        assertWriterOutputEquals(expectedOutput.dropLast(1) + " 42)") {
            stepInDirective(directiveOpcode)
            writeInt(42)
            stepOut()
        }
    }

    @Test
    fun `stepInDirective when not a directive should throw IonException`() {
        assertWriterOutputEquals("") {
            assertThrows<IonException> {
                stepInDirective(OpCode.BOOL_TRUE)
            }
        }
    }

    @Test
    fun `write a placeholder`() {
        assertWriterOutputEquals("(:?)") {
            writeTaggedPlaceholder()
        }
    }

    @Test
    fun `write a placeholder with default value`() {
        assertWriterOutputEquals("(:? 42)") {
            writeTaggedPlaceholderWithDefault({ it.writeInt(42) })
        }
    }

    @ParameterizedTest
    @EnumSource(TaglessScalarType::class)
    fun `write a tagless placeholder`(taglessScalarType: TaglessScalarType) {
        assertWriterOutputEquals("(:? {#${taglessScalarType.textEncodingName}})") {
            writeTaglessPlaceholder(taglessScalarType.getOpcode())
        }
    }

    @ParameterizedTest
    @EnumSource(TaglessScalarType::class, mode = EnumSource.Mode.MATCH_ALL, names = ["U?INT.*"])
    fun `write a tagless element list with integers`(elementType: TaglessScalarType) {
        assertWriterOutputEquals("[{#${elementType.textEncodingName}} 1,2,3]") {
            stepInTaglessElementList(elementType.getOpcode())
            writeTaglessInt(elementType.getOpcode(), 1)
            writeTaglessInt(elementType.getOpcode(), 2L)
            writeTaglessInt(elementType.getOpcode(), 3.toBigInteger())
            stepOut()
        }
    }

    @Test
    fun `write a tagless element list with macro-shape`() {
        assertWriterOutputEquals("[{:foo} (1 2),(3 4)]") {
            stepInTaglessElementList(1, "foo")
            stepInTaglessEExp()
            writeInt(1)
            writeInt(2)
            stepOut()
            stepInTaglessEExp()
            writeInt(3)
            writeInt(4)
            stepOut()
            stepOut()
        }
    }

    @ParameterizedTest
    @EnumSource(TaglessScalarType::class, mode = EnumSource.Mode.MATCH_ALL, names = ["FLOAT.*"])
    fun `write a tagless element sexp with floats`(elementType: TaglessScalarType) {
        assertWriterOutputEquals("({#${elementType.textEncodingName}} 1e0 2e0)") {
            stepInTaglessElementSExp(elementType.getOpcode())
            writeTaglessFloat(elementType.getOpcode(), 1.0f)
            writeTaglessFloat(elementType.getOpcode(), 2.0)
            stepOut()
        }
    }

    @Test
    fun `write a tagless element sexp with macro-shape`() {
        assertWriterOutputEquals("({:123} (1 2) (3 4))") {
            stepInTaglessElementSExp(123, null)
            stepInTaglessEExp()
            writeInt(1)
            writeInt(2)
            stepOut()
            stepInTaglessEExp()
            writeInt(3)
            writeInt(4)
            stepOut()
            stepOut()
        }
    }

    @Test
    fun `write tagless macro arguments`() {
        assertWriterOutputEquals("(:foo 1. 2025-01-02 $4 version)") {
            stepInEExp("foo")
            writeTaglessDecimal(TaglessScalarType.SMALL_DECIMAL.getOpcode(), Decimal.ONE)
            writeTaglessTimestamp(TaglessScalarType.TIMESTAMP_DAY.getOpcode(), Timestamp.valueOf("2025-01-02T"))
            writeTaglessSymbol(TaglessScalarType.SYMBOL.getOpcode(), 4)
            writeTaglessSymbol(TaglessScalarType.SYMBOL.getOpcode(), "version")
            stepOut()
        }
    }

    @Test
    fun `write something complex`() {
        assertWriterOutputEquals(
            """${'$'}ion_1_1 {name:"Fido",age:years::4,birthday:2012-03-01,toys:[ball,rope],weight:pounds::41.2,buzz:{{VG8gaW5maW5pdHkuLi4gYW5kIGJleW9uZCE=}}}"""
        ) {
            writeIVM()
            writeStruct {
                writeFieldName("name")
                writeString("Fido")
                writeFieldName("age")
                writeAnnotations("years")
                writeInt(4)
                writeFieldName("birthday")
                writeTimestamp(Timestamp.valueOf("2012-03-01T"))
                writeFieldName("toys")
                writeList {
                    writeSymbol("ball")
                    writeSymbol("rope")
                }
                writeFieldName("weight")
                writeAnnotations("pounds")
                writeDecimal(BigDecimal.valueOf(41.2))
                writeFieldName("buzz")
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

    @Test
    fun `write something complex and pretty`() {
        val expected = """
            ${'$'}ion_1_1
            {
              name: "Fido",
              age: years::4,
              birthday: 2012-03-01,
              toys: [
                ball,
                rope
              ],
              weight: pounds::41.2,
              buzz: {{ VG8gaW5maW5pdHkuLi4gYW5kIGJleW9uZCE= }}
            }
        """.trimIndent()
        assertWriterOutputEquals(
            text = expected,
            builderConfigurator = { withPrettyPrinting() }
        ) {
            writeIVM()
            writeStruct {
                writeFieldName("name")
                writeString("Fido")
                writeFieldName("age")
                writeAnnotations("years")
                writeInt(4)
                writeFieldName("birthday")
                writeTimestamp(Timestamp.valueOf("2012-03-01T"))
                writeFieldName("toys")
                writeList {
                    writeSymbol("ball")
                    writeSymbol("rope")
                }
                writeFieldName("weight")
                writeAnnotations("pounds")
                writeDecimal(BigDecimal.valueOf(41.2))
                writeFieldName("buzz")
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

    @Test
    fun `write something complex and compact`() {
        val expected = """
            ${'$'}ion_1_1
            {name:"Fido",age:years::4,birthday:2012-03-01,toys:[ball,rope],weight:pounds::41.2,buzz:{{VG8gaW5maW5pdHkuLi4gYW5kIGJleW9uZCE=}}}
            {name:"Rufus",age:years::5,birthday:2012-03-02,toys:[textbook],weight:pounds::98.5}
        """.trimIndent()
        assertWriterOutputEquals(
            text = expected,
            builderConfigurator = { withWriteTopLevelValuesOnNewLines(true) }
        ) {
            writeIVM()
            writeStruct {
                writeFieldName("name")
                writeString("Fido")
                writeFieldName("age")
                writeAnnotations("years")
                writeInt(4)
                writeFieldName("birthday")
                writeTimestamp(Timestamp.valueOf("2012-03-01T"))
                writeFieldName("toys")
                writeList {
                    writeSymbol("ball")
                    writeSymbol("rope")
                }
                writeFieldName("weight")
                writeAnnotations("pounds")
                writeDecimal(BigDecimal.valueOf(41.2))
                writeFieldName("buzz")
                writeBlob(
                    byteArrayOf(
                        84, 111, 32, 105, 110, 102, 105, 110, 105,
                        116, 121, 46, 46, 46, 32, 97, 110, 100,
                        32, 98, 101, 121, 111, 110, 100, 33
                    )
                )
            }
            writeStruct {
                writeFieldName("name")
                writeString("Rufus")
                writeFieldName("age")
                writeAnnotations("years")
                writeInt(5)
                writeFieldName("birthday")
                writeTimestamp(Timestamp.valueOf("2012-03-02T"))
                writeFieldName("toys")
                writeList {
                    writeSymbol("textbook")
                }
                writeFieldName("weight")
                writeAnnotations("pounds")
                writeDecimal(BigDecimal.valueOf(98.5))
            }
        }
    }

    @Test
    fun `write something pretty with a macro`() {
        val expected = """
            ${'$'}ion_1_1
            {
              name: (:foo
                "F"
                "ido"
              )
            }
        """.trimIndent()
        assertWriterOutputEquals(
            text = expected,
            builderConfigurator = { withPrettyPrinting() }
        ) {
            writeIVM()
            writeStruct {
                writeFieldName("name")
                stepInEExp("foo")
                writeString("F")
                writeString("ido")
                stepOut()
            }
        }
    }

    @Test
    fun `when pretty printing, empty containers should be on one line`() {
        val expected = """
            ${'$'}ion_1_1
            {
              a: {}
            }
            [
              []
            ]
            (
              ()
            )
            (:foo
              (:foo)
            )
            (:1
              (:1)
            )
        """.trimIndent()
        assertWriterOutputEquals(
            text = expected,
            builderConfigurator = { withPrettyPrinting() }
        ) {
            writeIVM()
            writeStruct {
                writeFieldName("a")
                stepInStruct(false); stepOut()
            }
            writeList { writeList { } }
            writeSexp { writeSexp { } }
            writeEExp("foo") { writeEExp("foo") { } }
            writeEExp(1) { writeEExp(1) { } }
        }
    }

    /*
     * Helper functions that steps into a container, applies the contents of [block] to
     * the writer, and then steps out of that container.
     * Using these functions makes it easy for the indentation of the writer code to
     * match the indentation of the equivalent pretty-printed Ion.
     */

    private inline fun IonRawWriter_1_1.writeStruct(block: IonRawWriter_1_1.() -> Unit) {
        stepInStruct(true)
        block()
        stepOut()
    }

    private inline fun IonRawWriter_1_1.writeList(block: IonRawWriter_1_1.() -> Unit) {
        stepInList(true)
        block()
        stepOut()
    }

    private inline fun IonRawWriter_1_1.writeSexp(block: IonRawWriter_1_1.() -> Unit) {
        stepInSExp(true)
        block()
        stepOut()
    }

    private inline fun IonRawWriter_1_1.writeEExp(name: String, block: IonRawWriter_1_1.() -> Unit) {
        stepInEExp(name)
        block()
        stepOut()
    }

    private inline fun IonRawTextWriter_1_1.writeEExp(id: Int, block: IonRawWriter_1_1.() -> Unit) {
        stepInEExp(id)
        block()
        stepOut()
    }
}
