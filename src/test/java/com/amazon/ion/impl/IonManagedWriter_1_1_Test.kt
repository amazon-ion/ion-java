// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl

import com.amazon.ion.Decimal
import com.amazon.ion.IonException
import com.amazon.ion.IonType
import com.amazon.ion.IonWriter
import com.amazon.ion.SystemSymbols
import com.amazon.ion.TextToBinaryUtils.cleanCommentedHexBytes
import com.amazon.ion.TextToBinaryUtils.hexStringToByteArray
import com.amazon.ion.Timestamp
import com.amazon.ion.impl.bin.IonRawBinaryWriter_1_1
import com.amazon.ion.ion_1_1.IonWriter_1_1
import com.amazon.ion.ion_1_1.MacroBuilder
import com.amazon.ion.ion_1_1.MacroImpl
import com.amazon.ion.ion_1_1.TaglessScalarType
import com.amazon.ion.system.IonBinaryWriterBuilder
import com.amazon.ion.system.IonTextWriterBuilder
import com.amazon.ion.system.IonTextWriterBuilder.NewLineType
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.Arguments.arguments
import org.junit.jupiter.params.provider.MethodSource
import java.io.ByteArrayOutputStream
import java.math.BigInteger

/**
 * TODO: Ensure that tests cover all of [IonManagedWriter_1_1].
 *
 * Missing test coverage includes, but not limited to:
 *   - type validation for tagless macro arguments
 *   - type validation for TE sequences with scalars
 *   - type validation for TE sequences with macros
 *   - Field names and annotations when they are/aren't allowed
 *   - Absent argument cases, including filling in missing arguments
 *   - Clobs, tagless floats, TE SExps, IVMs
 *   - Ion 1.0 symbol tables (including `$ion_symbol_table::null.struct`)
 *   - Symbols, with and without text, with and without SIDs
 */
internal class IonManagedWriter_1_1_Test {

    internal fun IonWriter.writeObject(obj: WriteAsIon) {
        if (this is IonWriter_1_1) {
            obj.writeToMacroAware(this)
        } else {
            obj.writeTo(this)
        }
    }

    @OptIn(ExperimentalStdlibApi::class)
    fun ByteArray.toPrettyHexString(bytesPerWord: Int = 4, wordsPerLine: Int = 8): String {
        return map { it.toHexString(HexFormat.UpperCase) }
            .windowed(bytesPerWord, bytesPerWord, partialWindows = true)
            .windowed(wordsPerLine, wordsPerLine, partialWindows = true)
            .joinToString("\n") { it.joinToString("   ") { it.joinToString(" ") } }
    }

    companion object {
        // Some symbols that are annoying to use with Kotlin's string substitution.
        val ion_1_1 = "\$ion_1_1"
        val ion = "\$ion"

        // Some symbol tokens so that we don't have to keep declaring them
        private val fooMacro = MacroBuilder.newBuilder().stringValue("foo").build() as MacroImpl
        private val barMacro = MacroBuilder.newBuilder().stringValue("bar").build() as MacroImpl

        // Helper function that writes to a writer and returns the text Ion
        private fun write(
            topLevelValuesOnNewLines: Boolean = true,
            closeWriter: Boolean = true,
            pretty: Boolean = false,
            symbolInliningStrategy: SymbolInliningStrategy = SymbolInliningStrategy.ALWAYS_INLINE,
            block: IonManagedWriter_1_1.() -> Unit
        ): String {
            val sb = StringBuilder()
            val appendable = {
                _Private_IonTextAppender.forAppendable(BufferedAppendableFastAppendable(sb))
            }

            val textWriterBuilder = IonTextWriterBuilder.standard().apply {
                if (topLevelValuesOnNewLines) withWriteTopLevelValuesOnNewLines(true)
                if (pretty) withPrettyPrinting()
                withNewLineType(NewLineType.LF)
            } as _Private_IonTextWriterBuilder

            val writer = IonManagedWriter_1_1(
                userData = IonRawTextWriter_1_1(textWriterBuilder, appendable()),
                systemData = IonRawTextWriter_1_1(textWriterBuilder, appendable()),
                options = ManagedWriterOptions_1_1(
                    internEncodingDirectiveSymbols = false,
                    symbolInliningStrategy = symbolInliningStrategy,
                    lengthPrefixStrategy = LengthPrefixStrategy.NEVER_PREFIXED,
                    eExpressionIdentifierStrategy = ManagedWriterOptions_1_1.EExpressionIdentifierStrategy.BY_NAME,
                ),
                onClose = {}
            )

            writer.apply(block)
            if (closeWriter) writer.close()
            return sb.toString().trim()
        }

        // Helper function that writes to a writer and returns the binary Ion
        private fun writeBinary(
            closeWriter: Boolean = true,
            symbolInliningStrategy: SymbolInliningStrategy = SymbolInliningStrategy.NEVER_INLINE,
            lengthPrefixStrategy: LengthPrefixStrategy = LengthPrefixStrategy.CONTAINERS_PREFIXED,
            block: IonManagedWriter_1_1.() -> Unit
        ): ByteArray {
            val output = ByteArrayOutputStream()

            val writer = newIon11BinaryWriter(output, symbolInliningStrategy, lengthPrefixStrategy)

            writer.apply(block)
            if (closeWriter) writer.close()
            return output.toByteArray()
        }

        private fun newIon11BinaryWriter(
            output: ByteArrayOutputStream,
            symbolInliningStrategy: SymbolInliningStrategy = SymbolInliningStrategy.NEVER_INLINE,
            lengthPrefixStrategy: LengthPrefixStrategy = LengthPrefixStrategy.CONTAINERS_PREFIXED,
        ) = IonManagedWriter_1_1(
            userData = IonRawBinaryWriter_1_1.from(output, 1024, 1),
            systemData = IonRawBinaryWriter_1_1.from(output, 1024, 1),
            options = ManagedWriterOptions_1_1(
                internEncodingDirectiveSymbols = false,
                symbolInliningStrategy = symbolInliningStrategy,
                lengthPrefixStrategy = lengthPrefixStrategy,
                eExpressionIdentifierStrategy = ManagedWriterOptions_1_1.EExpressionIdentifierStrategy.BY_ADDRESS,
            ),
            onClose = output::close,
        )
    }

    @Test
    fun `attempting to manually write a symbol table throws an exception`() {
        write(closeWriter = false) {
            addTypeAnnotation(SystemSymbols.ION_SYMBOL_TABLE)
            assertThrows<IonException> { stepIn(IonType.STRUCT) }
        }
    }

    @Test
    fun `attempting to step into a scalar type throws an exception`() {
        write {
            assertThrows<IllegalArgumentException> { stepIn(IonType.NULL) }
        }
    }

    @Test
    fun `write various data model values`() {
        // TODO: Split this up and make sure edge cases, particularly around
        //       field names an annotations, are handled correctly.
        val expected = """
            $ion_1_1
            null
            null.list
            foo::true
            [1,10]
            bar::{a:"Hello world!",b:d::e::+inf,f:(baz -0. {{AQID}} {{"abc"}}),g:2025-11-04T12:34:56.000Z}
        """.trimIndent()

        val actual = write {
            writeNull()
            writeNull(IonType.LIST)
            addTypeAnnotation("foo")
            writeBool(true)
            stepIn(IonType.LIST)
            writeInt(1)
            writeInt(BigInteger.TEN)
            stepOut()
            addTypeAnnotation("bar")
            stepIn(IonType.STRUCT)
            setFieldName("a")
            writeString("Hello world!")
            setFieldName("b")
            setTypeAnnotations(arrayOf("d", "e"))
            writeFloat(Double.POSITIVE_INFINITY)
            setFieldName("f")
            stepIn(IonType.SEXP)
            writeSymbol("baz")
            writeDecimal(Decimal.NEGATIVE_ZERO)
            writeBlob(byteArrayOf(1, 2, 3))
            writeClob(byteArrayOf(0x61, 0x62, 0x63))
            stepOut()
            setFieldName("g")
            writeTimestamp(Timestamp.valueOf("2025-11-04T12:34:56.000Z"))
            stepOut()
        }

        assertEquals(expected, actual)
    }

    @Test
    fun `write an IVM`() {
        assertEquals(
            """
            $ion_1_1
            $ion_1_1
            """.trimIndent(),
            write { writeIonVersionMarker() }
        )
    }

    @Test
    fun `write an IVM in a container should throw an exception`() {
        write(closeWriter = false) {
            stepIn(IonType.LIST)
            assertThrows<IonException> { writeIonVersionMarker() }
        }
    }

    @Test
    fun `write an encoding directive with a non-empty macro table`() {
        val expected = """
            $ion_1_1
            (:$ion set_macros (null "foo"))
            (:0)
        """.trimIndent()

        val actual = write {
            startMacro(fooMacro)
            endMacro()
        }

        assertEquals(expected, actual)
    }

    @Test
    fun `write an e-expression by name`() {
        val expected = """
            $ion_1_1
            (:$ion set_macros (a "foo"))
            (:a)
        """.trimIndent()

        val actual = write {
            startMacro("a", fooMacro)
            endMacro()
        }

        assertEquals(expected, actual)
    }

    @Test
    fun `write an e-expression by address`() {
        val expected = """
            $ion_1_1
            (:$ion set_macros (null "foo"))
            (:0)
        """.trimIndent()

        val actual = write {
            startMacro(fooMacro)
            endMacro()
        }

        assertEquals(expected, actual)
    }

    @Test
    fun `write an e-expression with tagless parameter in text`() {
        val expected = """
            $ion_1_1
            (:$ion set_macros (null [(:? {#int8})]))
            (:0 1)
        """.trimIndent()

        val macro = MacroBuilder.newBuilder()
            .listValue { it.taglessPlaceholder(TaglessScalarType.INT_8) }
            .build()

        val actual = write {
            startMacro(macro)
            writeInt(1)
            endMacro()
        }
        assertEquals(expected, actual)
    }

    @Test
    fun `write an e-expression with tagless parameter in binary`() {
        val expected = """
            E0 01 01 EA
            E3                 | set_macros
               F1              | delim sexp start
                  8E           | null
                  F0 EB 61 EF  | [(:? {#int8})]
               EF
            EF
            00 01
        """.cleanCommentedHexBytes().hexStringToByteArray()

        val macro = MacroBuilder.newBuilder()
            .listValue { it.taglessPlaceholder(TaglessScalarType.INT_8) }
            .build()

        val actual = writeBinary {
            startMacro(macro)
            writeInt(1)
            endMacro()
        }

        assertArrayEquals(expected, actual)
    }

    @Test
    fun `write a tagless-element list with scalar type`() {
        val expectedText = """
            $ion_1_1
            [{#int8} 1,2]
        """.trimIndent()

        val expectedBinary = """
            E0 01 01 EA
            5B 61 05 01 02
        """.cleanCommentedHexBytes().hexStringToByteArray()

        val writeFn: IonManagedWriter_1_1.() -> Unit = {
            stepInTaglessElementList(TaglessScalarType.INT_8)
            writeInt(1)
            writeInt(2)
            stepOut()
        }
        val actualText = write(block = writeFn)
        assertEquals(expectedText, actualText)

        val actualBinary = writeBinary(block = writeFn)
        assertEquals(expectedBinary.toPrettyHexString(), actualBinary.toPrettyHexString())
    }

    @Test
    fun `write a tagless-element list with macro shape`() {
        val expected = """
            $ion_1_1
            (:$ion set_macros (null [(:? {#int8})]))
            [{:0} (1),(2),(3)]
        """.trimIndent()

        val macro = MacroBuilder.newBuilder()
            .listValue { it.taglessPlaceholder(TaglessScalarType.INT_8) }
            .build()

        val actual = write {
            stepInTaglessElementList(null, macro)
            startMacro(macro)
            writeInt(1)
            endMacro()
            startMacro(macro)
            writeInt(2)
            endMacro()
            startMacro(macro)
            writeInt(3)
            endMacro()
            stepOut()
        }
        assertEquals(expected, actual)
    }

    @Test
    fun `write an encoding directive with a non-empty symbol table`() {
        val expected = """
            $ion_1_1
            (:$ion set_symbols "foo")
            $10
        """.trimIndent()

        val actual = write(symbolInliningStrategy = SymbolInliningStrategy.NEVER_INLINE) {
            writeSymbol("foo")
        }

        assertEquals(expected, actual)
    }

    @Test
    fun `calling flush() causes the next encoding directive to append to a macro table`() {
        val expected = """
            $ion_1_1
            (:$ion set_macros (null "foo"))
            (:0)
            (:$ion add_macros (null "bar"))
            (:0)
            (:1)
        """.trimIndent()

        val actual = write {
            startMacro(fooMacro)
            endMacro()
            flush()
            startMacro(fooMacro)
            endMacro()
            startMacro(barMacro)
            endMacro()
        }

        assertEquals(expected, actual)
    }

    @Test
    fun `calling flush() causes the next encoding directive to append to the symbol table`() {
        val expected = """
            $ion_1_1
            (:$ion set_symbols "foo")
            $10
            (:$ion add_symbols "bar")
            $11
        """.trimIndent()

        val actual = write(symbolInliningStrategy = SymbolInliningStrategy.NEVER_INLINE) {
            writeSymbol("foo")
            flush()
            writeSymbol("bar")
        }

        assertEquals(expected, actual)
    }

    @Test
    fun `calling finish() causes the next encoding directive to NOT append to a macro table`() {
        val expected = """
            $ion_1_1
            (:$ion set_macros (null "foo"))
            (:0)
            $ion_1_1
            (:$ion set_macros (null "bar"))
            (:0)
        """.trimIndent()

        val actual = write {
            startMacro(fooMacro)
            endMacro()
            finish()
            startMacro(barMacro)
            endMacro()
        }

        assertEquals(expected, actual)
    }

    @Test
    fun `calling finish() causes the next encoding directive to NOT append to the symbol table`() {
        val expected = """
            $ion_1_1
            (:$ion set_symbols "foo")
            $10
            $ion_1_1
            (:$ion set_symbols "bar")
            $10
        """.trimIndent()

        val actual = write(symbolInliningStrategy = SymbolInliningStrategy.NEVER_INLINE) {
            writeSymbol("foo")
            finish()
            writeSymbol("bar")
        }

        assertEquals(expected, actual)
    }

    @Test
    fun `adding to the macro table should preserve existing symbols`() {
        val expected = """
            $ion_1_1
            (:$ion set_symbols "foo")
            $10
            (:$ion set_macros (null "bar"))
            (:0)
            $10
        """.trimIndent()

        val actual = write(symbolInliningStrategy = SymbolInliningStrategy.NEVER_INLINE) {
            writeSymbol("foo")
            flush()
            startMacro(barMacro)
            endMacro()
            writeSymbol("foo")
        }

        assertEquals(expected, actual)
    }

    @Test
    fun `adding to the symbol table should preserve existing macros`() {
        val expected = """
            $ion_1_1
            (:$ion set_macros (null "foo"))
            (:0)
            (:$ion set_symbols "foo")
            $10
            (:0)
        """.trimIndent()

        val actual = write(symbolInliningStrategy = SymbolInliningStrategy.NEVER_INLINE) {
            val theMacro = fooMacro
            startMacro(theMacro)
            endMacro()
            flush()
            writeSymbol("foo")
            startMacro(theMacro)
            endMacro()
        }

        assertEquals(expected, actual)
    }

    /** Holds a static factory method with the test cases for [testWritingMacroDefinitions]. */
    object TestWritingMacroDefinitions {
        const val THE_METHOD = "com.amazon.ion.impl.IonManagedWriter_1_1_Test\$TestWritingMacroDefinitions#cases"

        @JvmStatic
        fun cases(): List<Arguments> {
            fun case(
                name: String,
                body: MacroBuilder.TemplateBody.() -> MacroBuilder.FinalState = { nullValue() },
                expectedBody: String = "null"
            ) = arguments(name, MacroBuilder.newBuilder().body().build(), expectedBody)

            return listOf(
                case(
                    "null",
                    body = { nullValue() },
                    expectedBody = "null"
                ),
                // Annotations on `null` are representative for all types that don't have special annotation logic
                case(
                    "annotated null",
                    body = { annotated("foo").nullValue() },
                    expectedBody = "foo::null"
                ),
                case(
                    "null annotated with $0",
                    body = { annotated(null).nullValue() },
                    expectedBody = "$0::null"
                ),
                case(
                    "bool",
                    body = { boolValue(true) },
                    expectedBody = "true"
                ),
                case(
                    "int",
                    body = { intValue(1) },
                    expectedBody = "1"
                ),
                case(
                    "(big) int",
                    body = { intValue(BigInteger.ONE) },
                    expectedBody = "1"
                ),
                case(
                    "float",
                    body = { floatValue(Double.POSITIVE_INFINITY) },
                    expectedBody = "+inf"
                ),
                case(
                    "decimal",
                    body = { decimalValue(Decimal.valueOf(1.1)) },
                    expectedBody = "1.1"
                ),
                case(
                    "timestamp",
                    body = { timestampValue(Timestamp.valueOf("2024T")) },
                    expectedBody = "2024T"
                ),
                case(
                    "symbol",
                    body = { symbolValue("foo") },
                    expectedBody = "foo"
                ),
                case(
                    "unknown symbol",
                    body = { symbolValue(null) },
                    expectedBody = "$0"
                ),
                case(
                    "annotated symbol",
                    body = {
                        annotated("foo").symbolValue("bar")
                    },
                    expectedBody = "foo::bar"
                ),
                case(
                    "symbol annotated with $0",
                    body = {
                        annotated(null).symbolValue("bar")
                    },
                    expectedBody = "$0::bar"
                ),
                case(
                    "string",
                    body = { stringValue("abc") },
                    expectedBody = "\"abc\""
                ),
                case(
                    "blob",
                    body = { blobValue(byteArrayOf()) },
                    expectedBody = "{{}}"
                ),
                case(
                    "clob",
                    body = { clobValue(byteArrayOf()) },
                    expectedBody = "{{\"\"}}"
                ),
                case(
                    "list",
                    body = { listValue { l -> l.intValue(1) } },
                    expectedBody = "[1]"
                ),
                case(
                    "sexp",
                    body = { sexpValue { intValue(1) } },
                    expectedBody = "(1)"
                ),
                case(
                    "empty sexp",
                    body = { sexpValue { } },
                    expectedBody = "()"
                ),
                case(
                    "annotated sexp",
                    body = { annotated("foo").sexpValue { intValue(1) } },
                    expectedBody = "foo::(1)"
                ),
                case(
                    "sexp with $0 annotation",
                    body = { annotated(null).sexpValue { intValue(1) } },
                    expectedBody = "$0::(1)"
                ),
                case(
                    "struct",
                    body = { structValue { it.fieldName("foo").intValue(1) } },
                    expectedBody = "{foo:1}"
                ),
                case(
                    "struct with $0 field name",
                    body = { structValue { it.fieldName(null).intValue(1) } },
                    expectedBody = "{$0:1}"
                ),
                case(
                    "variable",
                    body = { listValue { l -> l.placeholder() } },
                    expectedBody = "[(:?)]"
                ),

                case(
                    "variable with default value",
                    body = {
                        listValue { l ->
                            l.placeholderWithDefault {
                                d ->
                                d.structValue { s ->
                                    s.fieldName("foo").intValue(1)
                                }
                            }
                        }
                    },
                    expectedBody = "[(:? {foo:1})]"
                ),
                case(
                    "multiple variables",
                    body = { listValue { l -> l.placeholder().placeholder().placeholder() } },
                    expectedBody = "[(:?),(:?),(:?)]"
                ),
                case(
                    "nested expressions in body",
                    body = {
                        listValue {
                            sexpValue { intValue(1) }
                            structValue { s -> s.fieldName("foo").intValue(2) }
                        }
                    },
                    expectedBody = "[(1),{foo:2}]"
                ),
            )
        }
    }

    @MethodSource(TestWritingMacroDefinitions.THE_METHOD)
    @ParameterizedTest(name = "a macro definition with {0}")
    fun testWritingMacroDefinitions(description: String, macro: MacroImpl, expectedSignatureAndBody: String) {
        val expected = """
            $ion_1_1
            (:$ion set_macros (null "foo") (null "bar") (null $expectedSignatureAndBody))
        """.trimIndent()

        val actual = write {
            getOrAssignMacroAddress(fooMacro)
            getOrAssignMacroAddress(barMacro)
            getOrAssignMacroAddress(macro)
        }

        assertEquals(expected, actual)
    }

    @Test
    fun `when pretty printing, directive expressions should have the clause name on the first line`() {
        // ...and look reasonably pleasant.
        // However, this should be held loosely.
        val expected = """
            $ion_1_1
            (:$ion set_symbols
              "foo"
              "bar"
              "baz"
            )
            (:$ion set_macros
              (
                null
                "foo"
              )
            )
            ${'$'}10
            ${'$'}11
            ${'$'}12
            (:0)
            (:$ion add_symbols
              "a"
              "b"
              "c"
            )
            (:$ion add_macros
              (
                null
                "bar"
              )
            )
            ${'$'}13
            ${'$'}14
            ${'$'}15
            (:1)
        """.trimIndent()

        val fooMacro = fooMacro

        val actual = write(symbolInliningStrategy = SymbolInliningStrategy.NEVER_INLINE, pretty = true) {
            writeSymbol("foo")
            writeSymbol("bar")
            writeSymbol("baz")
            startMacro(fooMacro)
            endMacro()
            flush()
            writeSymbol("a")
            writeSymbol("b")
            writeSymbol("c")
            startMacro(barMacro)
            endMacro()
        }

        assertEquals(expected, actual)
    }

    @Test
    fun `writeObject() should write something with a macro representation`() {
        val expected = """
            $ion_1_1
            (:$ion set_macros (Point2D {x:(:? {#int}),y:(:? {#int})}))
            (:Point2D 2 4)
        """.trimIndent()

        val actual = write {
            writeObject(Point2D(2, 4))
        }

        assertEquals(expected, actual)
    }

    @Test
    fun `writeObject() should write something without a macro representation`() {
        val expected = """
            $ion_1_1
            Red
            Yellow
            Green
            Blue
        """.trimIndent()

        val actual = write {
            Colors.entries.forEach {
                color ->
                writeObject(color)
            }
        }

        assertEquals(expected, actual)
    }

    @OptIn(ExperimentalStdlibApi::class)
    @Test
    fun `writeObject() should write something with nested macro representation`() {
        val expected = """
            $ion_1_1
            (:$ion set_macros (Polygon {vertices:(:?),fill:(:? {#symbol})}) (Point2D {x:(:? {#int}),y:(:? {#int})}))
            (:Polygon [{:Point2D} (0 0),(0 1),(1 1),(1 0)] Blue)
        """.trimIndent()

        val data = Polygon(
            listOf(
                Point2D(0, 0),
                Point2D(0, 1),
                Point2D(1, 1),
                Point2D(1, 0),
            ),
            Colors.Blue,
        )

        val actual = write {
            data.writeToMacroAware(this)
        }

        assertEquals(expected, actual)
    }

    @Test
    fun `ion 1,1 demo`() {
        // Writes some data in a few different ways and prints out comparisons.

        val data = Polygon(
            listOf(
                Point2D(0, 3),
                Point2D(0, 8),
                Point2D(4, 13),
                Point2D(8, 8),
                Point2D(8, 3),
                Point2D(4, 0),
            ),
            Colors.Blue,
        )

        println("\nIon 1.0 Text")
        println(StringBuilder().also { IonTextWriterBuilder.standard().build(it).use(data::writeTo) })

        println("\nIon 1.1 Text w/ macros")
        println(write { data.writeToMacroAware(this) })

        println("\nIon 1.0 Binary")
        calculateSystemAndUserSizes(IonBinaryWriterBuilder.standard()::build, data::writeTo)
        println("\nIon 1.1 Binary w/o macros")
        calculateSystemAndUserSizes(::newIon11BinaryWriter, data::writeTo)
        println("\nIon 1.1 Binary w/ macros")
        calculateSystemAndUserSizes(::newIon11BinaryWriter, data::writeToMacroAware)
    }

    private fun <T : IonWriter> calculateSystemAndUserSizes(newWriter: (ByteArrayOutputStream) -> T, writeValue: (T) -> Unit) {
        val oneValueBytes = ByteArrayOutputStream().also { newWriter(it).use(writeValue) }.toByteArray()
        val twoValueBytes = ByteArrayOutputStream().also { newWriter(it).use { w -> repeat(2) { writeValue(w) } } }.toByteArray()

        val valueSize = twoValueBytes.size - oneValueBytes.size
        val ivmSize = 4
        val systemValueSize = oneValueBytes.size - ivmSize - valueSize

        println("IVM    (4 bytes)  : ${oneValueBytes.copyOfRange(0, ivmSize).toPrettyHexString(wordsPerLine = 32)}")
        println("SYSTEM ($systemValueSize bytes) : ${oneValueBytes.copyOfRange(ivmSize, ivmSize + systemValueSize).toPrettyHexString(wordsPerLine = 32)}")
        println("USER   ($valueSize bytes) : ${oneValueBytes.copyOfRange(ivmSize + systemValueSize, oneValueBytes.size).toPrettyHexString(wordsPerLine = 32)}")
        println("TOTAL: ${oneValueBytes.size} bytes")
    }

    interface WriteAsIon {
        fun writeTo(writer: IonWriter)
        fun writeToMacroAware(writer: IonWriter_1_1)
    }

    private data class Polygon(val vertices: List<Point2D>, val fill: Colors) : WriteAsIon {
        init { require(vertices.size >= 3) { "A polygon must have at least 3 edges and 3 vertices" } }

        companion object {
            // Using the qualified class name would be verbose, but may be safer for general
            // use so that there is almost no risk of having a name conflict with another macro.
            private val MACRO_NAME = Polygon::class.simpleName!!.replace(".", "_")
            private val MACRO = MacroBuilder.newBuilder()
                .structValue {
                    it.fieldName("vertices")
                        .placeholder()
                        .fieldName("fill")
                        .taglessPlaceholder(TaglessScalarType.SYMBOL)
                }
                .build()
        }

        override fun writeTo(writer: IonWriter) {
            with(writer) {
                stepIn(IonType.STRUCT)
                setFieldName("vertices")
                stepIn(IonType.LIST)
                vertices.forEach { it.writeTo(writer) }
                stepOut()
                setFieldName("fill")
                fill.writeTo(this)
                stepOut()
            }
        }

        override fun writeToMacroAware(writer: IonWriter_1_1) {

            with(writer) {
                startMacro(MACRO_NAME, MACRO)
                stepInTaglessElementList(Point2D.MACRO_NAME, Point2D.MACRO)
                vertices.forEach { it.writeToMacroAware(this) }
                stepOut()
                fill.writeToMacroAware(this)
                endMacro()
            }
        }
    }

    private data class Point2D(val x: Long, val y: Long) : IonManagedWriter_1_1_Test.WriteAsIon {
        companion object {
            // This is a very long macro name, but by using the qualified class name,
            // there is almost no risk of having a name conflict with another macro.
            val MACRO_NAME = Point2D::class.simpleName!!.replace(".", "_")
            val MACRO = MacroBuilder.newBuilder().structValue {
                it.fieldName("x").taglessPlaceholder(TaglessScalarType.INT)
                    .fieldName("y").taglessPlaceholder(TaglessScalarType.INT)
            }.build()
        }

        override fun writeToMacroAware(writer: IonWriter_1_1) {
            with(writer) {
                startMacro(MACRO_NAME, MACRO)
                writeInt(x)
                writeInt(y)
                endMacro()
            }
        }

        override fun writeTo(writer: IonWriter) {
            with(writer) {
                stepIn(IonType.STRUCT)
                setFieldName("x")
                writeInt(x)
                setFieldName("y")
                writeInt(y)
                stepOut()
            }
        }
    }

    private enum class Colors : WriteAsIon {
        Red,
        Yellow,
        Green,
        Blue,
        ;
        override fun writeTo(writer: IonWriter) {
            writer.writeSymbol(this.name)
        }
        override fun writeToMacroAware(writer: IonWriter_1_1) = writeTo(writer)
    }
}
