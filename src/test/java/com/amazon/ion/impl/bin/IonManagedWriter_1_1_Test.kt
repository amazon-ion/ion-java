// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.bin

import com.amazon.ion.*
import com.amazon.ion.IonEncodingVersion.*
import com.amazon.ion.impl.*
import com.amazon.ion.impl.macro.*
import com.amazon.ion.impl.macro.ExpressionBuilderDsl.Companion.templateBody
import com.amazon.ion.impl.macro.Macro.*
import com.amazon.ion.system.*
import java.io.ByteArrayOutputStream
import java.math.BigInteger
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.Arguments.arguments
import org.junit.jupiter.params.provider.MethodSource

internal class IonManagedWriter_1_1_Test {

    companion object {
        // Some symbols that are annoying to use with Kotlin's string substitution.
        val ion = "\$ion"
        val ion_1_1 = "\$ion_1_1"
        val ion_encoding = "\$ion_encoding"

        // Some symbol tokens so that we don't have to keep declaring them
        private val fooSymbolToken = FakeSymbolToken("foo", -1)
        private val barSymbolToken = FakeSymbolToken("bar", -1)

        private val fooMacro = constantMacro { string("foo") }
        private val barMacro = constantMacro { string("bar") }

        // Helper function that writes to a writer and returns the text Ion
        private fun write(
            topLevelValuesOnNewLines: Boolean = true,
            closeWriter: Boolean = true,
            pretty: Boolean = false,
            symbolInliningStrategy: SymbolInliningStrategy = SymbolInliningStrategy.ALWAYS_INLINE,
            block: IonManagedWriter_1_1.() -> Unit
        ): String {
            val appendable = StringBuilder()
            val writer = ION_1_1.textWriterBuilder()
                .withWriteTopLevelValuesOnNewLines(topLevelValuesOnNewLines)
                .withSymbolInliningStrategy(symbolInliningStrategy)
                .apply { if (pretty) withPrettyPrinting() }
                .build(appendable) as IonManagedWriter_1_1
            writer.apply(block)
            if (closeWriter) writer.close()
            return appendable.toString().trim()
        }

        /** Helper function to create a constant (zero arg) template macro */
        fun constantMacro(body: TemplateDsl.() -> Unit) = TemplateMacro(emptyList(), templateBody(body))
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
    fun `write an IVM in a container should write a symbol`() {
        assertEquals(
            """
            $ion_1_1
            [$ion_1_1]
            """.trimIndent(),
            write {
                stepIn(IonType.LIST)
                writeIonVersionMarker()
                stepOut()
            }
        )
    }

    private fun `transform symbol IDS`(writeValuesFn: _Private_IonWriter.(IonReader) -> Unit) {
        // Craft the input data: {a: b::c}, encoded as {$10: $11::$12}
        val input = ByteArrayOutputStream()
        ION_1_0.binaryWriterBuilder().build(input).use {
            it.stepIn(IonType.STRUCT)
            it.setFieldName("a")
            it.addTypeAnnotation("b")
            it.writeSymbol("c")
            it.stepOut()
        }
        // Do a system-level transcode of the Ion 1.0 data to Ion 1.1, adding 32 to each local symbol ID.
        val system = IonSystemBuilder.standard().build() as _Private_IonSystem
        val output = ByteArrayOutputStream()
        system.newSystemReader(input.toByteArray()).use { reader ->
            (ION_1_1.binaryWriterBuilder().build(output) as _Private_IonWriter).use {
                it.writeValuesFn(reader)
            }
        }
        // Verify the transformed symbol IDs using another system read.
        system.newSystemReader(output.toByteArray()).use {
            while (it.next() == IonType.SYMBOL) {
                assertEquals("\$ion_1_1", it.stringValue())
            }
            assertEquals(IonType.STRUCT, it.next())
            it.stepIn()
            assertEquals(IonType.SYMBOL, it.next())
            assertEquals(42, it.fieldNameSymbol.sid)
            assertEquals(43, it.typeAnnotationSymbols[0].sid)
            assertEquals(44, it.symbolValue().sid)
            assertNull(it.next())
            it.stepOut()
        }
    }

    @Test
    fun `use writeValues to transform symbol IDS`() {
        `transform symbol IDS` { reader ->
            writeValues(reader) { sid -> sid + 32 }
        }
    }

    @Test
    fun `use writeValue to transform symbol IDS`() {
        `transform symbol IDS` { reader ->
            while (reader.next() != null) {
                writeValue(reader) { sid -> sid + 32 }
            }
        }
    }

    @Test
    fun `write an encoding directive with a non-empty macro table`() {
        val expected = """
            $ion_1_1
            $ion_encoding::((macro_table (macro null () "foo")))
        """.trimIndent()

        val actual = write {
            getOrAssignMacroAddress(constantMacro { string("foo") })
        }

        assertEquals(expected, actual)
    }

    @Test
    fun `write an e-expression by name`() {
        val expected = """
            $ion_1_1
            $ion_encoding::((macro_table (macro a () "foo")))
            (:a)
        """.trimIndent()

        val actual = write {
            startMacro("a", constantMacro { string("foo") })
            endMacro()
        }

        assertEquals(expected, actual)
    }

    @Test
    fun `write an e-expression by address`() {
        val expected = """
            $ion_1_1
            $ion_encoding::((macro_table (macro null () "foo")))
            (:0)
        """.trimIndent()

        val actual = write {
            startMacro(constantMacro { string("foo") })
            endMacro()
        }

        assertEquals(expected, actual)
    }

    @Test
    fun `write an e-expression with a expression group argument`() {
        val macro = TemplateMacro(
            signature = listOf(
                Parameter.zeroToManyTagged("a"),
                Parameter.zeroToManyTagged("b"),
            ),
            body = templateBody { string("foo") }
        )

        val expected = """
            $ion_1_1
            $ion_encoding::((macro_table (macro null (a* b*) "foo")))
            (:0 (:) (: 1 2 3))
        """.trimIndent()

        val actual = write {
            startMacro(macro)

            startExpressionGroup()
            endExpressionGroup()

            startExpressionGroup()
            writeInt(1)
            writeInt(2)
            writeInt(3)
            endExpressionGroup()

            endMacro()
        }

        assertEquals(expected, actual)
    }

    @Test
    fun `getOrAssignMacroAddress can add a system macro to the macro table`() {
        val expected = """
            $ion_1_1
            $ion_encoding::((macro_table (export $ion::make_string)))
        """.trimIndent()

        val actual = write {
            getOrAssignMacroAddress(SystemMacro.MakeString)
        }

        assertEquals(expected, actual)
    }

    @Test
    fun `write an encoding directive with a non-empty symbol table`() {
        val expected = """
            $ion_1_1
            $ion_encoding::((symbol_table ["foo"]))
            $1
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
            $ion_encoding::((macro_table (macro null () "foo")))
            (:0)
            $ion_encoding::((macro_table $ion_encoding (macro null () "bar")))
            (:0)
            (:1)
        """.trimIndent()

        val actual = write {
            val fooMacro = constantMacro { string("foo") }
            startMacro(fooMacro)
            endMacro()
            flush()
            startMacro(fooMacro)
            endMacro()
            startMacro(constantMacro { string("bar") })
            endMacro()
        }

        assertEquals(expected, actual)
    }

    @Test
    fun `calling flush() causes the next encoding directive to append to the symbol table`() {
        val expected = """
            $ion_1_1
            $ion_encoding::((symbol_table ["foo"]))
            $1
            $ion_encoding::((symbol_table $ion_encoding ["bar"]))
            $2
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
            $ion_encoding::((macro_table (macro null () "foo")))
            (:0)
            $ion_encoding::((macro_table (macro null () "bar")))
            (:0)
        """.trimIndent()

        val actual = write {
            startMacro(constantMacro { string("foo") })
            endMacro()
            finish()
            startMacro(constantMacro { string("bar") })
            endMacro()
        }

        assertEquals(expected, actual)
    }

    @Test
    fun `calling finish() causes the next encoding directive to NOT append to the symbol table`() {
        val expected = """
            $ion_1_1
            $ion_encoding::((symbol_table ["foo"]))
            $1
            $ion_encoding::((symbol_table ["bar"]))
            $1
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
            $ion_encoding::((symbol_table ["foo"]))
            $1
            $ion_encoding::((symbol_table $ion_encoding) (macro_table (macro null () "foo")))
        """.trimIndent()

        val actual = write(symbolInliningStrategy = SymbolInliningStrategy.NEVER_INLINE) {
            writeSymbol("foo")
            flush()
            getOrAssignMacroAddress(constantMacro { string("foo") })
        }

        assertEquals(expected, actual)
    }

    @Test
    fun `adding to the symbol table should preserve existing macros`() {
        val expected = """
            $ion_1_1
            $ion_encoding::((macro_table (macro null () "foo")))
            $ion_encoding::((symbol_table ["foo"]) (macro_table $ion_encoding))
            $1
            (:0)
        """.trimIndent()

        val actual = write(symbolInliningStrategy = SymbolInliningStrategy.NEVER_INLINE) {
            val theMacro = constantMacro { string("foo") }
            getOrAssignMacroAddress(theMacro)
            flush()
            writeSymbol("foo")
            startMacro(theMacro)
            endMacro()
        }

        assertEquals(expected, actual)
    }

    /** Holds a static factory method with the test cases for [testWritingMacroDefinitions]. */
    object TestWritingMacroDefinitions {
        const val THE_METHOD = "com.amazon.ion.impl.bin.IonManagedWriter_1_1_Test\$TestWritingMacroDefinitions#cases"

        @JvmStatic
        fun cases(): List<Arguments> {
            fun case(
                name: String,
                signature: List<Parameter> = emptyList(),
                body: TemplateDsl.() -> Unit = { nullValue() },
                expectedSignature: String = "()",
                expectedBody: String = "null"
            ) = arguments(name, TemplateMacro(signature, templateBody(body)), "$expectedSignature $expectedBody")

            return listOf(
                case(
                    "single required parameter",
                    signature = listOf(Parameter.exactlyOneTagged("x")),
                    expectedSignature = "(x)"
                ),
                case(
                    "multiple required parameters",
                    signature = listOf(
                        Parameter.exactlyOneTagged("x"),
                        Parameter.exactlyOneTagged("y")
                    ),
                    expectedSignature = "(x y)"
                ),
                case(
                    "optional parameter",
                    signature = listOf(Parameter("x", ParameterEncoding.Tagged, ParameterCardinality.ZeroOrOne)),
                    expectedSignature = "(x?)"
                ),
                case(
                    "zero-to-many parameter",
                    signature = listOf(Parameter("x", ParameterEncoding.Tagged, ParameterCardinality.ZeroOrMore)),
                    expectedSignature = "(x*)"
                ),
                case(
                    "one-to-many parameter",
                    signature = listOf(Parameter("x", ParameterEncoding.Tagged, ParameterCardinality.OneOrMore)),
                    expectedSignature = "(x+)"
                ),
                case(
                    "tagless parameter",
                    signature = listOf(Parameter("x", ParameterEncoding.Int32, ParameterCardinality.ExactlyOne)),
                    expectedSignature = "(int32::x)"
                ),
                case(
                    "variety of parameters",
                    signature = listOf(
                        Parameter("a", ParameterEncoding.Int32, ParameterCardinality.ExactlyOne),
                        Parameter("b", ParameterEncoding.Tagged, ParameterCardinality.OneOrMore),
                        Parameter("c", ParameterEncoding.CompactSymbol, ParameterCardinality.ZeroOrMore),
                        Parameter("d", ParameterEncoding.Float64, ParameterCardinality.ZeroOrOne),
                    ),
                    expectedSignature = "(int32::a b+ compact_symbol::c* float64::d?)"
                ),
                case(
                    "null",
                    body = { nullValue() },
                    expectedBody = "null"
                ),
                // Annotations on `null` are representative for all types that don't have special annotation logic
                case(
                    "annotated null",
                    body = {
                        annotated(listOf(fooSymbolToken), ::nullValue, IonType.NULL)
                    },
                    expectedBody = "foo::null"
                ),
                case(
                    "null annotated with $0",
                    body = {
                        annotated(listOf(FakeSymbolToken(null, 0)), ::nullValue, IonType.NULL)
                    },
                    expectedBody = "$0::null"
                ),
                case(
                    "bool",
                    body = { bool(true) },
                    expectedBody = "true"
                ),
                case(
                    "int",
                    body = { int(1) },
                    expectedBody = "1"
                ),
                case(
                    "(big) int",
                    body = { int(BigInteger.ONE) },
                    expectedBody = "1"
                ),
                case(
                    "float",
                    body = { float(Double.POSITIVE_INFINITY) },
                    expectedBody = "+inf"
                ),
                case(
                    "decimal",
                    body = { decimal(Decimal.valueOf(1.1)) },
                    expectedBody = "1.1"
                ),
                case(
                    "timestamp",
                    body = { timestamp(Timestamp.valueOf("2024T")) },
                    expectedBody = "2024T"
                ),
                case(
                    "symbol",
                    body = { symbol(FakeSymbolToken("foo", -1)) },
                    expectedBody = "foo"
                ),
                case(
                    "unknown symbol",
                    body = { symbol(FakeSymbolToken(null, 0)) },
                    expectedBody = "$0"
                ),
                case(
                    "annotated symbol",
                    body = {
                        annotated(listOf(fooSymbolToken), ::symbol, barSymbolToken)
                    },
                    expectedBody = "foo::bar"
                ),
                case(
                    "symbol annotated with $0",
                    body = {
                        annotated(listOf(FakeSymbolToken(null, 0)), ::symbol, barSymbolToken)
                    },
                    expectedBody = "$0::bar"
                ),
                case(
                    "string",
                    body = { string("abc") },
                    expectedBody = "\"abc\""
                ),
                case(
                    "blob",
                    body = { blob(byteArrayOf()) },
                    expectedBody = "{{}}"
                ),
                case(
                    "clob",
                    body = { clob(byteArrayOf()) },
                    expectedBody = "{{\"\"}}"
                ),
                case(
                    "list",
                    body = { list { int(1) } },
                    expectedBody = "[1]"
                ),
                case(
                    "sexp",
                    body = { sexp { int(1) } },
                    expectedBody = "(1)"
                ),
                case(
                    "empty sexp",
                    body = { sexp { } },
                    expectedBody = "()"
                ),
                case(
                    "annotated sexp",
                    body = { annotated(listOf(fooSymbolToken), ::sexp) { int(1) } },
                    expectedBody = "foo::(1)"
                ),
                case(
                    "sexp with $0 annotation",
                    body = { annotated(listOf(FakeSymbolToken(null, 0)), ::sexp) { int(1) } },
                    expectedBody = "$0::(1)"
                ),
                case(
                    "struct",
                    body = { struct { fieldName("foo"); int(1) } },
                    expectedBody = "{foo:1}"
                ),
                case(
                    "struct with $0 field name",
                    body = { struct { fieldName(FakeSymbolToken(null, 0)); int(1) } },
                    expectedBody = "{$0:1}"
                ),
                case(
                    "macro invoked by id",
                    body = { macro(barMacro) {} },
                    expectedBody = "(.1)"
                ),
                case(
                    "macro invoked by name",
                    body = { macro(fooMacro) {} },
                    expectedBody = "(.foo)"
                ),
                case(
                    "macro with an argument",
                    body = { macro(fooMacro) { int(1) } },
                    expectedBody = "(.foo 1)"
                ),
                case(
                    "macro with an empty argument group",
                    body = { macro(fooMacro) { expressionGroup { } } },
                    expectedBody = "(.foo (..))"
                ),
                case(
                    "macro with a non-empty argument group",
                    body = {
                        macro(fooMacro) {
                            expressionGroup {
                                int(1)
                                int(2)
                                int(3)
                            }
                        }
                    },
                    expectedBody = "(.foo (.. 1 2 3))"
                ),
                case(
                    "variable",
                    signature = listOf(Parameter.exactlyOneTagged("x")),
                    expectedSignature = "(x)",
                    body = {
                        variable(0)
                    },
                    expectedBody = "(%x)"
                ),
                case(
                    "multiple variables",
                    signature = listOf("x", "y", "z").map(Parameter::exactlyOneTagged),
                    expectedSignature = "(x y z)",
                    body = {
                        list {
                            variable(0)
                            variable(1)
                            variable(2)
                        }
                    },
                    expectedBody = "[(%x),(%y),(%z)]"
                ),
                case(
                    "nested expressions in body",
                    body = {
                        list {
                            sexp { int(1) }
                            struct {
                                fieldName("foo")
                                int(2)
                            }
                        }
                    },
                    expectedBody = "[(1),{foo:2}]"
                ),

            )
        }
    }

    @MethodSource(TestWritingMacroDefinitions.THE_METHOD)
    @ParameterizedTest(name = "a macro definition with {0}")
    fun testWritingMacroDefinitions(description: String, macro: Macro, expectedSignatureAndBody: String) {
        val expected = """
            $ion_1_1
            $ion_encoding::((macro_table (macro foo () "foo") (macro null () "bar") (macro null $expectedSignatureAndBody)))
        """.trimIndent()

        val actual = write {
            getOrAssignMacroAddressAndName("foo", fooMacro)
            getOrAssignMacroAddress(barMacro)
            getOrAssignMacroAddress(macro)
        }

        assertEquals(expected, actual)
    }

    @Test
    fun `when pretty printing, system s-expressions should have the clause name on the first line`() {
        // ...and look reasonably pleasant.
        // However, this should be held loosely.
        val expected = """
            $ion_1_1
            $ion_encoding::(
              (symbol_table ["foo","bar","baz"])
              (macro_table
                (macro null () "foo")
                (macro null (x) (.0 (%x) "bar" (..) (.. "baz"))))
            )
            $1
            $2
            $3
            (:0)
            (:1)
            $ion_encoding::(
              (symbol_table
                $ion_encoding
                ["a","b","c"])
              (macro_table
                $ion_encoding
                (macro null () "abc"))
            )
            $4
            $5
            $6
            (:2)
        """.trimIndent()

        val fooMacro = constantMacro { string("foo") }

        val actual = write(symbolInliningStrategy = SymbolInliningStrategy.NEVER_INLINE, pretty = true) {
            writeSymbol("foo")
            writeSymbol("bar")
            writeSymbol("baz")
            startMacro(fooMacro)
            endMacro()
            startMacro(
                TemplateMacro(
                    listOf(Parameter.exactlyOneTagged("x")),
                    templateBody {
                        macro(fooMacro) {
                            variable(0)
                            string("bar")
                            expressionGroup { }
                            expressionGroup {
                                string("baz")
                            }
                        }
                    }
                )
            )
            endMacro()
            flush()
            writeSymbol("a")
            writeSymbol("b")
            writeSymbol("c")
            startMacro(constantMacro { string("abc") })
            endMacro()
        }

        assertEquals(expected, actual)
    }

    @Test
    fun `writeObject() should write something with a macro representation`() {
        val expected = """
            $ion_1_1
            $ion_encoding::((macro_table (macro Point2D (x y) {x:(%x),y:(%y)})))
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

    @Test
    fun `writeObject() should write something with nested macro representation`() {
        val expected = """
            $ion_1_1
            $ion_encoding::((macro_table (macro null (x*) (%x)) (macro Polygon (vertices+ compact_symbol::fill?) {vertices:[(%vertices)],fill:(.0 (%fill))}) (macro Point2D (x y) {x:(%x),y:(%y)})))
            (:Polygon (: (:Point2D 0 0) (:Point2D 0 1) (:Point2D 1 1) (:Point2D 1 0)) Blue)
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
            writeObject(data)
        }

        assertEquals(expected, actual)
    }

    private data class Polygon(val vertices: List<Point2D>, val fill: Colors?) : WriteAsIon {
        init { require(vertices.size >= 3) { "A polygon must have at least 3 edges and 3 vertices" } }

        companion object {
            // Using the qualified class name would be verbose, but may be safer for general
            // use so that there is almost no risk of having a name conflict with another macro.
            private val MACRO_NAME = Polygon::class.simpleName!!.replace(".", "_")
            private val IDENTITY = TemplateMacro(listOf(Parameter.zeroToManyTagged("x")), templateBody { variable(0) })
            private val MACRO = TemplateMacro(
                signature = listOf(
                    // TODO: Change this to a macro shape when they are supported
                    Parameter("vertices", ParameterEncoding.Tagged, ParameterCardinality.OneOrMore),
                    Parameter("fill", ParameterEncoding.CompactSymbol, ParameterCardinality.ZeroOrOne),
                ),
                templateBody {
                    struct {
                        fieldName("vertices")
                        list {
                            variable(0)
                        }
                        fieldName("fill")
                        macro(IDENTITY) {
                            variable(1)
                        }
                    }
                }
            )
        }

        override fun writeTo(writer: IonWriter) {
            with(writer) {
                stepIn(IonType.STRUCT)
                setFieldName("vertices")
                stepIn(IonType.LIST)
                vertices.forEach { writeObject(it) }
                stepOut()
                if (fill != null) {
                    setFieldName("fill")
                    writeObject(fill)
                }
                stepOut()
            }
        }

        override fun writeToMacroAware(writer: MacroAwareIonWriter) {
            with(writer) {
                startMacro(MACRO_NAME, MACRO)
                startExpressionGroup()
                vertices.forEach { writer.writeObject(it) }
                endExpressionGroup()
                fill?.let { writeObject(it) }
                endMacro()
            }
        }
    }

    private data class Point2D(val x: Long, val y: Long) : WriteAsIon {
        companion object {
            // This is a very long macro name, but by using the qualified class name,
            // there is almost no risk of having a name conflict with another macro.
            private val MACRO_NAME = Point2D::class.simpleName!!.replace(".", "_")
            private val MACRO = TemplateMacro(
                signature = listOf(
                    Parameter.exactlyOneTagged("x"),
                    Parameter.exactlyOneTagged("y"),
                ),
                templateBody {
                    struct {
                        fieldName("x")
                        variable(0)
                        fieldName("y")
                        variable(1)
                    }
                }
            )
        }

        override fun writeToMacroAware(writer: MacroAwareIonWriter) {
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
                setFieldName("x")
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
    }
}
