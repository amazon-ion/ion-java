// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.bin

import com.amazon.ion.*
import com.amazon.ion.IonEncodingVersion.*
import com.amazon.ion.impl.*
import com.amazon.ion.impl.macro.*
import com.amazon.ion.impl.macro.ExpressionBuilderDsl.Companion.templateBody
import com.amazon.ion.impl.macro.Macro.*
import com.amazon.ion.impl.macro.ParameterFactory.exactlyOneTagged
import com.amazon.ion.impl.macro.ParameterFactory.zeroToManyTagged
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

        // Helper function that writes to a writer and returns the binary Ion
        private fun writeBinary(
            closeWriter: Boolean = true,
            symbolInliningStrategy: SymbolInliningStrategy = SymbolInliningStrategy.ALWAYS_INLINE,
            block: IonManagedWriter_1_1.() -> Unit
        ): ByteArray {
            val out = ByteArrayOutputStream()
            val writer = ION_1_1.binaryWriterBuilder()
                .withSymbolInliningStrategy(symbolInliningStrategy)
                .build(out) as IonManagedWriter_1_1
            writer.apply(block)
            if (closeWriter) writer.close()
            return out.toByteArray()
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

    private fun newSystemReader(input: ByteArray): IonReader {
        val system = IonSystemBuilder.standard().build() as _Private_IonSystem
        return system.newSystemReader(input)
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
        val output = ByteArrayOutputStream()
        newSystemReader(input.toByteArray()).use { reader ->
            (ION_1_1.binaryWriterBuilder().build(output) as _Private_IonWriter).use {
                it.writeValuesFn(reader)
            }
        }
        // Verify the transformed symbol IDs using another system read.
        newSystemReader(output.toByteArray()).use {
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
    fun `write a symbol value using a system symbol ID in binary`() {
        val actual = writeBinary(symbolInliningStrategy = SymbolInliningStrategy.NEVER_INLINE) {
            writeSymbol(SystemSymbols_1_1.SYMBOLS.text)
        }
        val reader = newSystemReader(actual)
        assertEquals(IonType.SYMBOL, reader.next())
        assertEquals(ion_1_1, reader.stringValue())
        assertEquals(IonType.SYMBOL, reader.next())
        assertEquals(SystemSymbols_1_1.SYMBOLS.text, reader.stringValue())
        assertNull(reader.next())
        reader.close()
    }

    @Test
    fun `write an annotation using a system symbol ID in binary`() {
        val actual = writeBinary(symbolInliningStrategy = SymbolInliningStrategy.NEVER_INLINE) {
            addTypeAnnotation(SystemSymbols_1_1.SYMBOLS.text)
            writeInt(123)
        }
        val reader = newSystemReader(actual)
        assertEquals(IonType.SYMBOL, reader.next())
        assertEquals(ion_1_1, reader.stringValue())
        assertEquals(IonType.INT, reader.next())
        assertEquals(SystemSymbols_1_1.SYMBOLS.text, reader.typeAnnotations[0])
        assertEquals(123, reader.intValue())
        assertNull(reader.next())
        reader.close()
    }

    @Test
    fun `write a field name using a system symbol ID in binary`() {
        val actual = writeBinary(symbolInliningStrategy = SymbolInliningStrategy.NEVER_INLINE) {
            stepIn(IonType.STRUCT)
            setFieldName(SystemSymbols_1_1.SYMBOLS.text)
            writeInt(123)
            stepOut()
        }
        val reader = newSystemReader(actual)
        assertEquals(IonType.SYMBOL, reader.next())
        assertEquals(ion_1_1, reader.stringValue())
        assertEquals(IonType.STRUCT, reader.next())
        reader.stepIn()
        assertEquals(IonType.INT, reader.next())
        assertEquals(SystemSymbols_1_1.SYMBOLS.text, reader.fieldName)
        assertEquals(123, reader.intValue())
        assertNull(reader.next())
        reader.stepOut()
        reader.close()
    }

    @Test
    fun `re-write a binary Ion 1-1 stream using a system reader`() {
        val binary = TestUtils.hexStringToByteArray(
            TestUtils.cleanCommentedHexBytes(
                """
            E0 01 01 EA | IVM
            E7 01 61    | $ion::
            CA          | (
            EE 10       |    module
            A1 5F       |    _
            C5          |    (
            EE 0F       |       symbol_table
            B2 91 61    |       ["a"]
                        |    )
                        | )
            E1 01       | Symbol value 1 = "a"
                """.trimIndent()
            )
        )
        val systemReader = newSystemReader(binary)
        val actual = writeBinary(symbolInliningStrategy = SymbolInliningStrategy.NEVER_INLINE) {
            writeValues(systemReader)
        }
        systemReader.close()

        val reader = IonReaderBuilder.standard().build(actual)
        assertEquals(IonType.SYMBOL, reader.next())
        assertEquals("a", reader.stringValue())
        assertNull(reader.next())
        reader.close()
    }

    @Test
    fun `write an encoding directive with a non-empty macro table`() {
        val expected = """
            $ion_1_1
            (:$ion::set_macros (:: (macro null () "foo")))
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
            (:$ion::set_macros (:: (macro a () "foo")))
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
            (:$ion::set_macros (:: (macro null () "foo")))
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
                zeroToManyTagged("a"),
                zeroToManyTagged("b"),
            ),
            body = templateBody { string("foo") }
        )

        val expected = """
            $ion_1_1
            (:$ion::set_macros (:: (macro null (a* b*) "foo")))
            (:0 (::) (:: 1 2 3))
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
            (:$ion::set_macros (:: (export $ion::make_string)))
        """.trimIndent()

        val actual = write {
            getOrAssignMacroAddress(SystemMacro.MakeString)
        }

        assertEquals(expected, actual)
    }

    @Test
    fun `when a system macro is shadowed, it should be written using the system e-exp syntax`() {
        val expected = """
            $ion_1_1
            (:$ion::set_macros (:: (macro make_string () "make")))
            (:make_string)
            (:$ion::make_string (:: "a" b))
        """.trimIndent()

        // Makes the word "make" as a string
        val makeStringShadow = constantMacro {
            string("make")
        }

        val actual = write {
            startMacro("make_string", makeStringShadow)
            endMacro()
            startMacro(SystemMacro.MakeString)
            startExpressionGroup()
            writeString("a")
            writeSymbol("b")
            endExpressionGroup()
            endMacro()
        }

        assertEquals(expected, actual)
    }

    @Test
    fun `it is possible to invoke a system macro using an alias`() {
        val expected = """
            $ion_1_1
            (:$ion::set_macros (:: (export $ion::make_string foo)))
            (:foo (:: "a" b))
        """.trimIndent()

        val actual = write {
            startMacro("foo", SystemMacro.MakeString)
            startExpressionGroup()
            writeString("a")
            writeSymbol("b")
            endExpressionGroup()
            endMacro()
        }
        assertEquals(expected, actual)
    }

    @Test
    fun `write an encoding directive with a non-empty symbol table`() {
        val expected = """
            $ion_1_1
            (:$ion::set_symbols (:: "foo"))
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
            (:$ion::set_macros (:: (macro null () "foo")))
            (:0)
            (:$ion::add_macros (:: (macro null () "bar")))
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
            (:$ion::set_symbols (:: "foo"))
            $1
            (:$ion::add_symbols (:: "bar"))
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
            (:$ion::set_macros (:: (macro null () "foo")))
            (:0)
            $ion_1_1
            (:$ion::set_macros (:: (macro null () "bar")))
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
            (:$ion::set_symbols (:: "foo"))
            $1
            $ion_1_1
            (:$ion::set_symbols (:: "bar"))
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
            (:$ion::set_symbols (:: "foo"))
            $1
            (:$ion::set_macros (:: (macro null () "foo")))
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
            (:$ion::set_macros (:: (macro null () "foo")))
            (:$ion::set_symbols (:: "foo"))
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
                    signature = listOf(exactlyOneTagged("x")),
                    expectedSignature = "(x)"
                ),
                case(
                    "multiple required parameters",
                    signature = listOf(
                        exactlyOneTagged("x"),
                        exactlyOneTagged("y")
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
                        Parameter("c", ParameterEncoding.FlexSym, ParameterCardinality.ZeroOrMore),
                        Parameter("d", ParameterEncoding.Float64, ParameterCardinality.ZeroOrOne),
                    ),
                    expectedSignature = "(int32::a b+ flex_sym::c* float64::d?)"
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
                    signature = listOf(exactlyOneTagged("x")),
                    expectedSignature = "(x)",
                    body = {
                        variable(0)
                    },
                    expectedBody = "(%x)"
                ),
                case(
                    "multiple variables",
                    signature = listOf("x", "y", "z").map(::exactlyOneTagged),
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
            (:$ion::set_macros (:: (macro foo () "foo") (macro null () "bar") (macro null $expectedSignatureAndBody)))
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
            (:$ion::set_symbols
              (:: "foo" "bar" "baz"))
            (:$ion::set_macros
              (::
                (macro null () "foo")
                (macro null (x) (.0 (%x) "bar" (..) (.. "baz")))
              )
            )
            $1
            $2
            $3
            (:0)
            (:1)
            (:$ion::add_symbols
              (:: "a" "b" "c"))
            (:$ion::add_macros
              (::
                (macro null () "abc")
              )
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
                    listOf(exactlyOneTagged("x")),
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
            (:$ion::set_macros (:: (macro Point2D (x y) {x:(%x),y:(%y)})))
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
            (:$ion::set_macros (:: (macro null (x*) (%x)) (macro Polygon (vertices+ flex_sym::fill?) {vertices:[(%vertices)],fill:(.0 (%fill))}) (macro Point2D (x y) {x:(%x),y:(%y)})))
            (:Polygon (:: (:Point2D 0 0) (:Point2D 0 1) (:Point2D 1 1) (:Point2D 1 0)) Blue)
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
            private val IDENTITY = TemplateMacro(listOf(zeroToManyTagged("x")), templateBody { variable(0) })
            private val MACRO = TemplateMacro(
                signature = listOf(
                    // TODO: Change this to a macro shape when they are supported
                    Parameter("vertices", ParameterEncoding.Tagged, ParameterCardinality.OneOrMore),
                    Parameter("fill", ParameterEncoding.FlexSym, ParameterCardinality.ZeroOrOne),
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
                    exactlyOneTagged("x"),
                    exactlyOneTagged("y"),
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
