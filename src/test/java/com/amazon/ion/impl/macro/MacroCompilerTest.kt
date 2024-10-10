// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.macro

import com.amazon.ion.*
import com.amazon.ion.impl.*
import com.amazon.ion.impl.macro.Expression.*
import com.amazon.ion.impl.macro.Macro.*
import com.amazon.ion.impl.macro.Macro.ParameterEncoding.*
import com.amazon.ion.impl.macro.MacroRef.*
import com.amazon.ion.system.IonReaderBuilder
import com.amazon.ion.system.IonSystemBuilder
import java.math.BigDecimal
import java.math.BigInteger
import java.nio.charset.StandardCharsets
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.EnumSource
import org.junit.jupiter.params.provider.MethodSource

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MacroCompilerTest {

    val ion: IonSystem = IonSystemBuilder.standard().build()

    private val fakeMacroTable: (MacroRef) -> Macro? = {
        when (it) {
            ById(12) -> SystemMacro.Values
            ByName("values") -> SystemMacro.Values
            else -> null
        }
    }

    private data class MacroSourceAndTemplate(val source: String, val template: TemplateMacro) : Arguments {
        override fun get(): Array<Any> = arrayOf(source, template.signature, template.body)
    }

    private fun annotations(vararg a: String): List<SymbolToken> = a.map { FakeSymbolToken(it, -1) }

    private infix fun String.shouldCompileTo(macro: TemplateMacro) = MacroSourceAndTemplate(this, macro)

    private fun testCases() = listOf(
        "(macro identity (x) (%x))" shouldCompileTo TemplateMacro(
            listOf(Parameter("x", Tagged, ParameterCardinality.ExactlyOne)),
            listOf(VariableRef(0)),
        ),
        "(macro identity (any::x) (%x))" shouldCompileTo TemplateMacro(
            listOf(Parameter("x", Tagged, ParameterCardinality.ExactlyOne)),
            listOf(VariableRef(0)),
        ),
        "(macro pi () 3.141592653589793)" shouldCompileTo TemplateMacro(
            signature = emptyList(),
            body = listOf(DecimalValue(emptyList(), BigDecimal("3.141592653589793")))
        ),
        "(macro cardinality_test (x?) (%x))" shouldCompileTo TemplateMacro(
            signature = listOf(Parameter("x", Tagged, ParameterCardinality.ZeroOrOne)),
            body = listOf(VariableRef(0))
        ),
        "(macro cardinality_test (x!) (%x))" shouldCompileTo TemplateMacro(
            signature = listOf(Parameter("x", Tagged, ParameterCardinality.ExactlyOne)),
            body = listOf(VariableRef(0))
        ),
        "(macro cardinality_test (x+) (%x))" shouldCompileTo TemplateMacro(
            signature = listOf(Parameter("x", Tagged, ParameterCardinality.OneOrMore)),
            body = listOf(VariableRef(0))
        ),
        "(macro cardinality_test (x*) (%x))" shouldCompileTo TemplateMacro(
            signature = listOf(Parameter("x", Tagged, ParameterCardinality.ZeroOrMore)),
            body = listOf(VariableRef(0))
        ),
        // Outer '.values' call allows multiple expressions in the body
        // The second `.values` is a macro call that has a single argument: the variable `x`
        // The third `(values x)` is an uninterpreted s-expression.
        """(macro literal_test (x) (.values (.values (%x)) (values x)))""" shouldCompileTo TemplateMacro(
            signature = listOf(Parameter("x", Tagged, ParameterCardinality.ExactlyOne)),
            body = listOf(
                MacroInvocation(SystemMacro.Values, selfIndex = 0, endExclusive = 6),
                MacroInvocation(SystemMacro.Values, selfIndex = 1, endExclusive = 3),
                VariableRef(0),
                SExpValue(emptyList(), selfIndex = 3, endExclusive = 6),
                SymbolValue(emptyList(), FakeSymbolToken("values", -1)),
                SymbolValue(emptyList(), FakeSymbolToken("x", -1)),
            ),
        ),
        "(macro each_type () (.values null true 1 ${"9".repeat(50)} 1e0 1d0 2024-01-16T \"foo\" bar [] () {} {{}} {{\"\"}} ))" shouldCompileTo TemplateMacro(
            signature = emptyList(),
            body = listOf(
                MacroInvocation(SystemMacro.Values, 0, 15),
                NullValue(emptyList(), IonType.NULL),
                BoolValue(emptyList(), true),
                LongIntValue(emptyList(), 1),
                BigIntValue(emptyList(), BigInteger("9".repeat(50))),
                FloatValue(emptyList(), 1.0),
                DecimalValue(emptyList(), Decimal.ONE),
                TimestampValue(emptyList(), Timestamp.valueOf("2024-01-16T")),
                StringValue(emptyList(), "foo"),
                SymbolValue(emptyList(), FakeSymbolToken("bar", -1)),
                ListValue(emptyList(), selfIndex = 10, endExclusive = 11),
                SExpValue(emptyList(), selfIndex = 11, endExclusive = 12),
                StructValue(emptyList(), selfIndex = 12, endExclusive = 13, templateStructIndex = emptyMap()),
                BlobValue(emptyList(), ByteArray(0)),
                ClobValue(emptyList(), ByteArray(0))
            )
        ),
        """(macro foo () (.values 42 "hello" false))""" shouldCompileTo TemplateMacro(
            signature = emptyList(),
            body = listOf(
                MacroInvocation(SystemMacro.Values, selfIndex = 0, endExclusive = 4),
                LongIntValue(emptyList(), 42),
                StringValue(emptyList(), "hello"),
                BoolValue(emptyList(), false),
            )
        ),
        """(macro using_expr_group () (.values (.. 42 "hello" false)))""" shouldCompileTo TemplateMacro(
            signature = emptyList(),
            body = listOf(
                MacroInvocation(SystemMacro.Values, selfIndex = 0, endExclusive = 5),
                ExpressionGroup(selfIndex = 1, endExclusive = 5),
                LongIntValue(emptyList(), 42),
                StringValue(emptyList(), "hello"),
                BoolValue(emptyList(), false),
            )
        ),
        """(macro invoke_by_id () (.12 true false))""" shouldCompileTo TemplateMacro(
            signature = emptyList(),
            body = listOf(
                MacroInvocation(SystemMacro.Values, selfIndex = 0, endExclusive = 3),
                BoolValue(emptyList(), true),
                BoolValue(emptyList(), false),
            )
        ),
        "(macro null () \"abc\")" shouldCompileTo TemplateMacro(
            signature = emptyList(),
            body = listOf(StringValue(emptyList(), "abc"))
        ),
        "(macro foo (x y z) [100, [200, a::b::300], (%x), {y: [true, false, (%z)]}])" shouldCompileTo TemplateMacro(
            signature = listOf(
                Parameter("x", Tagged, ParameterCardinality.ExactlyOne),
                Parameter("y", Tagged, ParameterCardinality.ExactlyOne),
                Parameter("z", Tagged, ParameterCardinality.ExactlyOne)
            ),
            body = listOf(
                ListValue(emptyList(), selfIndex = 0, endExclusive = 12),
                LongIntValue(emptyList(), 100),
                ListValue(emptyList(), selfIndex = 2, endExclusive = 5),
                LongIntValue(emptyList(), 200),
                LongIntValue(annotations("a", "b"), 300),
                VariableRef(0),
                StructValue(emptyList(), selfIndex = 6, endExclusive = 12, templateStructIndex = mapOf("y" to listOf(8))),
                FieldName(FakeSymbolToken("y", -1)),
                ListValue(emptyList(), selfIndex = 8, endExclusive = 12),
                BoolValue(emptyList(), true),
                BoolValue(emptyList(), false),
                VariableRef(2),
            )
        )
    )

    enum class ReaderType {
        ION_READER {
            override fun newMacroCompiler(reader: IonReader, macros: ((MacroRef) -> Macro?)): MacroCompiler {
                return MacroCompiler(macros, ReaderAdapterIonReader(reader))
            }
        },
        CONTINUABLE {
            override fun newMacroCompiler(reader: IonReader, macros: ((MacroRef) -> Macro?)): MacroCompiler {
                return MacroCompiler(macros, ReaderAdapterContinuable(reader as IonReaderContinuableCore))
            }
        };

        abstract fun newMacroCompiler(reader: IonReader, macros: ((MacroRef) -> Macro?)): MacroCompiler
    }

    private fun newReader(source: String): IonReader {
        // TODO these tests should be parameterized to exercise both text and binary input.
        return IonReaderBuilder.standard().build(TestUtils.ensureBinary(ion, source.toByteArray(StandardCharsets.UTF_8)))
    }

    private fun assertMacroCompilation(readerType: ReaderType, source: String, signature: List<Parameter>, body: List<TemplateBodyExpression>) {
        val reader = newReader(source)
        val compiler = readerType.newMacroCompiler(reader, fakeMacroTable)
        reader.next()
        val macroDef = compiler.compileMacro()
        val expectedDef = TemplateMacro(signature, body)
        assertEquals(expectedDef, macroDef)
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("testCases")
    fun assertMacroCompilationContinuable(source: String, signature: List<Parameter>, body: List<TemplateBodyExpression>) {
        assertMacroCompilation(ReaderType.CONTINUABLE, source, signature, body)
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("testCases")
    fun assertMacroCompilationIonReader(source: String, signature: List<Parameter>, body: List<TemplateBodyExpression>) {
        assertMacroCompilation(ReaderType.ION_READER, source, signature, body)
    }

    @ParameterizedTest
    @EnumSource(ReaderType::class)
    fun `test reading a list of macros`(readerType: ReaderType) {
        // This test case is essentially the same as the last one, except that it puts all the macro definitions into
        // one Ion list, and then compiles them sequentially from that list.
        // If this test fails, do not bother trying to fix it until all cases in the parameterized test are passing.
        val source = "[${testCases().joinToString(",") { it.source }}]"
        val templates = testCases().map { it.template }.iterator()

        val reader = newReader(source)
        val compiler = readerType.newMacroCompiler(reader, fakeMacroTable)
        // Advance and step into list
        reader.next(); reader.stepIn()
        while (reader.next() != null) {
            val macroDef = compiler.compileMacro()
            val expectedDef = templates.next()
            assertEquals(expectedDef, macroDef)
        }
        reader.stepOut()
        reader.close()
    }

    @ParameterizedTest
    @EnumSource(ReaderType::class)
    fun `macro compiler should return the correct name`(readerType: ReaderType) {
        val reader = newReader(
            """
            (macro foo (x) 1)
            (macro bar (y) 2)
            (macro null (z) 3)
        """
        )
        val compiler = readerType.newMacroCompiler(reader, fakeMacroTable)
        assertNull(compiler.macroName)
        reader.next()
        compiler.compileMacro()
        assertEquals("foo", compiler.macroName)
        reader.next()
        compiler.compileMacro()
        assertEquals("bar", compiler.macroName)
        reader.next()
        compiler.compileMacro()
        assertNull(compiler.macroName)
    }

    // macro with invalid variable
    // try compiling something that is not a sexp
    // macro missing keyword
    // macro has invalid name
    // macro has annotations

    private fun badMacros() = listOf(
        // There should be exactly one thing wrong in each of these samples.

        // Problems up to and including the macro name
        "[macro, pi, (), 3.141592653589793]", // Macro def must be a sexp
        "foo::(macro pi () 3.141592653589793)", // Macros cannot be annotated
        """("macro" pi () 3.141592653589793)""", // 'macro' must be a symbol
        "(pi () 3.141592653589793)", // doesn't start with 'macro'
        "(macaroon pi () 3.141592653589793)", // doesn't start with 'macro'
        "(macroeconomics pi () 3.141592653589793)", // will the demand for digits of pi ever match the supply?
        "(macro pi::pi () 3.141592653589793)", // Illegal annotation on macro name
        "(macro () 3.141592653589793)", // No macro name
        "(macro 2.5 () 3.141592653589793)", // Macro name is not a symbol
        """(macro "pi"() 3.141592653589793)""", // Macro name is not a symbol
        "(macro \$0 () 3.141592653589793)", // Macro name must have known text
        "(macro + () 123)", // Macro name cannot be an operator symbol
        "(macro 'a.b' () 123)", // Macro name must be a symbol that can be unquoted (i.e. an identifier symbol)
        "(macro 'false' () 123)", // Macro name must be a symbol that can be unquoted (i.e. an identifier symbol)

        // Problems in the signature
        "(macro identity x x)", // Missing sexp around signature
        "(macro identity [x] x)", // Using list instead of sexp for signature
        "(macro identity any::(x) x)", // Signature sexp should not be annotated
        "(macro identity (foo::x) x)", // Unknown type in signature
        "(macro identity (x any::*) x)", // Annotation should be on parameter name, not the cardinality
        "(macro identity (x! !) x)", // Dangling cardinality modifier
        "(macro identity (x%) x)", // Not a real cardinality sigil
        "(macro identity (x x) x)", // Repeated parameter name
        """(macro identity ("x") x)""", // Parameter name must be a symbol, not a string

        // Problems in the body
        "(macro empty ())", // No body expression
        "(macro transform (x) (%y))", // Unknown variable
        "(macro transform (x) foo::(%x))", // Variable expansion cannot be annotated
        "(macro transform (x) (foo::%x))", // Variable expansion operator cannot be annotated
        "(macro transform (x) (%foo::x))", // Variable name cannot be annotated
        "(macro transform (x) foo::(.values x))", // Macro invocation cannot be annotated
        "(macro transform (x) (foo::.values x))", // Macro invocation operator cannot be annotated
        "(macro transform (x) (.))", // Macro invocation operator must be followed by macro reference
        """(macro transform (x) (."values" x))""", // Macro invocation must start with a symbol or integer id
        """(macro transform (x) (.values foo::(..)))""", // Expression group may not be annotated
        """(macro transform (x) (.values (foo::..)))""", // Expression group operator may not be annotated
        "(macro transform (x) 1 2)", // Template body must be one expression
    )

    @ParameterizedTest
    @MethodSource("badMacros")
    fun assertCompilationFailsContinuable(source: String) {
        assertCompilationFails(ReaderType.CONTINUABLE, source)
    }

    @ParameterizedTest
    @MethodSource("badMacros")
    fun assertCompilationFailsIonReader(source: String) {
        assertCompilationFails(ReaderType.ION_READER, source)
    }

    private fun assertCompilationFails(readerType: ReaderType, source: String) {
        val reader = newReader(source)
        reader.next()
        val compiler = readerType.newMacroCompiler(reader, fakeMacroTable)
        assertThrows<IonException> { compiler.compileMacro() }
    }
}
