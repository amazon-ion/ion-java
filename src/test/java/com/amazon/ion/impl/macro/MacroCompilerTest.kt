// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.macro

import com.amazon.ion.*
import com.amazon.ion.impl.macro.Macro.*
import com.amazon.ion.impl.macro.Macro.ParameterEncoding.*
import com.amazon.ion.impl.macro.MacroRef.*
import com.amazon.ion.impl.macro.TemplateBodyExpression.*
import com.amazon.ion.system.IonSystemBuilder
import java.math.BigDecimal
import java.math.BigInteger
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.ValueSource

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MacroCompilerTest {

    val ion = IonSystemBuilder.standard().build()

    private data class MacroSourceAndTemplate(val source: String, val template: TemplateMacro) : Arguments {
        override fun get(): Array<Any> = arrayOf(source, template.signature, template.body)
    }

    private fun annotations(vararg a: String): List<SymbolToken> = a.map { FakeSymbolToken(it, -1) }

    private infix fun String.shouldCompileTo(macro: TemplateMacro) = MacroSourceAndTemplate(this, macro)

    private fun testCases() = listOf(
        "(macro identity (x) x)" shouldCompileTo TemplateMacro(
            listOf(Parameter("x", Tagged, ParameterCardinality.One)),
            listOf(Variable(0)),
        ),
        "(macro identity (any::x) x)" shouldCompileTo TemplateMacro(
            listOf(Parameter("x", Tagged, ParameterCardinality.One)),
            listOf(Variable(0)),
        ),
        "(macro pi () 3.141592653589793)" shouldCompileTo TemplateMacro(
            signature = emptyList(),
            body = listOf(DecimalValue(emptyList(), BigDecimal("3.141592653589793")))
        ),
        "(macro cardinality_test (x?) x)" shouldCompileTo TemplateMacro(
            signature = listOf(Parameter("x", Tagged, ParameterCardinality.AtMostOne)),
            body = listOf(Variable(0))
        ),
        "(macro cardinality_test (x!) x)" shouldCompileTo TemplateMacro(
            signature = listOf(Parameter("x", Tagged, ParameterCardinality.One)),
            body = listOf(Variable(0))
        ),
        "(macro cardinality_test (x+) x)" shouldCompileTo TemplateMacro(
            signature = listOf(Parameter("x", Tagged, ParameterCardinality.AtLeastOne)),
            body = listOf(Variable(0))
        ),
        "(macro cardinality_test (x*) x)" shouldCompileTo TemplateMacro(
            signature = listOf(Parameter("x", Tagged, ParameterCardinality.Any)),
            body = listOf(Variable(0))
        ),
        // Outer 'values' call allows multiple expressions in the body
        // The second `values` is a macro call that has a single argument: the variable `x`
        // The `literal` call causes the third (inner) `(values x)` to be an uninterpreted s-expression.
        """(macro literal_test (x) (values (values x) (literal (values x))))""" shouldCompileTo TemplateMacro(
            signature = listOf(Parameter("x", Tagged, ParameterCardinality.One)),
            body = listOf(
                MacroInvocation(ByName("values"), startInclusive = 0, endInclusive = 5),
                MacroInvocation(ByName("values"), startInclusive = 1, endInclusive = 2),
                Variable(0),
                SExpValue(emptyList(), startInclusive = 3, endInclusive = 5),
                SymbolValue(emptyList(), FakeSymbolToken("values", -1)),
                SymbolValue(emptyList(), FakeSymbolToken("x", -1)),
            ),
        ),
        "(macro each_type () (values null true 1 ${"9".repeat(50)} 1e0 1d0 2024-01-16T \"foo\" (literal bar) [] (literal ()) {} {{}} {{\"\"}} ))" shouldCompileTo TemplateMacro(
            signature = emptyList(),
            body = listOf(
                MacroInvocation(ByName("values"), 0, 14),
                NullValue(emptyList(), IonType.NULL),
                BoolValue(emptyList(), true),
                IntValue(emptyList(), 1),
                BigIntValue(emptyList(), BigInteger("9".repeat(50))),
                FloatValue(emptyList(), 1.0),
                DecimalValue(emptyList(), Decimal.ONE),
                TimestampValue(emptyList(), Timestamp.valueOf("2024-01-16T")),
                StringValue(emptyList(), "foo"),
                SymbolValue(emptyList(), FakeSymbolToken("bar", -1)),
                ListValue(emptyList(), startInclusive = 10, endInclusive = 10),
                SExpValue(emptyList(), startInclusive = 11, endInclusive = 11),
                StructValue(emptyList(), startInclusive = 12, endInclusive = 12, templateStructIndex = emptyMap()),
                BlobValue(emptyList(), ByteArray(0)),
                ClobValue(emptyList(), ByteArray(0))
            )
        ),
        """(macro foo () (values 42 "hello" false))""" shouldCompileTo TemplateMacro(
            signature = emptyList(),
            body = listOf(
                MacroInvocation(ByName("values"), startInclusive = 0, endInclusive = 3),
                IntValue(emptyList(), 42),
                StringValue(emptyList(), "hello"),
                BoolValue(emptyList(), false),
            )
        ),
        """(macro invoke_by_id () (12 true false))""" shouldCompileTo TemplateMacro(
            signature = emptyList(),
            body = listOf(
                MacroInvocation(ById(12), startInclusive = 0, endInclusive = 2),
                BoolValue(emptyList(), true),
                BoolValue(emptyList(), false),
            )
        ),
        "(macro null () \"abc\")" shouldCompileTo TemplateMacro(
            signature = emptyList(),
            body = listOf(StringValue(emptyList(), "abc"))
        ),
        "(macro foo (x y z) [100, [200, a::b::300], x, {y: [true, false, z]}])" shouldCompileTo TemplateMacro(
            signature = listOf(
                Parameter("x", Tagged, ParameterCardinality.One),
                Parameter("y", Tagged, ParameterCardinality.One),
                Parameter("z", Tagged, ParameterCardinality.One)
            ),
            body = listOf(
                ListValue(emptyList(), startInclusive = 0, endInclusive = 11),
                IntValue(emptyList(), 100),
                ListValue(emptyList(), startInclusive = 2, endInclusive = 4),
                IntValue(emptyList(), 200),
                IntValue(annotations("a", "b"), 300),
                Variable(0),
                StructValue(emptyList(), startInclusive = 6, endInclusive = 11, templateStructIndex = mapOf("y" to listOf(8))),
                FieldName(FakeSymbolToken("y", -1)),
                ListValue(emptyList(), startInclusive = 8, endInclusive = 11),
                BoolValue(emptyList(), true),
                BoolValue(emptyList(), false),
                Variable(2),
            )
        )
    )

    @ParameterizedTest(name = "{0}")
    @MethodSource("testCases")
    fun assertMacroCompilation(source: String, signature: List<Parameter>, body: List<TemplateBodyExpression>) {
        val reader = ion.newReader(source)
        val compiler = MacroCompiler(reader)
        reader.next()
        val macroDef = compiler.compileMacro()
        val expectedDef = TemplateMacro(signature, body)
        assertEquals(expectedDef, macroDef)
    }

    @Test
    fun `test reading a list of macros`() {
        // This test case is essentially the same as the last one, except that it puts all the macro definitions into
        // one Ion list, and then compiles them sequentially from that list.
        // If this test fails, do not bother trying to fix it until all cases in the parameterized test are passing.
        val source = "[${testCases().joinToString(",") { it.source }}]"
        val templates = testCases().map { it.template }.iterator()

        val reader = ion.newReader(source)
        val compiler = MacroCompiler(reader)
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

    @Test
    fun `macro compiler should return the correct name`() {
        val reader = ion.newReader(
            """
            (macro foo (x) 1)
            (macro bar (y) 2)
            (macro null (z) 3)
        """
        )
        val compiler = MacroCompiler(reader)
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

    @ParameterizedTest
    @ValueSource(
        strings = [
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
            "(macro transform (x) y)", // Unknown variable
            "(macro transform (x) foo::x)", // Variable cannot be annotated
            "(macro transform (x) foo::(literal x))", // Macro invocation cannot be annotated
            """(macro transform (x) ("literal" x))""", // Macro invocation must start with a symbol or integer id
            "(macro transform (x) 1 2)", // Template body must be one expression
        ]
    )
    fun assertCompilationFails(source: String) {
        val reader = ion.newReader(source)
        reader.next()
        val compiler = MacroCompiler(reader)
        assertThrows<IonException> { compiler.compileMacro() }
    }
}