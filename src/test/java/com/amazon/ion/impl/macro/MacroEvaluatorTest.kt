// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.macro

import com.amazon.ion.FakeSymbolToken
import com.amazon.ion.IonType
import com.amazon.ion.impl.macro.Expression.*
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class MacroEvaluatorTest {

    // Helper object with macro table entries that can be used to make the tests more concise.
    private object Macros {
        val MAKE_STRING = "make_string" to SystemMacro.MakeString
        val VALUES = "values" to SystemMacro.Values
        val IDENTITY = "identity" to template("x!", listOf(VariableRef(0)))

        val PI = "pi" to template("", listOf(FloatValue(emptyList(), 3.14159)))

        val FOO_STRUCT = "foo_struct" to template(
            "x*",
            listOf(
                StructValue(emptyList(), 0, 3, mapOf("foo" to listOf(2))),
                FieldName(FakeSymbolToken("foo", -1)),
                VariableRef(0),
            )
        )
    }

    @Test
    fun `a trivial constant macro evaluation`() {
        // Given:
        //   (macro pi () 3.14159)
        // When:
        //   (:pi)
        // Then:
        //   3.14159

        val evaluator = evaluatorWithMacroTable(Macros.PI)

        evaluator.initExpansion(
            listOf(
                EExpression(MacroRef.ByName("pi"), 0, 1)
            )
        )
        assertEquals(FloatValue(emptyList(), 3.14159), evaluator.expandNext())
        assertEquals(null, evaluator.expandNext())
    }

    @Test
    fun `a nested constant macro evaluation`() {
        // Given:
        //   (macro pi () 3.14159)
        //   (macro special_number () (pi))
        // When:
        //   (:special_number)
        // Then:
        //   3.14159

        val evaluator = evaluatorWithMacroTable(
            Macros.PI,
            "special_number" to template(
                "",
                listOf(
                    MacroInvocation(MacroRef.ByName("pi"), 0, 1)
                )
            ),
        )

        evaluator.initExpansion(
            listOf(
                EExpression(MacroRef.ByName("special_number"), 0, 1)
            )
        )
        assertEquals(FloatValue(emptyList(), 3.14159), evaluator.expandNext())
        assertEquals(null, evaluator.expandNext())
    }

    @Test
    fun `constant macro with empty list`() {
        // Given:
        //   (macro foo () [])
        // When:
        //   (:foo)
        // Then:
        //   []

        val evaluator = evaluatorWithMacroTable(
            "foo" to template(
                "",
                listOf(
                    ListValue(emptyList(), 0, 1)
                )
            )
        )

        evaluator.initExpansion(
            listOf(
                EExpression(MacroRef.ByName("foo"), 0, 1)
            )
        )

        assertIsInstance<ListValue>(evaluator.expandNext())
        evaluator.stepIn()
        assertEquals(null, evaluator.expandNext())
        evaluator.stepOut()
        assertEquals(null, evaluator.expandNext())
    }

    @Test
    fun `constant macro with single element list`() {
        // Given:
        //   (macro foo () ["a"])
        // When:
        //   (:foo)
        // Then:
        //   ["a"]

        val evaluator = evaluatorWithMacroTable(
            "foo" to template(
                "",
                listOf(
                    ListValue(emptyList(), 0, 2),
                    StringValue(value = "a"),
                )
            )
        )

        evaluator.initExpansion(
            listOf(
                EExpression(MacroRef.ByName("foo"), 0, 1)
            )
        )

        assertIsInstance<ListValue>(evaluator.expandNext())
        evaluator.stepIn()
        assertEquals(StringValue(value = "a"), evaluator.expandNext())
        assertEquals(null, evaluator.expandNext())
        evaluator.stepOut()
        assertEquals(null, evaluator.expandNext())
    }

    @Test
    fun `constant macro with multi element list`() {
        // Given:
        //   (macro ABCs () ["a", "b", "c"])
        // When:
        //   (:ABCs)
        // Then:
        //   [ "a", "b", "c" ]

        val evaluator = evaluatorWithMacroTable(
            "ABCs" to template(
                "",
                listOf(
                    ListValue(emptyList(), 0, 4),
                    StringValue(value = "a"),
                    StringValue(value = "b"),
                    StringValue(value = "c"),
                )
            )
        )

        evaluator.initExpansion(
            listOf(
                EExpression(MacroRef.ByName("ABCs"), 0, 1)
            )
        )

        assertIsInstance<ListValue>(evaluator.expandNext())
        evaluator.stepIn()
        assertEquals(StringValue(value = "a"), evaluator.expandNext())
        assertEquals(StringValue(value = "b"), evaluator.expandNext())
        assertEquals(StringValue(value = "c"), evaluator.expandNext())
        assertEquals(null, evaluator.expandNext())
        evaluator.stepOut()
        assertEquals(null, evaluator.expandNext())
    }

    @Test
    fun `it should be possible to step out of a container before the end is reached`() {
        // Given:
        //   (macro ABCs () ["a", "b", "c"])
        // When:
        //   (:ABCs)
        // Then:
        //   [ "a", "b", "c" ]

        val evaluator = evaluatorWithMacroTable(
            "ABCs" to template(
                "",
                listOf(
                    ListValue(emptyList(), 0, 4),
                    StringValue(value = "a"),
                    StringValue(value = "b"),
                    StringValue(value = "c"),
                )
            )
        )

        evaluator.initExpansion(
            listOf(
                EExpression(MacroRef.ByName("ABCs"), 0, 1)
            )
        )

        assertIsInstance<ListValue>(evaluator.expandNext())
        evaluator.stepIn()
        assertEquals(StringValue(value = "a"), evaluator.expandNext())
        evaluator.stepOut()
        assertEquals(null, evaluator.expandNext())
    }

    @Test
    fun `a trivial variable substitution`() {
        // Given:
        //   (macro identity (x!) x)
        // When:
        //   (:identity true)
        // Then:
        //   true

        val evaluator = evaluatorWithMacroTable(Macros.IDENTITY)

        evaluator.initExpansion(
            listOf(
                EExpression(MacroRef.ByName("identity"), 0, 2),
                BoolValue(emptyList(), true)
            )
        )

        assertEquals(BoolValue(emptyList(), true), evaluator.expandNext())
        assertEquals(null, evaluator.expandNext())
    }

    @Test
    fun `a trivial variable substitution with empty list`() {
        // Given:
        //   (macro identity (x!) x)
        // When:
        //   (:identity [])
        // Then:
        //   []

        val evaluator = evaluatorWithMacroTable(Macros.IDENTITY)

        evaluator.initExpansion(
            listOf(
                EExpression(MacroRef.ByName("identity"), 0, 2),
                ListValue(emptyList(), 1, 2)
            )
        )

        assertIsInstance<ListValue>(evaluator.expandNext())
        evaluator.stepIn()
        assertEquals(null, evaluator.expandNext())
        evaluator.stepOut()
        assertEquals(null, evaluator.expandNext())
    }

    @Test
    fun `a trivial variable substitution with single element list`() {
        // Given:
        //   (macro identity (x!) x)
        // When:
        //   (:identity ["a"])
        // Then:
        //   ["a"]

        val evaluator = evaluatorWithMacroTable(Macros.IDENTITY)

        evaluator.initExpansion(
            listOf(
                EExpression(MacroRef.ByName("identity"), 0, 3),
                ListValue(emptyList(), 1, 3),
                StringValue(value = "a"),
            )
        )

        assertIsInstance<ListValue>(evaluator.expandNext())
        evaluator.stepIn()
        assertEquals(StringValue(value = "a"), evaluator.expandNext())
        assertEquals(null, evaluator.expandNext())
        evaluator.stepOut()
        assertEquals(null, evaluator.expandNext())
    }

    @Test
    fun `a variable that gets used twice`() {
        // Given:
        //   (macro double_identity (x!) [x, x])
        // When:
        //   (:double_identity "a")
        // Then:
        //   ["a", "a"]

        val evaluator = evaluatorWithMacroTable(
            "double_identity" to template(
                "x!",
                listOf(
                    ListValue(emptyList(), 0, 3),
                    VariableRef(0),
                    VariableRef(0),
                )
            )
        )

        evaluator.initExpansion(
            listOf(
                EExpression(MacroRef.ByName("double_identity"), 0, 2),
                StringValue(value = "a"),
            )
        )

        assertIsInstance<ListValue>(evaluator.expandNext())
        evaluator.stepIn()
        assertEquals(StringValue(value = "a"), evaluator.expandNext())
        assertEquals(StringValue(value = "a"), evaluator.expandNext())
        assertEquals(null, evaluator.expandNext())
        evaluator.stepOut()
        assertEquals(null, evaluator.expandNext())
    }

    @Test
    fun `invoke values with scalars`() {
        // Given: <system_macros>
        // When:
        //   (:values 1 "a")
        // Then:
        //   1 "a"

        val evaluator = evaluatorWithMacroTable(Macros.VALUES)

        evaluator.initExpansion(
            listOf(
                EExpression(MacroRef.ByName("values"), 0, 4),
                ExpressionGroup(1, 4),
                LongIntValue(emptyList(), 1),
                StringValue(emptyList(), "a")
            )
        )

        assertEquals(LongIntValue(emptyList(), 1), evaluator.expandNext())
        assertEquals(StringValue(emptyList(), "a"), evaluator.expandNext())
        assertEquals(null, evaluator.expandNext())
    }

    @Test
    fun `a trivial nested variable substitution`() {
        // Given:
        //   (macro identity (x!) x)
        //   (macro nested_identity (x!) (identity x))
        // When:
        //   (:nested_identity true)
        // Then:
        //   true

        val evaluator = evaluatorWithMacroTable(
            Macros.IDENTITY,
            "nested_identity" to template(
                "x!",
                listOf(
                    MacroInvocation(MacroRef.ByName("identity"), 0, 2),
                    VariableRef(0)
                )
            ),
        )

        evaluator.initExpansion(
            listOf(
                EExpression(MacroRef.ByName("nested_identity"), 0, 2),
                BoolValue(emptyList(), true)
            )
        )

        assertEquals(BoolValue(emptyList(), true), evaluator.expandNext())
        assertEquals(null, evaluator.expandNext())
    }

    @Test
    fun `a trivial void variable substitution`() {
        // Given:
        //   (macro voidable_identity (x?) x)
        // When:
        //   (:voidable_identity (:))
        // Then:
        //   <nothing>

        val evaluator = evaluatorWithMacroTable(
            "voidable_identity" to template(
                "x?",
                listOf(
                    VariableRef(0)
                )
            )
        )

        evaluator.initExpansion(
            listOf(
                EExpression(MacroRef.ByName("voidable_identity"), 0, 1),
            )
        )

        assertEquals(null, evaluator.expandNext())
    }

    @Test
    fun `simple make_string`() {
        // Given: <system macros>
        // When:
        //   (:make_string "a" "b" "c")
        // Then:
        //   "abc"

        val evaluator = evaluatorWithMacroTable(Macros.MAKE_STRING)

        evaluator.initExpansion(
            listOf(
                EExpression(MacroRef.ByName("make_string"), 0, 5),
                ExpressionGroup(1, 5),
                StringValue(emptyList(), "a"),
                StringValue(emptyList(), "b"),
                StringValue(emptyList(), "c"),
            )
        )

        assertEquals(StringValue(emptyList(), "abc"), evaluator.expandNext())
        assertEquals(null, evaluator.expandNext())
    }

    @Test
    fun `nested make_string`() {
        // Given: <system macros>
        // When:
        //   (:make_string "a" (:make_string "b" "c" "d"))
        // Then:
        //   "abcd"

        val evaluator = evaluatorWithMacroTable(Macros.MAKE_STRING)

        evaluator.initExpansion(
            listOf(
                EExpression(MacroRef.ByName("make_string"), 0, 8),
                ExpressionGroup(1, 8),
                StringValue(emptyList(), "a"),
                EExpression(MacroRef.ByName("make_string"), 3, 8),
                ExpressionGroup(4, 8),
                StringValue(emptyList(), "b"),
                StringValue(emptyList(), "c"),
                StringValue(emptyList(), "d"),
            )
        )

        assertEquals(StringValue(emptyList(), "abcd"), evaluator.expandNext())
        assertEquals(null, evaluator.expandNext())
    }

    @Test
    fun `macro with a variable substitution in struct field position`() {
        // Given:
        //   (macro foo_struct (x*) {foo: x})
        // When:
        //   (:foo_struct bar)
        // Then:
        //   {foo: bar}

        val evaluator = evaluatorWithMacroTable(Macros.FOO_STRUCT)

        evaluator.initExpansion(
            listOf(
                EExpression(MacroRef.ByName("foo_struct"), 0, 2),
                StringValue(value = "bar")
            )
        )

        assertIsInstance<StructValue>(evaluator.expandNext())
        evaluator.stepIn()
        assertEquals(FieldName(FakeSymbolToken("foo", -1)), evaluator.expandNext())
        assertEquals(StringValue(value = "bar"), evaluator.expandNext())
        assertEquals(null, evaluator.expandNext())
        evaluator.stepOut()
        assertEquals(null, evaluator.expandNext())
    }

    @Test
    fun `macro with a variable substitution in struct field position with multiple arguments`() {
        // Given:
        //   (macro foo_struct (x*) {foo: x})
        // When:
        //   (:foo_struct (: bar baz))
        // Then:
        //   {foo: bar, foo: baz}

        val evaluator = evaluatorWithMacroTable(Macros.FOO_STRUCT)

        evaluator.initExpansion(
            listOf(
                EExpression(MacroRef.ByName("foo_struct"), 0, 4),
                ExpressionGroup(1, 4),
                StringValue(value = "bar"),
                StringValue(value = "baz")
            )
        )

        assertIsInstance<StructValue>(evaluator.expandNext())
        evaluator.stepIn()
        // Yes, the field name should be here only once. The Ion reader that wraps the evaluator
        // is responsible for carrying the field name over to any values that follow.
        assertEquals(FieldName(FakeSymbolToken("foo", -1)), evaluator.expandNext())
        assertEquals(StringValue(value = "bar"), evaluator.expandNext())
        assertEquals(StringValue(value = "baz"), evaluator.expandNext())
        assertEquals(null, evaluator.expandNext())
        evaluator.stepOut()
        assertEquals(null, evaluator.expandNext())
    }

    @Test
    fun `macro with a variable substitution in struct field position with void argument`() {
        // Given:
        //   (macro foo_struct (x*) {foo: x})
        // When:
        //   (:foo_struct (:))
        // Then:
        //   {}

        val evaluator = evaluatorWithMacroTable(Macros.FOO_STRUCT)

        evaluator.initExpansion(
            listOf(
                EExpression(MacroRef.ByName("foo_struct"), 0, 1),
                ExpressionGroup(1, 2),
            )
        )

        assertEquals(IonType.STRUCT, (evaluator.expandNext() as? DataModelValue)?.type)
        evaluator.stepIn()
        // Yes, the field name should be here. The Ion reader that wraps the evaluator
        // is responsible for discarding the field name if no values follow.
        assertEquals(FieldName(FakeSymbolToken("foo", -1)), evaluator.expandNext())
        assertEquals(null, evaluator.expandNext())
        evaluator.stepOut()
        assertEquals(null, evaluator.expandNext())
    }

    @Test
    fun `e-expression with another e-expression as one of the arguments`() {
        // Given:
        //   (macro pi () 3.14159)
        //   (macro identity (x) x)
        // When:
        //   (:identity (:pi))
        // Then:
        //   3.14159

        val evaluator = evaluatorWithMacroTable(Macros.IDENTITY, Macros.PI)

        evaluator.initExpansion(
            listOf(
                EExpression(MacroRef.ByName("identity"), 0, 2),
                EExpression(MacroRef.ByName("pi"), 1, 2),
            )
        )

        assertEquals(FloatValue(emptyList(), 3.14159), evaluator.expandNext())
        assertEquals(null, evaluator.expandNext())
    }

    companion object {

        /** Helper function to create template macros */
        fun template(parameters: String, body: List<TemplateBodyExpression>) = TemplateMacro(signature(parameters), body)

        /** Helper function to build a MacroEvaluator set up with a specific macro table */
        private fun evaluatorWithMacroTable(vararg idsToMacros: Pair<Any, Macro>): MacroEvaluator {
            return MacroEvaluator(
                EncodingContext(
                    idsToMacros.associate { (k, v) ->
                        when (k) {
                            is Number -> MacroRef.ById(k.toInt())
                            is String -> MacroRef.ByName(k)
                            else -> throw IllegalArgumentException("Unsupported macro id $k")
                        } to v
                    }
                )
            )
        }

        /** Helper function to turn a string into a signature. */
        private fun signature(text: String): List<Macro.Parameter> {
            if (text.isBlank()) return emptyList()
            return text.split(Regex(" +")).map {
                val cardinality = Macro.ParameterCardinality.fromSigil("${it.last()}")
                if (cardinality == null) {
                    Macro.Parameter(it, Macro.ParameterEncoding.Tagged, Macro.ParameterCardinality.ExactlyOne)
                } else {
                    Macro.Parameter(it.dropLast(1), Macro.ParameterEncoding.Tagged, cardinality)
                }
            }
        }

        private inline fun <reified T> assertIsInstance(value: Any?) {
            if (value !is T) {
                val message = if (value == null) {
                    "Expected instance of ${T::class.qualifiedName}; was null"
                } else if (null is T) {
                    "Expected instance of ${T::class.qualifiedName}?; was instance of ${value::class.qualifiedName}"
                } else {
                    "Expected instance of ${T::class.qualifiedName}; was instance of ${value::class.qualifiedName}"
                }
                Assertions.fail<Nothing>(message)
            }
        }
    }
}
