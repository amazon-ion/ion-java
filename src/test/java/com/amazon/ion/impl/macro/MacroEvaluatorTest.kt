// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.macro

import com.amazon.ion.*
import com.amazon.ion.impl.*
import com.amazon.ion.impl.SystemSymbols_1_1.*
import com.amazon.ion.impl._Private_Utils.newSymbolToken
import com.amazon.ion.impl.bin.IonManagedWriter_1_1_Test.Companion.ion_encoding
import com.amazon.ion.impl.macro.Expression.*
import com.amazon.ion.impl.macro.ExpressionBuilderDsl.Companion.eExpBody
import com.amazon.ion.impl.macro.ExpressionBuilderDsl.Companion.templateBody
import com.amazon.ion.impl.macro.SystemMacro.*
import com.amazon.ion.system.IonSystemBuilder
import java.math.BigDecimal
import java.math.BigInteger
import java.util.Base64
import kotlin.contracts.ExperimentalContracts
import kotlin.contracts.contract
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments.arguments
import org.junit.jupiter.params.provider.MethodSource

class MacroEvaluatorTest {

    val IDENTITY_MACRO = template("x!") {
        variable(0)
    }

    val PI_MACRO = template() {
        float(3.14159)
    }

    val FOO_STRUCT_MACRO = template("x*") {
        struct {
            fieldName("foo")
            variable(0)
        }
    }

    val ABCs_LIST_MACRO = template {
        list {
            string("a")
            string("b")
            string("c")
        }
    }

    val evaluator = MacroEvaluator()

    @Test
    fun `the 'none' system macro`() {
        // Given: <system macros>
        // When:
        //   (:none)
        // Then:
        //   <nothing>

        evaluator.initExpansion {
            eexp(None) {}
        }

        assertEquals(null, evaluator.expandNext())
    }

    @Test
    fun `the 'none' system macro, invoked in TDL`() {
        // Given:
        //   (macro blackhole (any*) (.none))
        // When:
        //   (:blackhole "abc" 123 true)
        // Then:
        //   <nothing>

        val blackholeMacro = template("any*") {
            macro(None) {}
        }

        evaluator.initExpansion {
            eexp(blackholeMacro) {
                expressionGroup {
                    string("abc")
                    int(123)
                    bool(true)
                }
            }
        }

        assertEquals(null, evaluator.expandNext())
    }

    @Test
    fun `a trivial constant macro evaluation`() {
        // Given:
        //   (macro pi () 3.14159)
        // When:
        //   (:pi)
        // Then:
        //   3.14159

        evaluator.initExpansion {
            eexp(PI_MACRO) {}
        }

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

        val specialNumberMacro = template() {
            macro(PI_MACRO) {}
        }

        evaluator.initExpansion {
            eexp(specialNumberMacro) {}
        }

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

        val fooMacro = template() {
            list { }
        }

        evaluator.initExpansion {
            eexp(fooMacro) {}
        }

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

        val fooMacro = template() {
            list {
                string("a")
            }
        }

        evaluator.initExpansion {
            eexp(fooMacro) {}
        }

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

        evaluator.initExpansion {
            eexp(ABCs_LIST_MACRO) {}
        }

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

        evaluator.initExpansion {
            eexp(ABCs_LIST_MACRO) {}
        }

        assertIsInstance<ListValue>(evaluator.expandNext())
        evaluator.stepIn()
        assertEquals(StringValue(value = "a"), evaluator.expandNext())
        evaluator.stepOut()
        assertEquals(null, evaluator.expandNext())
    }

    @Test
    fun `a trivial variable substitution`() {
        // Given:
        //   (macro identity (x!) (%x))
        // When:
        //   (:identity true)
        // Then:
        //   true

        evaluator.initExpansion {
            eexp(IDENTITY_MACRO) {
                bool(true)
            }
        }

        assertEquals(BoolValue(emptyList(), true), evaluator.expandNext())
        assertEquals(null, evaluator.expandNext())
    }

    @Test
    fun `a trivial variable substitution with empty list`() {
        // Given:
        //   (macro identity (x!) (%x))
        // When:
        //   (:identity [])
        // Then:
        //   []

        evaluator.initExpansion {
            eexp(IDENTITY_MACRO) {
                list { }
            }
        }

        assertIsInstance<ListValue>(evaluator.expandNext())
        evaluator.stepIn()
        assertEquals(null, evaluator.expandNext())
        evaluator.stepOut()
        assertEquals(null, evaluator.expandNext())
    }

    @Test
    fun `a trivial variable substitution with single element list`() {
        // Given:
        //   (macro identity (x!) (%x))
        // When:
        //   (:identity ["a"])
        // Then:
        //   ["a"]

        evaluator.initExpansion {
            eexp(IDENTITY_MACRO) {
                list {
                    string("a")
                }
            }
        }

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
        //   (macro double_identity (x!) [(%x), (%x)])
        // When:
        //   (:double_identity "a")
        // Then:
        //   ["a", "a"]

        val doubleIdentity = template("x!") {
            list {
                variable(0)
                variable(0)
            }
        }

        evaluator.initExpansion {
            eexp(doubleIdentity) { string("a") }
        }

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

        evaluator.initExpansion {
            eexp(Values) {
                expressionGroup {
                    int(1)
                    string("a")
                }
            }
        }

        assertEquals(LongIntValue(emptyList(), 1), evaluator.expandNext())
        assertEquals(StringValue(emptyList(), "a"), evaluator.expandNext())
        assertEquals(null, evaluator.expandNext())
    }

    @Test
    fun `a trivial nested variable substitution`() {
        // Given:
        //   (macro identity (x!) (%x))
        //   (macro nested_identity (x!) (identity (%x)))
        // When:
        //   (:nested_identity true)
        // Then:
        //   true

        val nestedIdentity = template("x!") {
            macro(IDENTITY_MACRO) {
                variable(0)
            }
        }

        evaluator.initExpansion {
            eexp(nestedIdentity) {
                bool(true)
            }
        }

        assertEquals(BoolValue(emptyList(), true), evaluator.expandNext())
        assertEquals(null, evaluator.expandNext())
    }

    @Test
    fun `a trivial void variable substitution`() {
        // Given:
        //   (macro voidable_identity (x?) (%x))
        // When:
        //   (:voidable_identity (:))
        // Then:
        //   <nothing>

        val voidableIdentityMacro = template("x?") {
            variable(0)
        }

        evaluator.initExpansion {
            eexp(voidableIdentityMacro) {}
        }

        assertEquals(null, evaluator.expandNext())
    }

    @Test
    fun `simple make_string`() {
        // Given: <system macros>
        // When:
        //   (:make_string "a" "b" "c")
        // Then:
        //   "abc"

        evaluator.initExpansion {
            eexp(MakeString) {
                expressionGroup {
                    string("a")
                    string("b")
                    string("c")
                }
            }
        }

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

        evaluator.initExpansion {
            eexp(MakeString) {
                expressionGroup {
                    string("a")
                    eexp(MakeString) {
                        expressionGroup {
                            string("b")
                            string("c")
                            string("d")
                        }
                    }
                }
            }
        }

        assertEquals(StringValue(emptyList(), "abcd"), evaluator.expandNext())
        assertEquals(null, evaluator.expandNext())
    }

    @Test
    fun `simple make_symbol`() {
        // Given: <system macros>
        // When:
        //   (:make_symbol "a" "b" "c")
        // Then:
        //   abc

        evaluator.initExpansion {
            eexp(MakeSymbol) {
                expressionGroup {
                    string("a")
                    string("b")
                    string("c")
                }
            }
        }

        val expr = evaluator.expandNext()
        assertIsInstance<SymbolValue>(expr)
        assertEquals("abc", expr.value.text)
        assertEquals(null, evaluator.expandNext())
    }

    @Test
    fun `simple make_blob`() {
        // Given: <system macros>
        // When:
        //   (:make_blob {{"abc"}} {{ 4AEB6g== }})
        // Then:
        //  {{ YWJj4AEB6g== }}

        evaluator.initExpansion {
            eexp(MakeBlob) {
                expressionGroup {
                    clob("abc".toByteArray())
                    blob(Base64.getDecoder().decode("4AEB6g=="))
                }
            }
        }

        val expr = evaluator.expandNext()
        assertIsInstance<BlobValue>(expr)
        assertArrayEquals(Base64.getDecoder().decode("YWJj4AEB6g=="), expr.value)
        assertEquals(null, evaluator.expandNext())
    }

    @Test
    fun `simple make_decimal`() {
        // Given: <system macros>
        // When:
        //   (:make_decimal 2 4)
        // Then:
        //   2d4

        evaluator.initExpansion {
            eexp(MakeDecimal) {
                int(2)
                int(4)
            }
        }

        val expr = evaluator.expandNext()
        assertIsInstance<DecimalValue>(expr)
        assertTrue(BigDecimal.valueOf(20000).compareTo(expr.value) == 0)
        assertEquals(BigInteger.valueOf(2), expr.value.unscaledValue())
        assertEquals(-4, expr.value.scale())
        assertEquals(null, evaluator.expandNext())
    }

    @Test
    fun `make_decimal from nested expressions`() {
        // Given:
        //   (macro fixed_point (x) (.make_decimal (%x) (.values -2)))
        // When:
        //   (:fixed_point (:identity 123))
        // Then:
        //   1.23

        val fixedPointMacro = template("x") {
            macro(MakeDecimal) {
                variable(0)
                macro(Values) {
                    expressionGroup {
                        int(-2)
                    }
                }
            }
        }

        evaluator.initExpansion {
            eexp(fixedPointMacro) {
                eexp(IDENTITY_MACRO) {
                    int(123)
                }
            }
        }

        val expr = evaluator.expandNext()
        assertIsInstance<DecimalValue>(expr)
        assertEquals(BigDecimal.valueOf(123, 2), expr.value)
        assertEquals(null, evaluator.expandNext())
    }

    @Test
    fun `simple make_field`() {
        // Given: <system macros>
        // When:
        //   (:make_field foo 1)
        // Then:
        //   { foo: 1 }

        evaluator.initExpansion {
            eexp(MakeField) {
                symbol(newSymbolToken("foo"))
                int(1)
            }
        }

        assertEquals(FieldName(value = newSymbolToken("foo")), evaluator.expandNext())
        assertEquals(LongIntValue(value = 1), evaluator.expandNext())
        assertEquals(null, evaluator.expandNext())
    }

    @Test
    fun `simple annotate`() {
        // Given: <system macros>
        // When:
        //   (:annotate (:: "a" "b" "c") 1)
        // Then:
        //   a::b::c::1

        evaluator.initExpansion {
            eexp(Annotate) {
                expressionGroup {
                    string("a")
                    string("b")
                    string("c")
                }
                int(1)
            }
        }

        val expr = evaluator.expandNext()
        assertIsInstance<LongIntValue>(expr)
        assertEquals(listOf("a", "b", "c"), expr.annotations.map { it.text })
        assertEquals(1, expr.value)
        assertEquals(null, evaluator.expandNext())
    }

    @Test
    fun `annotate a value that already has some annotations`() {
        // Given: <system macros>
        // When:
        //   (:annotate (:: "a" "b") c::1)
        // Then:
        //   a::b::c::1

        evaluator.initExpansion {
            eexp(Annotate) {
                expressionGroup {
                    string("a")
                    string("b")
                }
                annotated(listOf(newSymbolToken("c")), ::int, 1)
            }
        }

        val expr = evaluator.expandNext()
        assertIsInstance<LongIntValue>(expr)
        assertEquals(listOf("a", "b", "c"), expr.annotations.map { it.text })
        assertEquals(1, expr.value)
        assertEquals(null, evaluator.expandNext())
    }

    @Test
    fun `annotate a container`() {
        // Given: <system macros>
        // When:
        //   (:annotate (:: "a" "b" "c") [1])
        // Then:
        //   a::b::c::[1]

        evaluator.initExpansion {
            eexp(Annotate) {
                expressionGroup {
                    string("a")
                    string("b")
                    string("c")
                }
                list {
                    int(1)
                }
            }
        }

        val expr = evaluator.expandNext()
        assertIsInstance<ListValue>(expr)
        assertEquals(listOf("a", "b", "c"), expr.annotations.map { it.text })
        evaluator.stepIn()
        assertEquals(LongIntValue(emptyList(), 1), evaluator.expandNext())
        assertEquals(null, evaluator.expandNext())
        evaluator.stepOut()
        assertEquals(null, evaluator.expandNext())
    }

    @Test
    fun `annotate with nested make_string`() {
        // Given: <system macros>
        // When:
        //   (:annotate (:make_string (:: "a" "b" "c")) 1)
        // Then:
        //   abc::1

        evaluator.initExpansion {
            eexp(Annotate) {
                eexp(MakeString) {
                    expressionGroup {
                        string("a")
                        string("b")
                        string("c")
                    }
                }
                int(1)
            }
        }

        val expr = evaluator.expandNext()
        assertIsInstance<LongIntValue>(expr)
        assertEquals(listOf("abc"), expr.annotations.map { it.text })
        assertEquals(1, expr.value)
        assertEquals(null, evaluator.expandNext())
    }

    @Test
    fun `annotate an e-expression result`() {
        // Given: <system macros>
        // When:
        //   (:annotate (:: "a" "b" "c") (:make_string "d" "e" "f"))
        // Then:
        //   a::b::c::"def"

        evaluator.initExpansion {
            eexp(Annotate) {
                expressionGroup {
                    string("a")
                    string("b")
                    string("c")
                }

                eexp(MakeString) {
                    expressionGroup {
                        string("d")
                        string("e")
                        string("f")
                    }
                }
            }
        }

        val expr = evaluator.expandNext()
        assertIsInstance<StringValue>(expr)
        assertEquals(listOf("a", "b", "c"), expr.annotations.map { it.text })
        assertEquals("def", expr.value)
        assertEquals(null, evaluator.expandNext())
    }

    @Test
    fun `annotate a TDL macro invocation result`() {
        // Given:
        //   (macro pi () 3.14159)
        //   (macro annotate_pi (x) (.annotate (..(%x)) (.pi)))
        // When:
        //   (:annotate_pi "foo")
        // Then:
        //   foo::3.14159

        val annotatePi = template("x") {
            macro(Annotate) {
                expressionGroup {
                    variable(0)
                }
                macro(PI_MACRO) {}
            }
        }

        evaluator.initExpansion {
            eexp(annotatePi) {
                string("foo")
            }
        }

        val expr = evaluator.expandNext()
        assertIsInstance<FloatValue>(expr)
        assertEquals(listOf("foo"), expr.annotations.map { it.text })
        assertEquals(3.14159, expr.value)
        assertEquals(null, evaluator.expandNext())
    }

    @Test
    fun `macro with a variable substitution in struct field position`() {
        // Given:
        //   (macro foo_struct (x*) {foo: (%x)})
        // When:
        //   (:foo_struct bar)
        // Then:
        //   {foo: bar}

        evaluator.initExpansion {
            eexp(FOO_STRUCT_MACRO) {
                string("bar")
            }
        }

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
        //   (macro foo_struct (x*) {foo: (%x)})
        // When:
        //   (:foo_struct (: bar baz))
        // Then:
        //   {foo: bar, foo: baz}

        evaluator.initExpansion {
            eexp(FOO_STRUCT_MACRO) {
                expressionGroup {
                    string("bar")
                    string("baz")
                }
            }
        }

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
        //   (macro foo_struct (x*) {foo: (%x)})
        // When:
        //   (:foo_struct (:))
        // Then:
        //   {}

        evaluator.initExpansion {
            eexp(FOO_STRUCT_MACRO) {
                expressionGroup { }
            }
        }

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
        //   (macro identity (x) (%x))
        // When:
        //   (:identity (:pi))
        // Then:
        //   3.14159

        evaluator.initExpansion {
            eexp(IDENTITY_MACRO) {
                eexp(PI_MACRO) {}
            }
        }

        assertEquals(FloatValue(emptyList(), 3.14159), evaluator.expandNext())
        assertEquals(null, evaluator.expandNext())
    }

    object IfExpanderTestParameters {
        val SINGLE_VALUE = template { int(1) }
        val SINGLE_VALUE_STREAM = template {
            macro(Values) {
                expressionGroup {
                    int(2)
                }
            }
        }
        val TWO_VALUE_STREAM = template {
            macro(Values) {
                expressionGroup {
                    int(3)
                    int(4)
                }
            }
        }

        @JvmStatic
        fun parameters() = listOf(
            arguments(IfNone, None, true),
            arguments(IfNone, SINGLE_VALUE, false),
            arguments(IfNone, SINGLE_VALUE_STREAM, false),
            arguments(IfNone, TWO_VALUE_STREAM, false),

            arguments(IfSome, None, false),
            arguments(IfSome, SINGLE_VALUE, true),
            arguments(IfSome, SINGLE_VALUE_STREAM, true),
            arguments(IfSome, TWO_VALUE_STREAM, true),

            arguments(IfSingle, None, false),
            arguments(IfSingle, SINGLE_VALUE, true),
            arguments(IfSingle, SINGLE_VALUE_STREAM, true),
            arguments(IfSingle, TWO_VALUE_STREAM, false),

            arguments(IfMulti, None, false),
            arguments(IfMulti, SINGLE_VALUE, false),
            arguments(IfMulti, SINGLE_VALUE_STREAM, false),
            arguments(IfMulti, TWO_VALUE_STREAM, true),
        )
    }

    @ParameterizedTest
    @MethodSource("com.amazon.ion.impl.macro.MacroEvaluatorTest\$IfExpanderTestParameters#parameters")
    fun `check 'if' expansion logic`(ifSpecialForm: SystemMacro, expressionToTest: Macro, expectMatches: Boolean) {
        // Given:
        //   (macro test_if (x*) (<ifSpecialForm> (%x) "a" "b"))
        // When:
        //   (:test_if <expressionToTest>)
        // Then:
        //   "a" or "b" depending on whether we expect it to match.

        val theMacro = template("x*") {
            macro(ifSpecialForm) {
                variable(0)
                string("a")
                string("b")
            }
        }

        evaluator.initExpansion { eexp(theMacro) { eexp(expressionToTest) {} } }

        val expectedString = if (expectMatches) "a" else "b"
        assertEquals(StringValue(value = expectedString), evaluator.expandNext())
        assertEquals(null, evaluator.expandNext())
    }

    companion object {
        /** Helper function to create template macros */
        internal fun template(vararg parameters: String, body: TemplateDsl.() -> Unit): Macro {
            val signature = parameters.map {
                val cardinality = Macro.ParameterCardinality.fromSigil("${it.last()}")
                if (cardinality == null) {
                    Macro.Parameter(it, Macro.ParameterEncoding.Tagged, Macro.ParameterCardinality.ExactlyOne)
                } else {
                    Macro.Parameter(it.dropLast(1), Macro.ParameterEncoding.Tagged, cardinality)
                }
            }
            return TemplateMacro(signature, templateBody(body))
        }

        /** Helper function to use Expression DSL for evaluator inputs */
        internal fun MacroEvaluator.initExpansion(eExpression: EExpDsl.() -> Unit) = initExpansion(eExpBody(eExpression))

        @OptIn(ExperimentalContracts::class)
        private inline fun <reified T> assertIsInstance(value: Any?) {
            contract { returns() implies (value is T) }
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

        /**
         * Helper function for testing the output of macro invocations.
         */
        fun MacroEvaluator.assertExpansion(expectedOutput: String) {
            val ion = IonSystemBuilder.standard().build() as _Private_IonSystem
            val actual = mutableListOf<IonValue>()
            ion.systemIterate(MacroEvaluatorAsIonReader(this)).forEachRemaining(actual::add)
            val expected = mutableListOf<IonValue>()
            ion.systemIterate(expectedOutput).forEachRemaining(expected::add)
            assertEquals(expected, actual)
        }
    }

    @Test
    fun `invoke repeat with n=1 and value is single expression`() {
        // Given: <system macros>
        // When:
        //   (:repeat 1 0)
        // Then:
        //   0

        evaluator.initExpansion {
            eexp(Repeat) {
                int(1)
                int(0)
            }
        }

        assertEquals(LongIntValue(value = 0), evaluator.expandNext())
        assertEquals(null, evaluator.expandNext())
    }

    @Test
    fun `invoke repeat with n=1 and value is multiple expressions`() {
        // Given: <system macros>
        // When:
        //   (:repeat 1 (:: "a" "b"))
        // Then:
        //   "a" "b"

        evaluator.initExpansion {
            eexp(Repeat) {
                int(1)
                expressionGroup {
                    string("a")
                    string("b")
                }
            }
        }

        assertEquals(StringValue(value = "a"), evaluator.expandNext())
        assertEquals(StringValue(value = "b"), evaluator.expandNext())
        assertEquals(null, evaluator.expandNext())
    }

    @Test
    fun `invoke repeat with n=2 and value is single expression`() {
        // Given: <system macros>
        // When:
        //   (:repeat 2 0)
        // Then:
        //   0 0

        evaluator.initExpansion {
            eexp(Repeat) {
                int(2)
                int(0)
            }
        }

        assertEquals(LongIntValue(value = 0), evaluator.expandNext())
        assertEquals(LongIntValue(value = 0), evaluator.expandNext())
        assertEquals(null, evaluator.expandNext())
    }

    @Test
    fun `invoke repeat with n=2 and value is multiple expressions`() {
        // Given: <system macros>
        // When:
        //   (:repeat 2 (:: "a" "b"))
        // Then:
        //   "a" "b" "a" "b"

        evaluator.initExpansion {
            eexp(Repeat) {
                int(2)
                expressionGroup {
                    string("a")
                    string("b")
                }
            }
        }

        assertEquals(StringValue(value = "a"), evaluator.expandNext())
        assertEquals(StringValue(value = "b"), evaluator.expandNext())
        assertEquals(StringValue(value = "a"), evaluator.expandNext())
        assertEquals(StringValue(value = "b"), evaluator.expandNext())
        assertEquals(null, evaluator.expandNext())
    }

    @Test
    fun `invoke repeat macro with a variable for n`() {
        // Given:
        //   (macro (x) (.make_string (.repeat (%x) "na ") "batman!"))
        // When:
        //   (:batman 16)
        // Then:
        //   "na na na na na na na na na na na na na na na na batman!"

        val theMacro = template("x") {
            macro(MakeString) {
                expressionGroup {
                    macro(Repeat) {
                        int(16)
                        string("na ")
                    }
                    string("batman!")
                }
            }
        }

        evaluator.initExpansion {
            eexp(theMacro) { int(16) }
        }

        assertEquals(StringValue(value = "na na na na na na na na na na na na na na na na batman!"), evaluator.expandNext())
        assertEquals(null, evaluator.expandNext())
    }

    @Test
    fun `invoke repeat with invalid 'n' argument`() {
        // Given:
        //   <system macros>
        // When:
        //   (:repeat -1 "a")
        // Then:
        //   <IonException>

        evaluator.initExpansion {
            eexp(Repeat) {
                int(-1)
                string("a")
            }
        }

        assertThrows<IonException> { evaluator.expandNext() }
    }

    @Test
    fun `invoke repeat with empty 'value' argument`() {
        // Given:
        //   <system macros>
        // When:
        //   (:repeat 3 (:values))
        // Then:
        //   <nothing>

        evaluator.initExpansion {
            eexp(Repeat) {
                int(3)
                eexp(Values) { expressionGroup { } }
            }
        }

        assertNull(evaluator.expandNext())
    }

    @Test
    fun `the comment macro expands to nothing`() {
        // Given: <system macros>
        // When:
        //   (:comment 1 2 3)
        // Then:
        //   <nothing>

        evaluator.initExpansion {
            eexp(Comment) {
                expressionGroup {
                    int(1)
                    int(2)
                    int(3)
                }
            }
        }

        assertNull(evaluator.expandNext())
    }

    @Test
    fun `set_symbols expands to an encoding directive that replaces the symbol table and preserves macros`() {
        evaluator.initExpansion {
            // (:set_symbols a b c)
            eexp(SetSymbols) {
                expressionGroup {
                    symbol("a")
                    symbol("b")
                    symbol("c")
                }
            }
        }
        evaluator.assertExpansion(
            """
            $ion_encoding::(
              (symbol_table [a, b, c])
              (macro_table $ion_encoding)
            )
            """
        )
    }

    @Test
    fun `add_symbols expands to an encoding directive that appends to the symbol table and preserves macros`() {
        evaluator.initExpansion {
            // (:add_symbols a b c)
            eexp(AddSymbols) {
                expressionGroup {
                    symbol("a")
                    symbol("b")
                    symbol("c")
                }
            }
        }
        evaluator.assertExpansion(
            """
            $ion_encoding::(
              (symbol_table $ion_encoding [a, b, c])
              (macro_table $ion_encoding)
            )
            """
        )
    }

    @Test
    fun `set_macros expands to an encoding directive that preserves symbols and replaces the macro table`() {
        evaluator.initExpansion {
            // (:set_macros (macro answer () 42))
            eexp(SetMacros) {
                expressionGroup {
                    sexp {
                        symbol(MACRO)
                        symbol("answer")
                        sexp { }
                        int(42)
                    }
                }
            }
        }
        evaluator.assertExpansion(
            """
            $ion_encoding::(
              (symbol_table $ion_encoding)
              (macro_table
                (macro answer () 42))
            )
            """
        )
    }

    @Test
    fun `add_macros expands to an encoding directive that preserves symbols and appends to the macro table`() {
        evaluator.initExpansion {
            // (:add_macros (macro answer () 42))
            eexp(AddMacros) {
                expressionGroup {
                    sexp {
                        symbol(MACRO)
                        symbol("answer")
                        sexp { }
                        int(42)
                    }
                }
            }
        }
        evaluator.assertExpansion(
            """
            $ion_encoding::(
              (symbol_table $ion_encoding)
              (macro_table
                $ion_encoding
                (macro answer () 42))
            )
            """
        )
    }

    @Test
    fun `use expands to an encoding directive that imports a module and appends it to the symbol and macro tables`() {
        evaluator.initExpansion {
            // (:use "com.amazon.Foo" 2)
            eexp(Use) {
                string("com.amazon.Foo")
                int(2)
            }
        }
        evaluator.assertExpansion(
            """
            $ion_encoding::(
              (import the_module "com.amazon.Foo" 2)
              (symbol_table $ion_encoding the_module)
              (macro_table $ion_encoding the_module)
            )
            """
        )
    }

    @Test
    fun `use defaults to version 1 if no version is provided`() {
        evaluator.initExpansion {
            // (:use "com.amazon.Foo")
            eexp(Use) {
                string("com.amazon.Foo")
                expressionGroup { }
            }
        }
        evaluator.assertExpansion(
            """
            $ion_encoding::(
              (import the_module "com.amazon.Foo" 1)
              (symbol_table $ion_encoding the_module)
              (macro_table $ion_encoding the_module)
            )
            """
        )
    }
}
