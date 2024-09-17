// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.macro

import com.amazon.ion.FakeSymbolToken
import com.amazon.ion.IonType
import com.amazon.ion.impl.macro.Expression.*
import com.amazon.ion.impl.macro.ExpressionBuilderDsl.Companion.eExpBody
import com.amazon.ion.impl.macro.ExpressionBuilderDsl.Companion.templateBody
import com.amazon.ion.impl.macro.SystemMacro.*
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

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

    val ABCs_MACRO = template() {
        list {
            string("a")
            string("b")
            string("c")
        }
    }

    val evaluator = MacroEvaluator()

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
            eexp(ABCs_MACRO) {}
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
            eexp(ABCs_MACRO) {}
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
        //   (macro identity (x!) x)
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
        //   (macro identity (x!) x)
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
        //   (macro identity (x!) x)
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
        //   (macro double_identity (x!) [x, x])
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
        //   (macro identity (x!) x)
        //   (macro nested_identity (x!) (identity x))
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
        //   (macro voidable_identity (x?) x)
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
    fun `macro with a variable substitution in struct field position`() {
        // Given:
        //   (macro foo_struct (x*) {foo: x})
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
        //   (macro foo_struct (x*) {foo: x})
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
        //   (macro foo_struct (x*) {foo: x})
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
        //   (macro identity (x) x)
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

    companion object {

        /** Helper function to create template macros */
        fun template(vararg parameters: String, body: TemplateDsl.() -> Unit): Macro {
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
        fun MacroEvaluator.initExpansion(eExpression: EExpDsl.() -> Unit) = initExpansion(eExpBody(eExpression))

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
