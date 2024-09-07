// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.macro

import com.amazon.ion.*
import com.amazon.ion.impl.macro.Expression.*
import java.math.BigInteger
import kotlin.reflect.KFunction1

/** A marker annotation for a [type-safe builder](https://kotlinlang.org/docs/type-safe-builders.html). */
@DslMarker
annotation class ExpressionBuilderDslMarker

/** Base DSL; functions are common for [DataModelExpression], [TemplateBodyExpression], and [EExpressionBodyExpression]. */
interface ValuesDsl {
    fun <T> annotated(annotations: List<SymbolToken>, valueFn: KFunction1<T, Unit>, value: T)
    fun nullValue(value: IonType = IonType.NULL)
    fun bool(value: Boolean)
    fun int(value: Long)
    fun int(value: BigInteger)
    fun float(value: Double)
    fun decimal(value: Decimal)
    fun timestamp(value: Timestamp)
    fun symbol(value: SymbolToken)
    fun string(value: String)
    fun clob(value: ByteArray)
    fun blob(value: ByteArray)

    /** Helper interface for use when building the content of a struct */
    interface Fields {
        fun fieldName(fieldName: SymbolToken)
        fun fieldName(fieldName: String) = fieldName(FakeSymbolToken(fieldName, -1))
    }
}

/** DSL for building [DataModelExpression] lists. */
@ExpressionBuilderDslMarker
interface DataModelDsl : ValuesDsl {
    fun list(content: DataModelDsl.() -> Unit)
    fun sexp(content: DataModelDsl.() -> Unit)
    fun struct(content: Fields.() -> Unit)

    @ExpressionBuilderDslMarker
    interface Fields : ValuesDsl.Fields, DataModelDsl
}

/** DSL for building [TemplateBodyExpression] lists. */
@ExpressionBuilderDslMarker
interface TemplateDsl : ValuesDsl {
    fun macro(macroRef: MacroRef, arguments: InvocationBody.() -> Unit)
    fun macro(id: Int, arguments: InvocationBody.() -> Unit) = macro(MacroRef.ById(id), arguments)
    fun macro(name: String, arguments: InvocationBody.() -> Unit) = macro(MacroRef.ByName(name), arguments)
    fun variable(signatureIndex: Int)
    fun list(content: TemplateDsl.() -> Unit)
    fun sexp(content: TemplateDsl.() -> Unit)
    fun struct(content: Fields.() -> Unit)

    @ExpressionBuilderDslMarker
    interface Fields : ValuesDsl.Fields, TemplateDsl

    @ExpressionBuilderDslMarker
    interface InvocationBody : TemplateDsl {
        fun expressionGroup(content: TemplateDsl.() -> Unit)
    }
}

/** DSL for building [EExpressionBodyExpression] lists. */
@ExpressionBuilderDslMarker
interface EExpDsl : ValuesDsl {
    fun eexp(macroRef: MacroRef, arguments: InvocationBody.() -> Unit)
    fun eexp(id: Int, arguments: InvocationBody.() -> Unit) = eexp(MacroRef.ById(id), arguments)
    fun eexp(name: String, arguments: InvocationBody.() -> Unit) = eexp(MacroRef.ByName(name), arguments)
    fun list(content: EExpDsl.() -> Unit)
    fun sexp(content: EExpDsl.() -> Unit)
    fun struct(content: Fields.() -> Unit)

    @ExpressionBuilderDslMarker
    interface Fields : ValuesDsl.Fields, EExpDsl

    @ExpressionBuilderDslMarker
    interface InvocationBody : EExpDsl {
        fun expressionGroup(content: EExpDsl.() -> Unit)
    }
}

/**
 * The implementation of all the expression builder DSL interfaces.
 *
 * How does this work? We implement everything in one class, but methods are exposed by being selective
 * about which interface we are using at any given time. For example, if you want to build a template
 * expression, you will get an interface that will not allow you to create an E-Expression. Likewise, if
 * you are building a struct, you will not get an interface with a method to create an expression group
 * in the middle of a struct (you must create a macro/eexp first).
 */
internal sealed class ExpressionBuilderDsl : ValuesDsl, ValuesDsl.Fields {

    companion object {
        // Entry points to the DSL builders.
        fun templateBody(block: TemplateDsl.() -> Unit): List<TemplateBodyExpression> = Template().apply(block).build()
        fun dataModel(block: DataModelDsl.() -> Unit): List<DataModelExpression> = DataModel().apply(block).build()
        fun eExpBody(block: EExpDsl.() -> Unit): List<EExpressionBodyExpression> = EExp().apply(block).build()
    }

    protected val expressions = mutableListOf<Expression>()
    private var pendingAnnotations = mutableListOf<SymbolToken>()

    override fun <T> annotated(annotations: List<SymbolToken>, valueFn: KFunction1<T, Unit>, value: T) {
        pendingAnnotations.addAll(annotations)
        valueFn.invoke(value)
    }

    override fun nullValue(value: IonType) = scalar(::NullValue, value)
    override fun bool(value: Boolean) = scalar(::BoolValue, value)
    override fun int(value: Long) = scalar(::LongIntValue, value)
    override fun int(value: BigInteger) = scalar(::BigIntValue, value)
    override fun float(value: Double) = scalar(::FloatValue, value)
    override fun decimal(value: Decimal) = scalar(::DecimalValue, value)
    override fun timestamp(value: Timestamp) = scalar(::TimestampValue, value)
    override fun symbol(value: SymbolToken) = scalar(::SymbolValue, value)
    override fun string(value: String) = scalar(::StringValue, value)
    override fun clob(value: ByteArray) = scalar(::ClobValue, value)
    override fun blob(value: ByteArray) = scalar(::BlobValue, value)

    override fun fieldName(fieldName: SymbolToken) { expressions.add(FieldName(fieldName)) }

    protected fun newStruct(annotations: List<SymbolToken>, structStart: Int, structEndExclusive: Int): StructValue {
        val nestedStructs = expressions
            .subList(structStart + 1, structEndExclusive)
            .filterIsInstance<StructValue>()
        val templateStructIndex = expressions
            .mapIndexed { i, it -> it to i }
            // Find all field names that are _not_ part of a nested struct
            .filter { (expr, i) ->
                expr is FieldName &&
                    nestedStructs.none { i > it.selfIndex && i < it.endExclusive } &&
                    structStart < i &&
                    i < structEndExclusive
            }
            .groupBy({ (expr, _) -> (expr as FieldName).value.text }) { (_, index) -> index + 1 }
        return StructValue(annotations, structStart, structEndExclusive, templateStructIndex)
    }

    fun <T : Expression> build(): List<T> = expressions.map { it as T }

    // Helpers
    private fun takePendingAnnotations(): List<SymbolToken> = pendingAnnotations.also { pendingAnnotations = mutableListOf() }

    private fun <T> scalar(constructor: (List<SymbolToken>, T) -> Expression, value: T) {
        expressions.add(constructor(takePendingAnnotations(), value))
    }

    protected fun <T : ExpressionBuilderDsl> container(content: T.() -> Unit, constructor: (Int, Int) -> Expression) {
        val selfIndex = expressions.size
        expressions.add(Placeholder)
        (this as T).content()
        expressions[selfIndex] = constructor(selfIndex, /* endExclusive= */ expressions.size)
    }

    protected fun <T : ExpressionBuilderDsl> containerWithAnnotations(content: T.() -> Unit, constructor: (List<SymbolToken>, Int, Int) -> Expression) {
        val ann = takePendingAnnotations()
        container(content) { start, end -> constructor(ann, start, end) }
    }

    // Subclasses for each expression variant so that we don't have conflicting signatures between their list, sexp, etc. implementations.

    class DataModel : ExpressionBuilderDsl(), DataModelDsl, DataModelDsl.Fields {
        override fun list(content: DataModelDsl.() -> Unit) = containerWithAnnotations(content, ::ListValue)
        override fun sexp(content: DataModelDsl.() -> Unit) = containerWithAnnotations(content, ::SExpValue)
        override fun struct(content: DataModelDsl.Fields.() -> Unit) = containerWithAnnotations(content, ::newStruct)
    }

    class EExp : ExpressionBuilderDsl(), EExpDsl, EExpDsl.Fields, EExpDsl.InvocationBody {
        override fun sexp(content: EExpDsl.() -> Unit) = containerWithAnnotations(content, ::SExpValue)
        override fun list(content: EExpDsl.() -> Unit) = containerWithAnnotations(content, ::ListValue)
        override fun struct(content: EExpDsl.Fields.() -> Unit) = containerWithAnnotations(content, ::newStruct)
        override fun eexp(macroRef: MacroRef, arguments: EExpDsl.InvocationBody.() -> Unit) = container(arguments) { start, end -> EExpression(macroRef, start, end) }
        override fun expressionGroup(content: EExpDsl.() -> Unit) = container(content, ::ExpressionGroup)
    }

    class Template : ExpressionBuilderDsl(), TemplateDsl, TemplateDsl.Fields, TemplateDsl.InvocationBody {
        override fun list(content: TemplateDsl.() -> Unit) = containerWithAnnotations(content, ::ListValue)
        override fun sexp(content: TemplateDsl.() -> Unit) = containerWithAnnotations(content, ::SExpValue)
        override fun struct(content: TemplateDsl.Fields.() -> Unit) = containerWithAnnotations(content, ::newStruct)
        override fun variable(signatureIndex: Int) { expressions.add(VariableRef(signatureIndex)) }
        override fun macro(macroRef: MacroRef, arguments: TemplateDsl.InvocationBody.() -> Unit) = container(arguments) { start, end -> MacroInvocation(macroRef, start, end) }
        override fun expressionGroup(content: TemplateDsl.() -> Unit) = container(content, ::ExpressionGroup)
    }
}
