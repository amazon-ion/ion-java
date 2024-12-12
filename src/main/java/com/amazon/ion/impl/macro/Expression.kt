// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.macro

import com.amazon.ion.*
import java.math.BigDecimal
import java.math.BigInteger

/**
 * In-memory expression model.
 *
 * We cannot use [`IonValue`](com.amazon.ion.IonValue) for this because `IonValue` requires references to parent
 * containers and to an IonSystem which makes it impractical for reading and writing macros definitions. Furthermore,
 * there is information we need to capture that cannot be expressed in the IonValue model, such as macro invocations
 * and variable references.
 *
 * Template bodies are compiled into a list of expressions, without nesting, for ease and efficiency of evaluating
 * e-expressions. Because of this, the container types do not have other values nested in them; rather they contain a
 * range that indicates which of the following expressions are part of that container.
 *
 * TODO: Consider creating an enum or integer-based expression type id so that we can `switch` efficiently on it.
 */
sealed interface Expression {

    /** Interface for expressions that "contain" other expressions */
    sealed interface HasStartAndEnd : Expression {
        /**
         * The position of this expression in its containing list.
         * Child expressions (if any) start at `selfIndex + 1`.
         */
        val selfIndex: Int
        /**
         * The index of the first child expression (if any).
         * Always equal to `selfIndex + 1`.
         */
        val startInclusive: Int get() = selfIndex + 1
        /**
         * The exclusive end of the child expressions (if any).
         * If there are no child expressions, will be equal to [startInclusive].
         */
        val endExclusive: Int
    }

    /** Marker interface representing expressions that can be present in E-Expressions. */
    sealed interface EExpressionBodyExpression : Expression

    /** Marker interface representing expressions in the body of a template. */
    sealed interface TemplateBodyExpression : Expression

    /**
     * Marker interface for things that are part of the Ion data model.
     * These expressions are the only ones that may be the output from the macro evaluator.
     * All [DataModelExpression]s are also valid to use as [TemplateBodyExpression]s and [EExpressionBodyExpression]s.
     */
    sealed interface DataModelExpression : Expression, EExpressionBodyExpression, TemplateBodyExpression, ExpansionOutputExpression

    sealed interface ExpansionOutputExpressionOrContinue

    sealed interface ExpansionOutputExpression : ExpansionOutputExpressionOrContinue

    /**
     * Interface for expressions that are _values_ in the Ion data model.
     */
    sealed interface DataModelValue : DataModelExpression {
        val annotations: List<SymbolToken>
        val type: IonType

        fun withAnnotations(annotations: List<SymbolToken>): DataModelValue
    }

    /** Expressions that represent Ion container types */
    sealed interface DataModelContainer : HasStartAndEnd, DataModelValue {
        val isConstructedFromMacro: Boolean
    }

    data object ContinueExpansion : ExpansionOutputExpressionOrContinue
    data object EndOfExpansion : ExpansionOutputExpression

    // TODO: See if we can remove this
    data object EndOfContainer : ExpansionOutputExpression // , DataModelExpression

    /**
     * A temporary placeholder that is used only while a macro or e-expression is partially compiled.
     *
     * TODO: See if we can get rid of this by e.g. using nulls during macro compilation.
     */
    data object Placeholder : TemplateBodyExpression, EExpressionBodyExpression

    /**
     * A group of expressions that form the argument for one macro parameter.
     *
     * TODO: Should we include the parameter name for ease of debugging?
     *       We'll hold off for now and see how the macro evaluator shakes out.
     *
     * @property selfIndex the index of the first expression of the expression group (i.e. this instance)
     * @property endExclusive the index of the last expression contained in the expression group
     */
    data class ExpressionGroup(override val selfIndex: Int, override val endExclusive: Int) : EExpressionBodyExpression, TemplateBodyExpression, HasStartAndEnd

    // Scalars
    data class NullValue(override val annotations: List<SymbolToken> = emptyList(), override val type: IonType) : DataModelValue {
        override fun withAnnotations(annotations: List<SymbolToken>) = copy(annotations = annotations)
    }

    data class BoolValue(override val annotations: List<SymbolToken> = emptyList(), val value: Boolean) : DataModelValue {
        override val type: IonType get() = IonType.BOOL
        override fun withAnnotations(annotations: List<SymbolToken>) = copy(annotations = annotations)
    }

    sealed interface IntValue : DataModelValue {
        val bigIntegerValue: BigInteger
        val longValue: Long
    }

    data class LongIntValue(override val annotations: List<SymbolToken> = emptyList(), val value: Long) : IntValue {
        override val type: IonType get() = IonType.INT
        override fun withAnnotations(annotations: List<SymbolToken>) = copy(annotations = annotations)
        override val bigIntegerValue: BigInteger get() = BigInteger.valueOf(value)
        override val longValue: Long get() = value
    }

    data class BigIntValue(override val annotations: List<SymbolToken> = emptyList(), val value: BigInteger) : IntValue {
        override val type: IonType get() = IonType.INT
        override fun withAnnotations(annotations: List<SymbolToken>) = copy(annotations = annotations)
        override val bigIntegerValue: BigInteger get() = value
        override val longValue: Long get() = value.longValueExact()
    }

    data class FloatValue(override val annotations: List<SymbolToken> = emptyList(), val value: Double) : DataModelValue {
        override val type: IonType get() = IonType.FLOAT
        override fun withAnnotations(annotations: List<SymbolToken>) = copy(annotations = annotations)
    }

    data class DecimalValue(override val annotations: List<SymbolToken> = emptyList(), val value: BigDecimal) : DataModelValue {
        override val type: IonType get() = IonType.DECIMAL
        override fun withAnnotations(annotations: List<SymbolToken>) = copy(annotations = annotations)
    }

    data class TimestampValue(override val annotations: List<SymbolToken> = emptyList(), val value: Timestamp) : DataModelValue {
        override val type: IonType get() = IonType.TIMESTAMP
        override fun withAnnotations(annotations: List<SymbolToken>) = copy(annotations = annotations)
    }

    sealed interface TextValue : DataModelValue {
        val stringValue: String
    }

    data class StringValue(override val annotations: List<SymbolToken> = emptyList(), val value: String) : TextValue {
        override val type: IonType get() = IonType.STRING
        override val stringValue: String get() = value
        override fun withAnnotations(annotations: List<SymbolToken>) = copy(annotations = annotations)
    }

    data class SymbolValue(override val annotations: List<SymbolToken> = emptyList(), val value: SymbolToken) : TextValue {
        override val type: IonType get() = IonType.SYMBOL
        override val stringValue: String get() = value.assumeText()
        override fun withAnnotations(annotations: List<SymbolToken>) = copy(annotations = annotations)
    }

    sealed interface LobValue : DataModelValue {
        // TODO: Consider replacing this with a ByteArray "View" that is backed by the original
        //       data source to avoid eagerly copying data.
        val value: ByteArray
    }

    // We must override hashcode and equals in the lob types because `value` is a `byte[]`
    data class BlobValue(override val annotations: List<SymbolToken> = emptyList(), override val value: ByteArray) : LobValue {
        override val type: IonType get() = IonType.BLOB
        override fun withAnnotations(annotations: List<SymbolToken>) = copy(annotations = annotations)
        override fun hashCode(): Int = annotations.hashCode() * 31 + value.contentHashCode()
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is BlobValue) return false
            if (other.annotations != this.annotations) return false
            return value === other.value || value.contentEquals(other.value)
        }
    }

    data class ClobValue(override val annotations: List<SymbolToken> = emptyList(), override val value: ByteArray) : LobValue {
        override val type: IonType get() = IonType.CLOB
        override fun withAnnotations(annotations: List<SymbolToken>) = copy(annotations = annotations)
        override fun hashCode(): Int = annotations.hashCode() * 31 + value.contentHashCode()
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is ClobValue) return false
            if (other.annotations != this.annotations) return false
            return value === other.value || value.contentEquals(other.value)
        }
    }

    /**
     * An Ion List that could contain variables or macro invocations.
     *
     * @property selfIndex the index of the first expression of the list (i.e. this instance)
     * @property endExclusive the index of the last expression contained in the list
     */
    data class ListValue @JvmOverloads constructor(
        override val annotations: List<SymbolToken> = emptyList(),
        override val selfIndex: Int,
        override val endExclusive: Int,
        override val isConstructedFromMacro: Boolean = false,
    ) : DataModelContainer {
        override val type: IonType get() = IonType.LIST
        override fun withAnnotations(annotations: List<SymbolToken>) = copy(annotations = annotations)
    }

    /**
     * An Ion SExp that could contain variables or macro invocations.
     */
    data class SExpValue @JvmOverloads constructor(
        override val annotations: List<SymbolToken> = emptyList(),
        override val selfIndex: Int,
        override val endExclusive: Int,
        override val isConstructedFromMacro: Boolean = false,
    ) : DataModelContainer {
        override val type: IonType get() = IonType.SEXP
        override fun withAnnotations(annotations: List<SymbolToken>) = copy(annotations = annotations)
    }

    /**
     * An Ion Struct that could contain variables or macro invocations.
     */
    data class StructValue @JvmOverloads constructor(
        override val annotations: List<SymbolToken> = emptyList(),
        override val selfIndex: Int,
        override val endExclusive: Int,
        val templateStructIndex: Map<String, List<Int>> = emptyMap(),
        override val isConstructedFromMacro: Boolean = false,
    ) : DataModelContainer {
        override val type: IonType get() = IonType.STRUCT
        override fun withAnnotations(annotations: List<SymbolToken>) = copy(annotations = annotations)
    }

    data class FieldName(val value: SymbolToken) : DataModelExpression

    /**
     * A reference to a variable that needs to be expanded.
     */
    data class VariableRef(val signatureIndex: Int) : TemplateBodyExpression

    /**
     * A macro invocation that needs to be expanded.
     */
    data class MacroInvocation(
        val macro: Macro,
        override val selfIndex: Int,
        override val endExclusive: Int
    ) : TemplateBodyExpression, HasStartAndEnd

    /**
     * An e-expression that needs to be expanded.
     */
    data class EExpression(
        val macro: Macro,
        override val selfIndex: Int,
        override val endExclusive: Int
    ) : EExpressionBodyExpression, HasStartAndEnd
}
