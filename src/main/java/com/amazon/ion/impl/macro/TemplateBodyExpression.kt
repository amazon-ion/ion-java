// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ion.impl.macro

import com.amazon.ion.IonType
import com.amazon.ion.SymbolToken
import com.amazon.ion.Timestamp
import java.math.BigDecimal
import java.math.BigInteger

/**
 * Represents an expression in the body of a template.
 *
 * We cannot use [`IonValue`](com.amazon.ion.IonValue) for this because `IonValue` requires references to parent
 * containers and to an IonSystem which makes it impractical for reading and writing macros definitions. Furthermore,
 * there is information we need to capture that cannot be expressed in the IonValue model, such as macro invocations
 * and variable references.
 *
 * A template body is compiled into a list of expressions, without nesting, for ease and efficiency of evaluating
 * e-expressions. Because of this, the container types do not have other values nested in them; rather they contain a
 * range that indicates which of the following expressions are part of that container.
 */
sealed interface TemplateBodyExpression {
    // TODO: Special Forms (if_void, for, ...)?

    /**
     * A temporary placeholder that is used only while a macro is partially compiled.
     */
    object Placeholder : TemplateBodyExpression

    // Scalars
    data class NullValue(val annotations: List<SymbolToken> = emptyList(), val type: IonType) : TemplateBodyExpression
    data class BoolValue(val annotations: List<SymbolToken> = emptyList(), val value: Boolean) : TemplateBodyExpression
    data class IntValue(val annotations: List<SymbolToken> = emptyList(), val value: Long) : TemplateBodyExpression
    data class BigIntValue(val annotations: List<SymbolToken> = emptyList(), val value: BigInteger) : TemplateBodyExpression
    data class FloatValue(val annotations: List<SymbolToken> = emptyList(), val value: Double) : TemplateBodyExpression
    data class DecimalValue(val annotations: List<SymbolToken> = emptyList(), val value: BigDecimal) : TemplateBodyExpression
    data class TimestampValue(val annotations: List<SymbolToken> = emptyList(), val value: Timestamp) : TemplateBodyExpression
    data class StringValue(val annotations: List<SymbolToken> = emptyList(), val value: String) : TemplateBodyExpression
    data class SymbolValue(val annotations: List<SymbolToken> = emptyList(), val value: SymbolToken) : TemplateBodyExpression
    // We must override hashcode and equals in the lob types because `value` is a `byte[]`
    data class BlobValue(val annotations: List<SymbolToken> = emptyList(), val value: ByteArray) : TemplateBodyExpression {
        override fun hashCode(): Int = annotations.hashCode() * 31 + value.contentHashCode()
        override fun equals(other: Any?): Boolean = other is BlobValue && annotations == other.annotations && value.contentEquals(other.value)
    }
    data class ClobValue(val annotations: List<SymbolToken> = emptyList(), val value: ByteArray) : TemplateBodyExpression {
        override fun hashCode(): Int = annotations.hashCode() * 31 + value.contentHashCode()
        override fun equals(other: Any?): Boolean = other is ClobValue && annotations == other.annotations && value.contentEquals(other.value)
    }

    /**
     * An Ion List that could contain variables or macro invocations.
     *
     * @property startInclusive the index of the first expression of the list (i.e. this instance)
     * @property endInclusive the index of the last expression contained in the list
     */
    data class ListValue(val annotations: List<SymbolToken> = emptyList(), val startInclusive: Int, val endInclusive: Int) : TemplateBodyExpression

    /**
     * An Ion SExp that could contain variables or macro invocations.
     */
    data class SExpValue(val annotations: List<SymbolToken> = emptyList(), val startInclusive: Int, val endInclusive: Int) : TemplateBodyExpression

    /**
     * An Ion Struct that could contain variables or macro invocations.
     */
    data class StructValue(val annotations: List<SymbolToken> = emptyList(), val startInclusive: Int, val endInclusive: Int, val templateStructIndex: Map<String, List<Int>>) : TemplateBodyExpression

    data class FieldName(val value: SymbolToken) : TemplateBodyExpression

    /**
     * A reference to a variable that needs to be expanded.
     */
    data class Variable(val signatureIndex: Int) : TemplateBodyExpression

    /**
     * A macro invocation that needs to be expanded.
     */
    data class MacroInvocation(val macro: MacroRef, val startInclusive: Int, val endInclusive: Int) : TemplateBodyExpression
}
