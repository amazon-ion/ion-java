// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.macro

import com.amazon.ion.eexp.*
import com.amazon.ion.impl.TaglessEncoding

/**
 * A [Macro] is either a [SystemMacro] or a [TemplateMacro].
 */
sealed interface Macro {
    val signature: List<Parameter>
    val body: List<Expression.TemplateBodyExpression>?
    val dependencies: Iterable<Macro>

    fun createInvocation(): EExpressionArgumentBuilder<PreparedEExpression> = PreparedEExpressionArgumentBuilder(null, this)
    fun createInvocation(name: String): EExpressionArgumentBuilder<PreparedEExpression> = PreparedEExpressionArgumentBuilder(name, this)

    data class Parameter(val variableName: String, val type: ParameterEncoding, val cardinality: ParameterCardinality) {
        override fun toString() = "$type::$variableName${cardinality.sigil}"
    }

    // TODO: See if we can DRY up ParameterEncoding and PrimitiveType
    enum class ParameterEncoding(val ionTextName: String, @JvmField val taglessEncodingKind: TaglessEncoding? = null) {
        // TODO: Update this to support macro shapes
        Tagged("any"),
        Uint8("uint8", TaglessEncoding.UINT8),
        Uint16("uint16", TaglessEncoding.UINT16),
        Uint32("uint32", TaglessEncoding.UINT32),
        Uint64("uint64", TaglessEncoding.UINT64),
        FlexUint("flex_uint", TaglessEncoding.FLEX_UINT),
        Int8("int8", TaglessEncoding.INT8),
        Int16("int16", TaglessEncoding.INT16),
        Int32("int32", TaglessEncoding.INT32),
        Int64("int64", TaglessEncoding.INT64),
        FlexInt("flex_int", TaglessEncoding.FLEX_INT),
        Float16("float16", TaglessEncoding.FLOAT16),
        Float32("float32", TaglessEncoding.FLOAT32),
        Float64("float64", TaglessEncoding.FLOAT64),
        FlexString("flex_string", TaglessEncoding.FLEX_STRING),
        FlexSym("flex_sym", TaglessEncoding.FLEX_SYM),
        ;
        companion object {
            @JvmStatic
            fun fromPrimitiveType(taglessEncoding: TaglessEncoding) = when (taglessEncoding) {
                TaglessEncoding.UINT8 -> Uint8
                TaglessEncoding.UINT16 -> Uint16
                TaglessEncoding.UINT32 -> Uint32
                TaglessEncoding.UINT64 -> Uint64
                TaglessEncoding.FLEX_UINT -> FlexUint
                TaglessEncoding.INT8 -> Int8
                TaglessEncoding.INT16 -> Int16
                TaglessEncoding.INT32 -> Int32
                TaglessEncoding.INT64 -> Int64
                TaglessEncoding.FLEX_INT -> FlexInt
                TaglessEncoding.FLOAT16 -> Float16
                TaglessEncoding.FLOAT32 -> Float32
                TaglessEncoding.FLOAT64 -> Float64
                TaglessEncoding.FLEX_STRING -> FlexString
                TaglessEncoding.FLEX_SYM -> FlexSym
            }
        }
    }

    enum class ParameterCardinality(@JvmField val sigil: Char, @JvmField val canBeVoid: Boolean, @JvmField val canBeMulti: Boolean) {
        ZeroOrOne('?', true, false),
        ExactlyOne('!', false, false),
        OneOrMore('+', false, true),
        ZeroOrMore('*', true, true);

        companion object {
            @JvmStatic
            fun fromSigil(sigil: String): ParameterCardinality? = when (sigil.singleOrNull()) {
                '?' -> ZeroOrOne
                '!' -> ExactlyOne
                '+' -> OneOrMore
                '*' -> ZeroOrMore
                else -> null
            }
        }
    }
}
