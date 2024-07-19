// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.macro

import com.amazon.ion.impl.*

/**
 * A [Macro] is either a [SystemMacro] or a [TemplateMacro].
 */
sealed interface Macro {
    val signature: List<Parameter>

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
        CompactUInt("compact_uint", TaglessEncoding.FLEX_UINT),
        Int8("int8", TaglessEncoding.INT8),
        Int16("int16", TaglessEncoding.INT16),
        Int32("int32", TaglessEncoding.INT32),
        Int64("int64", TaglessEncoding.INT64),
        CompactInt("compact_int", TaglessEncoding.FLEX_INT),
        Float16("float16", TaglessEncoding.FLOAT16),
        Float32("float32", TaglessEncoding.FLOAT32),
        Float64("float64", TaglessEncoding.FLOAT64),
        CompactSymbol("compact_symbol", TaglessEncoding.COMPACT_SYMBOL),
        ;
        companion object {
            @JvmStatic
            fun fromPrimitiveType(taglessEncoding: TaglessEncoding) = when (taglessEncoding) {
                TaglessEncoding.UINT8 -> Uint8
                TaglessEncoding.UINT16 -> Uint16
                TaglessEncoding.UINT32 -> Uint32
                TaglessEncoding.UINT64 -> Uint64
                TaglessEncoding.FLEX_UINT -> CompactUInt
                TaglessEncoding.INT8 -> Int8
                TaglessEncoding.INT16 -> Int16
                TaglessEncoding.INT32 -> Int32
                TaglessEncoding.INT64 -> Int64
                TaglessEncoding.FLEX_INT -> CompactInt
                TaglessEncoding.FLOAT16 -> Float16
                TaglessEncoding.FLOAT32 -> Float32
                TaglessEncoding.FLOAT64 -> Float64
                TaglessEncoding.COMPACT_SYMBOL -> CompactSymbol
            }
        }
    }

    enum class ParameterCardinality(@JvmField val sigil: Char) {
        ZeroOrOne('?'),
        ExactlyOne('!'),
        OneOrMore('+'),
        ZeroOrMore('*');

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

/**
 * Represents a template macro. A template macro is defined by a signature, and a list of template expressions.
 * A template macro only gains a name and/or ID when it is added to a macro table.
 */
data class TemplateMacro(override val signature: List<Macro.Parameter>, val body: List<TemplateBodyExpression>) : Macro {
    private val cachedHashCode by lazy { signature.hashCode() * 31 + body.hashCode() }
    override fun hashCode(): Int = cachedHashCode

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is TemplateMacro) return false
        // Check the hashCode as a quick check before we dive into the actual data.
        if (cachedHashCode != other.cachedHashCode) return false
        if (signature != other.signature) return false
        if (body != other.body) return false
        return true
    }
}

/**
 * Macros that are built in, rather than being defined by a template.
 */
enum class SystemMacro(override val signature: List<Macro.Parameter>) : Macro {
    // TODO: replace these placeholders
    Stream(emptyList()), // A stream is technically not a macro, but we can implement it as a macro that is the identity function.
    Annotate(emptyList()),
    MakeString(listOf(Macro.Parameter("text", Macro.ParameterEncoding.Tagged, Macro.ParameterCardinality.ZeroOrMore))),
    // TODO: Other system macros
}
