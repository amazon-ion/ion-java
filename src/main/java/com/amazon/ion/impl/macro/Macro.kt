// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.macro

import com.amazon.ion.impl.*
import com.amazon.ion.impl.macro.Macro.Parameter.Companion.zeroToManyTagged

/**
 * A [Macro] is either a [SystemMacro] or a [TemplateMacro].
 */
sealed interface Macro {
    val signature: List<Parameter>
    val dependencies: Iterable<Macro>

    data class Parameter(val variableName: String, val type: ParameterEncoding, val cardinality: ParameterCardinality) {
        override fun toString() = "$type::$variableName${cardinality.sigil}"

        companion object {
            @JvmStatic
            fun zeroToManyTagged(name: String) = Parameter(name, Macro.ParameterEncoding.Tagged, Macro.ParameterCardinality.ZeroOrMore)
            @JvmStatic
            fun exactlyOneTagged(name: String) = Parameter(name, Macro.ParameterEncoding.Tagged, Macro.ParameterCardinality.ExactlyOne)
        }
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

/**
 * Represents a template macro. A template macro is defined by a signature, and a list of template expressions.
 * A template macro only gains a name and/or ID when it is added to a macro table.
 */
data class TemplateMacro(override val signature: List<Macro.Parameter>, val body: List<Expression.TemplateBodyExpression>) : Macro {
    // TODO: Consider rewriting the body of the macro if we discover that there are any macros invoked using only
    //       constants as arguments—either at compile time or lazily.
    //       For example, the body of: (macro foo (x)  (values (make_string "foo" "bar") x))
    //       could be rewritten as: (values "foobar" x)

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

    override val dependencies: List<Macro> by lazy {
        body.filterIsInstance<Expression.MacroInvocation>()
            .map { it.macro }
            .distinct()
    }
}

/**
 * Macros that are built in, rather than being defined by a template.
 */
enum class SystemMacro(val macroName: String, override val signature: List<Macro.Parameter>) : Macro {
    Values("values", listOf(zeroToManyTagged("values"))),
    MakeString("make_string", listOf(zeroToManyTagged("text"))),
    // TODO: Other system macros
    ;

    override val dependencies: List<Macro>
        get() = emptyList()
}
