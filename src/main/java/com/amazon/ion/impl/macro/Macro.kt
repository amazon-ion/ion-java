// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.macro

/**
 * A [Macro] is either a [SystemMacro] or a [TemplateMacro].
 */
sealed interface Macro {
    val signature: List<Parameter>

    data class Parameter(val variableName: String, val type: ParameterEncoding, val cardinality: ParameterCardinality) {
        override fun toString() = "$type::$variableName${cardinality.sigil}"
    }

    enum class ParameterEncoding(val ionTextName: String, val isTagless: Boolean = false) {
        Tagged("any"),
        uint8("uint8", isTagless = true),
        // TODO: List all of the possible tagless encodings
        // TODO: Update this to support macro shapes
    }

    enum class ParameterCardinality(val sigil: Char) {
        // TODO: Rename to match spec.
        AtMostOne('?'),
        One('!'),
        AtLeastOne('+'),
        Any('*');

        companion object {
            @JvmStatic
            fun fromSigil(sigil: String): ParameterCardinality? = when (sigil.singleOrNull()) {
                '?' -> AtMostOne
                '!' -> One
                '+' -> AtLeastOne
                '*' -> Any
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
    MakeString(listOf(Macro.Parameter("text", Macro.ParameterEncoding.Tagged, Macro.ParameterCardinality.Any))),
    // TODO: Other system macros
}
