package com.amazon.ion.impl.macro

/**
 * A [Macro] is either a [SystemMacro] or a [TemplateMacro].
 */
sealed interface Macro {
    val signature: List<Parameter>

    data class Parameter(val variableName: String, val type: ParameterEncoding, val grouped: Boolean)

    enum class ParameterEncoding(val ionTextName: String) {
        Tagged("any"),
        // TODO: List all of the possible tagless encodings
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
    MakeString(listOf(Macro.Parameter("text", Macro.ParameterEncoding.Tagged, grouped = true))),
    // TODO: Other system macros
}
