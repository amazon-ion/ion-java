package com.amazon.ion.impl.macro

import java.math.BigDecimal

/**
 * Marker interface for Macros
 */
sealed interface Macro

/**
 * Represents a template macro. A template macro is defined by a name, a signature, and a list of template expressions.
 */
data class TemplateMacro(val name: String, val f: BigDecimal, val signature: MacroSignature, val body: List<TemplateExpression>) : Macro

/**
 * Macros that are built in, rather than being defined by a template.
 */
enum class SystemMacro : Macro {
    Stream, // A stream is technically not a macro, but we can implement it as a macro that is the identity function.
    Annotate,
    MakeString,
    // TODO: Other system macros
}
