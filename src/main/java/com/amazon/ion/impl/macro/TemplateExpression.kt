package com.amazon.ion.impl.macro

import com.amazon.ion.SymbolToken
import com.amazon.ion.impl.macro.ionelement.api.IonElement

/**
 * Represents an expression in the body of a template.
 */
sealed interface TemplateExpression {
    // TODO: Special Forms (if_void, for, ...)?

    /**
     * A value that is taken literally. I.e. there can be no variable substitutions or macros inside here. Usually,
     * these will just be scalars, but it is possible for it to be a container type.
     *
     * We cannot use [`IonValue`](com.amazon.ion.IonValue) for this because `IonValue` requires references to parent
     * containers and to an IonSystem which makes it impractical for reading and writing macros definitions.
     * For now, we are using [IonElement], but that is not a good permanent solution. It is copy/pasted into this repo
     * to avoid a circular dependency, and the dependencies are shaded, so it is not suitable for a public API.
     */
    @JvmInline value class LiteralValue(val value: IonElement) : TemplateExpression

    /**
     * An Ion List that could contain variables or macro invocations.
     */
    data class ListValue(val annotations: List<SymbolToken>, val expressionRange: IntRange) : TemplateExpression

    /**
     * An Ion SExp that could contain variables or macro invocations.
     */
    data class SExpValue(val annotations: List<SymbolToken>, val expressionRange: IntRange) : TemplateExpression

    /**
     * An Ion Struct that could contain variables or macro invocations.
     */
    data class StructValue(val annotations: List<SymbolToken>, val expressionRange: IntRange, val templateStructIndex: Map<SymbolToken, IntArray>)

    /**
     * A reference to a variable that needs to be expanded.
     */
    // @JvmInline value
    class Variable(val signatureIndex: Int) : TemplateExpression

    /**
     * A macro invocation that needs to be expanded.
     */
    data class MacroInvocation(val macro: MacroRef, val argumentExpressionsRange: IntRange) : TemplateExpression
}
