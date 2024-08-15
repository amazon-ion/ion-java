package com.amazon.ion.impl.macro

import com.amazon.ion.*
import com.amazon.ion.impl.macro.Expression.*

/**
 * Evaluates an EExpression from a List of [EExpressionBodyExpression] and the [TemplateBodyExpression]s
 * given in the macro table of the [EncodingContext].
 *
 * General Usage:
 *  - To start evaluating an e-expression, call [initExpansion]
 *  - Call [expandNext] to get the next field name or value, or null
 *    if the end of the container or end of expansion has been reached.
 *  - Call [stepIn] when positioned on a container to step into that container.
 *  - Call [stepOut] to step out of the current container.
 */
class MacroEvaluator(
    private val encodingContext: EncodingContext,
    // TODO: Add expansion limit
) {

    /**
     * Initialize the macro evaluator with an E-Expression.
     */
    fun initExpansion(encodingExpressions: List<EExpressionBodyExpression>) {
        TODO()
    }

    /**
     * Evaluate the macro expansion until the next [DataModelExpression] can be returned.
     * Returns null if at the end of a container or at the end of the expansion.
     */
    fun expandNext(): DataModelExpression? {
        TODO()
    }

    /**
     * Steps out of the current [DataModelContainer].
     */
    fun stepOut() {
        TODO()
    }

    /**
     * Steps in to the current [DataModelContainer].
     * Throws [IonException] if not positioned on a container.
     */
    fun stepIn() {
        TODO()
    }
}
