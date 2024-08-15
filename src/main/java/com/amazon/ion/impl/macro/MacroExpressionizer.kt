package com.amazon.ion.impl.macro

import com.amazon.ion.*

/**
 * A [MacroExpressionizer] reads an E-Expression from an [IonReader], [IonCursor], or similar and constructs
 * a list of [Expression]s representing the E-Expression and its arguments.
 *
 * There are two sources of expressions. The template macro definitions, and the macro arguments.
 * The macro expander merges those.
 *
 * The [Expression] model does not (yet) support lazily reading values, so for now, all macro arguments must
 * be read eagerly.
 */
class MacroExpressionizer(private val reader: IonReader) {
    /**
     * Reads an E-Expression into the [Expression] model for evaluation by the [MacroEvaluator].
     *
     * Caller is responsible for ensuring that the reader is positioned at the op-code of an E-Expression.
     *
     * **Implementation Overview:**
     * 1. Read macro id
     * 2. Get corresponding macro signature from encoding context
     * 3. Read macro args
     *     1. Recurse/loop to 1 if an E-Expression is found in the arguments
     *
     */
    fun readEExpression(encodingContext: EncodingContext): List<Expression.EExpressionBodyExpression> {
        TODO()
    }
}
