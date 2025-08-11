// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.eexp

import com.amazon.ion.IonWriter
import com.amazon.ion.MacroAwareIonWriter
import com.amazon.ion.impl.macro.Macro
import java.math.BigInteger
import java.util.ArrayList
import java.util.function.Consumer

/**
 * A builder for creating prepared encoding expressions.
 *
 * This builder collects and validates arguments for a prepared encoding expression,
 * which can be reused multiple times. It ensures that all arguments conform to the
 * macro's parameter specifications before the expression is built.
 *
 * The arguments are not written to an Ion stream immediately, but are stored
 * (or deferred) for later use.
 *
 * There is one function call per parameter. Omitted trailing args are implicitly absent.
 */
class PreparedEExpressionArgumentBuilder(val macroName: String?, internal val macroDefinition: Macro) : EExpressionArgumentBuilder<PreparedEExpression> {

    override fun build(): PreparedEExpression {
        for (j in i until macroDefinition.signature.size) {
            withAbsentArgument()
        }
        return PreparedEExpression(this)
    }

    private var i = 0
    internal val arguments = ArrayList<EExpressionArgumentBuilder<*>.() -> Unit>()

    override fun withAbsentArgument(): PreparedEExpressionArgumentBuilder = apply {
        val parameter = macroDefinition.signature[i++]
        require(parameter.cardinality.canBeVoid) { "Parameter ${parameter.variableName} requires an argument" }
        arguments.add { withAbsentArgument() }
    }

    override fun withIntArgument(value: Long): PreparedEExpressionArgumentBuilder = apply {
        val parameter = macroDefinition.signature[i++]
        checkArgumentEncodingCompatibility(parameter, value)
        arguments.add { withIntArgument(value) }
    }

    override fun withIntArgument(value: BigInteger): PreparedEExpressionArgumentBuilder = apply {
        val parameter = macroDefinition.signature[i++]
        checkArgumentEncodingCompatibility(parameter, value)
        arguments.add { withIntArgument(value) }
    }

    override fun withFloatArgument(value: Double): PreparedEExpressionArgumentBuilder = apply {
        val parameter = macroDefinition.signature[i++]
        checkArgumentEncodingCompatibility(parameter, value)
        arguments.add { withFloatArgument(value) }
    }

    override fun withStringArgument(value: String): PreparedEExpressionArgumentBuilder = apply {
        val parameter = macroDefinition.signature[i++]
        checkArgumentEncodingCompatibility(parameter, value)
        arguments.add { withStringArgument(value) }
    }

    override fun withArgument(values: Consumer<IonWriter>): PreparedEExpressionArgumentBuilder = apply {
        val parameter = macroDefinition.signature[i++]

        val recorder = IonWriterRecorder()
        val validator = ArgumentValidatingIonWriterDecorator(parameter, recorder)
        values.accept(validator)

        val numberOfArguments = validator.numberOfExpressions
        when (parameter.cardinality) {
            Macro.ParameterCardinality.ZeroOrOne -> require(numberOfArguments <= 1) { "Parameter ${parameter.variableName} must have 0 or 1 arguments" }
            Macro.ParameterCardinality.ExactlyOne -> require(numberOfArguments == 1) { "Parameter ${parameter.variableName} must have exactly 1 argument" }
            Macro.ParameterCardinality.OneOrMore -> require(numberOfArguments > 0) { "Parameter ${parameter.variableName} must have 1 or more arguments" }
            Macro.ParameterCardinality.ZeroOrMore -> {}
        }

        if (numberOfArguments == 0) {
            arguments.add { withAbsentArgument() }
        } else {
            arguments.add { withArgument { recorder.replay(it as MacroAwareIonWriter) } }
        }
    }
}
