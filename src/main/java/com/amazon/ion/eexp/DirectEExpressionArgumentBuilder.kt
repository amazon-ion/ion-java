// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.eexp

import com.amazon.ion.*
import com.amazon.ion.impl.bin.*
import com.amazon.ion.impl.macro.*
import java.math.BigInteger
import java.util.function.Consumer

/**
 * An implementation of [EExpressionArgumentBuilder] that directly writes encoding expressions.
 *
 * This builder handles the immediate writing of encoding expression arguments to an Ion writer.
 * It validates arguments against the macro's parameter specifications and ensures proper encoding
 * based on the parameter types and cardinality constraints.
 *
 * @property macro The macro definition that specifies the parameters and their constraints
 * @property managedWriter The Ion writer to which the encoding expression will be written
 */
internal class DirectEExpressionArgumentBuilder(
    private val macro: Macro,
    private val managedWriter: IonManagedWriter_1_1,
) : EExpressionArgumentBuilder<DirectEExpression> {

    private var i = 0

    override fun withAbsentArgument(): EExpressionArgumentBuilder<DirectEExpression> {
        val parameter = macro.signature[i++]
        require(parameter.cardinality.canBeVoid) { "Parameter ${parameter.variableName} requires an argument" }
        managedWriter.startExpressionGroup()
        managedWriter.endExpressionGroup()
        return this
    }

    override fun withIntArgument(value: Long): EExpressionArgumentBuilder<DirectEExpression> {
        val parameter = macro.signature[i++]
        checkArgumentEncodingCompatibility(parameter, value)
        managedWriter.writeInt(value)
        return this
    }

    override fun withIntArgument(value: BigInteger): EExpressionArgumentBuilder<DirectEExpression> {
        // TODO: Some of this logic is partially duplicated in the binary raw writer.
        //       Once the writer APIs are stabilized, consolidate the logic to a single location.
        val parameter = macro.signature[i++]
        checkArgumentEncodingCompatibility(parameter, value)
        managedWriter.writeInt(value)
        return this
    }

    override fun withFloatArgument(value: Double): EExpressionArgumentBuilder<DirectEExpression> {
        val parameter = macro.signature[i++]
        checkArgumentEncodingCompatibility(parameter, value)
        managedWriter.writeFloat(value)
        return this
    }

    override fun withStringArgument(value: String): EExpressionArgumentBuilder<DirectEExpression> {
        val parameter = macro.signature[i++]
        val encoding = parameter.type
        when (encoding) {
            Macro.ParameterEncoding.Tagged,
            Macro.ParameterEncoding.FlexString -> managedWriter.writeString(value)
            else -> throw IllegalArgumentException("Parameter ${parameter.variableName} must be a ${parameter.type.ionTextName}")
        }
        return this
    }

    override fun withArgument(values: Consumer<IonWriter>): EExpressionArgumentBuilder<DirectEExpression> {
        val parameter = macro.signature[i++]
        // require(parameter.type == Macro.ParameterEncoding.Tagged) { "Parameter ${parameter.variableName} must be a ${parameter.type.ionTextName}" }

        val validatedArgumentWriter = ArgumentValidatingIonWriterDecorator(parameter, managedWriter)

        when (parameter.cardinality) {
            Macro.ParameterCardinality.ZeroOrOne -> {
                managedWriter.startExpressionGroup()
                values.accept(validatedArgumentWriter)
                managedWriter.endExpressionGroup()
                val numberOfArguments = validatedArgumentWriter.numberOfExpressions
                require(numberOfArguments <= 1) { "Parameter ${parameter.variableName} must have 0 or 1 arguments" }
            }
            Macro.ParameterCardinality.ExactlyOne -> {
                values.accept(validatedArgumentWriter)
                val numberOfArguments = validatedArgumentWriter.numberOfExpressions
                require(numberOfArguments == 1) { "Parameter ${parameter.variableName} must have exactly 1 argument" }
            }
            Macro.ParameterCardinality.OneOrMore -> {
                managedWriter.startExpressionGroup()
                values.accept(validatedArgumentWriter)
                managedWriter.endExpressionGroup()
                val numberOfArguments = validatedArgumentWriter.numberOfExpressions
                require(numberOfArguments > 0) { "Parameter ${parameter.variableName} must have 1 or more arguments" }
            }
            Macro.ParameterCardinality.ZeroOrMore -> {
                managedWriter.startExpressionGroup()
                values.accept(validatedArgumentWriter)
                managedWriter.endExpressionGroup()
            }
        }
        return this
    }

    override fun build(): DirectEExpression {
        for (j in i until macro.signature.size) {
            withAbsentArgument()
        }
        managedWriter.endMacro()
        return DirectEExpression
    }
}
