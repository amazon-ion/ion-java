// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.eexp

import com.amazon.ion.IonWriter
import com.amazon.ion.WriteAsIon
import com.amazon.ion.impl.macro.Macro

/**
 * A prepared encoding expression that can be reused multiple times.
 *
 * This class represents a pre-configured encoding expression with its macro name,
 * definition, and argument builders. It can be efficiently reused to write the same
 * expression multiple times without needing to reconfigure the parameters each time.
 *
 * @property macroName The name of the macro to be used, or null for anonymous expressions
 * @property macroDefinition The macro definition that defines the parameter types and constraints
 * @property arguments List of argument builder functions to configure the expression's parameters
 */
class PreparedEExpression private constructor(
    val macroName: String?,
    val macroDefinition: Macro,
    private val arguments: ArrayList<EExpressionArgumentBuilder<*>.() -> Unit>,
) : WriteAsIon, EExpression {

    internal constructor(builder: PreparedEExpressionArgumentBuilder) : this(
        builder.macroName,
        builder.macroDefinition,
        builder.arguments,
    )

    override fun writeWithEExpression(builder: EExpressionBuilder): EExpression {
        val argBuilder = builder.withName(macroName).withMacro(macroDefinition)
        arguments.forEach { it(argBuilder) }
        return argBuilder.build()
    }

    override fun writeTo(writer: IonWriter) {
        TODO("Evaluate this e-expression and write as a not an e-expression")
    }
}
