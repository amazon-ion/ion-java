// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.macro

/**
 * An `Environment` contains variable bindings for a given macro evaluation.
 *
 * The [arguments] is a list of expressions for the arguments that were passed to the current macro.
 * It may also contain other expressions if the current macro invocation is part of a larger evaluation.
 *
 * The [argumentIndices] is a mapping from parameter index to the start of the corresponding expression in [arguments].
 *
 * The [parentEnvironment] is an environment to use if any of the expressions in this environment
 * contains a variable that references something from an outer macro invocation.
 */
data class Environment private constructor(
    // Any variables found here have to be looked up in [parentEnvironment]
    val arguments: List<Expression>,
    // TODO: Replace with IntArray
    val argumentIndices: List<Int>,
    val parentEnvironment: Environment?,
) {
    fun createChild(arguments: List<Expression>, argumentIndices: List<Int>) = Environment(arguments, argumentIndices, this)

    override fun toString() = """
        |Environment(
        |    argumentIndices: $argumentIndices,
        |    argumentExpressions: [${arguments.mapIndexed { index, expression -> "\n|        $index. $expression" }.joinToString() }
        |    ],
        |    parent: ${parentEnvironment.toString().lines().joinToString("\n|        ")},
        |)
    """.trimMargin()

    companion object {
        @JvmStatic
        val EMPTY = Environment(emptyList(), emptyList(), null)
        @JvmStatic
        fun create(arguments: List<Expression>, argumentIndices: List<Int>) = Environment(arguments, argumentIndices, null)
    }
}
