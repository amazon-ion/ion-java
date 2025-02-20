// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.macro

import com.amazon.ion.impl.ExpressionTape

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
data class LazyEnvironment constructor(
    // Any variables found here have to be looked up in [parentEnvironment]
    val arguments: ExpressionTape?,
    val firstArgumentStartIndex: Int,
    val parentEnvironment: EnvironmentBase?,
    var useTape: Boolean
) : EnvironmentBase {

    fun startVariableEvaluation() {
        useTape = true
    }

    fun finishVariableEvaluation() {
        useTape = false
    }

    override fun createLazyChild(arguments: ExpressionTape, firstArgumentStartIndex: Int, useTape: Boolean) = LazyEnvironment(arguments, firstArgumentStartIndex, this, useTape)
    override fun createChild(arguments: List<Expression>, argumentIndices: IntArray): EnvironmentBase = Environment(arguments, argumentIndices, this)



    // TODO
    override fun toString() = """
        |Environment(
        |    parent: ${parentEnvironment.toString().lines().joinToString("\n|        ")},
        |)
    """.trimMargin()

    companion object {
        @JvmStatic
        val EMPTY = LazyEnvironment(null, -1, null, false)
        @JvmStatic
        fun create(arguments: ExpressionTape, firstArgumentStartIndex: Int) = LazyEnvironment(arguments, firstArgumentStartIndex, null, true)
    }
}
