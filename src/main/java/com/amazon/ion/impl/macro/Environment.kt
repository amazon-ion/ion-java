// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.macro

data class Environment private constructor(
    // Any variables found here have to be looked up in [parentEnvironment]
    val arguments: List<Expression>,
    // TODO: Replace with IntArray
    val argumentIndices: List<Int>,
    val parentEnvironment: Environment?,
) {
    fun createChild(arguments: List<Expression>, argumentIndices: List<Int>) = Environment(arguments, argumentIndices, this)
    companion object {
        @JvmStatic
        val EMPTY = Environment(emptyList(), emptyList(), null)
        @JvmStatic
        fun create(arguments: List<Expression>, argumentIndices: List<Int>) = Environment(arguments, argumentIndices, null)
    }
}
