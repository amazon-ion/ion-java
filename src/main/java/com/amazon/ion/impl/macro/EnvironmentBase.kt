package com.amazon.ion.impl.macro

import com.amazon.ion.impl.*

interface EnvironmentBase {
    fun createLazyChild(arguments: ExpressionTape, firstArgumentStartIndex: Int): EnvironmentBase
    fun createChild(arguments: List<Expression>, argumentIndices: IntArray): EnvironmentBase
}
