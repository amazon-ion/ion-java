// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.macro

import com.amazon.ion.*
import com.amazon.ion.impl.*
import com.amazon.ion.util.*
import java.math.BigDecimal
import java.math.BigInteger
import java.util.*

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
class LazyEnvironment {

    var arguments: ExpressionTape? = null
    val sideEffects: ExpressionTape = ExpressionTape(null, 4)
    val sideEffectContext: NestedContext = NestedContext(sideEffects, null, -1)
    var currentContext: NestedContext = NestedContext(null, null, -1)
    private var nestedContexts: Array<NestedContext?> = Array(16) {
        NestedContext(null, null, -1)
    }
    private var nestedContextIndex = 0

    fun reset(arguments: ExpressionTape?) {
        this.arguments = arguments
        nestedContextIndex = 0
        currentContext = nestedContexts[0]!!
        currentContext.tape = arguments
        currentContext.arguments = arguments // TODO ?
        currentContext.firstArgumentStartIndex = 0
    }

    data class NestedContext(var tape: ExpressionTape?, var arguments: ExpressionTape?, var firstArgumentStartIndex: Int) {

        fun annotations(): List<SymbolToken> {
            return tape!!.annotations()
        }

        fun context(): Any? {
            return tape!!.context()
        }

        fun type(): IonType {
            return tape!!.ionType()
        }

        fun longValue(): Long {
            return tape!!.readLong()
        }

        fun bigIntegerValue(): BigInteger {
            return tape!!.readBigInteger()
        }

        fun integerSize(): IntegerSize {
            return tape!!.readIntegerSize()
        }

        fun bigDecimalValue(): BigDecimal {
            return tape!!.readBigDecimal()
        }

        fun textValue(): String {
            return tape!!.readText()
        }

        fun lobValue(): ByteArray {
            return tape!!.readLob()
        }

        fun lobSize(): Int {
            return tape!!.lobSize()
        }

        fun symbolValue(): SymbolToken {
            return tape!!.readSymbol()
        }

        fun timestampValue(): Timestamp {
            return tape!!.readTimestamp()
        }

        fun doubleValue(): Double {
            return tape!!.readFloat()
        }

        fun isNullValue(): Boolean {
            return tape!!.isNullValue()
        }

        fun booleanValue(): Boolean {
            return tape!!.readBoolean()
        }

        fun highestVariableIndex(): Int {
            // TODO performance: store this to avoid recomputing
            return tape!!.highestVariableIndex()
        }
    }

    private fun growContextStack() {
        nestedContexts = nestedContexts.copyOf(nestedContexts.size * 2)
    }

    fun finishChildEnvironments(context: NestedContext) {
        // TODO consider storing the index in the context so that this can be done without looping“
        while (currentContext !== context) { // TODO verify this uses reference equality
            finishChildEnvironment()
        }
        finishChildEnvironment()
    }

    fun seekToArgument(indexRelativeToStart: Int): ExpressionTape? {
        var context = currentContext
        var contextIndex = nestedContextIndex
        var startIndex = context.firstArgumentStartIndex
        var searchIndex = indexRelativeToStart
        var sourceTape = context.arguments!!
        while (true) {
            when (sourceTape.seekToArgument(startIndex, searchIndex)) {
                ExpressionType.END_OF_EXPANSION -> return null
                ExpressionType.VARIABLE -> {
                    // The new search index is the index of the variable in the parent tape
                    searchIndex = sourceTape.context() as Int
                    // This pass-through variable has now been consumed.
                    sourceTape.prepareNext()
                    context = nestedContexts[--contextIndex]!!
                    startIndex = context.firstArgumentStartIndex
                    sourceTape = context.arguments!!

                }
                else -> {
                    return sourceTape
                }
            }
        }
    }

    fun seekPastFinalArgument() {
        // TODO perf: depending on the final design of the invocation stack, it might not be necessary for this to go
        //  upward recursively, e.g. if the invocation stack is still modeled recursively, this might be called at
        //  each depth anyway, so there would be duplicate work under the current implementation.
        var context = currentContext
        var contextIndex = nestedContextIndex
        var startIndex = context.firstArgumentStartIndex
        var searchIndex = context.highestVariableIndex()
        var sourceTape = context.arguments!!
        while (true) {
            when (sourceTape.seekToArgument(startIndex, searchIndex)) {
                ExpressionType.END_OF_EXPANSION -> break
                ExpressionType.VARIABLE -> {
                    // The new search index is the index of the variable in the parent tape
                    searchIndex = sourceTape.context() as Int
                    sourceTape.prepareNext() // Advance past this variable, which has been consumed.
                    context = nestedContexts[--contextIndex]!!
                    startIndex = context.firstArgumentStartIndex
                    sourceTape = context.arguments!!
                }
                else -> {
                    // Seek to the next argument, since the current one has been consumed.
                    sourceTape.seekPastExpression()
                    break
                }
            }
        }
    }

    fun tailCall(): NestedContext {
        // TODO do this earlier, so that 'currentContext' never needs to go on the stack
        val context = currentContext
        finishChildEnvironment()
        currentContext.tape = context.tape
        currentContext.arguments = context.arguments
        currentContext.firstArgumentStartIndex = context.firstArgumentStartIndex
        return currentContext
    }

    fun startChildEnvironment(tape: ExpressionTape, arguments: ExpressionTape, firstArgumentStartIndex: Int): NestedContext {
        if (++nestedContextIndex > nestedContexts.size) {
            growContextStack()
        }
        currentContext = nestedContexts[nestedContextIndex]!!
        currentContext.tape = tape
        currentContext.arguments = arguments
        currentContext.firstArgumentStartIndex = firstArgumentStartIndex
        return currentContext
    }

    fun finishChildEnvironment() {
        currentContext = nestedContexts[--nestedContextIndex]!!
    }

    // TODO
    /*
    override fun toString() = """
        |Environment(
        |    parent: ${parentEnvironment.toString().lines().joinToString("\n|        ")},
        |)
    """.trimMargin()

     */

    companion object {
        val EMPTY_ARRAY = IntArray(0)
        @JvmStatic
        val EMPTY = create()
        @JvmStatic
        fun create() = LazyEnvironment()
    }
}
