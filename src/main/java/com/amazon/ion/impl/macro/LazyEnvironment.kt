// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.macro

import com.amazon.ion.*
import com.amazon.ion.impl.*
import com.amazon.ion.util.*
import java.math.BigDecimal
import java.math.BigInteger
import java.util.*
import kotlin.collections.ArrayList

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
    val sideEffectContext: NestedContext = NestedContext(sideEffects, null, -1, -1, null)
    var currentContext: NestedContext = NestedContext(null, null, -1, -1, null) // TODO this is throwaway
    private var nestedContexts: Array<NestedContext?> = Array(16) {
        NestedContext(null, null, -1, -1)
    }
    private var nestedContextIndex = 0

    fun reset(arguments: ExpressionTape?) {
        this.arguments = arguments
        nestedContextIndex = 0
        currentContext = nestedContexts[0]!!
        currentContext.tape = arguments
        currentContext.arguments = null
        currentContext.nextVariablePointerIndex = 0
        currentContext.variablePointerStartIndex = 0
        //arguments!!.cacheExpressionPointers(currentContext) // TODO redundant? Done before the e-expression is evaluated. Try removing
    }

    // TODO variablePointerStartIndex is not doing anything
    // TODO looks like nextVariablePointerIndex isn't either
    data class NestedContext(var tape: ExpressionTape?, var arguments: ExpressionTape?, var variablePointerStartIndex: Int, var nextVariablePointerIndex: Int, var variablePointers: ArrayList<ExpressionTape.VariablePointer>? = ArrayList(8)) {

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

        /*
        fun highestVariableIndex(): Int {
            // TODO performance: store this to avoid recomputing
            return tape!!.highestVariableIndex()
        }

         */
    }

    private fun growContextStack() {
        nestedContexts = nestedContexts.copyOf(nestedContexts.size * 2)
        for (i in (nestedContexts.size / 2) until nestedContexts.size) {
            if (nestedContexts[i] == null) {
                nestedContexts[i] = NestedContext(null, null, -1, -1)
            }
        }
    }

    fun finishChildEnvironments(context: NestedContext) {
        // TODO consider storing the index in the context so that this can be done without looping“
        while (currentContext !== context) { // TODO verify this uses reference equality
            finishChildEnvironment()
        }
    }

    fun seekToArgument(indexRelativeToStart: Int): ExpressionTape? {
        // TODO this doesn't advance any pass-through variables.
        val sourceContext = currentContext.variablePointers!![indexRelativeToStart].visit()
        sourceContext.nextVariablePointerIndex = indexRelativeToStart + 1
        return sourceContext.tape
        /*
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
                    while (context.arguments === sourceTape) {
                        // The parent context is evaluating a variable. Go up one level to find the argument source.
                        context = nestedContexts[--contextIndex]!!
                    }
                    startIndex = context.firstArgumentStartIndex
                    sourceTape = context.arguments!!

                }
                else -> {
                    return sourceTape
                }
            }
        }

         */
    }

    fun seekPastFinalArgument() {
        // TODO perf: depending on the final design of the invocation stack, it might not be necessary for this to go
        //  upward recursively, e.g. if the invocation stack is still modeled recursively, this might be called at
        //  each depth anyway, so there would be duplicate work under the current implementation.
        val variablePointers = currentContext.variablePointers!!
        for (variablePointer in variablePointers) {
            // TODO this won't work for out-of-order arguments. Need to ensure only the highest one is visited.
            val sourceContext = variablePointer.visit()
            sourceContext.tape!!.seekPastExpression()
        }
        /*
        if (variablePointers.isNotEmpty()) { // TODO seek past the final contained argument for all parent tapes
            val sourceContext = currentContext.variablePointers!![currentContext.variablePointers!!.size - 1].visit()
            sourceContext.tape!!.seekPastExpression() // TODO might be able to pre-calculate this
        }

         */
        /*
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
                    sourceTape.prepareNext() // Advance past this variable, which has been consumed.
                    context = nestedContexts[--contextIndex]!!
                    searchIndex = context.highestVariableIndex()
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

         */
    }

    fun tailCall(): NestedContext {
        // TODO do this earlier, so that 'currentContext' never needs to go on the stack
        val context = currentContext
        finishChildEnvironment()
        currentContext.tape = context.tape
        currentContext.arguments = context.arguments
        currentContext.nextVariablePointerIndex = context.nextVariablePointerIndex
        currentContext.variablePointerStartIndex = context.variablePointerStartIndex
        return currentContext
    }

    fun startChildEnvironment(tape: ExpressionTape, arguments: ExpressionTape, resolveVariables: Boolean = false): NestedContext {
        val parentContext = nestedContexts[nestedContextIndex]!!
        if (++nestedContextIndex >= nestedContexts.size) {
            growContextStack()
        }
        currentContext = nestedContexts[nestedContextIndex]!!
        currentContext.tape = tape
        currentContext.arguments = arguments
        currentContext.nextVariablePointerIndex = 0
        currentContext.variablePointers!!.clear()
        if (resolveVariables) {
            // TODO just move this into the call site
            arguments.cacheExpressionPointers(parentContext, currentContext) // TODO make sure this is actually saving work
            /*
            tape.resolveVariables(
                parentContext,
                //arguments,
                currentContext.variablePointers
            )

             */
        } else {
            // This child environment is for a variable evaluation whose variable pointers have already been resolved.
            // TODO just add the one that applies to this variable
            currentContext.variablePointers!!.addAll(parentContext.variablePointers!!)
            currentContext.variablePointerStartIndex = 0
        }
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
