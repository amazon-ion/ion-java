// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.macro

import com.amazon.ion.*
import com.amazon.ion.impl.*
import com.amazon.ion.util.*
import com.amazon.ion.util.unreachable
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
class LazyEnvironment /*(
    // Any variables found here have to be looked up in [parentEnvironment]
    val arguments: ExpressionTape?,
    //val firstArgumentStartIndex: Int,
    //val parentEnvironment: LazyEnvironment?,
    //var useTape: Boolean,
    //val expressions: List<Expression>,
    //val argumentIndices: IntArray // TODO remove
) */{

    val arguments: ExpressionTape?
    val sideEffects: ExpressionTape = ExpressionTape(null, 4)
    val sideEffectContext: NestedContext = NestedContext(sideEffects, 0)
    var currentContext: NestedContext = NestedContext(null, -1)

    constructor(arguments: ExpressionTape?) {
        this.arguments = arguments
        currentContext = nestedContexts[++nestedContextIndex]!!
        currentContext.tape = arguments
        currentContext.firstArgumentStartIndex = 0
    }

    //data class NestedContext(var useTape: Boolean, var firstArgumentStartIndex: Int, var expressions: List<Expression>, var argumentIndices: IntArray) {
    data class NestedContext(var tape: ExpressionTape?, var firstArgumentStartIndex: Int) { // TODO track whether a single expression has been read; nested contexts read only one
        /*
        fun startVariableEvaluation() {
            useTape = true
        }

        fun finishVariableEvaluation() {
            useTape = false
        }

         */

        fun annotations(): List<SymbolToken> {
            return tape!!.annotations()
        }

        fun context(/*, expressionIndex: Int*/): Any? {
            /*
            if (isReadingFromTape()) {
                return arguments!!.expression()
            }
            return currentContext.expressions[expressionIndex]

             */
            return tape!!.context()
        }

        /*
        fun expression(/*, expressionIndex: Int*/): Expression? {
            /*
            if (isReadingFromTape()) {
                return arguments!!.expression()
            }
            return currentContext.expressions[expressionIndex]

             */
            return tape!!.expression()
        }

         */

        fun type(/*, expressionIndex: Int*/): IonType {
            /*
            if (isReadingFromTape()) {
                return arguments!!.ionType()
            }
            return (currentContext.expressions[expressionIndex] as Expression.DataModelValue).type

             */
            return tape!!.ionType()
        }

        // TODO add functions like intValue() stringValue()..., which demux from the correct source (expressions or tape)

        fun longValue(/*, expressionIndex: Int*/): Long {
            /*
            if (isReadingFromTape()) {
                return arguments!!.readLong()
            }
            return (currentContext.expressions[expressionIndex] as Expression.LongIntValue).longValue

             */
            return tape!!.readLong()
        }

        fun bigIntegerValue(/*, expressionIndex: Int*/): BigInteger {
            //if (isReadingFromTape()) {
            return tape!!.readBigInteger()
            /*}
            val expression = currentContext.expressions[expressionIndex]
            when (expression) {
                is Expression.LongIntValue -> return expression.bigIntegerValue
                is Expression.BigIntValue -> return expression.bigIntegerValue
                else -> throw IllegalStateException("Unexpected expression type: ${expression.javaClass}")
            }

             */
        }

        fun integerSize(): IntegerSize {
            return tape!!.readIntegerSize()
        }

        fun bigDecimalValue(/*, expressionIndex: Int*/): BigDecimal {
            //if (isReadingFromTape()) {
            return tape!!.readBigDecimal()
            /*}
            val expression = currentContext.expressions[expressionIndex]
            when (expression) {
                is Expression.LongIntValue -> return expression.longValue.toBigDecimal()
                is Expression.DecimalValue -> return expression.value
                else -> throw IllegalStateException("Unexpected expression type: ${expression.javaClass}")
            }

             */
        }

        fun textValue(/*, expressionIndex: Int*/): String {
            //if (isReadingFromTape()) {
            return tape!!.readText()
            /*}
            val expression = currentContext.expressions[expressionIndex]
            when (expression) {
                is Expression.StringValue -> return expression.stringValue
                is Expression.SymbolValue -> return expression.value.assumeText()
                else -> throw IllegalStateException("Unexpected expression type: ${expression.javaClass}")
            }

             */
        }

        fun lobValue(/*, expressionIndex: Int*/): ByteArray {
            //if (isReadingFromTape()) {
            return tape!!.readLob()
            /*}
            return (currentContext.expressions[expressionIndex] as Expression.LobValue).value

             */
        }

        fun lobSize(/*, expressionIndex: Int*/): Int {
            return tape!!.lobSize()
        }

        fun symbolValue(/*, expressionIndex: Int*/): SymbolToken {
            //if (isReadingFromTape()) {
            return tape!!.readSymbol()
            /*}
            val expression = currentContext.expressions[expressionIndex]
            when (expression) {
                is Expression.StringValue -> return _Private_Utils.newSymbolToken(expression.stringValue)
                is Expression.SymbolValue -> return expression.value
                else -> throw IllegalStateException("Unexpected expression type: ${expression.javaClass}")
            }

             */
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

        fun seekToArgument(argumentTape: ExpressionTape, indexRelativeToStart: Int): Boolean {
            return argumentTape.seekToArgument(firstArgumentStartIndex, indexRelativeToStart)
        }
    }

    private var nestedContexts: Array<NestedContext?> = Array(16) {
         NestedContext(null, -1)
    }
    private var nestedContextIndex = -1

    private fun growContextStack() {
        nestedContexts = nestedContexts.copyOf(nestedContexts.size * 2)
    }

    /*
    fun parent(): NestedContext {
        return nestedContexts[nestedContextIndex - 1]!!
    }

     */

    /*
    fun startVariableEvaluation() {
        currentContext.startVariableEvaluation()
    }

    fun finishVariableEvaluation() {
        currentContext.finishVariableEvaluation()
    }

     */

    /*
    fun createLazyChild(arguments: ExpressionTape, firstArgumentStartIndex: Int, useTape: Boolean) = LazyEnvironment(arguments, firstArgumentStartIndex, this, useTape, emptyList(), EMPTY_ARRAY)
    fun createChild(arguments: List<Expression>, argumentIndices: IntArray): LazyEnvironment = LazyEnvironment(this.arguments, this.firstArgumentStartIndex, this, false, arguments, argumentIndices)


     */

    fun startChildEnvironment(tape: ExpressionTape, firstArgumentStartIndex: Int, /*useTape: Boolean, expressions: List<Expression>, argumentIndices: IntArray*/): NestedContext {
        //return LazyEnvironment(this.arguments, firstArgumentStartIndex, this, useTape, expressions, argumentIndices)
        if (++nestedContextIndex > nestedContexts.size) {
            growContextStack()
        }
        currentContext = nestedContexts[nestedContextIndex]!!
        //currentContext.useTape = useTape
        //currentContext.expressions = expressions
        currentContext.tape = tape
        currentContext.firstArgumentStartIndex = firstArgumentStartIndex
        //currentContext.argumentIndices = argumentIndices
        return currentContext
    }

    fun finishChildEnvironment() {
        currentContext = nestedContexts[--nestedContextIndex]!! // TODO this might not work, if there's a possibility that the contexts won't be released in FIFO order
    }

    /*
    fun isReadingFromTape(): Boolean {
        return currentContext.useTape || currentContext.expressions.isEmpty()
    }

     */




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
        val EMPTY = LazyEnvironment(null) //LazyEnvironment(null, -1, null, false, emptyList(), EMPTY_ARRAY)
        @JvmStatic
        fun create(arguments: ExpressionTape) = LazyEnvironment(arguments)
    }
}
