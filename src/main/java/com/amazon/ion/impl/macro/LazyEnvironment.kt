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
class LazyEnvironment { // TODO can this be replaced entirely by giving ExpressionTape a reference to a parent tape? Or is even that unnecessary given that ExpressionPointers are pushed down?

    var arguments: ExpressionTape? = null
    val sideEffects: ExpressionTape = ExpressionTape(null, 4)
    val sideEffectContext: NestedContext = NestedContext(sideEffects, null)
    var currentContext: NestedContext = NestedContext(null, null) // TODO this is throwaway

    fun reset(arguments: ExpressionTape) {
        this.arguments = arguments
        currentContext.tape = arguments
        currentContext.arguments = null
    }

    // TODO get rid of NestedContext

    data class NestedContext(var tape: ExpressionTape?, var arguments: ExpressionTape?) {

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
