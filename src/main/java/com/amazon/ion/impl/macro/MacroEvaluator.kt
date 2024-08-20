// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.macro

import com.amazon.ion.*
import com.amazon.ion.impl.*
import com.amazon.ion.impl.macro.Expression.*

/**
 * Evaluates an EExpression from a List of [EExpressionBodyExpression] and the [TemplateBodyExpression]s
 * given in the macro table of the [EncodingContext].
 *
 * General Usage:
 *  - To start evaluating an e-expression, call [initExpansion]
 *  - Call [expandNext] to get the next field name or value, or null
 *    if the end of the container or end of expansion has been reached.
 *  - Call [stepIn] when positioned on a container to step into that container.
 *  - Call [stepOut] to step out of the current container.
 */
class MacroEvaluator(
    private val encodingContext: EncodingContext,
    // TODO: Add expansion limit
) {

    /**
     * Implementations must update [ExpansionInfo.i] in order for [ExpansionInfo.hasNext] to work properly.
     */
    private fun interface Expander {
        fun nextExpression(expansionInfo: ExpansionInfo, macroEvaluator: MacroEvaluator): Expression
    }

    private object SimpleExpander : Expander {
        override fun nextExpression(expansionInfo: ExpansionInfo, macroEvaluator: MacroEvaluator): Expression {
            return expansionInfo.nextSourceExpression()
        }
    }

    private object MakeStringExpander : Expander {
        override fun nextExpression(expansionInfo: ExpansionInfo, macroEvaluator: MacroEvaluator): Expression {
            // Tell the macro evaluator to treat this as a values expansion...
            macroEvaluator.expansionStack.peek().expansionKind = ExpansionKind.Values
            val minDepth = macroEvaluator.expansionStack.size()
            // ...But capture the output and turn it into a String
            val sb = StringBuilder()
            while (true) {
                when (val expr: DataModelExpression? = macroEvaluator.expandNext(minDepth)) {
                    is StringValue -> sb.append(expr.value)
                    is SymbolValue -> sb.append(expr.value.assumeText())
                    is NullValue -> {}
                    null -> break
                    is DataModelValue -> throw IonException("Invalid argument type for 'make_string': ${expr.type}")
                    is FieldName -> TODO("Unreachable. We shouldn't be able to get here without first encountering a StructValue.")
                }
            }
            return StringValue(value = sb.toString())
        }
    }

    private enum class ExpansionKind(val expander: Expander) {
        Container(SimpleExpander),
        TemplateBody(SimpleExpander),
        Values(SimpleExpander),
        MakeString(MakeStringExpander),
        ;

        companion object {
            @JvmStatic
            fun forSystemMacro(macro: SystemMacro): ExpansionKind {
                return when (macro) {
                    SystemMacro.Values -> Values
                    SystemMacro.MakeString -> MakeString
                }
            }
        }
    }

    private inner class ExpansionInfo : Iterator<Expression> {
        /** The [ExpansionKind]. */
        @JvmField var expansionKind: ExpansionKind = ExpansionKind.Values
        /**
         * The evaluation [Environment]—i.e. variable bindings.
         */
        @JvmField var environment: Environment? = null
        /**
         * The [Expression]s being expanded. This MUST be the original list, not a sublist because
         * (a) we don't want to be allocating new sublists all the time, and (b) the
         * start and end indices of the expressions may be incorrect if a sublist is taken.
         */
        @JvmField var expressions: List<Expression>? = null
        // /** Start of [expressions] that are applicable for this [ExpansionInfo] */
        // TODO: Do we actually need this for anything other than debugging?
        // @JvmField var startInclusive: Int = 0
        /** End of [expressions] that are applicable for this [ExpansionInfo] */
        @JvmField var endExclusive: Int = 0
        /** Current position within [expressions] of this expansion */
        @JvmField var i: Int = 0

        /** Checks if this expansion can produce any more expressions */
        override fun hasNext(): Boolean = i < endExclusive

        /** Returns the next expression from this expansion */
        override fun next(): Expression {
            return expansionKind.expander.nextExpression(this, this@MacroEvaluator)
        }

        /**
         * Returns the next expression from the input expressions ([expressions]) of this Expansion.
         * This is intended for use in [Expander] implementations.
         */
        fun nextSourceExpression(): Expression {
            val next = expressions!![i]
            i++
            if (next is HasStartAndEnd) i = next.endExclusive
            return next
        }

        override fun toString() = """
        |ExpansionInfo(
        |    expansionKind: $expansionKind,
        |    environment: $environment,
        |    expressions: [
        |        ${expressions!!.joinToString(",\n|        ") { it.toString() } }
        |    ],
        |    endExclusive: $endExclusive,
        |    i: $i,
        |)
        """.trimMargin()
    }

    private val expansionStack = _Private_RecyclingStack(8) { ExpansionInfo() }

    private var currentExpr: DataModelExpression? = null

    /**
     * Initialize the macro evaluator with an E-Expression.
     */
    fun initExpansion(encodingExpressions: List<EExpressionBodyExpression>) {
        val eExp = encodingExpressions[0]
        eExp as? EExpression ?: throw IllegalStateException("Attempted to initialize macro evaluator for an expression that is not an e-expression: $eExp")

        pushMacro(
            eExp.address,
            eExp.startInclusive,
            eExp.endExclusive,
            Environment.EMPTY,
            encodingExpressions,
        )
    }

    /**
     * Evaluate the macro expansion until the next [DataModelExpression] can be returned.
     * Returns null if at the end of a container or at the end of the expansion.
     */
    fun expandNext(): DataModelExpression? {
        return expandNext(-1)
    }

    /**
     * Evaluate the macro expansion until the next [DataModelExpression] can be returned.
     * Returns null if at the end of a container or at the end of the expansion.
     *
     * Treats [minDepth] as the minimum expansion depth allowed—i.e. it will not step out any further than
     * [minDepth]. This is used for built-in macros when they need to delegate something to the macro evaluator
     * but don't want the macro evaluator to step out beyond the invoking built-in macro.
     */
    private fun expandNext(minDepth: Int): DataModelExpression? {

        /*  ==== Evaluation Algorithm ====
        01 | Check the top expansion in the expansion stack
        02 |     If there is none, return null (macro expansion is over)
        03 |     If there is one, but it has no more expressions...
        04 |         If the expansion kind is a data-model container type, return null (user needs to step out)
        05 |         If the expansion kind is not a data-model container type, automatically step out
        06 |     If there is one, and it has more expressions...
        07 |         If it is a scalar, return that
        08 |         If it is a container, return that (user needs to step in)
        09 |         If it is a variable, using parent Environment, push variable ExpansionInfo onto the stack and goto 1
        10 |         If it is an expression group, using current Environment, push expression group ExpansionInfo onto the stack and goto 1
        11 |         If it is a macro invocation, create updated Environment, push ExpansionInfo onto stack, and goto 1
        12 |         If it is an e-expression, using empty Environment, push ExpansionInfo onto stack and goto 1
        */

        currentExpr = null
        while (!expansionStack.isEmpty) {
            if (!expansionStack.peek().hasNext()) {
                if (expansionStack.peek().expansionKind == ExpansionKind.Container) {
                    // End of container. User needs to step out.
                    // TODO: Do we need something to distinguish End-Of-Expansion from End-Of-Container?
                    return null
                } else {
                    // End of a macro invocation or something else that is not part of the data model,
                    // so we seamlessly close this out and continue with the parent expansion.
                    if (expansionStack.size() > minDepth) {
                        expansionStack.pop()
                        continue
                    } else {
                        // End of expansion for something internal.
                        return null
                    }
                }
            }
            when (val currentExpr = expansionStack.peek().next()) {
                Placeholder -> TODO("unreachable")
                is MacroInvocation -> pushTdlMacroExpansion(currentExpr)
                is EExpression -> pushEExpressionExpansion(currentExpr)
                is VariableRef -> pushVariableExpansion(currentExpr)
                is ExpressionGroup -> pushExpressionGroup(currentExpr)
                is DataModelExpression -> {
                    this.currentExpr = currentExpr
                    break
                }
            }
        }
        return currentExpr
    }

    /**
     * Steps out of the current [DataModelContainer].
     */
    fun stepOut() {
        // step out of anything we find until we have stepped out of a container.
        while (expansionStack.pop()?.expansionKind != ExpansionKind.Container) {
            if (expansionStack.isEmpty) throw IonException("Nothing to step out of.")
        }
    }

    /**
     * Steps in to the current [DataModelContainer].
     * Throws [IonException] if not positioned on a container.
     */
    fun stepIn() {
        val expression = requireNotNull(currentExpr) { "Not positioned on a value" }
        expression as? DataModelContainer ?: throw IonException("Not positioned on a container.")
        val currentExpansion = expansionStack.peek()
        pushExpansion(ExpansionKind.Container, expression.startInclusive, expression.endExclusive, currentExpansion.environment!!, currentExpansion.expressions!!)
    }

    /**
     * Push a variable onto the expansion stack.
     *
     * Variables are a little bit different from other expansions. There is only one (top) expression
     * in a variable expansion. It can be another variable, a value, a macro invocation, or an expression group.
     * Furthermore, the current environment becomes the "source expressions" for the expansion, and the
     * parent of the current environment becomes the environment in which the variable is expanded (thus
     * maintaining the proper scope of variables).
     */
    private fun pushVariableExpansion(expression: VariableRef) {
        val currentEnvironment = expansionStack.peek().environment ?: Environment.EMPTY
        val argumentExpressionIndex = currentEnvironment.argumentIndices[expression.signatureIndex]

        // Argument was elided; don't push anything so that we skip the empty expansion
        if (argumentExpressionIndex < 0) return

        pushExpansion(
            expansionKind = ExpansionKind.Values,
            argsStartInclusive = argumentExpressionIndex,
            // There can only be one expression for an argument. It's either a value, macro, or expression group.
            argsEndExclusive = argumentExpressionIndex + 1,
            environment = currentEnvironment.parentEnvironment ?: Environment.EMPTY,
            expressions = currentEnvironment.arguments
        )
    }

    private fun pushExpressionGroup(expr: ExpressionGroup) {
        val currentExpansion = expansionStack.peek()
        pushExpansion(ExpansionKind.Values, expr.startInclusive, expr.endExclusive, currentExpansion.environment!!, currentExpansion.expressions!!)
    }

    /**
     * Push a macro from a TDL macro invocation, found in the current expansion, to the expansion stack
     */
    private fun pushTdlMacroExpansion(expression: MacroInvocation) {
        val currentExpansion = expansionStack.peek()
        pushMacro(
            address = expression.address,
            argsStartInclusive = expression.startInclusive,
            argsEndExclusive = expression.endExclusive,
            currentExpansion.environment!!,
            encodingExpressions = currentExpansion.expressions!!,
        )
    }

    /**
     * Push a macro from the e-expression [expression] onto the expansionStack, handling concerns such as
     * looking up the macro reference, setting up the environment, etc.
     */
    private fun pushEExpressionExpansion(expression: EExpression) {
        val currentExpansion = expansionStack.peek()
        pushMacro(
            address = expression.address,
            argsStartInclusive = expression.startInclusive,
            argsEndExclusive = expression.endExclusive,
            environment = Environment.EMPTY,
            encodingExpressions = currentExpansion.expressions!!,
        )
    }

    /**
     * Pushes a macro invocation to the expansionStack
     */
    private fun pushMacro(
        address: MacroRef,
        argsStartInclusive: Int,
        argsEndExclusive: Int,
        environment: Environment,
        encodingExpressions: List<Expression>,
    ) {
        val macro = encodingContext.macroTable[address] ?: throw IonException("No such macro: $address")

        val argIndices = calculateArgumentIndices(macro, encodingExpressions, argsStartInclusive, argsEndExclusive)

        when (macro) {
            is TemplateMacro -> pushExpansion(
                ExpansionKind.TemplateBody,
                argsStartInclusive = 0,
                argsEndExclusive = macro.body.size,
                expressions = macro.body,
                environment = environment.createChild(encodingExpressions, argIndices)
            )
            // TODO: Values and MakeString have the same code in their blocks. As we get further along, see
            //       if this is generally applicable for all system macros.
            is SystemMacro -> {
                val kind = ExpansionKind.forSystemMacro(macro)
                pushExpansion(kind, argsStartInclusive, argsEndExclusive, environment, encodingExpressions,)
            }
        }
    }

    /**
     * Pushes an expansion to the expansion stack.
     */
    private fun pushExpansion(
        expansionKind: ExpansionKind,
        argsStartInclusive: Int,
        argsEndExclusive: Int,
        environment: Environment,
        expressions: List<Expression>,
    ) {
        expansionStack.push {
            it.expansionKind = expansionKind
            it.environment = environment
            it.expressions = expressions
            it.i = argsStartInclusive
            it.endExclusive = argsEndExclusive
        }
    }

    /**
     * Given a [Macro] (or more specifically, its signature), calculates the position of each of its arguments
     * in [encodingExpressions]. The result is a list that can be used to map from a parameter's
     * signature index to the encoding expression index. Any trailing, optional arguments that are
     * elided have a value of -1.
     *
     * This function also validates that the correct number of parameters are present. If there are
     * too many parameters or too few parameters, this will throw [IonException].
     */
    private fun calculateArgumentIndices(
        macro: Macro,
        encodingExpressions: List<Expression>,
        argsStartInclusive: Int,
        argsEndExclusive: Int
    ): List<Int> {
        // TODO: For TDL macro invocations, see if we can calculate this during the "compile" step.
        var numArgs = 0
        val argsIndices = IntArray(macro.signature.size)
        var currentArgIndex = argsStartInclusive
        for (p in macro.signature) {
            if (currentArgIndex >= argsEndExclusive) {
                if (!p.cardinality.canBeVoid) throw IonException("No value provided for parameter ${p.variableName}")
                // Elided rest parameter.
                argsIndices[numArgs] = -1
            } else {
                argsIndices[numArgs] = currentArgIndex
                currentArgIndex = when (val expr = encodingExpressions[currentArgIndex]) {
                    is HasStartAndEnd -> expr.endExclusive
                    else -> currentArgIndex + 1
                }
            }
            numArgs++
        }
        while (currentArgIndex < argsEndExclusive) {
            currentArgIndex = when (val expr = encodingExpressions[currentArgIndex]) {
                is HasStartAndEnd -> expr.endExclusive
                else -> currentArgIndex + 1
            }
            numArgs++
        }
        if (numArgs > macro.signature.size) {
            throw IonException("Too many arguments. Expected ${macro.signature.size}, but found $numArgs")
        }
        return argsIndices.toList()
    }
}
