// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.macro

import com.amazon.ion.IonException
import com.amazon.ion.SymbolToken
import com.amazon.ion.impl._Private_RecyclingStack
import com.amazon.ion.impl._Private_Utils.newSymbolToken
import com.amazon.ion.impl.macro.Expression.*
import com.amazon.ion.util.*
import java.io.ByteArrayOutputStream
import java.lang.IllegalStateException
import java.math.BigDecimal

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
 *
 *  TODO: Add expansion limit
 */
class MacroEvaluator {

    /**
     * Implementations must update [ExpansionInfo.i] in order for [ExpansionInfo.hasNext] to work properly.
     */
    private fun interface Expander {
        fun nextExpression(expansionInfo: ExpansionInfo, macroEvaluator: MacroEvaluator): Expression

        /**
         * Read the expanded values from one argument, returning exactly one value.
         * Throws an exception if there is not exactly one expanded value.
         */
        fun readExactlyOneExpandedArgumentValue(expansionInfo: ExpansionInfo, macroEvaluator: MacroEvaluator, argName: String): DataModelExpression {
            return readZeroOrOneExpandedArgumentValues(expansionInfo, macroEvaluator, argName)
                ?: throw IonException("Argument $argName expanded to nothing.")
        }

        /**
         * Read the expanded values from one argument, returning zero or one values.
         * Throws an exception if there is more than one expanded value.
         */
        fun readZeroOrOneExpandedArgumentValues(expansionInfo: ExpansionInfo, macroEvaluator: MacroEvaluator, argName: String): DataModelExpression? {
            var value: DataModelExpression? = null
            readExpandedArgumentValues(expansionInfo, macroEvaluator) {
                if (value == null) {
                    value = it
                } else {
                    throw IonException("Too many values for argument $argName")
                }
                true // Continue expansion
            }
            return value
        }

        /**
         * Reads the expanded values from one argument.
         *
         * The callback should return true to continue the expansion or false to abandon the expansion early.
         */
        fun readExpandedArgumentValues(expansionInfo: ExpansionInfo, macroEvaluator: MacroEvaluator, callback: (DataModelExpression) -> Boolean) {
            val i = expansionInfo.i
            expansionInfo.nextSourceExpression()

            macroEvaluator.pushExpansion(
                expansionKind = ExpansionKind.Values,
                argsStartInclusive = i,
                // There can only be one top-level expression for an argument (it's either a value, macro, or
                // expression group) so we can set the end to one more than the start.
                argsEndExclusive = i + 1,
                environment = expansionInfo.environment ?: Environment.EMPTY,
                expressions = expansionInfo.expressions!!,
            )

            val depth = macroEvaluator.expansionStack.size()
            var expr = macroEvaluator.expandNext(depth)
            var continueExpansion: Boolean
            while (expr != null) {
                continueExpansion = callback(expr)
                if (!continueExpansion) break
                expr = macroEvaluator.expandNext(depth)
            }
            // Step back out to the original depth (in case we exited the expansion early)
            while (macroEvaluator.expansionStack.size() > depth) {
                macroEvaluator.expansionStack.pop()
            }
        }

        /**
         * Reads the first expanded value from one argument.
         *
         * Does not perform any sort of cardinality check, and leaves the evaluator stepped into the level of the
         * returned expression. Returns null if the argument expansion produces no values.
         */
        fun readFirstExpandedArgumentValue(expansionInfo: ExpansionInfo, macroEvaluator: MacroEvaluator): DataModelExpression? {
            val i = expansionInfo.i
            expansionInfo.nextSourceExpression()

            macroEvaluator.pushExpansion(
                expansionKind = ExpansionKind.Values,
                argsStartInclusive = i,
                // There can only be one top-level expression for an argument (it's either a value, macro, or
                // expression group) so we can set the end to one more than the start.
                argsEndExclusive = i + 1,
                environment = expansionInfo.environment ?: Environment.EMPTY,
                expressions = expansionInfo.expressions!!,
            )

            val depth = macroEvaluator.expansionStack.size()
            return macroEvaluator.expandNext(depth)
        }
    }

    private object SimpleExpander : Expander {
        override fun nextExpression(expansionInfo: ExpansionInfo, macroEvaluator: MacroEvaluator): Expression {
            return expansionInfo.nextSourceExpression()
        }
    }

    private object AnnotateExpander : Expander {
        // TODO: Handle edge cases mentioned in https://github.com/amazon-ion/ion-docs/issues/347
        override fun nextExpression(expansionInfo: ExpansionInfo, macroEvaluator: MacroEvaluator): Expression {
            val annotations = mutableListOf<SymbolToken>()

            readExpandedArgumentValues(expansionInfo, macroEvaluator) {
                when (it) {
                    is StringValue -> annotations.add(newSymbolToken(it.value))
                    is SymbolValue -> annotations.add(it.value)
                    is DataModelValue -> throw IonException("Annotation arguments must be string or symbol; found: ${it.type}")
                    is FieldName -> TODO("Unreachable. Must encounter a StructValue first.")
                }
            }

            val valueToAnnotate = readExactlyOneExpandedArgumentValue(expansionInfo, macroEvaluator, SystemMacro.Annotate.signature[1].variableName)

            // It cannot be a FieldName expression because we haven't stepped into a struct, so it must be DataModelValue
            valueToAnnotate as DataModelValue
            // Combine the annotations
            annotations.addAll(valueToAnnotate.annotations)
            return valueToAnnotate.withAnnotations(annotations)
        }
    }

    private object MakeStringExpander : Expander {
        override fun nextExpression(expansionInfo: ExpansionInfo, macroEvaluator: MacroEvaluator): Expression {
            val sb = StringBuilder()
            readExpandedArgumentValues(expansionInfo, macroEvaluator) {
                when (it) {
                    is StringValue -> sb.append(it.value)
                    is SymbolValue -> sb.append(it.value.assumeText())
                    is DataModelValue -> throw IonException("Invalid argument type for 'make_string': ${it.type}")
                    is FieldName -> TODO("Unreachable. We shouldn't be able to get here without first encountering a StructValue.")
                }
                true // continue expansion
            }
            return StringValue(value = sb.toString())
        }
    }

    private object MakeSymbolExpander : Expander {
        override fun nextExpression(expansionInfo: ExpansionInfo, macroEvaluator: MacroEvaluator): Expression {
            val sb = StringBuilder()
            readExpandedArgumentValues(expansionInfo, macroEvaluator) {
                when (it) {
                    is StringValue -> sb.append(it.value)
                    is SymbolValue -> sb.append(it.value.assumeText())
                    is DataModelValue -> throw IonException("Invalid argument type for 'make_symbol': ${it.type}")
                    is FieldName -> TODO("Unreachable. We shouldn't be able to get here without first encountering a StructValue.")
                }
                true // continue expansion
            }
            return SymbolValue(value = newSymbolToken(sb.toString()))
        }
    }

    private object MakeBlobExpander : Expander {
        override fun nextExpression(expansionInfo: ExpansionInfo, macroEvaluator: MacroEvaluator): Expression {
            // TODO: See if we can create a `ByteArrayView` or similar class based on the principles of a Persistent
            //       Collection in order to minimize copying (and therefore allocation).
            val baos = ByteArrayOutputStream()
            readExpandedArgumentValues(expansionInfo, macroEvaluator) {
                when (it) {
                    is LobValue -> baos.write(it.value)
                    is DataModelValue -> throw IonException("Invalid argument type for 'make_blob': ${it.type}")
                    is FieldName -> TODO("Unreachable. We shouldn't be able to get here without first encountering a StructValue.")
                }
                true // continue expansion
            }
            return BlobValue(value = baos.toByteArray())
        }
    }

    private object MakeDecimalExpander : Expander {
        override fun nextExpression(expansionInfo: ExpansionInfo, macroEvaluator: MacroEvaluator): Expression {
            val coefficient = readExactlyOneExpandedArgumentValue(expansionInfo, macroEvaluator, SystemMacro.MakeDecimal.signature[0].variableName)
                .let { it as? IntValue }
                ?.bigIntegerValue
                ?: throw IonException("Coefficient must be an integer")
            val exponent = readExactlyOneExpandedArgumentValue(expansionInfo, macroEvaluator, SystemMacro.MakeDecimal.signature[1].variableName)
                .let { it as? IntValue }
                ?.bigIntegerValue
                ?: throw IonException("Exponent must be an integer")

            return DecimalValue(value = BigDecimal(coefficient, -1 * exponent.intValueExact()))
        }
    }

    private enum class IfExpander(private val minInclusive: Int, private val maxExclusive: Int) : Expander {
        IF_NONE(0, 1),
        IF_SOME(1, -1),
        IF_SINGLE(1, 2),
        IF_MULTI(2, -1),
        ;

        override fun nextExpression(expansionInfo: ExpansionInfo, macroEvaluator: MacroEvaluator): Expression {
            var n = 0
            readExpandedArgumentValues(expansionInfo, macroEvaluator) {
                n++
                // If there's no max, then we'll only continue the expansion if we haven't yet reached the min
                // If there is a max, then we'll continue the expansion until we reach the max
                if (maxExclusive < 0) n < minInclusive else n < maxExclusive
            }
            val isConditionTrue = n >= minInclusive && (maxExclusive < 0 || n < maxExclusive)
            // Save the current expansion index. This is the index of the "true" expression
            val trueExpressionPosition = expansionInfo.i
            // Now we are positioned on the "false" expression
            expansionInfo.nextSourceExpression()
            if (isConditionTrue) {
                // If the condition is true, we can set the EXCLUSIVE END of this expansion to the position of the
                // "false" expression, and then we reset the current index to the position of the "true" expression.
                expansionInfo.endExclusive = expansionInfo.i
                expansionInfo.i = trueExpressionPosition
            }
            return expansionInfo.nextSourceExpression()
        }
    }

    private object RepeatExpander : Expander {
        /**
         * Initializes the counter of the number of iterations remaining.
         * [ExpansionInfo.additionalState] is the number of iterations remaining. Once initialized, it is always `Int`.
         */
        private fun init(expansionInfo: ExpansionInfo, macroEvaluator: MacroEvaluator): Int {
            val nExpression = readExactlyOneExpandedArgumentValue(expansionInfo, macroEvaluator, "n")
            var iterationsRemaining = when (nExpression) {
                is LongIntValue -> nExpression.value.toInt()
                is BigIntValue -> {
                    if (nExpression.value.bitLength() >= Int.SIZE_BITS) {
                        throw IonException("ion-java does not support repeats of more than ${Int.MAX_VALUE}")
                    }
                    nExpression.value.intValueExact()
                }
                else -> throw IonException("The first argument of repeat must be a positive integer")
            }
            if (iterationsRemaining <= 0) {
                // TODO: Confirm https://github.com/amazon-ion/ion-docs/issues/350
                throw IonException("The first argument of repeat must be a positive integer")
            }
            // Decrement because we're starting the first iteration right away.
            iterationsRemaining--
            expansionInfo.additionalState = iterationsRemaining
            return iterationsRemaining
        }

        override fun nextExpression(expansionInfo: ExpansionInfo, macroEvaluator: MacroEvaluator): Expression {
            val repeatsRemaining = expansionInfo.additionalState as? Int
                ?: init(expansionInfo, macroEvaluator)

            val repeatedExpressionIndex = expansionInfo.i
            val next = readFirstExpandedArgumentValue(expansionInfo, macroEvaluator)
            next ?: throw IonException("repeat macro requires at least one value for value parameter")
            if (repeatsRemaining > 0) {
                expansionInfo.additionalState = repeatsRemaining - 1
                expansionInfo.i = repeatedExpressionIndex
            }
            return next
        }
    }

    private object MakeFieldExpander : Expander {
        override fun nextExpression(expansionInfo: ExpansionInfo, macroEvaluator: MacroEvaluator): Expression {
            /**
             * Uses [ExpansionInfo.additionalState] to track whether the expansion is on the field name or value.
             * If unset, reads the field name. If set to 0, reads the field value.
             */
            return when (expansionInfo.additionalState) {
                // First time, get the field name
                null -> {
                    val fieldName = readExactlyOneExpandedArgumentValue(expansionInfo, macroEvaluator, "field_name")
                    val fieldNameExpression = when (fieldName) {
                        is SymbolValue -> FieldName(fieldName.value)
                        else -> throw IonException("the first argument of make_field must expand to exactly one symbol value")
                    }
                    expansionInfo.additionalState = 0
                    fieldNameExpression
                }
                0 -> {
                    val value = readExactlyOneExpandedArgumentValue(expansionInfo, macroEvaluator, "value")
                    expansionInfo.additionalState = 1
                    value
                }
                else -> throw IllegalStateException("Unreachable")
            }
        }
    }

    private enum class ExpansionKind(val expander: Expander) {
        Container(SimpleExpander),
        TemplateBody(SimpleExpander),
        Values(SimpleExpander),
        Annotate(AnnotateExpander),
        MakeString(MakeStringExpander),
        MakeSymbol(MakeSymbolExpander),
        MakeBlob(MakeBlobExpander),
        MakeDecimal(MakeDecimalExpander),
        MakeField(MakeFieldExpander),
        IfNone(IfExpander.IF_NONE),
        IfSome(IfExpander.IF_SOME),
        IfSingle(IfExpander.IF_SINGLE),
        IfMulti(IfExpander.IF_MULTI),
        Repeat(RepeatExpander),
        ;

        companion object {
            @JvmStatic
            fun forSystemMacro(macro: SystemMacro): ExpansionKind {
                return when (macro) {
                    SystemMacro.None -> Values // "none" takes no args, so we can treat it as an empty "values" expansion
                    SystemMacro.Values -> Values
                    SystemMacro.Annotate -> Annotate
                    SystemMacro.MakeString -> MakeString
                    SystemMacro.MakeSymbol -> MakeSymbol
                    SystemMacro.MakeBlob -> MakeBlob
                    SystemMacro.MakeDecimal -> MakeDecimal
                    SystemMacro.IfNone -> IfNone
                    SystemMacro.IfSome -> IfSome
                    SystemMacro.IfSingle -> IfSingle
                    SystemMacro.IfMulti -> IfMulti
                    SystemMacro.Repeat -> Repeat
                    SystemMacro.MakeField -> MakeField
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

        /**
         * Field for storing any additional state required in an expander.
         *
         * TODO: Once all system macros are implemented, see if we can make this an int instead
         *
         * There is currently some lost space in ExpansionInfo. We can add one more `additionalState` field without
         * actually increasing the object size.
         */
        @JvmField
        var additionalState: Any? = null

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
        // Pretend that the whole thing is a "values" expansion so that we don't have to care about what
        // the first expression actually is.
        pushExpansion(ExpansionKind.Values, 0, encodingExpressions.size, Environment.EMPTY, encodingExpressions)
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
            macro = expression.macro,
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
            macro = expression.macro,
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
        macro: Macro,
        argsStartInclusive: Int,
        argsEndExclusive: Int,
        environment: Environment,
        encodingExpressions: List<Expression>,
    ) {
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
            it.additionalState = null
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
