// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.macro

import com.amazon.ion.*
import com.amazon.ion.impl._Private_RecyclingStack
import com.amazon.ion.impl._Private_Utils.*
import com.amazon.ion.impl.macro.Expression.*
import com.amazon.ion.impl.macro.MacroEvaluator.ExpanderKind.*
import com.amazon.ion.util.*
import java.io.ByteArrayOutputStream
import java.lang.StringBuilder
import java.math.BigDecimal
import java.math.BigInteger

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
class MacroEvaluator {

    private var numExpandedExpressions = 0
    private val expansionLimit: Int = 1_000_000
    private val expanderPool: ArrayList<ExpansionFrame> = ArrayList(32)

    // TODO(PERF): Does it improve performance if we make this an `inner` class and remove the evaluator field?
    class MacroEvaluationSession(private val evaluator: MacroEvaluator) {

        fun getExpander(
            expanderKind: ExpanderKind,
            expressions: List<Expression>,
            startInclusive: Int,
            endExclusive: Int,
            environment: Environment,
        ): ExpansionFrame {
            val expansion = evaluator.expanderPool.removeLastOrNull() ?: ExpansionFrame(this)
            expansion.expanderKind = expanderKind
            expansion.expressions = expressions
            expansion.i = startInclusive
            expansion.endExclusive = endExclusive
            expansion.environment = environment
            expansion.additionalState = null
            expansion.expansionDelegate = null
            return expansion
        }

        fun returnExpander(ex: ExpansionFrame) {
            // TODO: This check is O(n). Remove this when confident.
            check(ex !in evaluator.expanderPool)
            evaluator.expanderPool.add(ex)
        }

        fun incrementStepCounter() {
            evaluator.numExpandedExpressions++
            if (evaluator.numExpandedExpressions > evaluator.expansionLimit) {
                // Technically, we are not counting "steps" because we don't have a true definition of what a "step" is,
                // but this is probably a more user-friendly message than trying to explain what we're actually counting.
                throw IonException("Macro expansion exceeded limit of ${evaluator.expansionLimit} steps.")
            }
        }
    }

    private data class ContainerInfo(var type: Type = Type.Uninitialized, private var _expansion: ExpansionFrame? = null) {
        enum class Type { TopLevel, List, Sexp, Struct, Uninitialized }

        fun releaseResources() {
            _expansion?.drop()
            _expansion = null
            type = Type.Uninitialized
        }

        var expansion: ExpansionFrame
            get() = _expansion!!
            set(value) { _expansion = value }

        fun produceNext(): ExpansionOutputExpression {
            return expansion.produceNext()
        }
    }

    private val session = MacroEvaluationSession(this)
    private val containerStack = _Private_RecyclingStack(8) { ContainerInfo() }
    private var currentExpr: DataModelExpression? = null

    private fun resetSession() { this.numExpandedExpressions = 0 }

    fun getArguments(): List<Expression> {
        return containerStack.iterator().next().expansion.expressions
    }

    /**
     * Initialize the macro evaluator with an E-Expression.
     */
    fun initExpansion(encodingExpressions: List<EExpressionBodyExpression>) {
        resetSession()
        containerStack.push { ci ->
            ci.type = ContainerInfo.Type.TopLevel
            ci.expansion = session.getExpander(Stream, encodingExpressions, 0, encodingExpressions.size, Environment.EMPTY)
        }
    }

    /**
     * Evaluate the macro expansion until the next [DataModelExpression] can be returned.
     * Returns null if at the end of a container or at the end of the expansion.
     */
    fun expandNext(): ExpansionOutputExpression? {
        currentExpr = null
        while (currentExpr == null && !containerStack.isEmpty()) {
            val currentContainer = containerStack.peek()

            when (val nextExpansionOutput = currentContainer.produceNext()) {
                is DataModelExpression -> currentExpr = nextExpansionOutput
                EndOfExpansion -> {
                    if (currentContainer.type == ContainerInfo.Type.TopLevel) {
                        currentContainer.releaseResources()
                        containerStack.pop()
                    }
                    return null
                }
            }
        }
        return currentExpr
    }

    /**
     * Steps out of the current [DataModelContainer].
     */
    fun stepOut() {
        // TODO: We should be able to step out of a "TopLevel" container and/or we need some way to close the evaluation.
        if (containerStack.size() <= 1) throw IonException("Nothing to step out of.")
        val popped = containerStack.pop()
        popped.releaseResources()
    }

    /**
     * Steps in to the current [DataModelContainer].
     * Throws [IonException] if not positioned on a container.
     */
    fun stepIn() {
        val expression = requireNotNull(currentExpr) { "Not positioned on a value" }
        if (expression is DataModelContainer) {
            val currentContainer = containerStack.peek()
            val topExpansion = currentContainer.expansion.top()
            containerStack.push { ci ->
                ci.type = when (expression.type) {
                    IonType.LIST -> ContainerInfo.Type.List
                    IonType.SEXP -> ContainerInfo.Type.Sexp
                    IonType.STRUCT -> ContainerInfo.Type.Struct
                    else -> unreachable()
                }
                ci.expansion = session.getExpander(
                    expanderKind = Stream,
                    expressions = topExpansion.expressions,
                    startInclusive = expression.startInclusive,
                    endExclusive = expression.endExclusive,
                    environment = topExpansion.environment,
                )
            }
            currentExpr = null
        } else {
            throw IonException("Not positioned on a container.")
        }
    }

    // TODO(PERF): It might be possible to optimize this by changing it to an enum without any methods (or even a set of
    //             integer constants) and converting all their implementations to static methods.
    enum class ExpanderKind {
        Uninitialized {
            override fun produceNext(thisExpansion: ExpansionFrame): ExpansionOutputExpressionOrContinue {
                throw IllegalStateException("ExpansionInfo not initialized.")
            }
        },
        Empty {
            override fun produceNext(thisExpansion: ExpansionFrame): ExpansionOutputExpressionOrContinue = EndOfExpansion
        },
        Stream {
            private fun invokeMacro(thisExpansion: ExpansionFrame, macro: Macro, next: HasStartAndEnd): ContinueExpansion {
                val argIndices = macro.calculateArgumentIndices(
                    encodingExpressions = thisExpansion.expressions,
                    argsStartInclusive = next.startInclusive,
                    argsEndExclusive = next.endExclusive,
                )
                val argsIndicesByName = macro.calculateArgumentIndicesByName(thisExpansion.expressions, next.startInclusive, next.endExclusive)
                val newEnvironment = thisExpansion.environment.createChild(thisExpansion.expressions, argIndices, argsIndicesByName)
                val expanderKind = if (macro.body != null) Stream else getExpanderKindForSystemMacro(macro as SystemMacro)
                thisExpansion.expansionDelegate = thisExpansion.session.getExpander(
                    expanderKind = expanderKind,
                    expressions = macro.body ?: emptyList(),
                    startInclusive = 0,
                    endExclusive = macro.body?.size ?: 0,
                    environment = newEnvironment,
                )

                return ContinueExpansion
            }

            override fun produceNext(thisExpansion: ExpansionFrame): ExpansionOutputExpressionOrContinue {
                // If there's a delegate, we'll try that first.
                val delegate = thisExpansion.expansionDelegate
                check(thisExpansion != delegate)
                if (delegate != null) {
                    return when (val result = delegate.produceNext()) {
                        is DataModelExpression -> result
                        EndOfExpansion -> {
                            delegate.drop()
                            thisExpansion.expansionDelegate = null
                            ContinueExpansion
                        }
                    }
                }

                if (thisExpansion.i >= thisExpansion.endExclusive) {
                    thisExpansion.expanderKind = Empty
                    return ContinueExpansion
                }

                val next = thisExpansion.expressions[thisExpansion.i]
                thisExpansion.i++
                if (next is HasStartAndEnd) thisExpansion.i = next.endExclusive

                return when (next) {
                    is DataModelExpression -> next
                    is EExpression -> invokeMacro(thisExpansion, next.macro, next)
                    is MacroInvocation -> invokeMacro(thisExpansion, next.macro, next)
                    is ExpressionGroup -> {
                        thisExpansion.expansionDelegate = thisExpansion.session.getExpander(
                            expanderKind = Stream,
                            expressions = thisExpansion.expressions,
                            startInclusive = next.startInclusive,
                            endExclusive = next.endExclusive,
                            environment = thisExpansion.environment,
                        )

                        ContinueExpansion
                    }

                    is VariableRef -> {
                        thisExpansion.expansionDelegate = thisExpansion.readArgument(next)
                        ContinueExpansion
                    }
                    Placeholder -> unreachable()
                }
            }
        },
        // TODO: Move this into the variable expansion?
        ExactlyOneValueStream {
            override fun produceNext(thisExpansion: ExpansionFrame): ExpansionOutputExpressionOrContinue {
                if (thisExpansion.additionalState != 1) {
                    return when (val firstValue = Stream.produceNext(thisExpansion)) {
                        is DataModelExpression -> {
                            thisExpansion.additionalState = 1
                            firstValue
                        }
                        ContinueExpansion -> ContinueExpansion
                        EndOfExpansion -> throw IonException("Expected one value, found 0")
                    }
                } else {
                    return when (val secondValue = Stream.produceNext(thisExpansion)) {
                        is DataModelExpression -> throw IonException("Expected one value, found multiple")
                        ContinueExpansion -> ContinueExpansion
                        EndOfExpansion -> secondValue
                    }
                }
            }
        },
        NonEmptyStream {
            override fun produceNext(thisExpansion: ExpansionFrame): ExpansionOutputExpressionOrContinue {
                return when (val firstValue = Stream.produceNext(thisExpansion)) {
                    EndOfExpansion -> throw IonException("Expected at least one value, found 0")
                    ContinueExpansion -> ContinueExpansion
                    is DataModelExpression -> {
                        thisExpansion.expanderKind = Stream
                        firstValue
                    }
                }
            }
        },
        AtMostOneValueStream {
            override fun produceNext(thisExpansion: ExpansionFrame): ExpansionOutputExpressionOrContinue {
                if (thisExpansion.additionalState != 1) {
                    return when (val firstValue = Stream.produceNext(thisExpansion)) {
                        is DataModelExpression -> {
                            thisExpansion.additionalState = 1
                            firstValue
                        }
                        ContinueExpansion -> ContinueExpansion
                        EndOfExpansion -> EndOfExpansion
                    }
                } else {
                    return when (val secondValue = Stream.produceNext(thisExpansion)) {
                        is DataModelExpression -> throw IonException("Expected one value, found multiple")
                        ContinueExpansion -> ContinueExpansion
                        EndOfExpansion -> secondValue
                    }
                }
            }
        },

        IfNone {
            override fun produceNext(thisExpansion: ExpansionFrame) = checkExpansionSize(thisExpansion) { it == 0 }
        },
        IfSome {
            override fun produceNext(thisExpansion: ExpansionFrame) = checkExpansionSize(thisExpansion) { it > 0 }
        },
        IfSingle {
            override fun produceNext(thisExpansion: ExpansionFrame) = checkExpansionSize(thisExpansion) { it == 1 }
        },
        IfMulti {
            override fun produceNext(thisExpansion: ExpansionFrame) = checkExpansionSize(thisExpansion) { it > 1 }
        },
        Annotate {

            private val ANNOTATIONS_ARG = VariableRef(0)
            private val VALUE_TO_ANNOTATE_ARG = VariableRef(1)

            override fun produceNext(thisExpansion: ExpansionFrame): ExpansionOutputExpressionOrContinue {
                val annotations = thisExpansion.map(ANNOTATIONS_ARG) {
                    when (it) {
                        is StringValue -> newSymbolToken(it.value)
                        is SymbolValue -> it.value
                        is DataModelValue -> throw IonException("Invalid argument type for 'make_string': ${it.type}")
                        else -> unreachable("Unreachable without stepping in to a container")
                    }
                }

                val valueToAnnotateExpansion = thisExpansion.readArgument(VALUE_TO_ANNOTATE_ARG)

                val annotatedExpression = valueToAnnotateExpansion.produceNext().let {
                    it as? DataModelValue ?: throw IonException("Required at least one value.")
                    it.withAnnotations(annotations + it.annotations)
                }

                return annotatedExpression.also {
                    thisExpansion.tailCall(valueToAnnotateExpansion)
                    thisExpansion.expanderKind = ExactlyOneValueStream
                }
            }
        },
        MakeString {
            private val STRINGS_ARG = VariableRef(0)

            override fun produceNext(thisExpansion: ExpansionFrame): ExpansionOutputExpressionOrContinue {
                val sb = StringBuilder()
                thisExpansion.forEach(STRINGS_ARG) {
                    when (it) {
                        is StringValue -> sb.append(it.value)
                        is SymbolValue -> sb.append(it.value.assumeText())
                        is DataModelValue -> throw IonException("Invalid argument type for 'make_string': ${it.type}")
                        is FieldName -> unreachable()
                    }
                }
                thisExpansion.expanderKind = Empty
                return StringValue(value = sb.toString())
            }
        },
        MakeSymbol {
            private val STRINGS_ARG = VariableRef(0)

            override fun produceNext(thisExpansion: ExpansionFrame): ExpansionOutputExpressionOrContinue {
                if (thisExpansion.additionalState != null) return EndOfExpansion
                thisExpansion.additionalState = Unit

                val sb = StringBuilder()
                thisExpansion.forEach(STRINGS_ARG) {
                    when (it) {
                        is StringValue -> sb.append(it.value)
                        is SymbolValue -> sb.append(it.value.assumeText())
                        is DataModelValue -> throw IonException("Invalid argument type for 'make_symbol': ${it.type}")
                        is FieldName -> unreachable()
                    }
                }
                return SymbolValue(value = newSymbolToken(sb.toString()))
            }
        },
        MakeBlob {
            private val LOB_ARG = VariableRef(0)

            override fun produceNext(thisExpansion: ExpansionFrame): ExpansionOutputExpressionOrContinue {
                if (thisExpansion.additionalState != null) return EndOfExpansion
                thisExpansion.additionalState = Unit

                val baos = ByteArrayOutputStream()
                thisExpansion.forEach(LOB_ARG) {
                    when (it) {
                        is LobValue -> baos.write(it.value)
                        is DataModelValue -> throw IonException("Invalid argument type for 'make_blob': ${it.type}")
                        is FieldName -> unreachable()
                    }
                }
                return BlobValue(value = baos.toByteArray())
            }
        },
        MakeDecimal {
            private val COEFFICIENT_ARG = VariableRef(0)
            private val EXPONENT_ARG = VariableRef(1)

            override fun produceNext(thisExpansion: ExpansionFrame): ExpansionOutputExpressionOrContinue {
                if (thisExpansion.additionalState != null) return EndOfExpansion
                thisExpansion.additionalState = Unit

                val coefficient = thisExpansion.readExactlyOneArgument<IntValue>(COEFFICIENT_ARG).bigIntegerValue
                val exponent = thisExpansion.readExactlyOneArgument<IntValue>(EXPONENT_ARG).bigIntegerValue
                return DecimalValue(value = BigDecimal(coefficient, -1 * exponent.intValueExact()))
            }
        },
        MakeTimestamp {
            private val YEAR_ARG = VariableRef(0)
            private val MONTH_ARG = VariableRef(1)
            private val DAY_ARG = VariableRef(2)
            private val HOUR_ARG = VariableRef(3)
            private val MINUTE_ARG = VariableRef(4)
            private val SECOND_ARG = VariableRef(5)
            private val OFFSET_ARG = VariableRef(6)

            override fun produceNext(thisExpansion: ExpansionFrame): ExpansionOutputExpressionOrContinue {
                val year = thisExpansion.readExactlyOneArgument<IntValue>(YEAR_ARG).longValue.toInt()
                val month = thisExpansion.readZeroOrOneArgument<IntValue>(MONTH_ARG)?.longValue?.toInt()
                val day = thisExpansion.readZeroOrOneArgument<IntValue>(DAY_ARG)?.longValue?.toInt()
                val hour = thisExpansion.readZeroOrOneArgument<IntValue>(HOUR_ARG)?.longValue?.toInt()
                val minute = thisExpansion.readZeroOrOneArgument<IntValue>(MINUTE_ARG)?.longValue?.toInt()
                val second = thisExpansion.readZeroOrOneArgument<DataModelValue>(SECOND_ARG)?.let {
                    when (it) {
                        is DecimalValue -> it.value
                        is IntValue -> it.longValue.toBigDecimal()
                        else -> throw IonException("second must be an integer or decimal")
                    }
                }

                val offsetMinutes = thisExpansion.readZeroOrOneArgument<IntValue>(OFFSET_ARG)?.longValue?.toInt()

                try {
                    val ts = if (second != null) {
                        month ?: throw IonException("make_timestamp: month is required when second is present")
                        day ?: throw IonException("make_timestamp: day is required when second is present")
                        hour ?: throw IonException("make_timestamp: hour is required when second is present")
                        minute ?: throw IonException("make_timestamp: minute is required when second is present")
                        Timestamp.forSecond(year, month, day, hour, minute, second, offsetMinutes)
                    } else if (minute != null) {
                        month ?: throw IonException("make_timestamp: month is required when minute is present")
                        day ?: throw IonException("make_timestamp: day is required when minute is present")
                        hour ?: throw IonException("make_timestamp: hour is required when minute is present")
                        Timestamp.forMinute(year, month, day, hour, minute, offsetMinutes)
                    } else if (hour != null) {
                        throw IonException("make_timestamp: minute is required when hour is present")
                    } else {
                        if (offsetMinutes != null) throw IonException("make_timestamp: offset_minutes is prohibited when hours and minute are not present")
                        if (day != null) {
                            month ?: throw IonException("make_timestamp: month is required when day is present")
                            Timestamp.forDay(year, month, day)
                        } else if (month != null) {
                            Timestamp.forMonth(year, month)
                        } else {
                            Timestamp.forYear(year)
                        }
                    }
                    thisExpansion.expanderKind = Empty
                    return TimestampValue(value = ts)
                } catch (e: IllegalArgumentException) {
                    throw IonException(e.message)
                }
            }
        },
        _Private_MakeFieldNameAndValue {
            private val FIELD_NAME = VariableRef(0)
            private val FIELD_VALUE = VariableRef(1)

            override fun produceNext(thisExpansion: ExpansionFrame): ExpansionOutputExpressionOrContinue {
                val fieldName = thisExpansion.readExactlyOneArgument<TextValue>(FIELD_NAME)
                val fieldNameExpression = when (fieldName) {
                    is SymbolValue -> FieldName(fieldName.value)
                    is StringValue -> FieldName(newSymbolToken(fieldName.value))
                }

                thisExpansion.readExactlyOneArgument<DataModelValue>(FIELD_VALUE)

                val valueExpansion = thisExpansion.readArgument(FIELD_VALUE)

                return fieldNameExpression.also {
                    thisExpansion.tailCall(valueExpansion)
                    thisExpansion.expanderKind = ExactlyOneValueStream
                }
            }
        },

        _Private_FlattenStruct {
            private val STRUCTS = VariableRef(0)

            override fun produceNext(thisExpansion: ExpansionFrame): ExpansionOutputExpressionOrContinue {
                var argumentExpansion: ExpansionFrame? = thisExpansion.additionalState as ExpansionFrame?
                if (argumentExpansion == null) {
                    argumentExpansion = thisExpansion.readArgument(STRUCTS)
                    thisExpansion.additionalState = argumentExpansion
                }

                val currentChildExpansion = thisExpansion.expansionDelegate

                return when (val next = currentChildExpansion?.produceNext()) {
                    is DataModelExpression -> next
                    EndOfExpansion -> thisExpansion.dropDelegateAndContinue()
                    // Only possible if expansionDelegate is null
                    null -> when (val nextSequence = argumentExpansion.produceNext()) {
                        is StructValue -> {
                            thisExpansion.expansionDelegate = thisExpansion.session.getExpander(
                                expanderKind = Stream,
                                expressions = argumentExpansion.top().expressions,
                                startInclusive = nextSequence.startInclusive,
                                endExclusive = nextSequence.endExclusive,
                                environment = argumentExpansion.top().environment,
                            )
                            ContinueExpansion
                        }
                        EndOfExpansion -> EndOfExpansion
                        is DataModelExpression -> throw IonException("invalid argument; make_struct expects structs")
                    }
                }
            }
        },

        Flatten {
            private val SEQUENCES = VariableRef(0)

            override fun produceNext(thisExpansion: ExpansionFrame): ExpansionOutputExpressionOrContinue {
                var argumentExpansion: ExpansionFrame? = thisExpansion.additionalState as ExpansionFrame?
                if (argumentExpansion == null) {
                    argumentExpansion = thisExpansion.readArgument(SEQUENCES)
                    thisExpansion.additionalState = argumentExpansion
                }

                val currentChildExpansion = thisExpansion.expansionDelegate

                return when (val next = currentChildExpansion?.produceNext()) {
                    is DataModelExpression -> next
                    EndOfExpansion -> thisExpansion.dropDelegateAndContinue()
                    // Only possible if expansionDelegate is null
                    null -> when (val nextSequence = argumentExpansion.produceNext()) {
                        is StructValue -> throw IonException("invalid argument; flatten expects sequences")
                        is DataModelContainer -> {
                            thisExpansion.expansionDelegate = thisExpansion.session.getExpander(
                                expanderKind = Stream,
                                expressions = argumentExpansion.top().expressions,
                                startInclusive = nextSequence.startInclusive,
                                endExclusive = nextSequence.endExclusive,
                                environment = argumentExpansion.top().environment,
                            )

                            ContinueExpansion
                        }
                        EndOfExpansion -> EndOfExpansion
                        is DataModelExpression -> throw IonException("invalid argument; flatten expects sequences")
                    }
                }
            }
        },
        Sum {
            private val ARG_A = VariableRef(0)
            private val ARG_B = VariableRef(1)

            override fun produceNext(thisExpansion: ExpansionFrame): ExpansionOutputExpressionOrContinue {
                if (thisExpansion.additionalState != null) return EndOfExpansion
                thisExpansion.additionalState = Unit

                val a = thisExpansion.readExactlyOneArgument<IntValue>(ARG_A).bigIntegerValue
                val b = thisExpansion.readExactlyOneArgument<IntValue>(ARG_B).bigIntegerValue
                return BigIntValue(value = a + b)
            }
        },
        Delta {
            private val ARGS = VariableRef(0)

            // Initial value = 0
            override fun produceNext(thisExpansion: ExpansionFrame): ExpansionOutputExpressionOrContinue {
                // TODO: Optimize to use LongIntValue when possible
                var delegate = thisExpansion.expansionDelegate
                val runningTotal = thisExpansion.additionalState as? BigInteger ?: BigInteger.ZERO
                if (delegate == null) {
                    delegate = thisExpansion.readArgument(ARGS)
                    thisExpansion.expansionDelegate = delegate
                }

                when (val nextExpandedArg = delegate.produceNext()) {
                    is IntValue -> {
                        val nextDelta = nextExpandedArg.bigIntegerValue
                        val nextOutput = runningTotal + nextDelta
                        thisExpansion.additionalState = nextOutput
                        return BigIntValue(value = nextOutput)
                    }
                    EndOfExpansion -> return nextExpandedArg
                    is DataModelValue -> throw IonException("delta arguments must be integers")
                    is FieldName -> unreachable()
                }
            }
        },
        Repeat {
            private val COUNT_ARG = VariableRef(0)
            private val THING_TO_REPEAT = VariableRef(1)

            override fun produceNext(thisExpansion: ExpansionFrame): ExpansionOutputExpressionOrContinue {
                var n = thisExpansion.additionalState as Long?
                if (n == null) {
                    n = thisExpansion.readExactlyOneArgument<IntValue>(COUNT_ARG).longValue
                    if (n < 0) throw IonException("invalid argument; 'n' must be non-negative")
                    thisExpansion.additionalState = n
                }

                if (thisExpansion.expansionDelegate == null) {
                    if (n > 0) {
                        thisExpansion.expansionDelegate = thisExpansion.readArgument(THING_TO_REPEAT)
                        thisExpansion.additionalState = n - 1
                    } else {
                        return EndOfExpansion
                    }
                }

                val repeated = thisExpansion.expansionDelegate!!
                return when (val maybeNext = repeated.produceNext()) {
                    is DataModelExpression -> maybeNext
                    EndOfExpansion -> thisExpansion.dropDelegateAndContinue()
                }
            }
        },
        ;

        abstract fun produceNext(thisExpansion: ExpansionFrame): ExpansionOutputExpressionOrContinue

        internal inline fun checkExpansionSize(thisExpansion: ExpansionFrame, condition: (Int) -> Boolean): ContinueExpansion {
            val argToTest = VariableRef(0)
            val trueBranch = VariableRef(1)
            val falseBranch = VariableRef(2)

            val testArg = thisExpansion.readArgument(argToTest)
            var n = 0
            while (n < 2) {
                if (testArg.produceNext() is EndOfExpansion) break
                n++
            }
            testArg.drop()

            val branch = if (condition(n)) trueBranch else falseBranch
            val branchExpansion = thisExpansion.readArgument(branch)

            thisExpansion.tailCall(branchExpansion)
            return ContinueExpansion
        }

        internal fun VariableRef.readFrom(environment: Environment, session: MacroEvaluationSession): ExpansionFrame {
            val argIndex = environment.argumentIndices[signatureIndex]
            if (argIndex < 0) {
                // Argument was elided.
                return session.getExpander(Empty, emptyList(), 0, 0, Environment.EMPTY)
            }
            val firstArgExpression = environment.arguments[argIndex]

            return session.getExpander(
                expanderKind = Stream,
                expressions = environment.arguments,
                startInclusive = if (firstArgExpression is ExpressionGroup) firstArgExpression.startInclusive else argIndex,
                endExclusive = if (firstArgExpression is HasStartAndEnd) firstArgExpression.endExclusive else argIndex + 1,
                environment = environment.parentEnvironment!!
            )
        }

        internal fun ExpansionFrame.readArgument(variableRef: VariableRef): ExpansionFrame {
            // println("Reading argument for $variableRef")
            // println("From $environment")
            val argIndex = environment.argumentIndices[variableRef.signatureIndex]
            if (argIndex < 0) {
                // Argument was elided.
                return session.getExpander(Empty, emptyList(), 0, 0, Environment.EMPTY)
            }
            val firstArgExpression = environment.arguments[argIndex]
            return session.getExpander(
                expanderKind = Stream,
                expressions = environment.arguments,
                startInclusive = if (firstArgExpression is ExpressionGroup) firstArgExpression.startInclusive else argIndex,
                endExclusive = if (firstArgExpression is HasStartAndEnd) firstArgExpression.endExclusive else argIndex + 1,
                environment = environment.parentEnvironment!!
            )//.also { println("Variable $variableRef $it") }
        }

        internal inline fun ExpansionFrame.forEach(variableRef: VariableRef, action: (DataModelExpression) -> Unit) {
            val variableExpansion = readArgument(variableRef)
            while (true) {
                when (val next = variableExpansion.produceNext()) {
                    EndOfExpansion -> return
                    is DataModelExpression -> action(next)
                }
            }
        }

        internal inline fun <T> ExpansionFrame.map(variableRef: VariableRef, action: (DataModelExpression) -> T): List<T> {
            val variableExpansion = readArgument(variableRef)
            val result = mutableListOf<T>()
            while (true) {
                when (val next = variableExpansion.produceNext()) {
                    EndOfExpansion -> return result
                    is DataModelExpression -> result.add(action(next))
                }
            }
        }

        internal inline fun <reified T : DataModelValue> ExpansionFrame.readZeroOrOneArgument(variableRef: VariableRef): T? {
            val argExpansion = readArgument(variableRef)
            var argValue: T? = null
            while (true) {
                when (val it = argExpansion.produceNext()) {
                    is T -> if (argValue == null) {
                        argValue = it
                    } else {
                        throw IonException("invalid argument; too many values")
                    }
                    is DataModelValue -> throw IonException("invalid argument; found ${it.type}")
                    EndOfExpansion -> break
                    is FieldName -> unreachable("Unreachable without stepping into a container")
                }
            }
            return argValue
        }

        internal inline fun <reified T : DataModelValue> ExpansionFrame.readExactlyOneArgument(variableRef: VariableRef): T {
            return readZeroOrOneArgument<T>(variableRef) ?: throw IonException("invalid argument; no value when one is expected")
        }

        companion object {
            @JvmStatic
            fun getExpanderKindForSystemMacro(systemMacro: SystemMacro) = when (systemMacro) {
                SystemMacro.Annotate -> Annotate
                SystemMacro.MakeString -> MakeString
                SystemMacro.MakeSymbol -> MakeSymbol
                SystemMacro.MakeDecimal -> MakeDecimal
                SystemMacro.Repeat -> Repeat
                SystemMacro.Sum -> Sum
                SystemMacro.Delta -> Delta
                SystemMacro.MakeBlob -> MakeBlob
                SystemMacro.Flatten -> Flatten
                SystemMacro._Private_FlattenStruct -> _Private_FlattenStruct
                SystemMacro.MakeTimestamp -> MakeTimestamp
                SystemMacro._Private_MakeFieldNameAndValue -> _Private_MakeFieldNameAndValue
                SystemMacro.IfNone -> IfNone
                SystemMacro.IfSome -> IfSome
                SystemMacro.IfSingle -> IfSingle
                SystemMacro.IfMulti -> IfMulti
                else -> if (systemMacro.body != null) {
                    throw IllegalStateException("SystemMacro ${systemMacro.name} should be using its template body.")
                } else {
                    TODO("Not implemented yet: ${systemMacro.name}")
                }
            }
        }
    }

    class ExpansionFrame(
        @JvmField val session: MacroEvaluationSession,
        @JvmField var expanderKind: ExpanderKind = Uninitialized,
        /**
         * The [Expression]s being expanded. This MUST be the original list, not a sublist because
         * (a) we don't want to be allocating new sublists all the time, and (b) the
         * start and end indices of the expressions may be incorrect if a sublist is taken.
         */
        @JvmField var expressions: List<Expression> = emptyList(),
        /** Current position within [expressions] of this expansion */
        @JvmField var i: Int = 0,
        /** End of [expressions] that are applicable for this [ExpansionFrame] */
        @JvmField var endExclusive: Int = 0,
        /** The evaluation [Environment]—i.e. variable bindings. */
        @JvmField var environment: Environment = Environment.EMPTY,
        @JvmField var _expansionDelegate: ExpansionFrame? = null,
        @JvmField var additionalState: Any? = null,
    ) {

        var expansionDelegate: ExpansionFrame?
            get() = _expansionDelegate
            set(value) {
                check(value != this)
                _expansionDelegate = value
            }

        fun dropDelegateAndContinue(): ContinueExpansion {
            expansionDelegate?.drop()
            expansionDelegate = null
            return ContinueExpansion
        }

        fun top(): ExpansionFrame = expansionDelegate?.top() ?: this

        fun drop() {
            expanderKind = Uninitialized
            additionalState = null
            environment = Environment.EMPTY
            expressions = emptyList()
            expansionDelegate?.drop()
            expansionDelegate = null
            session.returnExpander(this)
        }

        fun initExpansion(
            expanderKind: ExpanderKind,
            expressions: List<Expression>,
            startInclusive: Int,
            endExclusive: Int,
            environment: Environment,
        ) {
            this.expanderKind = expanderKind
            this.expressions = expressions
            this.i = startInclusive
            this.endExclusive = endExclusive
            this.environment = environment
            additionalState = null
            expansionDelegate = null
        }

        fun tailCall(other: ExpansionFrame) {
            this.expanderKind = other.expanderKind
            this.expressions = other.expressions
            this.i = other.i
            this.endExclusive = other.endExclusive
            this.expansionDelegate = other.expansionDelegate
            this.additionalState = other.additionalState
            this.environment = other.environment
            // Drop `other`
            other.expansionDelegate = null
            other.drop()
        }

        fun produceNext(): ExpansionOutputExpression {
            while (true) {
                val next = expanderKind.produceNext(this)
                if (next is ExpansionOutputExpression) return next
                // Implied:
                // if (next is ContinueExpansion) continue
            }
        }

        override fun toString() = """
        |ExpansionFrame(
        |    expansionKind: $expanderKind,
        |    environment: ${environment.toString().lines().joinToString("\n|        ")},
        |    expressions: [
        |        ${expressions.mapIndexed { index, expression -> "$index. $expression" }.joinToString(",\n|        ") { it.toString() } }
        |    ],
        |    endExclusive: $endExclusive,
        |    i: $i,
        |    child: ${expansionDelegate?.expanderKind}
        |    additionalState: $additionalState,
        |)
        """.trimMargin()
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
private fun Macro.calculateArgumentIndices(
    encodingExpressions: List<Expression>,
    argsStartInclusive: Int,
    argsEndExclusive: Int
): List<Int> {
    // TODO: For TDL macro invocations, see if we can calculate this during the "compile" step.
    var numArgs = 0
    val argsIndices = IntArray(signature.size)
    var currentArgIndex = argsStartInclusive

    for (p in signature) {
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
    if (numArgs > signature.size) {
        throw IonException("Too many arguments. Expected ${signature.size}, but found $numArgs")
    }
    return argsIndices.toList()
}

private fun Macro.calculateArgumentIndicesByName(
    encodingExpressions: List<Expression>,
    argsStartInclusive: Int,
    argsEndExclusive: Int
): Map<Macro.Parameter, Int> {
    // TODO: For TDL macro invocations, see if we can calculate this during the "compile" step.
    var numArgs = 0
    val argsIndices = IntArray(signature.size)
    var currentArgIndex = argsStartInclusive

    for (p in signature) {
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
    if (numArgs > signature.size) {
        throw IonException("Too many arguments. Expected ${signature.size}, but found $numArgs")
    }
    return argsIndices.mapIndexed { i, it -> signature[i] to it }.toMap()
}
