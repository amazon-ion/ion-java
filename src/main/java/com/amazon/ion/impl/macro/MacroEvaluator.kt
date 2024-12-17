// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.macro

import com.amazon.ion.*
import com.amazon.ion.impl._Private_RecyclingStack
import com.amazon.ion.impl._Private_Utils.newSymbolToken
import com.amazon.ion.impl.macro.Expression.*
import com.amazon.ion.util.unreachable
import java.io.ByteArrayOutputStream
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
 *
 * TODO: Make expansion limit configurable.
 *
 * ### Implementation Overview:
 *
 * The macro evaluator can be thought of as a stack of containers where each container has a stack of expansion frames
 * (i.e. [ExpansionInfo]). To get the next value at the current depth, the macro evaluator starts with the bottom frame
 * of the top container. Expansion frames may delegate to and/or intercept other expansions that are further up the stack.
 *
 * One might visualize it like this:
 * ```
 * 3. List     : Stream --> Delta --> Variable
 * 2. List     : Stream --> Flatten --> Stream
 * 1. Struct   : Stream --> Variable --> TemplateBody --> Stream --> TemplateBody
 * 0. TopLevel : Stream --> TemplateBody --> TemplateBody
 * ```
 *
 * When calling [expandNext], the evaluator looks at the first expansion frame of the top container in the stack.
 * Then it calls `produceNext` for the first expansion in that container. That expansion may produce a result all on its
 * own, or it may call the next expansion in the chain and return that value (with or without further modification).
 *
 * In practice, it is a little more complex. A single expansion frame may hold more than one child frame sequentially
 * (e.g. `repeat`, `annotate`) or concurrently (e.g. `for`, `flatten`).
 */
class MacroEvaluator {

    /**
     * Holds state that is shared across all macro evaluations that are part of this evaluator.
     * This state pertains to a single "session" of the macro evaluator, and is reset every time [initExpansion] is called.
     * For now, this includes managing the pool of [ExpansionInfo] and tracking the expansion step limit.
     */
    private class Session(
        /** Number of expansion steps at which the evaluation session should be aborted. */
        private val expansionLimit: Int = 1_000_000
    ) {
        /** Internal state for tracking the number of expansion steps. */
        private var numExpandedExpressions = 0
        /** Pool of [ExpansionInfo] to minimize allocation and garbage collection. */
        private val expanderPool: ArrayList<ExpansionInfo> = ArrayList(32)

        /** Gets an [ExpansionInfo] from the pool (or allocates a new one if necessary), initializing it with the provided values. */
        fun getExpander(expansionKind: ExpansionKind, expressions: List<Expression>, startInclusive: Int, endExclusive: Int, environment: Environment): ExpansionInfo {
            val expansion = expanderPool.removeLastOrNull() ?: ExpansionInfo(this)
            expansion.expansionKind = expansionKind
            expansion.expressions = expressions
            expansion.i = startInclusive
            expansion.endExclusive = endExclusive
            expansion.environment = environment
            expansion.additionalState = null
            expansion.childExpansion = null
            return expansion
        }

        /** Reclaims an [ExpansionInfo] to the available pool. */
        fun reclaimExpander(ex: ExpansionInfo) {
            // TODO: This check is O(n). Consider removing this when confident there are no double frees.
            check(ex !in expanderPool)
            expanderPool.add(ex)
        }

        fun incrementStepCounter() {
            numExpandedExpressions++
            if (numExpandedExpressions > expansionLimit) {
                // Technically, we are not counting "steps" because we don't have a true definition of what a "step" is,
                // but this is probably a more user-friendly message than trying to explain what we're actually counting.
                throw IonException("Macro expansion exceeded limit of $expansionLimit steps.")
            }
        }

        fun reset() {
            numExpandedExpressions = 0
        }
    }

    /**
     * A container in the macro evaluator's [containerStack].
     */
    private data class ContainerInfo(var type: Type = Type.Uninitialized, private var _expansion: ExpansionInfo? = null) {
        enum class Type { TopLevel, List, Sexp, Struct, Uninitialized }

        fun close() {
            _expansion?.close()
            _expansion = null
            type = Type.Uninitialized
        }

        var expansion: ExpansionInfo
            get() = _expansion!!
            set(value) { _expansion = value }

        fun produceNext(): ExpansionOutputExpression {
            return expansion.produceNext()
        }
    }

    /**
     * Stateless functions that operate on the expansion frames (i.e. [ExpansionInfo]).
     */
    // TODO(PERF): It might be possible to optimize this by changing it to an enum without any methods (or even a set of
    //             integer constants) and converting all their implementations to static methods.
    private enum class ExpansionKind {
        Uninitialized {
            override fun produceNext(thisExpansion: ExpansionInfo): Nothing = throw IllegalStateException("ExpansionInfo not initialized.")
        },
        Empty {
            override fun produceNext(thisExpansion: ExpansionInfo): ExpansionOutputExpressionOrContinue = EndOfExpansion
        },
        Stream {
            override fun produceNext(thisExpansion: ExpansionInfo): ExpansionOutputExpressionOrContinue {
                // If there's a delegate, we'll try that first.
                val delegate = thisExpansion.childExpansion
                check(thisExpansion != delegate)
                if (delegate != null) {
                    return when (val result = delegate.produceNext()) {
                        is DataModelExpression -> result
                        EndOfExpansion -> {
                            delegate.close()
                            thisExpansion.childExpansion = null
                            ContinueExpansion
                        }
                    }
                }

                if (thisExpansion.i >= thisExpansion.endExclusive) {
                    thisExpansion.expansionKind = Empty
                    return ContinueExpansion
                }

                val next = thisExpansion.expressions[thisExpansion.i]
                thisExpansion.i++
                if (next is HasStartAndEnd) thisExpansion.i = next.endExclusive

                return when (next) {
                    is DataModelExpression -> next
                    is InvokableExpression -> {
                        val macro = next.macro
                        val argIndices = calculateArgumentIndices(macro, thisExpansion.expressions, next.startInclusive, next.endExclusive)
                        val newEnvironment = thisExpansion.environment.createChild(thisExpansion.expressions, argIndices)
                        val expansionKind = ExpansionKind.forMacro(macro)
                        thisExpansion.childExpansion = thisExpansion.session.getExpander(
                            expansionKind = expansionKind,
                            expressions = macro.body ?: emptyList(),
                            startInclusive = 0,
                            endExclusive = macro.body?.size ?: 0,
                            environment = newEnvironment,
                        )
                        ContinueExpansion
                    }
                    is ExpressionGroup -> {
                        thisExpansion.childExpansion = thisExpansion.session.getExpander(
                            expansionKind = ExprGroup,
                            expressions = thisExpansion.expressions,
                            startInclusive = next.startInclusive,
                            endExclusive = next.endExclusive,
                            environment = thisExpansion.environment,
                        )

                        ContinueExpansion
                    }

                    is VariableRef -> {
                        thisExpansion.childExpansion = thisExpansion.readArgument(next)
                        ContinueExpansion
                    }
                    Placeholder -> unreachable()
                }
            }
        },
        /** Alias of [Stream] to aid in debugging */
        Variable {
            override fun produceNext(thisExpansion: ExpansionInfo): ExpansionOutputExpressionOrContinue {
                return Stream.produceNext(thisExpansion)
            }
        },
        /** Alias of [Stream] to aid in debugging */
        TemplateBody {
            override fun produceNext(thisExpansion: ExpansionInfo): ExpansionOutputExpressionOrContinue {
                return Stream.produceNext(thisExpansion)
            }
        },
        /** Alias of [Stream] to aid in debugging */
        ExprGroup {
            override fun produceNext(thisExpansion: ExpansionInfo): ExpansionOutputExpressionOrContinue {
                return Stream.produceNext(thisExpansion)
            }
        },
        ExactlyOneValueStream {
            override fun produceNext(thisExpansion: ExpansionInfo): ExpansionOutputExpressionOrContinue {
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

        IfNone {
            override fun produceNext(thisExpansion: ExpansionInfo) = thisExpansion.branchIf { it == 0 }
        },
        IfSome {
            override fun produceNext(thisExpansion: ExpansionInfo) = thisExpansion.branchIf { it > 0 }
        },
        IfSingle {
            override fun produceNext(thisExpansion: ExpansionInfo) = thisExpansion.branchIf { it == 1 }
        },
        IfMulti {
            override fun produceNext(thisExpansion: ExpansionInfo) = thisExpansion.branchIf { it > 1 }
        },
        Annotate {

            private val ANNOTATIONS_ARG = VariableRef(0)
            private val VALUE_TO_ANNOTATE_ARG = VariableRef(1)

            override fun produceNext(thisExpansion: ExpansionInfo): ExpansionOutputExpressionOrContinue {
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
                if (valueToAnnotateExpansion.produceNext() != EndOfExpansion) {
                    throw IonException("Can only annotate exactly one value")
                }

                return annotatedExpression.also {
                    thisExpansion.tailCall(valueToAnnotateExpansion)
                }
            }
        },
        MakeString {
            private val STRINGS_ARG = VariableRef(0)

            override fun produceNext(thisExpansion: ExpansionInfo): ExpansionOutputExpressionOrContinue {
                val sb = StringBuilder()
                thisExpansion.forEach(STRINGS_ARG) {
                    when (it) {
                        is StringValue -> sb.append(it.value)
                        is SymbolValue -> sb.append(it.value.assumeText())
                        is DataModelValue -> throw IonException("Invalid argument type for 'make_string': ${it.type}")
                        is FieldName -> unreachable()
                    }
                }
                thisExpansion.expansionKind = Empty
                return StringValue(value = sb.toString())
            }
        },
        MakeSymbol {
            private val STRINGS_ARG = VariableRef(0)

            override fun produceNext(thisExpansion: ExpansionInfo): ExpansionOutputExpressionOrContinue {
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

            override fun produceNext(thisExpansion: ExpansionInfo): ExpansionOutputExpressionOrContinue {
                val baos = ByteArrayOutputStream()
                thisExpansion.forEach(LOB_ARG) {
                    when (it) {
                        is LobValue -> baos.write(it.value)
                        is DataModelValue -> throw IonException("Invalid argument type for 'make_blob': ${it.type}")
                        is FieldName -> unreachable()
                    }
                }
                thisExpansion.expansionKind = Empty
                return BlobValue(value = baos.toByteArray())
            }
        },
        MakeDecimal {
            private val COEFFICIENT_ARG = VariableRef(0)
            private val EXPONENT_ARG = VariableRef(1)

            override fun produceNext(thisExpansion: ExpansionInfo): ExpansionOutputExpressionOrContinue {
                val coefficient = thisExpansion.readExactlyOneArgument<IntValue>(COEFFICIENT_ARG).bigIntegerValue
                val exponent = thisExpansion.readExactlyOneArgument<IntValue>(EXPONENT_ARG).bigIntegerValue
                thisExpansion.expansionKind = Empty
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

            override fun produceNext(thisExpansion: ExpansionInfo): ExpansionOutputExpressionOrContinue {
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
                    thisExpansion.expansionKind = Empty
                    return TimestampValue(value = ts)
                } catch (e: IllegalArgumentException) {
                    throw IonException(e.message)
                }
            }
        },
        _Private_MakeFieldNameAndValue {
            private val FIELD_NAME = VariableRef(0)
            private val FIELD_VALUE = VariableRef(1)

            override fun produceNext(thisExpansion: ExpansionInfo): ExpansionOutputExpressionOrContinue {
                val fieldName = thisExpansion.readExactlyOneArgument<TextValue>(FIELD_NAME)
                val fieldNameExpression = when (fieldName) {
                    is SymbolValue -> FieldName(fieldName.value)
                    is StringValue -> FieldName(newSymbolToken(fieldName.value))
                }

                thisExpansion.readExactlyOneArgument<DataModelValue>(FIELD_VALUE)

                val valueExpansion = thisExpansion.readArgument(FIELD_VALUE)

                return fieldNameExpression.also {
                    thisExpansion.tailCall(valueExpansion)
                    thisExpansion.expansionKind = ExactlyOneValueStream
                }
            }
        },

        _Private_FlattenStruct {
            private val STRUCTS = VariableRef(0)

            override fun produceNext(thisExpansion: ExpansionInfo): ExpansionOutputExpressionOrContinue {
                var argumentExpansion: ExpansionInfo? = thisExpansion.additionalState as ExpansionInfo?
                if (argumentExpansion == null) {
                    argumentExpansion = thisExpansion.readArgument(STRUCTS)
                    thisExpansion.additionalState = argumentExpansion
                }

                val currentChildExpansion = thisExpansion.childExpansion

                return when (val next = currentChildExpansion?.produceNext()) {
                    is DataModelExpression -> next
                    EndOfExpansion -> thisExpansion.closeDelegateAndContinue()
                    // Only possible if expansionDelegate is null
                    null -> when (val nextSequence = argumentExpansion.produceNext()) {
                        is StructValue -> {
                            thisExpansion.childExpansion = thisExpansion.session.getExpander(
                                expansionKind = Stream,
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

        /**
         * Iterates over the sequences, returning the values contained in the sequences.
         * The expansion for the sequences argument is stored in [ExpansionInfo.additionalState].
         * When
         */
        Flatten {
            private val SEQUENCES = VariableRef(0)

            override fun produceNext(thisExpansion: ExpansionInfo): ExpansionOutputExpressionOrContinue {
                var argumentExpansion: ExpansionInfo? = thisExpansion.additionalState as ExpansionInfo?
                if (argumentExpansion == null) {
                    argumentExpansion = thisExpansion.readArgument(SEQUENCES)
                    thisExpansion.additionalState = argumentExpansion
                }

                val currentChildExpansion = thisExpansion.childExpansion

                return when (val next = currentChildExpansion?.produceNext()) {
                    is DataModelExpression -> next
                    EndOfExpansion -> thisExpansion.closeDelegateAndContinue()
                    // Only possible if expansionDelegate is null
                    null -> when (val nextSequence = argumentExpansion.produceNext()) {
                        is StructValue -> throw IonException("invalid argument; flatten expects sequences")
                        is DataModelContainer -> {
                            thisExpansion.childExpansion = thisExpansion.session.getExpander(
                                expansionKind = Stream,
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

            override fun produceNext(thisExpansion: ExpansionInfo): ExpansionOutputExpressionOrContinue {
                // TODO(PERF): consider checking whether the value would fit in a long and returning a `LongIntValue`.
                val a = thisExpansion.readExactlyOneArgument<IntValue>(ARG_A).bigIntegerValue
                val b = thisExpansion.readExactlyOneArgument<IntValue>(ARG_B).bigIntegerValue
                thisExpansion.expansionKind = Empty
                return BigIntValue(value = a + b)
            }
        },
        Delta {
            private val ARGS = VariableRef(0)

            override fun produceNext(thisExpansion: ExpansionInfo): ExpansionOutputExpressionOrContinue {
                // TODO(PERF): Optimize to use LongIntValue when possible
                var delegate = thisExpansion.childExpansion
                val runningTotal = thisExpansion.additionalState as? BigInteger ?: BigInteger.ZERO
                if (delegate == null) {
                    delegate = thisExpansion.readArgument(ARGS)
                    thisExpansion.childExpansion = delegate
                }

                when (val nextExpandedArg = delegate.produceNext()) {
                    is IntValue -> {
                        val nextDelta = nextExpandedArg.bigIntegerValue
                        val nextOutput = runningTotal + nextDelta
                        thisExpansion.additionalState = nextOutput
                        return BigIntValue(value = nextOutput)
                    }
                    EndOfExpansion -> return EndOfExpansion
                    else -> throw IonException("delta arguments must be integers")
                }
            }
        },
        Repeat {
            private val COUNT_ARG = VariableRef(0)
            private val THING_TO_REPEAT = VariableRef(1)

            override fun produceNext(thisExpansion: ExpansionInfo): ExpansionOutputExpressionOrContinue {
                var n = thisExpansion.additionalState as Long?
                if (n == null) {
                    n = thisExpansion.readExactlyOneArgument<IntValue>(COUNT_ARG).longValue
                    if (n < 0) throw IonException("invalid argument; 'n' must be non-negative")
                    thisExpansion.additionalState = n
                }

                if (thisExpansion.childExpansion == null) {
                    if (n > 0) {
                        thisExpansion.childExpansion = thisExpansion.readArgument(THING_TO_REPEAT)
                        thisExpansion.additionalState = n - 1
                    } else {
                        return EndOfExpansion
                    }
                }

                val repeated = thisExpansion.childExpansion!!
                return when (val maybeNext = repeated.produceNext()) {
                    is DataModelExpression -> maybeNext
                    EndOfExpansion -> thisExpansion.closeDelegateAndContinue()
                }
            }
        },
        ;

        /**
         * Produces the next value, [EndOfExpansion], or [ContinueExpansion].
         * Each enum variant must implement this method.
         */
        abstract fun produceNext(thisExpansion: ExpansionInfo): ExpansionOutputExpressionOrContinue

        /** Helper function for the `if_*` macros */
        inline fun ExpansionInfo.branchIf(condition: (Int) -> Boolean): ContinueExpansion {
            val argToTest = VariableRef(0)
            val trueBranch = VariableRef(1)
            val falseBranch = VariableRef(2)

            val testArg = readArgument(argToTest)
            var n = 0
            while (n < 2) {
                if (testArg.produceNext() is EndOfExpansion) break
                n++
            }
            testArg.close()

            val branch = if (condition(n)) trueBranch else falseBranch

            tailCall(readArgument(branch))
            return ContinueExpansion
        }

        /**
         * Returns an expansion for the given variable.
         */
        fun ExpansionInfo.readArgument(variableRef: VariableRef): ExpansionInfo {
            val argIndex = environment.argumentIndices[variableRef.signatureIndex]
            if (argIndex < 0) {
                // Argument was elided.
                return session.getExpander(Empty, emptyList(), 0, 0, Environment.EMPTY)
            }
            val firstArgExpression = environment.arguments[argIndex]
            return session.getExpander(
                expansionKind = Variable,
                expressions = environment.arguments,
                startInclusive = if (firstArgExpression is ExpressionGroup) firstArgExpression.startInclusive else argIndex,
                endExclusive = if (firstArgExpression is HasStartAndEnd) firstArgExpression.endExclusive else argIndex + 1,
                environment = environment.parentEnvironment!!
            )
        }

        /**
         * Performs the given [action] for each value produced by the expansion of [variableRef].
         */
        inline fun ExpansionInfo.forEach(variableRef: VariableRef, action: (DataModelExpression) -> Unit) {
            val variableExpansion = readArgument(variableRef)
            while (true) {
                when (val next = variableExpansion.produceNext()) {
                    EndOfExpansion -> return
                    is DataModelExpression -> action(next)
                }
            }
        }

        /**
         * Performs the given [transform] on each value produced by the expansion of [variableRef], returning a list
         * of the results.
         */
        inline fun <T> ExpansionInfo.map(variableRef: VariableRef, transform: (DataModelExpression) -> T): List<T> {
            val variableExpansion = readArgument(variableRef)
            val result = mutableListOf<T>()
            while (true) {
                when (val next = variableExpansion.produceNext()) {
                    EndOfExpansion -> return result
                    is DataModelExpression -> result.add(transform(next))
                }
            }
        }

        /**
         * Reads and returns zero or one values from the expansion of the given [variableRef].
         * Throws an [IonException] if more than one value is present in the variable expansion.
         * Throws an [IonException] if the value is not the expected type [T].
         */
        inline fun <reified T : DataModelValue> ExpansionInfo.readZeroOrOneArgument(variableRef: VariableRef): T? {
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
            argExpansion.close()
            return argValue
        }

        /**
         * Reads and returns exactly one value from the expansion of the given [variableRef].
         * Throws an [IonException] if the expansion of [variableRef] does not produce exactly one value.
         * Throws an [IonException] if the value is not the expected type [T].
         */
        inline fun <reified T : DataModelValue> ExpansionInfo.readExactlyOneArgument(variableRef: VariableRef): T {
            return readZeroOrOneArgument<T>(variableRef) ?: throw IonException("invalid argument; no value when one is expected")
        }

        companion object {
            /**
             * Gets the [ExpansionKind] for the given [macro].
             */
            @JvmStatic
            fun forMacro(macro: Macro): ExpansionKind {
                return if (macro.body != null) {
                    TemplateBody
                } else when (macro as SystemMacro) {
                    SystemMacro.IfNone -> IfNone
                    SystemMacro.IfSome -> IfSome
                    SystemMacro.IfSingle -> IfSingle
                    SystemMacro.IfMulti -> IfMulti
                    SystemMacro.Annotate -> Annotate
                    SystemMacro.MakeString -> MakeString
                    SystemMacro.MakeSymbol -> MakeSymbol
                    SystemMacro.MakeDecimal -> MakeDecimal
                    SystemMacro.MakeTimestamp -> MakeTimestamp
                    SystemMacro.MakeBlob -> MakeBlob
                    SystemMacro.Repeat -> Repeat
                    SystemMacro.Sum -> Sum
                    SystemMacro.Delta -> Delta
                    SystemMacro.Flatten -> Flatten
                    SystemMacro._Private_FlattenStruct -> _Private_FlattenStruct
                    SystemMacro._Private_MakeFieldNameAndValue -> _Private_MakeFieldNameAndValue
                    else -> TODO("Not implemented yet: ${macro.name}")
                }
            }
        }
    }

    /**
     * Represents a frame in the expansion stack for a particular container.
     *
     * TODO: "info" is very non-specific; rename to ExpansionFrame next time there's a
     *       non-functional refactoring in this class.
     */
    private class ExpansionInfo(@JvmField val session: Session) {

        /** The [ExpansionKind]. */
        @JvmField var expansionKind: ExpansionKind = ExpansionKind.Uninitialized
        /**
         * The evaluation [Environment]—i.e. variable bindings.
         */
        @JvmField var environment: Environment = Environment.EMPTY
        /**
         * The [Expression]s being expanded. This MUST be the original list, not a sublist because
         * (a) we don't want to be allocating new sublists all the time, and (b) the
         * start and end indices of the expressions may be incorrect if a sublist is taken.
         */
        @JvmField var expressions: List<Expression> = emptyList()
        /** End of [expressions] that are applicable for this [ExpansionInfo] */
        @JvmField var endExclusive: Int = 0
        /** Current position within [expressions] of this expansion */
        @JvmField var i: Int = 0

        /**
         * Field for storing any additional state required by an ExpansionKind.
         */
        @JvmField
        var additionalState: Any? = null

        /**
         * Additional state in the form of a child [ExpansionInfo].
         */
        var childExpansion: ExpansionInfo? = null
            // TODO: if childExpansion == this, it will cause an infinite loop or stack overflow somewhere.
            // In practice, it should never happen, so we may wish to remove the custom setter to avoid any performance impact.
            set(value) {
                check(value != this)
                field = value
            }

        /**
         * Convenience function to close the [childExpansion] and return it to the pool.
         */
        fun closeDelegateAndContinue(): ContinueExpansion {
            childExpansion?.close()
            childExpansion = null
            return ContinueExpansion
        }

        /**
         * Gets the [ExpansionInfo] at the top of the stack of [childExpansion]s.
         */
        fun top(): ExpansionInfo = childExpansion?.top() ?: this

        /**
         * Returns this [ExpansionInfo] to the expander pool, recursively closing [childExpansion]s in the process.
         * Could also be thought of as a `free` function.
         */
        fun close() {
            expansionKind = ExpansionKind.Uninitialized
            environment = Environment.EMPTY
            expressions = emptyList()
            additionalState?.let { if (it is ExpansionInfo) it.close() }
            additionalState = null
            childExpansion?.close()
            childExpansion = null
            session.reclaimExpander(this)
        }

        /**
         * Replaces the state of `this` [ExpansionInfo] with the state of [other]—effectively a tail-call optimization.
         * After transferring the state, `other` is returned to the expansion pool.
         */
        fun tailCall(other: ExpansionInfo) {
            this.expansionKind = other.expansionKind
            this.expressions = other.expressions
            this.i = other.i
            this.endExclusive = other.endExclusive
            this.childExpansion = other.childExpansion
            this.additionalState = other.additionalState
            this.environment = other.environment
            // Close `other`
            other.childExpansion = null
            other.close()
        }

        /**
         * Produces the next value from this expansion.
         */
        fun produceNext(): ExpansionOutputExpression {
            while (true) {
                val next = expansionKind.produceNext(this)
                if (next is ContinueExpansion) continue
                // This the only place where we count the expansion steps.
                // It is theoretically possible to have macro expansions that are millions of levels deep because this
                // only counts macro invocations at the end of their expansion, but this will still work to catch things
                // like a  billion laughs attack because it does place a limit on the number of _values_ produced.
                // This counts every value _at every level_, so most values will be counted multiple times. If possible
                // without impacting performance, count values only once in order to have more predictable behavior.
                session.incrementStepCounter()
                return next as ExpansionOutputExpression
            }
        }

        override fun toString() = """
        |ExpansionInfo(
        |    expansionKind: $expansionKind,
        |    environment: ${environment.toString().lines().joinToString("\n|        ")},
        |    expressions: [
        |        ${expressions.mapIndexed { i, expr -> "$i. $expr" }.joinToString(",\n|        ") }
        |    ],
        |    endExclusive: $endExclusive,
        |    i: $i,
        |    child: ${childExpansion?.expansionKind}
        |    additionalState: $additionalState,
        |)
        """.trimMargin()
    }

    private val session = Session(expansionLimit = 1_000_000)
    private val containerStack = _Private_RecyclingStack(8) { ContainerInfo() }
    private var currentExpr: DataModelExpression? = null

    /**
     * Returns the e-expression argument expressions that this MacroEvaluator would evaluate.
     */
    fun getArguments(): List<Expression> {
        return containerStack.iterator().next().expansion.expressions
    }

    /**
     * Initialize the macro evaluator with an E-Expression.
     */
    fun initExpansion(encodingExpressions: List<EExpressionBodyExpression>) {
        session.reset()
        containerStack.push { ci ->
            ci.type = ContainerInfo.Type.TopLevel
            ci.expansion = session.getExpander(ExpansionKind.Stream, encodingExpressions, 0, encodingExpressions.size, Environment.EMPTY)
        }
    }

    /**
     * Evaluate the macro expansion until the next [DataModelExpression] can be returned.
     * Returns null if at the end of a container or at the end of the expansion.
     */
    fun expandNext(): DataModelExpression? {
        currentExpr = null
        val currentContainer = containerStack.peek()
        when (val nextExpansionOutput = currentContainer.produceNext()) {
            is DataModelExpression -> currentExpr = nextExpansionOutput
            EndOfExpansion -> {
                if (currentContainer.type == ContainerInfo.Type.TopLevel) {
                    currentContainer.close()
                    containerStack.pop()
                }
            }
        }
        return currentExpr
    }

    /**
     * Steps out of the current [DataModelContainer].
     */
    fun stepOut() {
        // TODO: We should be able to step out of a "TopLevel" container and/or we need some way to close the evaluation early.
        if (containerStack.size() <= 1) throw IonException("Nothing to step out of.")
        val popped = containerStack.pop()
        popped.close()
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
                    expansionKind = ExpansionKind.Stream,
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
