// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.macro

import com.amazon.ion.*
import com.amazon.ion.impl._Private_RecyclingStack
import com.amazon.ion.impl._Private_Utils
import com.amazon.ion.impl._Private_Utils.*
import com.amazon.ion.impl.macro.Expression.*
import com.amazon.ion.impl.macro.MacroEvaluator.ExpanderKind.*
import java.io.ByteArrayOutputStream
import java.lang.StringBuilder
import java.math.BigDecimal
import java.math.BigInteger
import java.util.IdentityHashMap

private fun getExpanderKindForSystemMacro(systemMacro: SystemMacro) = when (systemMacro) {
    SystemMacro.Annotate -> Annotate
    SystemMacro.MakeString -> MakeString
    SystemMacro.MakeSymbol -> MakeSymbol
    SystemMacro.MakeDecimal -> MakeDecimal
    SystemMacro.Repeat -> Repeat
    SystemMacro.Sum -> Sum
    SystemMacro.Delta -> Delta
    SystemMacro.MakeBlob -> MakeBlob
    SystemMacro.Flatten -> Flatten
    SystemMacro.FlattenStruct -> FlattenStruct
    SystemMacro.MakeTimestamp -> MakeTimestamp
    SystemMacro.MakeFieldNameAndValue -> MakeFieldNameAndValue
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

    // TODO:
    data class ContainerInfo(var type: Type = Type.Uninitialized, private var _expansion: Expansion? = null) {
        enum class Type { TopLevel, List, Sexp, Struct, Uninitialized }

        fun release() {
            _expansion?.release()
            _expansion = null
            type = Type.Uninitialized
        }

        var expansion: Expansion
            get() = _expansion!!
            set(value) { _expansion = value }
    }

    private val expansionPool = Pool { pool -> Expansion(pool) }
    private val containerStack = _Private_RecyclingStack(8) { ContainerInfo() }
    private var currentExpr: DataModelExpression? = null

    fun getArguments(): List<Expression> {
        return containerStack.iterator().next().expansion.expressions
    }

    /**
     * Initialize the macro evaluator with an E-Expression.
     */
    fun initExpansion(encodingExpressions: List<EExpressionBodyExpression>) {
        containerStack.push { ci ->
            ci.type = ContainerInfo.Type.TopLevel
            ci.expansion = expansionPool.acquire {
                it.initExpansion(Stream, encodingExpressions, 0, encodingExpressions.size, Environment.EMPTY)
            }
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

            val nextExpansionOutput = currentContainer.expansion.produceNext()
            when (nextExpansionOutput) {
                is DataModelExpression -> currentExpr = nextExpansionOutput
                EndOfExpansion -> {
                    // TODO: Do we need to release anything?
                    // TODO: Is there a better way to do this?
                    if (currentContainer.type == ContainerInfo.Type.TopLevel) {
                        currentContainer.release()
                        containerStack.pop()
                    }
                    return null
                }
                EndOfContainer -> {
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
        // step out of anything we find until we have stepped out of a container.
        if (containerStack.size() <= 1) throw IonException("Nothing to step out of.")
        val popped = containerStack.pop()
        popped.release()
    }

    /**
     * Steps in to the current [DataModelContainer].
     * Throws [IonException] if not positioned on a container.
     */
    fun stepIn() {
        val expression = requireNotNull(currentExpr) { "Not positioned on a value" }
        if (expression is DataModelContainer) {
            val currentContainer = containerStack.peek()
            if (expression.isConstructedFromMacro) {
                val currentTop = currentContainer.expansion.top()
            } else {
                containerStack.push { ci ->
                    ci.type = when (expression.type) {
                        IonType.LIST -> ContainerInfo.Type.List
                        IonType.SEXP -> ContainerInfo.Type.Sexp
                        IonType.STRUCT -> ContainerInfo.Type.Struct
                        else -> TODO("Unreachable")
                    }
                    ci.expansion = expansionPool.acquire {
                        val topExpansion = currentContainer.expansion.top()
                        it.initExpansion(
                            expanderKind = Stream,
                            expressions = topExpansion.expressions,
                            startInclusive = expression.startInclusive,
                            endExclusive = expression.endExclusive,
                            environment = topExpansion.environment!!,
                        )
                    }
                }
            }
            currentExpr = null
        } else {
            throw IonException("Not positioned on a container.")
        }
    }

    enum class ExpanderKind {
        Uninitialized {
            override fun produceNext(expansion: Expansion): ExpansionOutputExpressionOrContinue {
                throw IllegalStateException("ExpansionInfo not initialized.")
            }
        },
        Empty {
            override fun produceNext(expansion: Expansion): ExpansionOutputExpressionOrContinue = EndOfExpansion
        },
        Stream {
            override fun produceNext(expansion: Expansion): ExpansionOutputExpressionOrContinue {
                val self = expansion

                // If there's a delegate, we'll try that first.
                val delegate = self.expansionDelegate
                if (delegate != null) {
                    val result = delegate.produceNext()
                    return when (result) {
                        is DataModelExpression -> result
                        // TODO: figure out some way to stick on this... or maybe it's not necessary.
                        //       Test this by attempting to go beyond the end of containers.
                        EndOfContainer -> EndOfContainer
                        EndOfExpansion -> {
                            delegate.release()
                            self.expansionDelegate = null
                            ContinueExpansion
                        }
                    }
                }

                if (self.i >= self.endExclusive) {
                    expansion.expanderKind = Empty
                    return ContinueExpansion
                }

                val next = self.expressions[self.i]
                self.i++
                if (next is HasStartAndEnd) self.i = next.endExclusive

                return when (next) {
                    is DataModelExpression -> next
                    is EExpression -> {
                        val macro = next.macro
                        val argIndices = macro.calculateArgumentIndices(
                            encodingExpressions = expansion.expressions,
                            argsStartInclusive = next.startInclusive,
                            argsEndExclusive = next.endExclusive,
                        )
                        val newEnvironment = self.environment.createChild(self.expressions, argIndices)
                        if (macro.body != null) {
                            self.expansionDelegate = self.expansionPool.acquire { new ->
                                new.initExpansion(
                                    expanderKind = Stream,
                                    expressions = macro.body!!,
                                    startInclusive = 0,
                                    endExclusive = macro.body!!.size,
                                    environment = newEnvironment,
                                )
                            }
                        } else {
                            val expanderKind = getExpanderKindForSystemMacro(macro as SystemMacro)
                            self.expansionDelegate = self.expansionPool.acquire { new ->
                                new.initExpansion(
                                    expanderKind = expanderKind,
                                    expressions = emptyList(),
                                    startInclusive = 0,
                                    endExclusive = 0,
                                    environment = newEnvironment,
                                )
                            }
                        }
                        ContinueExpansion
                    }
                    is MacroInvocation -> {
                        // TODO: Verify if this is correct
                        val macro = next.macro
                        val argIndices = macro.calculateArgumentIndices(
                            encodingExpressions = expansion.expressions,
                            argsStartInclusive = next.startInclusive,
                            argsEndExclusive = next.endExclusive,
                        )
                        val newEnvironment = self.environment.createChild(self.expressions, argIndices)
                        if (macro.body != null) {
                            self.expansionDelegate = self.expansionPool.acquire { new ->
                                new.initExpansion(
                                    expanderKind = Stream,
                                    expressions = macro.body!!,
                                    startInclusive = 0,
                                    endExclusive = macro.body!!.size,
                                    environment = newEnvironment,
                                )
                            }
                        } else {
                            val expanderKind = getExpanderKindForSystemMacro(macro as SystemMacro)
                            self.expansionDelegate = self.expansionPool.acquire { new ->
                                new.initExpansion(
                                    expanderKind = expanderKind,
                                    expressions = emptyList(),
                                    startInclusive = 0,
                                    endExclusive = 0,
                                    environment = newEnvironment,
                                )
                            }
                        }
                        ContinueExpansion
                    }
                    is ExpressionGroup -> {
                        self.expansionDelegate = self.expansionPool.acquire { new ->
                            new.initExpansion(
                                expanderKind = Stream,
                                expressions = self.expressions,
                                startInclusive = next.startInclusive,
                                endExclusive = next.endExclusive,
                                environment = self.environment,
                            )
                        }
                        ContinueExpansion
                    }

                    is VariableRef -> {
                        self.expansionDelegate = self.readArgument(next)
                        ContinueExpansion
                    }

                    Placeholder -> TODO("Unreachable")
                }
            }
        },
        OneValuedStream {
            override fun produceNext(expansion: Expansion): ExpansionOutputExpressionOrContinue {
                if (expansion.additionalState != 1) {
                    return when (val firstValue = Stream.produceNext(expansion)) {
                        is DataModelExpression -> {
                            expansion.additionalState = 1
                            firstValue
                        }
                        ContinueExpansion -> ContinueExpansion
                        EndOfExpansion -> throw IonException("Expected one value, found 0")
                        EndOfContainer -> TODO("Unused?")
                    }
                } else {
                    return when (val secondValue = Stream.produceNext(expansion)) {
                        is DataModelExpression -> throw IonException("Expected one value, found multiple")
                        ContinueExpansion -> ContinueExpansion
                        EndOfExpansion -> secondValue
                        EndOfContainer -> TODO("Unused?")
                    }
                }
            }
        },
        IfNone {
            private val ARG_TO_TEST = VariableRef(0)
            private val TRUE_BRANCH = VariableRef(1)
            private val FALSE_BRANCH = VariableRef(2)

            override fun produceNext(expansion: Expansion): ExpansionOutputExpressionOrContinue {
                val testArg = expansion.readArgument(ARG_TO_TEST)
                var n = 0
                while (n < 2) {
                    if (testArg.produceNext() is EndOfExpansion) break
                    n++
                }

                val branch = if (n > 0) FALSE_BRANCH else TRUE_BRANCH
                val branchExpansion = expansion.readArgument(branch)
                expansion.reInitializeFrom(branchExpansion)
                branchExpansion.release()
                testArg.release()
                return ContinueExpansion
            }
        },
        IfSome {
            private val ARG_TO_TEST = VariableRef(0)
            private val TRUE_BRANCH = VariableRef(1)
            private val FALSE_BRANCH = VariableRef(2)

            override fun produceNext(expansion: Expansion): ExpansionOutputExpressionOrContinue {
                val testArg = expansion.readArgument(ARG_TO_TEST)
                var n = 0
                while (n < 2) {
                    if (testArg.produceNext() is EndOfExpansion) break
                    n++
                }
                testArg.release()

                val branch = if (n > 0) TRUE_BRANCH else FALSE_BRANCH
                val branchExpansion = expansion.readArgument(branch)
                expansion.reInitializeFrom(branchExpansion)
                branchExpansion.release()
                return ContinueExpansion
            }
        },
        IfSingle {
            private val ARG_TO_TEST = VariableRef(0)
            private val TRUE_BRANCH = VariableRef(1)
            private val FALSE_BRANCH = VariableRef(2)

            override fun produceNext(expansion: Expansion): ExpansionOutputExpressionOrContinue {
                val testArg = expansion.readArgument(ARG_TO_TEST)
                var n = 0
                while (n < 2) {
                    if (testArg.produceNext() is EndOfExpansion) break
                    n++
                }
                testArg.release()

                val branch = if (n == 1) TRUE_BRANCH else FALSE_BRANCH
                val branchExpansion = expansion.readArgument(branch)
                expansion.reInitializeFrom(branchExpansion)
                branchExpansion.release()
                return ContinueExpansion
            }
        },
        IfMulti {
            private val ARG_TO_TEST = VariableRef(0)
            private val TRUE_BRANCH = VariableRef(1)
            private val FALSE_BRANCH = VariableRef(2)

            override fun produceNext(expansion: Expansion): ExpansionOutputExpressionOrContinue {
                val testArg = expansion.readArgument(ARG_TO_TEST)
                var n = 0
                while (n < 2) {
                    if (testArg.produceNext() is EndOfExpansion) break
                    n++
                }
                testArg.release()

                val branch = if (n > 1) TRUE_BRANCH else FALSE_BRANCH
                val branchExpansion = expansion.readArgument(branch)
                expansion.reInitializeFrom(branchExpansion)
                branchExpansion.release()
                return ContinueExpansion
            }
        },

        Annotate {

            private val ANNOTATIONS_ARG = VariableRef(0)
            private val VALUE_TO_ANNOTATE_ARG = VariableRef(1)

            override fun produceNext(expansion: Expansion): ExpansionOutputExpressionOrContinue {
                val annotations = expansion.readArgument(ANNOTATIONS_ARG).map {
                    when (it) {
                        is StringValue -> _Private_Utils.newSymbolToken(it.value)
                        is SymbolValue -> it.value
                        is DataModelValue -> throw IonException("Invalid argument type for 'make_string': ${it.type}")
                        else -> TODO("Unreachable without stepping in to a container")
                    }
                }

                val valueToAnnotateExpansion = expansion.readArgument(VALUE_TO_ANNOTATE_ARG)

                val annotatedExpression = valueToAnnotateExpansion.produceNext().let {
                    it as? DataModelValue ?: throw IonException("Required at least one value.")
                    it.withAnnotations(annotations + it.annotations)
                }
                // Tail-recursion-like optimization
                expansion.reInitializeFrom(valueToAnnotateExpansion)
                expansion.expanderKind = OneValuedStream
                return annotatedExpression
            }
        },
        MakeString {
            private val STRINGS_ARG = VariableRef(0)

            override fun produceNext(expansion: Expansion): ExpansionOutputExpressionOrContinue {
                val sb = StringBuilder()
                expansion.readArgument(STRINGS_ARG).forEach {
                    when (it) {
                        is StringValue -> sb.append(it.value)
                        is SymbolValue -> sb.append(it.value.assumeText())
                        is DataModelValue -> throw IonException("Invalid argument type for 'make_string': ${it.type}")
                        is FieldName -> TODO("Unreachable.")
                    }
                }
                expansion.expanderKind = Empty
                return StringValue(value = sb.toString())
            }
        },
        MakeSymbol {
            private val STRINGS_ARG = VariableRef(0)

            override fun produceNext(expansion: Expansion): ExpansionOutputExpressionOrContinue {
                if (expansion.additionalState != null) return EndOfExpansion
                expansion.additionalState = Unit

                val sb = StringBuilder()
                expansion.readArgument(STRINGS_ARG).forEach {
                    when (it) {
                        is StringValue -> sb.append(it.value)
                        is SymbolValue -> sb.append(it.value.assumeText())
                        is DataModelValue -> throw IonException("Invalid argument type for 'make_symbol': ${it.type}")
                        is FieldName -> TODO("Unreachable.")
                    }
                }
                return SymbolValue(value = _Private_Utils.newSymbolToken(sb.toString()))
            }
        },
        MakeBlob {
            private val LOB_ARG = VariableRef(0)

            override fun produceNext(expansion: Expansion): ExpansionOutputExpressionOrContinue {
                // TODO: Optimize to see if we can create a Byte "view" over the existing byte arrays.
                if (expansion.additionalState != null) return EndOfExpansion
                expansion.additionalState = Unit

                val baos = ByteArrayOutputStream()
                expansion.readArgument(LOB_ARG).forEach {
                    when (it) {
                        is LobValue -> baos.write(it.value)
                        is DataModelValue -> throw IonException("Invalid argument type for 'make_blob': ${it.type}")
                        is FieldName -> TODO("Unreachable.")
                    }
                }
                return BlobValue(value = baos.toByteArray())
            }
        },
        MakeDecimal {
            private val COEFFICIENT_ARG = VariableRef(0)
            private val EXPONENT_ARG = VariableRef(1)

            override fun produceNext(expansion: Expansion): ExpansionOutputExpressionOrContinue {
                if (expansion.additionalState != null) return EndOfExpansion
                expansion.additionalState = Unit

                val coefficient = expansion.readExactlyOneArgument<IntValue>(COEFFICIENT_ARG).bigIntegerValue
                val exponent = expansion.readExactlyOneArgument<IntValue>(EXPONENT_ARG).bigIntegerValue
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

            override fun produceNext(expansion: Expansion): ExpansionOutputExpressionOrContinue {
                val year = expansion.readExactlyOneArgument<IntValue>(YEAR_ARG).longValue.toInt()
                val month = expansion.readZeroOrOneArgument<IntValue>(MONTH_ARG)?.longValue?.toInt()
                val day = expansion.readZeroOrOneArgument<IntValue>(DAY_ARG)?.longValue?.toInt()
                val hour = expansion.readZeroOrOneArgument<IntValue>(HOUR_ARG)?.longValue?.toInt()
                val minute = expansion.readZeroOrOneArgument<IntValue>(MINUTE_ARG)?.longValue?.toInt()
                val second = expansion.readZeroOrOneArgument<DataModelValue>(SECOND_ARG)?.let {
                    when (it) {
                        is DecimalValue -> it.value
                        is IntValue -> it.longValue.toBigDecimal()
                        else -> throw IonException("second must be an integer or decimal")
                    }
                }

                val offsetMinutes = expansion.readZeroOrOneArgument<IntValue>(OFFSET_ARG)?.longValue?.toInt()

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
                    expansion.expanderKind = Empty
                    return TimestampValue(value = ts)
                } catch (e: IllegalArgumentException) {
                    throw IonException(e.message)
                }
            }
        },
        MakeFieldNameAndValue {
            private val FIELD_NAME = VariableRef(0)
            private val FIELD_VALUE = VariableRef(1)

            override fun produceNext(expansion: Expansion): ExpansionOutputExpressionOrContinue {
                val fieldName = expansion.readExactlyOneArgument<TextValue>(FIELD_NAME)
                val fieldNameExpression = when (fieldName) {
                    is SymbolValue -> FieldName(fieldName.value)
                    is StringValue -> FieldName(newSymbolToken(fieldName.value))
                }

                expansion.readExactlyOneArgument<DataModelValue>(FIELD_VALUE)

                val valueExpansion = expansion.readArgument(FIELD_VALUE)

                expansion.reInitializeFrom(valueExpansion)
                expansion.expanderKind = OneValuedStream
                return fieldNameExpression
            }
        },

        FlattenStruct {
            private val STRUCTS = VariableRef(0)

            override fun produceNext(expansion: Expansion): ExpansionOutputExpressionOrContinue {
                var argumentExpansion: Expansion? = expansion.additionalState as Expansion?
                if (argumentExpansion == null) {
                    argumentExpansion = expansion.readArgument(STRUCTS)
                    expansion.additionalState = argumentExpansion
                }

                val currentChildExpansion = expansion.expansionDelegate

                return when (val next = currentChildExpansion?.produceNext()) {
                    is DataModelExpression -> next
                    EndOfContainer -> TODO("I think this is unused!")
                    EndOfExpansion -> {
                        expansion.expansionDelegate!!.release()
                        expansion.expansionDelegate = null
                        ContinueExpansion
                    }
                    // Only possible if expansionDelegate is null
                    null -> when (val nextSequence = argumentExpansion.produceNext()) {
                        is StructValue -> {
                            expansion.expansionDelegate = expansion.expansionPool.acquire { child ->
                                child.initExpansion(
                                    expanderKind = Stream,
                                    expressions = argumentExpansion.top().expressions,
                                    startInclusive = nextSequence.startInclusive,
                                    endExclusive = nextSequence.endExclusive,
                                    environment = argumentExpansion.top().environment,
                                )
                            }
                            ContinueExpansion
                        }
                        EndOfExpansion -> EndOfExpansion
                        is DataModelExpression -> throw IonException("invalid argument; make_struct expects structs")
                        EndOfContainer -> TODO("Unreachable")
                    }
                }
            }
        },

        Flatten {
            private val SEQUENCES = VariableRef(0)

            override fun produceNext(expansion: Expansion): ExpansionOutputExpressionOrContinue {
                var argumentExpansion: Expansion? = expansion.additionalState as Expansion?
                if (argumentExpansion == null) {
                    argumentExpansion = expansion.readArgument(SEQUENCES)
                    expansion.additionalState = argumentExpansion
                }

                val currentChildExpansion = expansion.expansionDelegate

                return when (val next = currentChildExpansion?.produceNext()) {
                    is DataModelExpression -> next
                    EndOfContainer -> TODO("I think this is unused!")
                    EndOfExpansion -> {
                        expansion.expansionDelegate!!.release()
                        expansion.expansionDelegate = null
                        ContinueExpansion
                    }
                    // Only possible if expansionDelegate is null
                    null -> when (val nextSequence = argumentExpansion.produceNext()) {
                        is StructValue -> throw IonException("invalid argument; flatten expects sequences")
                        is DataModelContainer -> {
                            expansion.expansionDelegate = expansion.expansionPool.acquire { child ->
                                child.initExpansion(
                                    expanderKind = Stream,
                                    expressions = argumentExpansion.top().expressions,
                                    startInclusive = nextSequence.startInclusive,
                                    endExclusive = nextSequence.endExclusive,
                                    environment = argumentExpansion.top().environment,
                                )
                            }
                            ContinueExpansion
                        }
                        EndOfExpansion -> EndOfExpansion
                        is DataModelExpression -> throw IonException("invalid argument; flatten expects sequences")
                        EndOfContainer -> TODO("Unreachable")
                    }
                }
            }
        },
        Sum {
            private val ARG_A = VariableRef(0)
            private val ARG_B = VariableRef(1)

            override fun produceNext(expansion: Expansion): ExpansionOutputExpressionOrContinue {
                if (expansion.additionalState != null) return EndOfExpansion
                expansion.additionalState = Unit

                val a = expansion.readExactlyOneArgument<IntValue>(ARG_A).bigIntegerValue
                val b = expansion.readExactlyOneArgument<IntValue>(ARG_B).bigIntegerValue
                return BigIntValue(value = a + b)
            }
        },
        Delta {
            private val ARGS = VariableRef(0)

            // Initial value = 0
            override fun produceNext(expansion: Expansion): ExpansionOutputExpressionOrContinue {
                // TODO: Optimize to use LongIntValue when possible
                var delegate = expansion.expansionDelegate
                val runningTotal = expansion.additionalState as? BigInteger ?: BigInteger.ZERO
                if (delegate == null) {
                    delegate = expansion.readArgument(ARGS)
                    expansion.expansionDelegate = delegate
                }

                when (val nextExpandedArg = delegate.produceNext()) {
                    is IntValue -> {
                        val nextDelta = nextExpandedArg.bigIntegerValue
                        val nextOutput = runningTotal + nextDelta
                        expansion.additionalState = nextOutput
                        return BigIntValue(value = nextOutput)
                    }
                    EndOfExpansion -> return nextExpandedArg
                    is DataModelValue -> throw IonException("delta arguments must be integers")
                    is FieldName, EndOfContainer -> TODO("Unreachable")
                }
            }
        },
        Repeat {
            private val COUNT_ARG = VariableRef(0)
            private val THING_TO_REPEAT = VariableRef(1)

            override fun produceNext(expansion: Expansion): ExpansionOutputExpressionOrContinue {
                var n = expansion.additionalState as Long?
                if (n == null) {
                    n = expansion.readExactlyOneArgument<IntValue>(COUNT_ARG).longValue
                    if (n < 0) throw IonException("invalid argument; 'n' must be non-negative")
                    expansion.additionalState = n
                }

                if (expansion.expansionDelegate == null) {
                    if (n > 0) {
                        expansion.expansionDelegate = expansion.readArgument(THING_TO_REPEAT)
                        expansion.additionalState = n - 1
                    } else {
                        return EndOfExpansion
                    }
                }

                val repeated = expansion.expansionDelegate!!
                return when (val maybeNext = repeated.produceNext()) {
                    is DataModelExpression, EndOfContainer -> maybeNext
                    EndOfExpansion -> {
                        expansion.expansionDelegate!!.release()
                        expansion.expansionDelegate = null
                        ContinueExpansion
                    }
                }
            }
        },
        ;

        abstract fun produceNext(expansion: Expansion): ExpansionOutputExpressionOrContinue

        protected fun Expansion.readArgument(variableRef: VariableRef): Expansion {
            val argIndex = environment.argumentIndices[variableRef.signatureIndex]
            if (argIndex < 0) {
                // Argument was elided.
                return expansionPool.acquire { it.expanderKind = Empty }
            }
            val firstArgExpression = environment.arguments[argIndex]

            return expansionPool.acquire { new ->
                new.initExpansion(
                    expanderKind = Stream,
                    expressions = environment.arguments,
                    startInclusive = if (firstArgExpression is ExpressionGroup) firstArgExpression.startInclusive else argIndex,
                    endExclusive = if (firstArgExpression is HasStartAndEnd) firstArgExpression.endExclusive else argIndex + 1,
                    environment = environment.parentEnvironment!!
                )
            }
        }

        protected inline fun Expansion.forEach(action: (DataModelExpression) -> Unit) {
            while (true) {
                when (val next = produceNext()) {
                    EndOfContainer, EndOfExpansion -> return
                    is DataModelExpression -> action(next)
                }
            }
        }

        protected inline fun <T> Expansion.map(action: (DataModelExpression) -> T): List<T> {
            val result = mutableListOf<T>()
            while (true) {
                when (val next = produceNext()) {
                    EndOfContainer, EndOfExpansion -> return result
                    is DataModelExpression -> result.add(action(next))
                }
            }
        }

        protected inline fun <reified T : DataModelValue> Expansion.readZeroOrOneArgument(variableRef: VariableRef): T? {
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
                    EndOfContainer,
                    is FieldName -> TODO("Unreachable without stepping into a container")
                }
            }
            return argValue
        }

        protected inline fun <reified T : DataModelValue> Expansion.readExactlyOneArgument(variableRef: VariableRef): T {
            return readZeroOrOneArgument<T>(variableRef) ?: throw IonException("invalid argument; no value when one is expected")
        }
    }

    class Expansion(
        @JvmField val expansionPool: Pool<Expansion>,

        @JvmField var expanderKind: ExpanderKind = Uninitialized,
        /**
         * The [Expression]s being expanded. This MUST be the original list, not a sublist because
         * (a) we don't want to be allocating new sublists all the time, and (b) the
         * start and end indices of the expressions may be incorrect if a sublist is taken.
         */
        @JvmField var expressions: List<Expression> = emptyList(),
        /** Current position within [expressions] of this expansion */
        @JvmField var i: Int = 0,
        /** End of [expressions] that are applicable for this [ExpansionInfo] */
        @JvmField var endExclusive: Int = 0,
        /**
         * The evaluation [Environment]—i.e. variable bindings.
         */
        @JvmField var environment: Environment = Environment.EMPTY,
        // TODO: Should this be "additional state"?
        @JvmField var expansionDelegate: Expansion? = null,
        @JvmField var additionalState: Any? = null,
    ) {
        fun top(): Expansion = expansionDelegate?.top() ?: this

        fun release() {
            expanderKind = Uninitialized
            additionalState = null
            expansionDelegate?.release()
            expansionPool.take(this)
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

        fun reInitializeFrom(other: Expansion) {
            this.expanderKind = other.expanderKind
            this.expressions = other.expressions
            this.i = other.i
            this.endExclusive = other.endExclusive
            this.expansionDelegate = other.expansionDelegate
            this.additionalState = other.additionalState
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
        |ExpansionInfo(
        |    expansionKind: $expanderKind,
        |    environment: $environment,
        |    expressions: [
        |        ${expressions.joinToString(",\n|        ") { it.toString() } }
        |    ],
        |    endExclusive: $endExclusive,
        |    i: $i,
        |    child: ${expansionDelegate?.expanderKind}
        |    additionalState: $additionalState,
        |)
        """.trimMargin()
    }

    /**
     * Suitable for single-threaded use only.
     *
     * TODO: Clean up the debugging parts.
     */
    class Pool<T>(private val objectFactory: (Pool<T>) -> T) {
        private val availableElements = ArrayList<T>(32)
        private val allElements = IdentityHashMap<T, Int>(32)
        private var acquireCount = 0
        private var releaseCount = 0
        fun acquire(init: (T) -> Unit): T {
            val element = availableElements.removeLastOrNull() ?: objectFactory(this)
            element.apply(init)
            allElements[element] = 1
            // println("Pool(a=${++acquireCount},r=$releaseCount)")
            if (acquireCount - releaseCount > 1000) throw IllegalStateException("Probable runtime stack overflow or memory leak")
            return element
        }
        fun take(t: T) {
            check(allElements[t] != 0) { "Double return!" }
            if (allElements[t] == 1) {
                availableElements.add(t)
                allElements[t] = 0
            }
            // println("Pool(a=$acquireCount,r=${++releaseCount})")
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
