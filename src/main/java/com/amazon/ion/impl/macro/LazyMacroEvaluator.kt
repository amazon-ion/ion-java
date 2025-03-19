package com.amazon.ion.impl.macro

import com.amazon.ion.*
import com.amazon.ion.impl.*
import com.amazon.ion.impl.macro.Expression.*
import com.amazon.ion.impl.macro.LazyMacroEvaluator.*
import com.amazon.ion.impl.macro.LazyMacroEvaluator.ContainerInfo.*
import com.amazon.ion.util.*
import java.io.ByteArrayOutputStream
import java.math.BigDecimal
import java.math.BigInteger
import java.util.*

/**
 * Evaluates an EExpression from a List of [EExpressionBodyExpression] and the [TemplateBodyExpression]s
 * given in the macro table of the [EncodingContext].
 *
 * General Usage:
 *  - To start evaluating an e-expression, call [initExpansion]
 *  - Call [next] to get the next field name or value, or null
 *    if the end of the container or end of expansion has been reached.
 *  - Call [stepIn] when positioned on a container to step into that container.
 *  - Call [stepOut] to step out of the current container.
 *
 * TODO: Make expansion limit configurable.
 *
 * ### Implementation Overview:
 *
 * The macro evaluator consists of a stack of containers, each of which has an implicit stream (i.e. the
 * expressions in that container) which is modeled as an expansion frame ([ExpansionInfo]).
 *
 * When calling [next], the evaluator looks at the top container in the stack and requests the next value from
 * its expansion frame. That expansion frame may produce a result all on its own (i.e. if the next value is a literal
 * value), or it may create and delegate to a child expansion frame if the next source expression is something that
 * needs to be expanded (e.g. macro invocation, variable expansion, etc.). When delegating to a child expansion frame,
 * the value returned by the child could be intercepted and inspected, modified, or consumed.
 * In this way, the expansion frames model a lazily constructed expression tree over the flat list of expressions in the
 * input to the macro evaluator.
 */
class LazyMacroEvaluator : IonReader {

    /**
     * Holds state that is shared across all macro evaluations that are part of this evaluator.
     * This state pertains to a single "session" of the macro evaluator, and is reset every time [initExpansion] is called.
     * For now, this includes managing the pool of [ExpansionInfo] and tracking the expansion step limit.
     */
    internal class Session(
        /** Number of expansion steps at which the evaluation session should be aborted. */
        private val expansionLimit: Int = 1_000_000
    ) {
        /** Internal state for tracking the number of expansion steps. */
        private var numExpandedExpressions = 0
        /** Pool of [ExpansionInfo] to minimize allocation and garbage collection. */
        private val expanderPool: ArrayList<ExpansionInfo> = ArrayList(64)
        private var expanderPoolIndex = 0
        val environment: LazyEnvironment = LazyEnvironment.create()
        var sideEffectExpander: ExpansionInfo? = null
        var currentExpander: ExpansionInfo? = null
        var currentFieldName: SymbolToken? = null

        /**
         * Gets an [ExpansionInfo] from the pool, or allocates a new one if necessary. The returned ExpansionInfo is
         * valid until [reset] is called.
         */
        private fun getExpansion(): ExpansionInfo {
            val expansion: ExpansionInfo
            if (expanderPoolIndex >= expanderPool.size) {
                expansion = ExpansionInfo(this)
                expanderPool.add(expansion)
            } else {
                expansion = expanderPool[expanderPoolIndex]
            }
            expanderPoolIndex++
            return expansion
        }

        /** Gets an [ExpansionInfo] from the pool (or allocates a new one if necessary), initializing it with the provided values. */
        fun getExpander(expansionKind: ExpansionKind, environmentContext: LazyEnvironment.NestedContext): ExpansionInfo {
            val expansion = getExpansion()
            expansion.expansionKind = expansionKind
            expansion.environmentContext = environmentContext
            expansion.additionalState = null
            expansion.childExpansion = null
            expansion.parentExpansion = null
            expansion.reachedEndOfExpression = false
            currentExpander = expansion
            return expansion
        }

        fun finishVariable() {
            val variable = currentExpander!!
            currentExpander = variable.parentExpansion
            environment.finishChildEnvironment()
            variable.close()
        }

        fun produceNext(): ExpressionType {
            while (true) {
                currentExpander!!.environmentContext.tape?.next() // TODO if Empty doesn't exist and CONTINUE_EXPANSION isn't used with it, then this could be tape!!
                val next = currentExpander!!.expansionKind.produceNext(currentExpander!!)
                if (next == ExpressionType.CONTINUE_EXPANSION) continue
                // This the only place where we count the expansion steps.
                // It is theoretically possible to have macro expansions that are millions of levels deep because this
                // only counts macro invocations at the end of their expansion, but this will still work to catch things
                // like a  billion laughs attack because it does place a limit on the number of _values_ produced.
                // This counts every value _at every level_, so most values will be counted multiple times. If possible
                // without impacting performance, count values only once in order to have more predictable behavior.
                incrementStepCounter()
                return next
            }
        }

        private fun incrementStepCounter() {
            numExpandedExpressions++
            if (numExpandedExpressions > expansionLimit) {
                // Technically, we are not counting "steps" because we don't have a true definition of what a "step" is,
                // but this is probably a more user-friendly message than trying to explain what we're actually counting.
                throw IonException("Macro expansion exceeded limit of $expansionLimit steps.")
            }
        }

        fun reset(arguments: ExpressionTape) {
            numExpandedExpressions = 0
            expanderPoolIndex = 0
            environment.reset(arguments)
            sideEffectExpander = getExpander(ExpansionKind.Empty, this.environment.sideEffectContext)
            sideEffectExpander!!.keepAlive = true
            currentExpander = null
            currentFieldName = null
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
            currentFieldName = null
            container = null
            type = Type.Uninitialized
        }

        var expansion: ExpansionInfo
            get() = _expansion!!
            set(value) { _expansion = value }
        @JvmField var currentFieldName: SymbolToken? = null
        @JvmField var container: IonType? = null
    }

    /**
     * Stateless functions that operate on the expansion frames (i.e. [ExpansionInfo]).
     */
    // TODO(PERF): It might be possible to optimize this by changing it to an enum without any methods (or even a set of
    //             integer constants) and converting all their implementations to static methods.
    internal enum class ExpansionKind {
        Uninitialized {
            override fun produceNext(thisExpansion: ExpansionInfo): Nothing = throw IllegalStateException("ExpansionInfo not initialized.")
        },
        Empty { // TODO is this still needed? Can we just immediately do END_OF_EXPANSION and set currentExpander to the parent?
            override fun produceNext(thisExpansion: ExpansionInfo): ExpressionType =
                ExpressionType.END_OF_EXPANSION
        },
        Stream {

            override fun produceNext(thisExpansion: ExpansionInfo): ExpressionType {
                val expressionTape = thisExpansion.environmentContext.tape!!
                if (expressionTape.isExhausted) {
                    return ExpressionType.END_OF_EXPANSION
                }
                val nextType = expressionTape.type()
                if (nextType.isEnd) {
                    if (nextType == ExpressionType.EXPRESSION_GROUP_END || nextType == ExpressionType.E_EXPRESSION_END) {
                        // Expressions and expression groups do not rely on stepIn/stepOut for navigation, so the tape must be advanced
                        // here.
                        expressionTape.prepareNext()
                    }
                    if (nextType != ExpressionType.E_EXPRESSION_END) { // TODO why the special case?
                        return ExpressionType.END_OF_EXPANSION
                    }
                    return ExpressionType.CONTINUE_EXPANSION // TODO should end of expansion be conveyed in any case here?
                }

                return when (nextType) {
                    ExpressionType.FIELD_NAME -> {
                        thisExpansion.session.currentFieldName = expressionTape.context() as SymbolToken
                        expressionTape.prepareNext() // TODO set `currentFieldName` and continue?
                        ExpressionType.CONTINUE_EXPANSION
                    }
                    ExpressionType.ANNOTATION -> {
                        expressionTape.prepareNext() // TODO set `currentAnnotations` and continue
                        ExpressionType.ANNOTATION
                    }
                    ExpressionType.E_EXPRESSION -> {
                        val macro = expressionTape.context() as Macro
                        val macroBodyTape = ExpressionTape(macro.bodyTape) // TODO pool the tape instances, or move the indices into NestedContext
                        expressionTape.prepareNext()
                        expressionTape.next() // TODO adding this did nothing; try removing
                        val newEnvironment = thisExpansion.session.environment.startChildEnvironment(macroBodyTape, expressionTape, expressionTape.currentIndex())
                        val expansionKind = forMacro(macro)
                        thisExpansion.childExpansion = thisExpansion.session.getExpander(
                            expansionKind = expansionKind,
                            environmentContext = newEnvironment,
                        )
                        thisExpansion.childExpansion!!.parentExpansion = thisExpansion
                        ExpressionType.CONTINUE_EXPANSION
                    }
                    ExpressionType.E_EXPRESSION_END -> unreachable()
                    ExpressionType.EXPRESSION_GROUP -> {
                        expressionTape.prepareNext()
                        thisExpansion.childExpansion = thisExpansion.session.getExpander(
                            expansionKind = ExprGroup,
                            environmentContext = thisExpansion.environmentContext,
                        )
                        thisExpansion.childExpansion!!.parentExpansion = thisExpansion
                        ExpressionType.CONTINUE_EXPANSION
                    }
                    ExpressionType.EXPRESSION_GROUP_END -> unreachable()
                    ExpressionType.DATA_MODEL_SCALAR -> {
                        expressionTape.prepareNext()
                        ExpressionType.DATA_MODEL_SCALAR
                    }
                    ExpressionType.DATA_MODEL_CONTAINER -> {
                        expressionTape.prepareNext()
                        ExpressionType.DATA_MODEL_CONTAINER
                    }
                    ExpressionType.VARIABLE -> {
                        expressionTape.prepareNext()
                        thisExpansion.childExpansion = thisExpansion.readArgument(expressionTape.context() as Int)
                        thisExpansion.childExpansion!!.parentExpansion = thisExpansion
                        ExpressionType.CONTINUE_EXPANSION
                    }
                    ExpressionType.DATA_MODEL_CONTAINER_END -> unreachable()
                    ExpressionType.END_OF_EXPANSION -> unreachable()
                    ExpressionType.CONTINUE_EXPANSION -> unreachable()
                }
            }
        },
        /** Alias of [Stream] to aid in debugging */
        Variable {
            override fun produceNext(thisExpansion: ExpansionInfo): ExpressionType {
                if (thisExpansion.reachedEndOfExpression) {
                    thisExpansion.session.environment.finishChildEnvironment()
                    return ExpressionType.END_OF_EXPANSION
                }
                val expression = Stream.produceNext(thisExpansion)
                if (thisExpansion.childExpansion == null) {
                    if (expression == ExpressionType.DATA_MODEL_SCALAR) {
                        thisExpansion.reachedEndOfExpression = true
                    }
                }
                return expression
            }
        },
        /** Alias of [Stream] to aid in debugging */
        TemplateBody {
            override fun produceNext(thisExpansion: ExpansionInfo): ExpressionType {
                val expression = Stream.produceNext(thisExpansion)
                if (expression == ExpressionType.END_OF_EXPANSION) {
                    thisExpansion.session.environment.seekPastFinalArgument()
                }
                return expression
            }
        },
        /** Alias of [Stream] to aid in debugging */
        ExprGroup {
            override fun produceNext(thisExpansion: ExpansionInfo): ExpressionType {
                return Stream.produceNext(thisExpansion)
            }
        },
        ExactlyOneValueStream {
            override fun produceNext(thisExpansion: ExpansionInfo): ExpressionType {
                if (thisExpansion.additionalState != 1) {
                    val firstValue = Stream.produceNext(thisExpansion)
                    return when {
                        firstValue.isDataModelExpression -> {
                            thisExpansion.additionalState = 1
                            firstValue
                        }
                        firstValue == ExpressionType.CONTINUE_EXPANSION -> ExpressionType.CONTINUE_EXPANSION
                        firstValue == ExpressionType.END_OF_EXPANSION -> throw IonException("Expected one value, found 0")
                        else -> unreachable()
                    }
                } else {
                    val secondValue = Stream.produceNext(thisExpansion)
                    return when {
                        secondValue.isDataModelExpression -> throw IonException("Expected one value, found multiple")
                        secondValue == ExpressionType.CONTINUE_EXPANSION -> ExpressionType.CONTINUE_EXPANSION
                        secondValue == ExpressionType.END_OF_EXPANSION -> secondValue
                        else -> unreachable()
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

            private val ANNOTATIONS_ARG = 0
            private val VALUE_TO_ANNOTATE_ARG = 1

            override fun produceNext(thisExpansion: ExpansionInfo): ExpressionType {
                val annotations = thisExpansion.map(ANNOTATIONS_ARG) {
                    when (it) {
                        ExpressionType.DATA_MODEL_SCALAR -> asSymbol(thisExpansion)
                        else -> unreachable("Unreachable without stepping in to a container")
                    }
                }

                val valueToAnnotateExpansion = thisExpansion.readArgument(VALUE_TO_ANNOTATE_ARG)

                // TODO this needs to be fixed
                /*
                val annotatedExpression = valueToAnnotateExpansion.produceNext().let {
                    it as? Expression.DataModelValue ?: throw IonException("Required at least one value.")
                    it.withAnnotations(annotations + it.annotations)
                }

                 */

                val type = valueToAnnotateExpansion.session.produceNext() // TODO apply the annotations

                if (valueToAnnotateExpansion.session.produceNext() != ExpressionType.END_OF_EXPANSION) {
                    throw IonException("Can only annotate exactly one value")
                }

                /*
                return annotatedExpression.also {
                    thisExpansion.tailCall(valueToAnnotateExpansion)
                }

                 */
                thisExpansion.tailCall(valueToAnnotateExpansion)
                return type
            }
        },
        MakeString {
            private val STRINGS_ARG = 0

            override fun produceNext(thisExpansion: ExpansionInfo): ExpressionType {
                val sb = StringBuilder()
                thisExpansion.forEach(STRINGS_ARG) {
                    when {
                        IonType.isText(it.type()) -> sb.append(it.textValue())
                        else -> throw IonException("Invalid argument type for 'make_string': ${it.type()}")
                    }
                }
                thisExpansion.environmentContext.tape!!.advanceToAfterEndEExpression()
                thisExpansion.expansionKind = Empty
                thisExpansion.produceValueSideEffect(IonType.STRING, sb.toString())
                return ExpressionType.DATA_MODEL_SCALAR;
            }
        },
        MakeSymbol {
            private val STRINGS_ARG = 0

            override fun produceNext(thisExpansion: ExpansionInfo): ExpressionType {
                // TODO what is this for? Try removing
                if (thisExpansion.additionalState != null) return ExpressionType.END_OF_EXPANSION
                thisExpansion.additionalState = Unit

                val sb = StringBuilder()
                thisExpansion.forEach(STRINGS_ARG) {
                    when {
                        IonType.isText(it.type()) -> sb.append(it.textValue())
                        else -> throw IonException("Invalid argument type for 'make_string': ${it.type()}")
                    }
                }
                thisExpansion.environmentContext.tape!!.advanceToAfterEndEExpression()
                thisExpansion.expansionKind = Empty
                thisExpansion.produceValueSideEffect(IonType.SYMBOL, sb.toString())
                return ExpressionType.DATA_MODEL_SCALAR
            }
        },
        MakeBlob {
            private val LOB_ARG = 0

            override fun produceNext(thisExpansion: ExpansionInfo): ExpressionType {
                val baos = ByteArrayOutputStream()
                thisExpansion.forEach(LOB_ARG) {
                    baos.write(it.lobValue())
                }
                thisExpansion.expansionKind = Empty
                thisExpansion.produceValueSideEffect(IonType.BLOB, baos.toByteArray())
                return ExpressionType.DATA_MODEL_SCALAR
            }
        },
        MakeDecimal {
            private val COEFFICIENT_ARG = 0
            private val EXPONENT_ARG = 1

            override fun produceNext(thisExpansion: ExpansionInfo): ExpressionType {
                val coefficient = thisExpansion.readExactlyOneArgument(COEFFICIENT_ARG, ::asBigInteger)
                val exponent = thisExpansion.readExactlyOneArgument(EXPONENT_ARG, ::asBigInteger)
                thisExpansion.expansionKind = Empty
                thisExpansion.produceValueSideEffect(IonType.DECIMAL, BigDecimal(coefficient, -1 * exponent.intValueExact()))
                return ExpressionType.DATA_MODEL_SCALAR
            }
        },
        MakeTimestamp {
            private val YEAR_ARG = 0
            private val MONTH_ARG = 1
            private val DAY_ARG = 2
            private val HOUR_ARG = 3
            private val MINUTE_ARG = 4
            private val SECOND_ARG = 5
            private val OFFSET_ARG = 6

            override fun produceNext(thisExpansion: ExpansionInfo): ExpressionType {
                val year = thisExpansion.readExactlyOneArgument(YEAR_ARG, ::asLong).toInt()
                val month = thisExpansion.readZeroOrOneArgument(MONTH_ARG, ::asLong)?.toInt()
                val day = thisExpansion.readZeroOrOneArgument(DAY_ARG, ::asLong)?.toInt()
                val hour = thisExpansion.readZeroOrOneArgument(HOUR_ARG, ::asLong)?.toInt()
                val minute = thisExpansion.readZeroOrOneArgument(MINUTE_ARG, ::asLong)?.toInt()
                val second = thisExpansion.readZeroOrOneArgument(SECOND_ARG, ::asBigDecimal)

                val offsetMinutes = thisExpansion.readZeroOrOneArgument(OFFSET_ARG, ::asLong)?.toInt()

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
                    thisExpansion.produceValueSideEffect(IonType.TIMESTAMP, ts)
                    return ExpressionType.DATA_MODEL_SCALAR
                } catch (e: IllegalArgumentException) {
                    throw IonException(e.message)
                }
            }
        },
        _Private_MakeFieldNameAndValue {
            private val FIELD_NAME = 0
            private val FIELD_VALUE = 1

            override fun produceNext(thisExpansion: ExpansionInfo): ExpressionType {
                thisExpansion.session.currentFieldName = thisExpansion.readExactlyOneArgument(FIELD_NAME, ::asSymbol)

                val valueExpansion = thisExpansion.readArgument(FIELD_VALUE)

                thisExpansion.tailCall(valueExpansion)
                thisExpansion.expansionKind = ExactlyOneValueStream
                return ExpressionType.CONTINUE_EXPANSION
            }
        },

        _Private_FlattenStruct {
            private val STRUCTS = 0

            override fun produceNext(thisExpansion: ExpansionInfo): ExpressionType {
                var argumentExpansion: ExpansionInfo? = thisExpansion.additionalState as ExpansionInfo?
                if (argumentExpansion == null) {
                    argumentExpansion = thisExpansion.readArgument(STRUCTS)
                    thisExpansion.additionalState = argumentExpansion
                }

                val currentChildExpansion = thisExpansion.childExpansion

                val next = currentChildExpansion?.session?.produceNext()
                if (next == null) {
                    // Only possible if expansionDelegate is null
                    val nextSequence = argumentExpansion.session.produceNext()
                    thisExpansion.session.currentExpander = thisExpansion
                    return when {
                        nextSequence == ExpressionType.DATA_MODEL_CONTAINER -> {
                            // TODO require this to be a struct
                            //val expression = thisExpansion.environment.expressionAt(thisExpansion.tapeIndex) as? HasStartAndEnd
                            thisExpansion.childExpansion = thisExpansion.session.getExpander(
                                expansionKind = Stream,
                                environmentContext = argumentExpansion.top().environmentContext,
                            )
                            thisExpansion.childExpansion!!.parentExpansion = thisExpansion
                            ExpressionType.CONTINUE_EXPANSION
                        }
                        nextSequence == ExpressionType.END_OF_EXPANSION-> ExpressionType.END_OF_EXPANSION
                        nextSequence.isDataModelExpression -> throw IonException("invalid argument; make_struct expects structs")
                        else -> unreachable()
                    }
                }
                thisExpansion.session.currentExpander = thisExpansion
                return when {
                    next.isDataModelExpression -> next
                    next == ExpressionType.END_OF_EXPANSION-> return ExpressionType.END_OF_EXPANSION //thisExpansion.closeDelegateAndContinue()
                    else -> unreachable()
                }
            }
        },

        /**
         * Iterates over the sequences, returning the values contained in the sequences.
         * The expansion for the sequences argument is stored in [ExpansionInfo.additionalState].
         * When
         */
        Flatten {
            private val SEQUENCES = 0

            override fun produceNext(thisExpansion: ExpansionInfo): ExpressionType {
                var argumentExpansion: ExpansionInfo? = thisExpansion.additionalState as ExpansionInfo?
                if (argumentExpansion == null) {
                    argumentExpansion = thisExpansion.readArgument(SEQUENCES)
                    thisExpansion.additionalState = argumentExpansion
                }

                val currentChildExpansion = thisExpansion.childExpansion

                val next = currentChildExpansion?.session?.produceNext()
                if (next == null) {
                    val nextSequence = argumentExpansion.session.produceNext()
                    thisExpansion.session.currentExpander = thisExpansion
                    return when {
                        nextSequence == ExpressionType.DATA_MODEL_CONTAINER -> {
                            // TODO if type is struct, throw:
                            //is Expression.StructValue -> throw IonException("invalid argument; flatten expects sequences")
                            thisExpansion.childExpansion = thisExpansion.session.getExpander(
                                expansionKind = Stream,
                                environmentContext = argumentExpansion.top().environmentContext,
                            )

                            thisExpansion.childExpansion!!.parentExpansion = thisExpansion
                            ExpressionType.CONTINUE_EXPANSION
                        }

                        nextSequence == ExpressionType.END_OF_EXPANSION -> ExpressionType.END_OF_EXPANSION
                        nextSequence.isDataModelExpression -> throw IonException("invalid argument; flatten expects sequences")
                        else -> unreachable()
                    }
                }
                thisExpansion.session.currentExpander = thisExpansion
                return when {
                    next.isDataModelExpression -> next
                    next == ExpressionType.END_OF_EXPANSION -> ExpressionType.END_OF_EXPANSION
                    // Only possible if expansionDelegate is null

                    else -> unreachable()
                }
            }
        },
        Sum {
            private val ARG_A = 0
            private val ARG_B = 1

            override fun produceNext(thisExpansion: ExpansionInfo): ExpressionType {
                // TODO(PERF): consider checking whether the value would fit in a long and returning a `LongIntValue`.
                val a = thisExpansion.readExactlyOneArgument(ARG_A, ::asBigInteger)
                val b = thisExpansion.readExactlyOneArgument(ARG_B, ::asBigInteger)
                thisExpansion.expansionKind = Empty
                thisExpansion.produceValueSideEffect(IonType.INT, a + b)
                return ExpressionType.DATA_MODEL_SCALAR
            }
        },
        Delta {
            private val ARGS = 0

            override fun produceNext(thisExpansion: ExpansionInfo): ExpressionType {
                // TODO(PERF): Optimize to use LongIntValue when possible
                var delegate = thisExpansion.childExpansion
                val runningTotal = thisExpansion.additionalState as? BigInteger ?: BigInteger.ZERO
                if (delegate == null) {
                    delegate = thisExpansion.readArgument(ARGS)
                    thisExpansion.childExpansion = delegate
                    delegate.parentExpansion = thisExpansion
                }
                thisExpansion.session.currentExpander = delegate
                val nextExpandedArg = delegate.session.produceNext()
                when {
                    nextExpandedArg.isDataModelValue  -> {
                        val nextDelta = asBigInteger(delegate)
                        val nextOutput = runningTotal + nextDelta
                        // The first child expansion is the expression group; add another child to that.
                        delegate.childExpansion!!.produceValueSideEffect(IonType.INT, nextOutput)
                        // Resume the delta invocation once the side effect is consumed.
                        thisExpansion.session.currentExpander!!.parentExpansion = thisExpansion
                        thisExpansion.additionalState = nextOutput
                        return nextExpandedArg
                    }
                    nextExpandedArg == ExpressionType.END_OF_EXPANSION -> {
                        delegate.close() // TODO CHECK
                        thisExpansion.childExpansion = null
                        thisExpansion.session.currentExpander = thisExpansion
                        return ExpressionType.END_OF_EXPANSION
                    }
                    else -> throw IonException("delta arguments must be integers")
                }
            }
        },
        Repeat {
            private val COUNT_ARG = 0
            private val THING_TO_REPEAT = 1

            override fun produceNext(thisExpansion: ExpansionInfo): ExpressionType {
                var n = thisExpansion.additionalState as Long?
                if (n == null) {
                    n = thisExpansion.readExactlyOneArgument(COUNT_ARG, ::asLong)
                    if (n < 0) throw IonException("invalid argument; 'n' must be non-negative")
                    thisExpansion.additionalState = n
                }

                if (thisExpansion.childExpansion == null) {
                    if (n > 0) {
                        thisExpansion.childExpansion = thisExpansion.readArgument(THING_TO_REPEAT)
                        thisExpansion.childExpansion!!.parentExpansion = thisExpansion
                        thisExpansion.additionalState = n - 1
                    } else {
                        return ExpressionType.END_OF_EXPANSION
                    }
                }

                val repeated = thisExpansion.childExpansion!!
                val maybeNext = repeated.session.produceNext()
                thisExpansion.childExpansion = null // TODO repeated.close()?
                return when {
                    maybeNext.isDataModelExpression -> maybeNext
                    maybeNext == ExpressionType.END_OF_EXPANSION-> ExpressionType.END_OF_EXPANSION
                    else -> unreachable()
                }
            }
        },
        ;

        /**
         * Produces the next value, [EndOfExpansion], or [ContinueExpansion].
         * Each enum variant must implement this method.
         */
        abstract fun produceNext(thisExpansion: ExpansionInfo): ExpressionType

        /** Helper function for the `if_*` macros */
        inline fun ExpansionInfo.branchIf(condition: (Int) -> Boolean): ExpressionType {
            val testArg = readArgument(0)
            var n = 0
            while (n < 2) {
                if (testArg.session.produceNext() == ExpressionType.END_OF_EXPANSION) break
                n++
            }
            testArg.close()

            val branch = if (condition(n)) 1 else 2
            // Skip any unused expressions.
            environmentContext.arguments!!.setNextAfterEndOfEExpression()
            tailCall(readArgument(branch))
            return ExpressionType.CONTINUE_EXPANSION
        }

        /**
         * Returns an expansion for the given variable.
         */
        fun ExpansionInfo.readArgument(variableRef: Int): ExpansionInfo {
            val argumentTape = session.environment.seekToArgument(variableRef)
                ?: return session.getExpander(Empty, LazyEnvironment.EMPTY.currentContext) // Argument was elided.
            return session.getExpander(
                expansionKind = Variable,
                environmentContext = session.environment.startChildEnvironment(argumentTape, argumentTape, argumentTape.currentIndex())
            )
        }

        fun ExpansionInfo.produceValueSideEffect(type: IonType, value: Any) {
            val sideEffectTape = session.environment.sideEffects
            sideEffectTape.clear()
            sideEffectTape.addScalar(type, value)
            sideEffectTape.rewindTo(0)
            // The first child expansion is the expression group; add another child to that.
            childExpansion = session.sideEffectExpander
            childExpansion!!.parentExpansion = this
            session.currentExpander = childExpansion
        }

        fun ExpansionInfo.produceFieldNameSideEffect(value: Any) {
            val sideEffectTape = session.environment.sideEffects
            sideEffectTape.clear()
            sideEffectTape.addFieldName(value)
            sideEffectTape.rewindTo(0)
            // The first child expansion is the expression group; add another child to that.
            childExpansion = session.sideEffectExpander
            childExpansion!!.parentExpansion = this
            session.currentExpander = childExpansion
        }

        /**
         * Performs the given [action] for each value produced by the expansion of [variableRef].
         */
        inline fun ExpansionInfo.forEach(variableRef: Int, action: (LazyEnvironment.NestedContext) -> Unit) {
            val savedCurrent = session.currentExpander
            val variableExpansion = readArgument(variableRef)
            while (true) {
                val next = session.produceNext()
                when {
                    next == ExpressionType.END_OF_EXPANSION -> {
                        session.currentExpander = savedCurrent // TODO check this is necessary
                        return
                    }
                    next.isDataModelExpression -> action(variableExpansion.environmentContext)
                }
            }
        }

        /**
         * Performs the given [transform] on each value produced by the expansion of [variableRef], returning a list
         * of the results.
         */
        inline fun <T> ExpansionInfo.map(variableRef: Int, transform: (ExpressionType) -> T): List<T> {
            val variableExpansion = readArgument(variableRef)
            val result = mutableListOf<T>()
            while (true) {
                val next = variableExpansion.session.produceNext()
                when {
                    next == ExpressionType.END_OF_EXPANSION -> return result
                    next.isDataModelExpression -> result.add(transform(next))
                }
            }
        }

        fun asLong(expansion: ExpansionInfo): Long {
            return expansion.environmentContext.longValue()
        }

        fun asBigInteger(expansion: ExpansionInfo): BigInteger {
            return expansion.environmentContext.bigIntegerValue()
        }

        fun asText(expansion: ExpansionInfo): String {
            return expansion.environmentContext.textValue()
        }

        fun asSymbol(expansion: ExpansionInfo): SymbolToken {
            return expansion.environmentContext.symbolValue()
        }

        fun asBigDecimal(expansion: ExpansionInfo): BigDecimal {
            return expansion.environmentContext.bigDecimalValue()
        }

        fun asLob(expansion: ExpansionInfo): ByteArray {
            return expansion.environmentContext.lobValue()
        }

        /**
         * Reads and returns zero or one values from the expansion of the given [variableRef].
         * Throws an [IonException] if more than one value is present in the variable expansion.
         * Throws an [IonException] if the value is not the expected type [T].
         */
        inline fun <T> ExpansionInfo.readZeroOrOneArgument(
            variableRef: Int,
            converter: (ExpansionInfo) -> T
        ): T? {
            val argExpansion = readArgument(variableRef)
            var argValue: T? = null
            while (true) {
                val it = argExpansion.session.produceNext()
                when {
                    it.isDataModelValue -> {
                        if (argValue == null) {
                            argValue = converter(argExpansion)
                        } else {
                            throw IonException("invalid argument; too many values")
                        }
                    }
                    it == ExpressionType.END_OF_EXPANSION -> break
                    it == ExpressionType.FIELD_NAME -> unreachable("Unreachable without stepping into a container")
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
        inline fun <T> ExpansionInfo.readExactlyOneArgument(variableRef: Int, converter: (ExpansionInfo) -> T): T {
            return readZeroOrOneArgument(variableRef, converter) ?: throw IonException("invalid argument; no value when one is expected")
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
     *       Alternately, consider ExpansionOperator to reflect the fact that these are
     *       like operators in an expression tree.
     */
    internal class ExpansionInfo(@JvmField val session: Session) {

        /** The [ExpansionKind]. */
        @JvmField var expansionKind: ExpansionKind = ExpansionKind.Uninitialized
        /**
         * The evaluation [Environment]—i.e. variable bindings.
         */
        @JvmField var environmentContext: LazyEnvironment.NestedContext = LazyEnvironment.EMPTY.currentContext

        @JvmField var reachedEndOfExpression: Boolean = false

        /**
         * Field for storing any additional state required by an ExpansionKind.
         */
        @JvmField
        var additionalState: Any? = null

        @JvmField
        var keepAlive: Boolean = false

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

        var parentExpansion: ExpansionInfo? = null

        /**
         * Gets the [ExpansionInfo] at the top of the stack of [childExpansion]s.
         */
        fun top(): ExpansionInfo = childExpansion?.top() ?: this

        /**
         * Returns this [ExpansionInfo] to the expander pool, recursively closing [childExpansion]s in the process.
         * Could also be thought of as a `free` function.
         */
        fun close() {
            if (!keepAlive) {
                expansionKind = ExpansionKind.Uninitialized
                additionalState?.let { if (it is ExpansionInfo) it.close() }
                additionalState = null
                childExpansion?.close()
                childExpansion = null
                parentExpansion = null
                reachedEndOfExpression = false
            }
        }

        /**
         * Replaces the state of `this` [ExpansionInfo] with the state of [other]—effectively a tail-call optimization.
         * After transferring the state, `other` is returned to the expansion pool.
         */
        fun tailCall(other: ExpansionInfo) {
            this.expansionKind = other.expansionKind
            this.childExpansion = other.childExpansion
            // Note: this.parentExpansion remains unchanged.
            this.additionalState = other.additionalState
            this.environmentContext = session.environment.tailCall()
            this.reachedEndOfExpression = false
            session.currentExpander = this
            // Close `other`
            other.childExpansion = null
            other.close()
        }

        // TODO
        override fun toString() = """
        |ExpansionInfo(
        |    expansionKind: $expansionKind,
        |    child: ${childExpansion?.expansionKind}
        |    additionalState: $additionalState,
        |)
        """.trimMargin()
    }

    private val session = Session(expansionLimit = 1_000_000)
    private val containerStack = _Private_RecyclingStack(8) { ContainerInfo() }
    private var currentExpr: ExpressionType? = null

    private var currentAnnotations: List<SymbolToken>? = null
    private var currentValueType: IonType? = null

    /**
     * Initialize the macro evaluator with an E-Expression.
     */
    fun initExpansion(encodingExpressions: ExpressionTape) {
        session.reset(encodingExpressions)
        val ci = containerStack.push { _ -> }
        ci.type = ContainerInfo.Type.TopLevel

        ci.expansion = session.getExpander(ExpansionKind.Stream, session.environment.currentContext)
    }

    override fun next(): IonType? {
        currentValueType = null
        while (currentValueType == null) {
            currentExpr = session.produceNext()
            when {
                currentExpr == ExpressionType.ANNOTATION -> currentAnnotations = session.currentExpander!!.environmentContext.annotations()
                currentExpr!!.isDataModelValue -> currentValueType = session.currentExpander!!.environmentContext.type()
                currentExpr == ExpressionType.END_OF_EXPANSION -> { // TODO should this go in ExpansionInfo.produceNext?
                    val currentExpander = session.currentExpander!!
                    if (currentExpander.parentExpansion != null) {
                        session.currentExpander = currentExpander.parentExpansion
                        if (session.currentExpander!!.expansionKind != ExpansionKind.Delta) {
                            session.currentExpander!!.childExpansion = null // TODO temporary. Fix Delta so this is not necessary.
                        }
                        if (session.currentExpander!!.expansionKind == ExpansionKind.Variable) {
                            // The variable has been satisfied
                            session.finishVariable()
                        }
                        currentExpander.close()
                        continue
                    }
                    if (containerStack.peek().type == Type.TopLevel) {
                        containerStack.pop().close()
                    }
                    session.currentFieldName = null
                    currentAnnotations = null
                    return null
                }
                else -> unreachable()
            }
        }
        return currentValueType
    }

    @Deprecated("Deprecated in Java")
    override fun hasNext(): Boolean {
        throw UnsupportedOperationException("hasNext() not implemented. Call next() and check for null.")
    }

    /**
     * Steps out of the current [DataModelContainer].
     */
    override fun stepOut() {
        // TODO: We should be able to step out of a "TopLevel" container and/or we need some way to close the evaluation early.
        if (containerStack.size() <= 1) throw IonException("Nothing to step out of.")
        val popped = containerStack.pop()
        val currentContainer = containerStack.peek()
        session.currentExpander = currentContainer.expansion.top()
        if (session.currentExpander!!.expansionKind == ExpansionKind.Variable) {
            // The container being stepped out of was an argument to a variable.
            session.currentExpander!!.reachedEndOfExpression = true
        }
        popped.expansion.environmentContext.tape!!.advanceToAfterEndContainer()
        popped.expansion.session.environment.finishChildEnvironments(popped.expansion.environmentContext)
        popped.close()
        session.currentFieldName = currentContainer.currentFieldName
        currentValueType = null // Must call `next()` to get the next value
        currentAnnotations = null
    }

    /**
     * Steps in to the current [DataModelContainer].
     * Throws [IonException] if not positioned on a container.
     */
    override fun stepIn() {
        val expressionType = requireNotNull(currentExpr) { "Not positioned on a value" }
        if (expressionType == ExpressionType.DATA_MODEL_CONTAINER) {
            val currentContainer = containerStack.peek()
            currentContainer.currentFieldName = session.currentFieldName //this.currentFieldName
            val topExpansion = currentContainer.expansion.top()
            val ci = containerStack.push { _ -> }
            val topEnvironmentContext = topExpansion.environmentContext
            ci.container = currentValueType
            ci.type = when (currentValueType) {
                IonType.LIST -> ContainerInfo.Type.List
                IonType.SEXP -> ContainerInfo.Type.Sexp
                IonType.STRUCT -> ContainerInfo.Type.Struct
                else -> unreachable()
            }
            val environmentContext = topExpansion.session.environment.startChildEnvironment(topEnvironmentContext.tape!!, topEnvironmentContext.arguments!!, topEnvironmentContext.firstArgumentStartIndex)
            ci.expansion = session.getExpander(
                expansionKind = ExpansionKind.Stream,
                environmentContext = environmentContext,
            )
            ci.currentFieldName = null
            currentExpr = null
            session.currentFieldName = null
            currentValueType = null
            currentAnnotations = null
        } else {
            throw IonException("Not positioned on a container.")
        }
    }

    /**
     * Transcodes the e-expression argument expressions provided to this MacroEvaluator
     * without evaluation.
     * @param writer the writer to which the expressions will be transcoded.
     */
    fun transcodeArgumentsTo(writer: MacroAwareIonWriter) {
        var index = 0
        val arguments: ExpressionTape = session.environment.arguments!!
        arguments.rewindTo(0)
        currentAnnotations = null // Annotations are written only via Annotation expressions
        session.currentFieldName = null // Field names are written only via FieldName expressions
        while (index < arguments.size()) {
            currentValueType = null
            arguments.next()
            arguments.prepareNext()
            val argument = arguments.type()
            if (argument.isEnd) {
                currentAnnotations = null
                session.currentFieldName = null
                writer.stepOut()
                index++;
                continue
            }
            when (argument) {
                ExpressionType.ANNOTATION -> {
                    currentAnnotations = arguments.context() as List<SymbolToken>
                    writer.setTypeAnnotationSymbols(*currentAnnotations!!.toTypedArray())
                }
                ExpressionType.DATA_MODEL_CONTAINER -> {
                    currentValueType = arguments.ionType()
                    writer.stepIn(currentValueType)
                }
                ExpressionType.DATA_MODEL_SCALAR -> {
                    currentValueType = arguments.ionType()
                    when (currentValueType) {
                        IonType.NULL -> writer.writeNull()
                        IonType.BOOL -> writer.writeBool(this.booleanValue())
                        IonType.INT -> {
                            when (this.integerSize) {
                                IntegerSize.INT -> writer.writeInt(this.longValue())
                                IntegerSize.LONG -> writer.writeInt(this.longValue())
                                IntegerSize.BIG_INTEGER ->writer.writeInt(this.bigIntegerValue())
                            }
                        }
                        IonType.FLOAT -> writer.writeFloat(this.doubleValue())
                        IonType.DECIMAL -> writer.writeDecimal(this.decimalValue())
                        IonType.TIMESTAMP -> writer.writeTimestamp(this.timestampValue())
                        IonType.SYMBOL -> writer.writeSymbolToken(this.symbolValue())
                        IonType.STRING -> writer.writeString(this.stringValue())
                        IonType.BLOB -> writer.writeBlob(this.newBytes())
                        IonType.CLOB -> writer.writeClob(this.newBytes())
                        else -> throw IllegalStateException("Unexpected branch")
                    }
                }
                ExpressionType.FIELD_NAME -> {
                    session.currentFieldName = arguments.context() as SymbolToken
                    writer.setFieldNameSymbol(session.currentFieldName)
                }
                ExpressionType.E_EXPRESSION -> writer.startMacro(arguments.context() as Macro)
                ExpressionType.EXPRESSION_GROUP -> writer.startExpressionGroup()
                else -> throw IllegalStateException("Unexpected branch")
            }
            index++
        }
        arguments.rewindTo(0)
    }

    override fun close() { /* Nothing to do (yet) */ }
    override fun <T : Any?> asFacet(facetType: Class<T>?): Nothing? = null
    override fun getDepth(): Int = containerStack.size() - 1 // Note: the top-level pseudo-container is included in the stack.
    override fun getSymbolTable(): SymbolTable? = null

    override fun getType(): IonType? = currentValueType

    fun hasAnnotations(): Boolean = currentAnnotations != null && currentAnnotations!!.isNotEmpty()

    override fun getTypeAnnotations(): Array<String> = currentAnnotations?.let { Array(it.size) { i -> it[i].assumeText() } } ?: emptyArray()
    override fun getTypeAnnotationSymbols(): Array<SymbolToken> = currentAnnotations?.toTypedArray() ?: emptyArray()

    private class SymbolTokenAsStringIterator(val tokens: List<SymbolToken>) : MutableIterator<String> {

        var index = 0

        override fun hasNext(): Boolean {
            return index < tokens.size
        }

        override fun next(): String {
            if (index >= tokens.size) {
                throw NoSuchElementException()
            }
            return tokens[index++].assumeText()
        }

        override fun remove() {
            throw UnsupportedOperationException("This iterator does not support removal")
        }
    }

    override fun iterateTypeAnnotations(): MutableIterator<String> {
        return if (currentAnnotations?.isNotEmpty() == true) {
            SymbolTokenAsStringIterator(currentAnnotations!!)
        } else {
            Collections.emptyIterator()
        }
    }

    override fun isInStruct(): Boolean = containerStack.peek()?.container == IonType.STRUCT

    override fun getFieldId(): Int = session.currentFieldName?.sid ?: 0
    override fun getFieldName(): String? = session.currentFieldName?.text
    override fun getFieldNameSymbol(): SymbolToken? = session.currentFieldName

    /** TODO: Throw on data loss */
    override fun intValue(): Int = longValue().toInt()
    override fun decimalValue(): Decimal = Decimal.valueOf(bigDecimalValue())
    override fun dateValue(): Date = timestampValue().dateValue()

    override fun getBytes(buffer: ByteArray?, offset: Int, len: Int): Int {
        TODO("Not yet implemented")
    }
    override fun longValue(): Long  = session.currentExpander!!.environmentContext.longValue()
    override fun bigIntegerValue(): BigInteger = session.currentExpander!!.environmentContext.bigIntegerValue()
    override fun getIntegerSize(): IntegerSize = session.currentExpander!!.environmentContext.integerSize()
    override fun stringValue(): String = session.currentExpander!!.environmentContext.textValue()
    override fun symbolValue(): SymbolToken = session.currentExpander!!.environmentContext.symbolValue()
    override fun bigDecimalValue(): BigDecimal = session.currentExpander!!.environmentContext.bigDecimalValue()
    override fun byteSize(): Int = session.currentExpander!!.environmentContext.lobSize()
    override fun newBytes(): ByteArray = session.currentExpander!!.environmentContext.lobValue().copyOf()
    override fun doubleValue(): Double = session.currentExpander!!.environmentContext.doubleValue()
    override fun timestampValue(): Timestamp = session.currentExpander!!.environmentContext.timestampValue()
    override fun isNullValue(): Boolean = session.currentExpander!!.environmentContext.isNullValue()
    override fun booleanValue(): Boolean = session.currentExpander!!.environmentContext.booleanValue()
}
