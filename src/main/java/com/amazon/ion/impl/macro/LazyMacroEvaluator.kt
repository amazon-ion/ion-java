package com.amazon.ion.impl.macro

import com.amazon.ion.*
import com.amazon.ion.impl.*
import com.amazon.ion.impl.macro.Expression.*
import com.amazon.ion.impl.macro.LazyMacroEvaluator.*
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
 *  - Call [expandNext] to get the next field name or value, or null
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
 * When calling [expandNext], the evaluator looks at the top container in the stack and requests the next value from
 * its expansion frame. That expansion frame may produce a result all on its own (i.e. if the next value is a literal
 * value), or it may create and delegate to a child expansion frame if the next source expression is something that
 * needs to be expanded (e.g. macro invocation, variable expansion, etc.). When delegating to a child expansion frame,
 * the value returned by the child could be intercepted and inspected, modified, or consumed.
 * In this way, the expansion frames model a lazily constructed expression tree over the flat list of expressions in the
 * input to the macro evaluator.
 */
class LazyMacroEvaluator {

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
        /** Pool of [Expression] to minimize allocation and garbage collection. */
       // val expressionPool: PooledExpressionFactory = PooledExpressionFactory()
        /** Pool of [ExpansionInfo] to minimize allocation and garbage collection. */
        private val expanderPool: ArrayList<ExpansionInfo> = ArrayList(64)
        private var expanderPoolIndex = 0
        //val expressions: ArrayList<Expression.EExpressionBodyExpression> = ArrayList(128) // TODO temporary, until Expression no longer needed
        var environment: LazyEnvironment? = null
        var sideEffectExpander: ExpansionInfo? = null

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
        fun getExpander(expansionKind: ExpansionKind, tapeIndex: Int, environmentContext: LazyEnvironment.NestedContext): ExpansionInfo {
            val expansion = getExpansion()
            expansion.expansionKind = expansionKind
            //expansion.expressions = expressions
            //expansion.i = startInclusive
            //expansion.currentExpressionIndex = tapeIndex
            //expansion.nextExpressionIndex = tapeIndex
            //expansion.endExclusive = endExclusive
            expansion.environmentContext = environmentContext
            expansion.additionalState = null
            expansion.childExpansion = null
            expansion.reachedEndOfExpression = false
            return expansion
        }

        fun incrementStepCounter() {
            numExpandedExpressions++
            if (numExpandedExpressions > expansionLimit) {
                // Technically, we are not counting "steps" because we don't have a true definition of what a "step" is,
                // but this is probably a more user-friendly message than trying to explain what we're actually counting.
                throw IonException("Macro expansion exceeded limit of $expansionLimit steps.")
            }
        }

        fun reset(environment: LazyEnvironment) {
            numExpandedExpressions = 0
            //expressionPool.clear()
            expanderPoolIndex = 0
            this.environment = environment
            sideEffectExpander = getExpander(ExpansionKind.Empty, 0, this.environment!!.sideEffectContext)
            sideEffectExpander!!.keepAlive = true
            //intArrayPoolIndices.fill(0)
            //expressions.clear()
        }

        /** The pool of [IntArray]s for use during this session. */
        //private val intArrayPools = Array<MutableList<IntArray>>(32) { mutableListOf() }
        //private val intArrayPoolIndices = IntArray(32) { 0 }

        /**
         * Gets an [IntArray] from the pool, or allocates a new one if necessary. Returned arrays will not
         * necessarily be zeroed and are valid until [reset] is called.
         * */
        /*
        fun intArrayForSize(size: Int): IntArray {
            if (size >= intArrayPools.size) {
                // Don't attempt to pool arbitrarily large arrays.
                return IntArray(size)
            }
            val pool = intArrayPools[size]
            val index = intArrayPoolIndices[size]
            val array: IntArray
            if (index >= pool.size) {
                array = IntArray(size)
                pool.add(array)
            } else {
                array = pool[index]
            }
            intArrayPoolIndices[size] = index + 1
            return array
        }

         */
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

        fun produceNext(): ExpressionType {
            return expansion.produceNext()
        }
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
        Empty {
            override fun produceNext(thisExpansion: ExpansionInfo): ExpressionType =
                ExpressionType.END_OF_EXPANSION
        },
        Stream {

            fun produceNextFromLazyExpression(thisExpansion: ExpansionInfo): ExpressionType {
                val expressionTape = thisExpansion.environmentContext.tape!!
                if (expressionTape.isExhausted) { // TODO necessary?
                    // TODO ugly way of handling end. Should be detected elsewhere? Or protected in expressionTape.typeAt?
                    thisExpansion.expansionKind = Empty
                    return ExpressionType.CONTINUE_EXPANSION
                }
                val nextType = expressionTape.type()
                if (nextType.isEnd) {
                    if (nextType == ExpressionType.EXPRESSION_GROUP_END || nextType == ExpressionType.E_EXPRESSION_END) {
                        // Expressions and expression groups do not rely on stepIn/stepOut for navigation, so the tape must be advanced
                        // here.
                        expressionTape.prepareNext()
                    }
                    if (nextType != ExpressionType.E_EXPRESSION_END) { // TODO why the special case?
                        thisExpansion.expansionKind = Empty
                    }
                    return ExpressionType.CONTINUE_EXPANSION
                }

                return when (nextType) {
                    ExpressionType.FIELD_NAME -> {
                        // TODO record field name position so they can be read later
                        expressionTape.prepareNext()
                        ExpressionType.FIELD_NAME
                    }
                    ExpressionType.ANNOTATION -> {
                        // TODO record annotation position so they can be read later
                        expressionTape.prepareNext()
                        ExpressionType.ANNOTATION
                    }
                    ExpressionType.E_EXPRESSION -> {
                        val macro = expressionTape.context() as Macro
                        //val argIndices =
                        //    calculateArgumentIndices(macro, thisExpansion, 0, macro.body?.size ?: 0)
                        //val newEnvironment = thisExpansion.environment.createLazyChild(expressionTape, ++thisExpansion.tapeIndex, false)
                        val macroBodyTape = ExpressionTape.from(macro.body ?: emptyList()) // TODO put this in the compiler before measuring performance
                        expressionTape.prepareNext()
                        expressionTape.next() // TODO adding this did nothing; try removing
                        val newEnvironment = thisExpansion.session.environment!!.startChildEnvironment(macroBodyTape, expressionTape.currentIndex())
                        //val newEnvironment = thisExpansion.environment.createChild(macro.body ?: emptyList(), argIndices)
                        val expansionKind = forMacro(macro)
                        thisExpansion.childExpansion = thisExpansion.session.getExpander(
                            expansionKind = expansionKind,
                            /*
                            expressions = macro.body ?: emptyList(),
                            startInclusive = 0,
                            endExclusive = macro.body?.size ?: 0,

                             */
                            tapeIndex = 0, //thisExpansion.tapeIndex,
                            environmentContext = newEnvironment,
                        )
                        ExpressionType.CONTINUE_EXPANSION
                    }
                    ExpressionType.E_EXPRESSION_END -> unreachable()
                    ExpressionType.EXPRESSION_GROUP -> {
                        expressionTape.prepareNext()
                        thisExpansion.childExpansion = thisExpansion.session.getExpander(
                            expansionKind = ExprGroup,
                            //expressions = thisExpansion.expressions,
                            //startInclusive = thisExpansion.i,
                            //endExclusive = thisExpansion.endExclusive,
                            tapeIndex = 0, //++thisExpansion.currentExpressionIndex, // TODO tapeIndex might not even be needed
                            environmentContext = thisExpansion.environmentContext,
                        )
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
                        ExpressionType.CONTINUE_EXPANSION
                    }
                    ExpressionType.DATA_MODEL_CONTAINER_END -> unreachable()
                    ExpressionType.END_OF_EXPANSION -> unreachable()
                    ExpressionType.CONTINUE_EXPANSION -> unreachable()
                }
            }

            override fun produceNext(thisExpansion: ExpansionInfo): ExpressionType {
                // If there's a delegate, we'll try that first.
                val delegate = thisExpansion.childExpansion
                check(thisExpansion != delegate)
                if (delegate != null) {
                    val result = delegate.produceNext()
                    val expression = when {
                        result.isDataModelExpression -> result
                        result == ExpressionType.END_OF_EXPANSION -> {
                            delegate.close()
                            thisExpansion.childExpansion = null
                            ExpressionType.CONTINUE_EXPANSION
                        }
                        else -> unreachable()
                    }
                    /*
                    if (delegate.environmentContext == thisExpansion.environmentContext) {
                        thisExpansion.currentExpressionIndex = delegate.currentExpressionIndex
                        thisExpansion.nextExpressionIndex = thisExpansion.currentExpressionIndex
                    }

                     */
                    return expression
                }
                return produceNextFromLazyExpression(thisExpansion)
            }
        },
        /** Alias of [Stream] to aid in debugging */
        Variable {
            override fun produceNext(thisExpansion: ExpansionInfo): ExpressionType {
                if (thisExpansion.reachedEndOfExpression) {
                    thisExpansion.session.environment!!.finishChildEnvironment()
                    thisExpansion.expansionKind = Empty
                    thisExpansion.childExpansion = null
                    return ExpressionType.CONTINUE_EXPANSION
                }
                val expression = Stream.produceNext(thisExpansion)
                if (thisExpansion.childExpansion == null) {
                    if (expression == ExpressionType.DATA_MODEL_SCALAR) {
                        thisExpansion.reachedEndOfExpression = true
                    } else if (expression == ExpressionType.CONTINUE_EXPANSION) { // TODO end condition
                        //thisExpansion.environment.parent().finishVariableEvaluation()
                        thisExpansion.session.environment!!.finishChildEnvironment() // TODO the same needs to happen in all cases where a child environment was created (macro invocations/template body)
                        thisExpansion.expansionKind = Empty
                        thisExpansion.childExpansion = null
                    }
                }
                if (thisExpansion.additionalState != null) { // TODO still needed?
                    thisExpansion.environmentContext.tape!!.rewindTo(thisExpansion.additionalState as Int)
                    thisExpansion.additionalState = null
                }
                return expression
            }
        },
        /** Alias of [Stream] to aid in debugging */
        TemplateBody {
            override fun produceNext(thisExpansion: ExpansionInfo): ExpressionType {
                return Stream.produceNext(thisExpansion)
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

                val type = valueToAnnotateExpansion.produceNext() // TODO apply the annotations

                if (valueToAnnotateExpansion.produceNext() != ExpressionType.END_OF_EXPANSION) {
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
                    when (it) {
                        ExpressionType.DATA_MODEL_SCALAR -> sb.append(asText(thisExpansion))
                        ExpressionType.FIELD_NAME -> unreachable()
                        else -> throw IonException("Invalid argument type for 'make_string': $it")
                    }
                }
                thisExpansion.environmentContext.tape!!.advanceToAfterEndEExpression()
                thisExpansion.expansionKind = Empty
                thisExpansion.produceSideEffect(IonType.STRING, sb.toString())
                return ExpressionType.DATA_MODEL_SCALAR;
            }
        },
        MakeSymbol {
            private val STRINGS_ARG = 0

            override fun produceNext(thisExpansion: ExpansionInfo): ExpressionType {
                if (thisExpansion.additionalState != null) return ExpressionType.END_OF_EXPANSION
                thisExpansion.additionalState = Unit

                val sb = StringBuilder()
                thisExpansion.forEach(STRINGS_ARG) {
                    when (it) {
                        ExpressionType.DATA_MODEL_SCALAR -> sb.append(asText(thisExpansion))
                        ExpressionType.FIELD_NAME -> unreachable()
                        else -> throw IonException("Invalid argument type for 'make_string': $it")
                    }
                }
                thisExpansion.environmentContext.tape!!.advanceToAfterEndEExpression()
                thisExpansion.expansionKind = Empty
                thisExpansion.produceSideEffect(IonType.SYMBOL, sb.toString())
                return ExpressionType.DATA_MODEL_SCALAR
            }
        },
        MakeBlob {
            private val LOB_ARG = 0

            override fun produceNext(thisExpansion: ExpansionInfo): ExpressionType {
                val baos = ByteArrayOutputStream()
                thisExpansion.forEach(LOB_ARG) {
                    baos.write(asLob(thisExpansion))
                }
                thisExpansion.expansionKind = Empty
                thisExpansion.produceSideEffect(IonType.BLOB, baos.toByteArray())
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
                thisExpansion.produceSideEffect(IonType.DECIMAL, BigDecimal(coefficient, -1 * exponent.intValueExact()))
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
                    thisExpansion.produceSideEffect(IonType.TIMESTAMP, ts)
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
                val fieldName = thisExpansion.readExactlyOneArgument(FIELD_NAME, ::asSymbol)
                /*
                val fieldNameExpression = thisExpansion.session.expressionPool.createFieldName(
                    fieldName.asSymbol(thisExpansion)
                )

                 */

                val savedTapeIndex = thisExpansion.environmentContext.tape!!.currentIndex() // TODO determine if there needs to be some replacement to this now that the index is tracked in the tape

                //thisExpansion.readExactlyOneArgument(FIELD_VALUE)

                val valueExpansion = thisExpansion.readArgument(FIELD_VALUE)

                // Rewind the tape; this argument will be read again to satisfy the one-expression-at-a-time
                // design of the evaluator.
                //valueExpansion.currentExpressionIndex = savedTapeIndex
                thisExpansion.environmentContext.tape!!.rewindTo(savedTapeIndex)

                /*
                return fieldNameExpression.also {
                    thisExpansion.tailCall(valueExpansion)
                    thisExpansion.expansionKind = ExactlyOneValueStream
                }

                 */
                thisExpansion.tailCall(valueExpansion)
                thisExpansion.expansionKind = ExactlyOneValueStream
                // TODO store the field name somewhere
                return ExpressionType.DATA_MODEL_SCALAR // tODO will this be a scalar or container?
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

                val next = currentChildExpansion?.produceNext()
                if (next == null) {
                    // Only possible if expansionDelegate is null
                    val nextSequence = argumentExpansion.produceNext()
                    return when {
                        nextSequence == ExpressionType.DATA_MODEL_CONTAINER -> {
                            // TODO require this to be a struct
                            //val expression = thisExpansion.environment.expressionAt(thisExpansion.tapeIndex) as? HasStartAndEnd
                            thisExpansion.childExpansion = thisExpansion.session.getExpander(
                                expansionKind = Stream,
                                /*
                                expressions = argumentExpansion.top().expressions,
                                startInclusive = expression?.startInclusive ?: 0,
                                endExclusive = expression?.endExclusive ?: 0,

                                 */
                                tapeIndex = 0, //thisExpansion.currentExpressionIndex,
                                environmentContext = argumentExpansion.top().environmentContext,
                            )
                            ExpressionType.CONTINUE_EXPANSION
                        }
                        nextSequence == ExpressionType.END_OF_EXPANSION-> ExpressionType.END_OF_EXPANSION
                        nextSequence.isDataModelExpression -> throw IonException("invalid argument; make_struct expects structs")
                        else -> unreachable()
                    }
                }
                return when {
                    next.isDataModelExpression -> next
                    next == ExpressionType.END_OF_EXPANSION-> thisExpansion.closeDelegateAndContinue()
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

                val next = currentChildExpansion?.produceNext()
                if (next == null) {
                    val nextSequence = argumentExpansion.produceNext()
                    return when {
                        nextSequence == ExpressionType.DATA_MODEL_CONTAINER -> {
                            // TODO if type is struct, throw:
                            //is Expression.StructValue -> throw IonException("invalid argument; flatten expects sequences")

                            //val expression = thisExpansion.environment.expressionAt(thisExpansion.tapeIndex) as? HasStartAndEnd
                            thisExpansion.childExpansion = thisExpansion.session.getExpander(
                                expansionKind = Stream,
                                /*
                                expressions = argumentExpansion.top().expressions,
                                startInclusive = expression?.startInclusive ?: 0,
                                endExclusive = expression?.endExclusive ?: 0,

                                 */
                                tapeIndex = 0, //thisExpansion.currentExpressionIndex,
                                environmentContext = argumentExpansion.top().environmentContext,
                            )

                            ExpressionType.CONTINUE_EXPANSION
                        }

                        nextSequence == ExpressionType.END_OF_EXPANSION -> ExpressionType.END_OF_EXPANSION
                        nextSequence.isDataModelExpression -> throw IonException("invalid argument; flatten expects sequences")
                        else -> unreachable()
                    }
                }
                return when {
                    next.isDataModelExpression -> next
                    next == ExpressionType.END_OF_EXPANSION -> thisExpansion.closeDelegateAndContinue()
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
                thisExpansion.produceSideEffect(IonType.INT, a + b)
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
                }

                val nextExpandedArg = delegate.produceNext()
                when {
                    nextExpandedArg.isDataModelValue  -> {
                        val nextDelta = asBigInteger(delegate)
                        val nextOutput = runningTotal + nextDelta
                        // The first child expansion is the expression group; add another child to that.
                        delegate.childExpansion!!.produceSideEffect(IonType.INT, nextOutput)
                        thisExpansion.additionalState = nextOutput
                        return nextExpandedArg
                    }
                    nextExpandedArg == ExpressionType.END_OF_EXPANSION -> {
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
                        thisExpansion.additionalState = n - 1
                    } else {
                        return ExpressionType.END_OF_EXPANSION
                    }
                }

                val repeated = thisExpansion.childExpansion!!
                val maybeNext = repeated.produceNext()
                return when {
                    maybeNext.isDataModelExpression -> maybeNext
                    maybeNext == ExpressionType.END_OF_EXPANSION-> thisExpansion.closeDelegateAndContinue()
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
                if (testArg.produceNext() == ExpressionType.END_OF_EXPANSION) break
                n++
            }
            testArg.close()

            val branch = if (condition(n)) 1 else 2

            tailCall(readArgument(branch))
            if (branch == 2) {
                // The false branch comes second; skip the tape past its expression(s) if the branch is not taken.
                additionalState = testArg.session.environment!!.arguments!!.findIndexAfterEndEExpression()
            }
            return ExpressionType.CONTINUE_EXPANSION
        }

        /**
         * Returns an expansion for the given variable.
         */
        fun ExpansionInfo.readArgument(variableRef: Int): ExpansionInfo {
            val argumentTape = session.environment!!.arguments!! // TODO try including argument source in the environment context. This will be the encoded arguments except in the case of a nested invocation.
            if (!environmentContext.seekToArgument(argumentTape, variableRef)) {
                // Argument was elided.
                return session.getExpander(Empty, 0, LazyEnvironment.EMPTY.currentContext)
            }
            //environment.startVariableEvaluation()
            return session.getExpander(
                expansionKind = Variable,
                /*
                expressions = Collections.emptyList(), // The expressions will come from the tape, not the macro body
                startInclusive = 0,
                endExclusive = 0,

                 */
                tapeIndex = 0, //i,
                //environment = environment.createLazyChild(environment.arguments!!, i, true)
                environmentContext = session.environment!!.startChildEnvironment(argumentTape, argumentTape.currentIndex()) // TODO ensure this is the right source
            )
        }

        fun ExpansionInfo.produceSideEffect(type: IonType, value: Any) {
            val sideEffectTape = session.environment!!.sideEffects
            sideEffectTape.clear()
            sideEffectTape.addScalar(type, value)
            sideEffectTape.rewindTo(0)
            // The first child expansion is the expression group; add another child to that.
            childExpansion = session.sideEffectExpander
        }

        /**
         * Performs the given [action] for each value produced by the expansion of [variableRef].
         */
        inline fun ExpansionInfo.forEach(variableRef: Int, action: (ExpressionType) -> Unit) {
            val variableExpansion = readArgument(variableRef)
            while (true) {
                val next = variableExpansion.produceNext()
                when {
                    next == ExpressionType.END_OF_EXPANSION -> return
                    next.isDataModelExpression -> action(next)
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
                val next = variableExpansion.produceNext()
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
                val it = argExpansion.produceNext()
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
        /**
         * The [Expression]s being expanded. This MUST be the original list, not a sublist because
         * (a) we don't want to be allocating new sublists all the time, and (b) the
         * start and end indices of the expressions may be incorrect if a sublist is taken.
         */
        //@JvmField var expressions: List<Expression> = emptyList()
        /** End of [expressions] that are applicable for this [ExpansionInfo] */
        //@JvmField var endExclusive: Int = 0
        /** Current position within [expressions] of this expansion */
        //@JvmField var i: Int = 0
        /** Current position within the environment's expression tape */
        //@JvmField var currentExpressionIndex: Int = 0
        //@JvmField var nextExpressionIndex: Int = 0
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

        /**
         * Convenience function to close the [childExpansion] and return it to the pool.
         */
        fun closeDelegateAndContinue(): ExpressionType {
            if (childExpansion != null) {
                //currentExpressionIndex = childExpansion!!.currentExpressionIndex
                //nextExpressionIndex = currentExpressionIndex
                childExpansion!!.close()
                childExpansion = null
            }
            return ExpressionType.CONTINUE_EXPANSION
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
            if (!keepAlive) {
                expansionKind = ExpansionKind.Uninitialized
                additionalState?.let { if (it is ExpansionInfo) it.close() }
                additionalState = null
                childExpansion?.close()
                childExpansion = null
                //nextExpressionIndex = currentExpressionIndex
                reachedEndOfExpression = false
            }
        }

        /**
         * Replaces the state of `this` [ExpansionInfo] with the state of [other]—effectively a tail-call optimization.
         * After transferring the state, `other` is returned to the expansion pool.
         */
        fun tailCall(other: ExpansionInfo) {
            this.expansionKind = other.expansionKind
            //this.i = other.i
            //this.currentExpressionIndex = other.currentExpressionIndex
            //this.nextExpressionIndex = other.nextExpressionIndex
            //this.endExclusive = other.endExclusive
            this.childExpansion = other.childExpansion
            this.additionalState = other.additionalState
            //this.environmentContext = other.environmentContext // TODO how is this handled?
            this.reachedEndOfExpression = false // TODO check
            /*
            if (!this.environment.currentContext.useTape) {
                this.expressions = environment.currentContext.expressions
            }

             */
            // Close `other`
            other.childExpansion = null
            other.close()
        }

        /**
         * Produces the next value from this expansion, preparing the expression to be read from the environment.
         */
        fun produceNext(): ExpressionType {
            while (true) {
                environmentContext.tape!!.next();
                val next = expansionKind.produceNext(this)
                if (next == ExpressionType.CONTINUE_EXPANSION) continue
                // This the only place where we count the expansion steps.
                // It is theoretically possible to have macro expansions that are millions of levels deep because this
                // only counts macro invocations at the end of their expansion, but this will still work to catch things
                // like a  billion laughs attack because it does place a limit on the number of _values_ produced.
                // This counts every value _at every level_, so most values will be counted multiple times. If possible
                // without impacting performance, count values only once in order to have more predictable behavior.
                session.incrementStepCounter()
                return next
            }
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

    /**
     * Returns the e-expression argument expression tape that this MacroEvaluator would evaluate.
     */
    fun getArgumentTape(): ExpressionTape {
        return session.environment!!.arguments!!
    }

    /**
     * Initialize the macro evaluator with an E-Expression.
     */
    fun initExpansion(encodingExpressions: ExpressionTape) {
        val environment = LazyEnvironment.create(encodingExpressions)
        session.reset(environment)
        val ci = containerStack.push { _ -> }
        ci.type = ContainerInfo.Type.TopLevel

        ci.expansion = session.getExpander(ExpansionKind.Stream, 0, environment.currentContext)
    }

    /**
     * Evaluate the macro expansion until the next [DataModelExpression] can be returned.
     * Returns null if at the end of a container or at the end of the expansion.
     */
    fun expandNext(): ExpressionType? {
        currentExpr = null
        val currentContainer = containerStack.peek()
        val nextExpansionOutput = currentContainer.produceNext()
        when {
            nextExpansionOutput.isDataModelExpression -> currentExpr = nextExpansionOutput
            nextExpansionOutput == ExpressionType.END_OF_EXPANSION -> {
                if (currentContainer.type == ContainerInfo.Type.TopLevel) {
                    currentContainer.close()
                    containerStack.pop()
                }
            }
        }
        return currentExpr
    }

    private fun currentExpansion(): ExpansionInfo {
        val currentContainer = containerStack.peek()
        var expansion = currentContainer.expansion
        // TODO avoid having to do this traversal every time
        while (expansion.childExpansion != null) {
            expansion = expansion.childExpansion!!
        }
        return expansion
    }

    fun currentFieldName(): SymbolToken {
        return when {
            currentExpr!! == ExpressionType.FIELD_NAME -> {
                val expansion = currentExpansion()
                expansion.environmentContext.context() as SymbolToken
            }

            else -> throw IonException("Not positioned on a field name")
        }
    }

    fun currentAnnotations(): List<SymbolToken> {
        return when {
            currentExpr!! == ExpressionType.ANNOTATION -> {
                val expansion = currentExpansion()
                expansion.environmentContext.annotations()
            }
            else -> throw IonException("Not positioned on annotations")
        }
    }

    fun currentValueType(): IonType {
        return when {
            currentExpr!!.isDataModelValue -> {
                val expansion = currentExpansion()
                expansion.environmentContext.type()
            }
            else -> throw IonException("Not positioned on a value")
        }
    }

    /**
     * Steps out of the current [DataModelContainer].
     */
    fun stepOut() {
        // TODO: We should be able to step out of a "TopLevel" container and/or we need some way to close the evaluation early.
        if (containerStack.size() <= 1) throw IonException("Nothing to step out of.")
        val popped = containerStack.pop()
        containerStack.peek().expansion.environmentContext.tape!!.advanceToAfterEndContainer()
        popped.close()
    }

    /**
     * Steps in to the current [DataModelContainer].
     * Throws [IonException] if not positioned on a container.
     */
    fun stepIn() {
        val expressionType = requireNotNull(currentExpr) { "Not positioned on a value" }
        if (expressionType == ExpressionType.DATA_MODEL_CONTAINER) {
            val currentContainer = containerStack.peek()
            val topExpansion = currentContainer.expansion.top()
            val ci = containerStack.push { _ -> }
            /*
            ci.type = when (expression.type) {
                IonType.LIST -> ContainerInfo.Type.List
                IonType.SEXP -> ContainerInfo.Type.Sexp
                IonType.STRUCT -> ContainerInfo.Type.Struct
                else -> unreachable()
            }

             */
            ci.type = when (topExpansion.environmentContext.type()) {
                IonType.LIST -> ContainerInfo.Type.List
                IonType.SEXP -> ContainerInfo.Type.Sexp
                IonType.STRUCT -> ContainerInfo.Type.Struct
                else -> unreachable()
            }
            //val expression = currentContainer.expansion.environment.expressionAt(currentContainer.expansion.tapeIndex) as? HasStartAndEnd
            ci.expansion = session.getExpander(
                expansionKind = ExpansionKind.Stream,
                /*
                startInclusive = expression?.startInclusive ?: 0,
                endExclusive = expression?.endExclusive ?: 0,

                 */
                tapeIndex = 0, //currentContainer.expansion.currentExpressionIndex + 1,
                environmentContext = topExpansion.environmentContext,
            )
            currentExpr = null
        } else {
            throw IonException("Not positioned on a container.")
        }
    }

    fun longValue(): Long {
        val expansion = currentExpansion()
        return expansion.environmentContext.longValue()
    }

    fun bigIntegerValue(): BigInteger {
        val expansion = currentExpansion()
        return expansion.environmentContext.bigIntegerValue()
    }

    fun getIntegerSize(): IntegerSize {
        val expansion = currentExpansion()
        return expansion.environmentContext.integerSize()
    }

    fun stringValue(): String {
        val expansion = currentExpansion()
        return expansion.environmentContext.textValue()
    }

    fun symbolValue(): SymbolToken {
        val expansion = currentExpansion()
        return expansion.environmentContext.symbolValue()
    }

    fun bigDecimalValue(): BigDecimal {
        val expansion = currentExpansion()
        return expansion.environmentContext.bigDecimalValue()
    }

    fun lobSize(): Int {
        val expansion = currentExpansion()
        return expansion.environmentContext.lobSize()
    }

    fun lobValue(): ByteArray {
        val expansion = currentExpansion()
        return expansion.environmentContext.lobValue()
    }

    fun doubleValue(): Double {
        val expansion = currentExpansion()
        return expansion.environmentContext.doubleValue()
    }

    fun timestampValue(): Timestamp {
        val expansion = currentExpansion()
        return expansion.environmentContext.timestampValue()
    }

    fun isNullValue(): Boolean {
        val expansion = currentExpansion()
        return expansion.environmentContext.isNullValue()
    }

    fun booleanValue(): Boolean {
        val expansion = currentExpansion()
        return expansion.environmentContext.booleanValue()
    }
}

/**
 * Given a [Macro] (or more specifically, its signature), calculates the position of each of its arguments
 * in [expansion].expressions. The result is a list that can be used to map from a parameter's
 * signature index to the encoding expression index. Any trailing, optional arguments that are
 * elided have a value of -1.
 *
 * This function also validates that the correct number of parameters are present. If there are
 * too many parameters or too few parameters, this will throw [IonException].
 */
/*
private fun calculateArgumentIndices(
    macro: Macro,
    expansion: LazyMacroEvaluator.ExpansionInfo,
    argsStartInclusive: Int,
    argsEndExclusive: Int
): IntArray {
    // TODO: For TDL macro invocations, see if we can calculate this during the "compile" step.
    var numArgs = 0
    val argsIndices = expansion.session.intArrayForSize(macro.signature.size)
    var currentArgIndex = argsStartInclusive
    val encodingExpressions: List<Expression> = expansion.expressions

    for (i in 0 until macro.signature.size) {
        if (currentArgIndex >= argsEndExclusive) {
            if (!macro.signature[i].cardinality.canBeVoid) throw IonException("No value provided for parameter ${macro.signature[i].variableName}")
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
    return argsIndices
}

 */
