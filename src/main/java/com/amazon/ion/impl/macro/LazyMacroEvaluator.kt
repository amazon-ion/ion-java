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
        val expressionPool: PooledExpressionFactory = PooledExpressionFactory()
        /** Pool of [ExpansionInfo] to minimize allocation and garbage collection. */
        private val expanderPool: ArrayList<ExpansionInfo> = ArrayList(64)
        private var expanderPoolIndex = 0
        val expressions: ArrayList<Expression.EExpressionBodyExpression> = ArrayList(128) // TODO temporary, until Expression no longer needed

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
        fun getExpander(expansionKind: ExpansionKind, expressions: List<Expression>, startInclusive: Int, endExclusive: Int, tapeIndex: Int, environment: LazyEnvironment): ExpansionInfo {
            val expansion = getExpansion()
            expansion.expansionKind = expansionKind
            expansion.expressions = expressions
            expansion.i = startInclusive
            expansion.tapeIndex = tapeIndex
            expansion.endExclusive = endExclusive
            expansion.environment = environment
            expansion.additionalState = null
            expansion.childExpansion = null
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

        fun reset() {
            numExpandedExpressions = 0
            expressionPool.clear()
            expanderPoolIndex = 0
            intArrayPoolIndices.fill(0)
            expressions.clear()
        }

        /** The pool of [IntArray]s for use during this session. */
        private val intArrayPools = Array<MutableList<IntArray>>(32) { mutableListOf() }
        private val intArrayPoolIndices = IntArray(32) { 0 }

        /**
         * Gets an [IntArray] from the pool, or allocates a new one if necessary. Returned arrays will not
         * necessarily be zeroed and are valid until [reset] is called.
         * */
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

        fun produceNext(): Expression.ExpansionOutputExpression {
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
            override fun produceNext(thisExpansion: ExpansionInfo): Expression.ExpansionOutputExpressionOrContinue =
                Expression.EndOfExpansion
        },
        Stream {

            fun produceNextFromMaterializedExpression(thisExpansion: ExpansionInfo): Expression.ExpansionOutputExpressionOrContinue {
                if (thisExpansion.i >= thisExpansion.endExclusive) {
                    thisExpansion.expansionKind = Empty
                    return Expression.ContinueExpansion
                }

                val next = thisExpansion.expressions[thisExpansion.i]
                thisExpansion.i++
                if (next is Expression.HasStartAndEnd) thisExpansion.i = next.endExclusive

                return when (next) {
                    is Expression.DataModelExpression -> next
                    is Expression.InvokableExpression -> {
                        val macro = next.macro
                        val argIndices =
                            calculateArgumentIndices(macro, thisExpansion, next.startInclusive, next.endExclusive)
                        val newEnvironment = thisExpansion.environment.createChild(thisExpansion.expressions, argIndices)
                        val expansionKind = ExpansionKind.forMacro(macro)
                        thisExpansion.childExpansion = thisExpansion.session.getExpander(
                            expansionKind = expansionKind,
                            expressions = macro.body ?: emptyList(),
                            startInclusive = 0,
                            endExclusive = macro.body?.size ?: 0,
                            tapeIndex = thisExpansion.tapeIndex,
                            environment = newEnvironment,
                        )
                        Expression.ContinueExpansion
                    }
                    is Expression.ExpressionGroup -> {
                        thisExpansion.childExpansion = thisExpansion.session.getExpander(
                            expansionKind = ExprGroup,
                            expressions = thisExpansion.expressions,
                            startInclusive = next.startInclusive,
                            endExclusive = next.endExclusive,
                            tapeIndex = thisExpansion.tapeIndex,
                            environment = thisExpansion.environment,
                        )

                        Expression.ContinueExpansion
                    }

                    is Expression.VariableRef -> {
                        thisExpansion.childExpansion = thisExpansion.readArgument(next)
                        Expression.ContinueExpansion
                    }
                    Expression.Placeholder -> unreachable()
                }
            }

            fun produceNextFromLazyExpression(thisExpansion: ExpansionInfo): Expression.ExpansionOutputExpressionOrContinue {
                val expressionTape = thisExpansion.environment.arguments!!
                if (thisExpansion.tapeIndex >= expressionTape.size() || thisExpansion.tapeIndex < 0) {
                    // TODO ugly way of handling end. Should be detected elsewhere? Or protected in expressionTape.typeAt?
                    thisExpansion.expansionKind = Empty
                    return Expression.ContinueExpansion
                }
                val nextType = expressionTape.typeAt(thisExpansion.tapeIndex)
                if (nextType.isEnd) {
                    if (nextType == ExpressionType.EXPRESSION_GROUP_END || nextType == ExpressionType.E_EXPRESSION_END) {
                        // Expressions and expression groups do not rely on stepIn/stepOut for navigation, so the tape must be advanced
                        // here.
                        thisExpansion.tapeIndex++
                    }
                    if (nextType != ExpressionType.E_EXPRESSION_END) { // TODO why the special case?
                        thisExpansion.expansionKind = Empty
                    }
                    return Expression.ContinueExpansion
                }

                // TODO avoid materializing Expressions here
                return when (nextType) {
                    ExpressionType.FIELD_NAME -> expressionTape.expressionAt(thisExpansion.tapeIndex++) as DataModelExpression
                    ExpressionType.ANNOTATION -> {
                        val expression = expressionTape.expressionAt(thisExpansion.tapeIndex) as DataModelExpression
                        thisExpansion.tapeIndex += 2
                        return expression
                    }
                    ExpressionType.E_EXPRESSION -> {
                        val macro = expressionTape.contextAt(thisExpansion.tapeIndex) as Macro
                        //val argIndices =
                        //    calculateArgumentIndices(macro, thisExpansion, 0, macro.body?.size ?: 0)
                        val newEnvironment = thisExpansion.environment.createLazyChild(expressionTape, ++thisExpansion.tapeIndex, false)
                        //val newEnvironment = thisExpansion.environment.createChild(macro.body ?: emptyList(), argIndices)
                        val expansionKind = ExpansionKind.forMacro(macro)
                        thisExpansion.childExpansion = thisExpansion.session.getExpander(
                            expansionKind = expansionKind,
                            expressions = macro.body ?: emptyList(),
                            startInclusive = 0,
                            endExclusive = macro.body?.size ?: 0,
                            tapeIndex = thisExpansion.tapeIndex,
                            environment = newEnvironment,
                        )
                        Expression.ContinueExpansion
                    }
                    ExpressionType.E_EXPRESSION_END -> unreachable()
                    ExpressionType.EXPRESSION_GROUP -> {
                        thisExpansion.childExpansion = thisExpansion.session.getExpander(
                            expansionKind = ExprGroup,
                            expressions = thisExpansion.expressions,
                            startInclusive = thisExpansion.i,
                            endExclusive = thisExpansion.endExclusive,
                            tapeIndex = ++thisExpansion.tapeIndex,
                            environment = thisExpansion.environment,
                        )
                        Expression.ContinueExpansion
                    }
                    ExpressionType.EXPRESSION_GROUP_END -> unreachable()
                    ExpressionType.DATA_MODEL_SCALAR -> {
                        expressionTape.expressionAt(thisExpansion.tapeIndex++) as DataModelExpression
                    }
                    ExpressionType.DATA_MODEL_CONTAINER -> expressionTape.expressionAt(thisExpansion.tapeIndex++) as DataModelExpression
                    ExpressionType.DATA_MODEL_CONTAINER_END -> unreachable()
                }
            }

            override fun produceNext(thisExpansion: ExpansionInfo): Expression.ExpansionOutputExpressionOrContinue {
                // If there's a delegate, we'll try that first.
                val delegate = thisExpansion.childExpansion
                check(thisExpansion != delegate)
                if (delegate != null) {
                    val expression = when (val result = delegate.produceNext()) {
                        is Expression.DataModelExpression -> result
                        Expression.EndOfExpansion -> {
                            delegate.close()
                            thisExpansion.childExpansion = null
                            Expression.ContinueExpansion
                        }
                    }
                    thisExpansion.tapeIndex = delegate.tapeIndex
                    return expression
                }
                if (thisExpansion.i < thisExpansion.endExclusive && thisExpansion.i < thisExpansion.expressions.size) {
                    return produceNextFromMaterializedExpression(thisExpansion)
                } else if (thisExpansion.environment.useTape) {
                    return produceNextFromLazyExpression(thisExpansion)
                } else {
                    thisExpansion.expansionKind = Empty
                    return Expression.ContinueExpansion
                }
            }
        },
        /** Alias of [Stream] to aid in debugging */
        Variable {
            override fun produceNext(thisExpansion: ExpansionInfo): Expression.ExpansionOutputExpressionOrContinue {
                val expression = Stream.produceNext(thisExpansion)
                // TODO there are circumstances where thisExpansion.i < thisExpansion.endExclusive, yet thisExpansion.expressions is empty. This seems wrong, but adding a check here changes nothing
                if (thisExpansion.i >= thisExpansion.endExclusive && thisExpansion.childExpansion == null) {
                    thisExpansion.environment.parentEnvironment!!.finishVariableEvaluation()
                    thisExpansion.expansionKind = Empty
                    thisExpansion.childExpansion = null
                }
                if (thisExpansion.additionalState != null) {
                    thisExpansion.tapeIndex = thisExpansion.additionalState as Int
                    thisExpansion.additionalState = null
                }
                return expression
            }
        },
        /** Alias of [Stream] to aid in debugging */
        TemplateBody {
            override fun produceNext(thisExpansion: ExpansionInfo): Expression.ExpansionOutputExpressionOrContinue {
                return Stream.produceNext(thisExpansion)
            }
        },
        /** Alias of [Stream] to aid in debugging */
        ExprGroup {
            override fun produceNext(thisExpansion: ExpansionInfo): Expression.ExpansionOutputExpressionOrContinue {
                return Stream.produceNext(thisExpansion)
            }
        },
        ExactlyOneValueStream {
            override fun produceNext(thisExpansion: ExpansionInfo): Expression.ExpansionOutputExpressionOrContinue {
                if (thisExpansion.additionalState != 1) {
                    return when (val firstValue = Stream.produceNext(thisExpansion)) {
                        is Expression.DataModelExpression -> {
                            thisExpansion.additionalState = 1
                            firstValue
                        }
                        Expression.ContinueExpansion -> Expression.ContinueExpansion
                        Expression.EndOfExpansion -> throw IonException("Expected one value, found 0")
                    }
                } else {
                    return when (val secondValue = Stream.produceNext(thisExpansion)) {
                        is Expression.DataModelExpression -> throw IonException("Expected one value, found multiple")
                        Expression.ContinueExpansion -> Expression.ContinueExpansion
                        Expression.EndOfExpansion -> secondValue
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

            private val ANNOTATIONS_ARG = Expression.VariableRef(0)
            private val VALUE_TO_ANNOTATE_ARG = Expression.VariableRef(1)

            override fun produceNext(thisExpansion: ExpansionInfo): Expression.ExpansionOutputExpressionOrContinue {
                val annotations = thisExpansion.map(ANNOTATIONS_ARG) {
                    when (it) {
                        is Expression.StringValue -> _Private_Utils.newSymbolToken(it.value)
                        is Expression.SymbolValue -> it.value
                        is Expression.DataModelValue -> throw IonException("Invalid argument type for 'annotate': ${it.type}")
                        else -> unreachable("Unreachable without stepping in to a container")
                    }
                }

                val valueToAnnotateExpansion = thisExpansion.readArgument(VALUE_TO_ANNOTATE_ARG)

                val annotatedExpression = valueToAnnotateExpansion.produceNext().let {
                    it as? Expression.DataModelValue ?: throw IonException("Required at least one value.")
                    it.withAnnotations(annotations + it.annotations)
                }
                if (valueToAnnotateExpansion.produceNext() != Expression.EndOfExpansion) {
                    throw IonException("Can only annotate exactly one value")
                }

                return annotatedExpression.also {
                    thisExpansion.tailCall(valueToAnnotateExpansion)
                }
            }
        },
        MakeString {
            private val STRINGS_ARG = Expression.VariableRef(0)

            override fun produceNext(thisExpansion: ExpansionInfo): Expression.ExpansionOutputExpressionOrContinue {
                val sb = StringBuilder()
                thisExpansion.forEach(STRINGS_ARG) {
                    when (it) {
                        is Expression.StringValue -> sb.append(it.value)
                        is Expression.SymbolValue -> sb.append(it.value.assumeText())
                        is Expression.DataModelValue -> throw IonException("Invalid argument type for 'make_string': ${it.type}")
                        is Expression.FieldName -> unreachable()
                    }
                }
                // TODO determine where else this might need to be applied. MakeSymbol?
                // TODO should it be checked whether useTape is true?
                thisExpansion.tapeIndex = thisExpansion.environment.arguments!!.findIndexAfterEndEExpressionFrom(thisExpansion.tapeIndex)
                thisExpansion.expansionKind = Empty
                return thisExpansion.session.expressionPool.createStringValue(Collections.emptyList(), sb.toString())
            }
        },
        MakeSymbol {
            private val STRINGS_ARG = Expression.VariableRef(0)

            override fun produceNext(thisExpansion: ExpansionInfo): Expression.ExpansionOutputExpressionOrContinue {
                if (thisExpansion.additionalState != null) return Expression.EndOfExpansion
                thisExpansion.additionalState = Unit

                val sb = StringBuilder()
                thisExpansion.forEach(STRINGS_ARG) {
                    when (it) {
                        is Expression.StringValue -> sb.append(it.value)
                        is Expression.SymbolValue -> sb.append(it.value.assumeText())
                        is Expression.DataModelValue -> throw IonException("Invalid argument type for 'make_symbol': ${it.type}")
                        is Expression.FieldName -> unreachable()
                    }
                }
                return thisExpansion.session.expressionPool.createSymbolValue(
                    Collections.emptyList(),
                    _Private_Utils.newSymbolToken(sb.toString())
                )
            }
        },
        MakeBlob {
            private val LOB_ARG = Expression.VariableRef(0)

            override fun produceNext(thisExpansion: ExpansionInfo): Expression.ExpansionOutputExpressionOrContinue {
                val baos = ByteArrayOutputStream()
                thisExpansion.forEach(LOB_ARG) {
                    when (it) {
                        is Expression.LobValue -> baos.write(it.value)
                        is Expression.DataModelValue -> throw IonException("Invalid argument type for 'make_blob': ${it.type}")
                        is Expression.FieldName -> unreachable()
                    }
                }
                thisExpansion.expansionKind = Empty
                return thisExpansion.session.expressionPool.createBlobValue(Collections.emptyList(), baos.toByteArray())
            }
        },
        MakeDecimal {
            private val COEFFICIENT_ARG = Expression.VariableRef(0)
            private val EXPONENT_ARG = Expression.VariableRef(1)

            override fun produceNext(thisExpansion: ExpansionInfo): Expression.ExpansionOutputExpressionOrContinue {
                val coefficient = thisExpansion.readExactlyOneArgument<Expression.IntValue>(COEFFICIENT_ARG).bigIntegerValue
                val exponent = thisExpansion.readExactlyOneArgument<Expression.IntValue>(EXPONENT_ARG).bigIntegerValue
                thisExpansion.expansionKind = Empty
                return thisExpansion.session.expressionPool.createDecimalValue(
                    Collections.emptyList(),
                    BigDecimal(coefficient, -1 * exponent.intValueExact())
                )
            }
        },
        MakeTimestamp {
            private val YEAR_ARG = Expression.VariableRef(0)
            private val MONTH_ARG = Expression.VariableRef(1)
            private val DAY_ARG = Expression.VariableRef(2)
            private val HOUR_ARG = Expression.VariableRef(3)
            private val MINUTE_ARG = Expression.VariableRef(4)
            private val SECOND_ARG = Expression.VariableRef(5)
            private val OFFSET_ARG = Expression.VariableRef(6)

            override fun produceNext(thisExpansion: ExpansionInfo): Expression.ExpansionOutputExpressionOrContinue {
                val year = thisExpansion.readExactlyOneArgument<Expression.IntValue>(YEAR_ARG).longValue.toInt()
                val month = thisExpansion.readZeroOrOneArgument<Expression.IntValue>(MONTH_ARG)?.longValue?.toInt()
                val day = thisExpansion.readZeroOrOneArgument<Expression.IntValue>(DAY_ARG)?.longValue?.toInt()
                val hour = thisExpansion.readZeroOrOneArgument<Expression.IntValue>(HOUR_ARG)?.longValue?.toInt()
                val minute = thisExpansion.readZeroOrOneArgument<Expression.IntValue>(MINUTE_ARG)?.longValue?.toInt()
                val second = thisExpansion.readZeroOrOneArgument<Expression.DataModelValue>(SECOND_ARG)?.let {
                    when (it) {
                        is Expression.DecimalValue -> it.value
                        is Expression.IntValue -> it.longValue.toBigDecimal()
                        else -> throw IonException("second must be an integer or decimal")
                    }
                }

                val offsetMinutes = thisExpansion.readZeroOrOneArgument<Expression.IntValue>(OFFSET_ARG)?.longValue?.toInt()

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
                    return thisExpansion.session.expressionPool.createTimestampValue(Collections.emptyList(), ts)
                } catch (e: IllegalArgumentException) {
                    throw IonException(e.message)
                }
            }
        },
        _Private_MakeFieldNameAndValue {
            private val FIELD_NAME = Expression.VariableRef(0)
            private val FIELD_VALUE = Expression.VariableRef(1)

            override fun produceNext(thisExpansion: ExpansionInfo): Expression.ExpansionOutputExpressionOrContinue {
                val fieldName = thisExpansion.readExactlyOneArgument<Expression.TextValue>(FIELD_NAME)
                val fieldNameExpression = thisExpansion.session.expressionPool.createFieldName(
                    when (fieldName) {
                        is Expression.SymbolValue -> fieldName.value
                        is Expression.StringValue -> _Private_Utils.newSymbolToken(fieldName.value)
                    }
                )

                val savedTapeIndex = thisExpansion.tapeIndex

                thisExpansion.readExactlyOneArgument<Expression.DataModelValue>(FIELD_VALUE)

                val valueExpansion = thisExpansion.readArgument(FIELD_VALUE)

                // Rewind the tape; this argument will be read again to satisfy the one-expression-at-a-time
                // design of the evaluator.
                valueExpansion.tapeIndex = savedTapeIndex

                return fieldNameExpression.also {
                    thisExpansion.tailCall(valueExpansion)
                    thisExpansion.expansionKind = ExactlyOneValueStream
                }
            }
        },

        _Private_FlattenStruct {
            private val STRUCTS = Expression.VariableRef(0)

            override fun produceNext(thisExpansion: ExpansionInfo): Expression.ExpansionOutputExpressionOrContinue {
                var argumentExpansion: ExpansionInfo? = thisExpansion.additionalState as ExpansionInfo?
                if (argumentExpansion == null) {
                    argumentExpansion = thisExpansion.readArgument(STRUCTS)
                    thisExpansion.additionalState = argumentExpansion
                }

                val currentChildExpansion = thisExpansion.childExpansion

                return when (val next = currentChildExpansion?.produceNext()) {
                    is Expression.DataModelExpression -> next
                    Expression.EndOfExpansion -> thisExpansion.closeDelegateAndContinue()
                    // Only possible if expansionDelegate is null
                    null -> when (val nextSequence = argumentExpansion.produceNext()) {
                        is Expression.StructValue -> {
                            thisExpansion.childExpansion = thisExpansion.session.getExpander(
                                expansionKind = Stream,
                                expressions = argumentExpansion.top().expressions,
                                startInclusive = nextSequence.startInclusive,
                                endExclusive = nextSequence.endExclusive,
                                tapeIndex = thisExpansion.tapeIndex,
                                environment = argumentExpansion.top().environment,
                            )
                            Expression.ContinueExpansion
                        }
                        Expression.EndOfExpansion -> Expression.EndOfExpansion
                        is Expression.DataModelExpression -> throw IonException("invalid argument; make_struct expects structs")
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
            private val SEQUENCES = Expression.VariableRef(0)

            override fun produceNext(thisExpansion: ExpansionInfo): Expression.ExpansionOutputExpressionOrContinue {
                var argumentExpansion: ExpansionInfo? = thisExpansion.additionalState as ExpansionInfo?
                if (argumentExpansion == null) {
                    argumentExpansion = thisExpansion.readArgument(SEQUENCES)
                    thisExpansion.additionalState = argumentExpansion
                }

                val currentChildExpansion = thisExpansion.childExpansion

                return when (val next = currentChildExpansion?.produceNext()) {
                    is Expression.DataModelExpression -> next
                    Expression.EndOfExpansion -> thisExpansion.closeDelegateAndContinue()
                    // Only possible if expansionDelegate is null
                    null -> when (val nextSequence = argumentExpansion.produceNext()) {
                        is Expression.StructValue -> throw IonException("invalid argument; flatten expects sequences")
                        is Expression.DataModelContainer -> {
                            thisExpansion.childExpansion = thisExpansion.session.getExpander(
                                expansionKind = Stream,
                                expressions = argumentExpansion.top().expressions,
                                startInclusive = nextSequence.startInclusive,
                                endExclusive = nextSequence.endExclusive,
                                tapeIndex = thisExpansion.tapeIndex,
                                environment = argumentExpansion.top().environment,
                            )

                            Expression.ContinueExpansion
                        }
                        Expression.EndOfExpansion -> Expression.EndOfExpansion
                        is Expression.DataModelExpression -> throw IonException("invalid argument; flatten expects sequences")
                    }
                }
            }
        },
        Sum {
            private val ARG_A = Expression.VariableRef(0)
            private val ARG_B = Expression.VariableRef(1)

            override fun produceNext(thisExpansion: ExpansionInfo): Expression.ExpansionOutputExpressionOrContinue {
                // TODO(PERF): consider checking whether the value would fit in a long and returning a `LongIntValue`.
                val a = thisExpansion.readExactlyOneArgument<Expression.IntValue>(ARG_A).bigIntegerValue
                val b = thisExpansion.readExactlyOneArgument<Expression.IntValue>(ARG_B).bigIntegerValue
                thisExpansion.expansionKind = Empty
                return thisExpansion.session.expressionPool.createBigIntValue(Collections.emptyList(), a + b)
            }
        },
        Delta {
            private val ARGS = Expression.VariableRef(0)

            override fun produceNext(thisExpansion: ExpansionInfo): Expression.ExpansionOutputExpressionOrContinue {
                // TODO(PERF): Optimize to use LongIntValue when possible
                var delegate = thisExpansion.childExpansion
                val runningTotal = thisExpansion.additionalState as? BigInteger ?: BigInteger.ZERO
                if (delegate == null) {
                    delegate = thisExpansion.readArgument(ARGS)
                    thisExpansion.childExpansion = delegate
                }

                when (val nextExpandedArg = delegate.produceNext()) {
                    is Expression.IntValue -> {
                        val nextDelta = nextExpandedArg.bigIntegerValue
                        val nextOutput = runningTotal + nextDelta
                        thisExpansion.additionalState = nextOutput
                        return thisExpansion.session.expressionPool.createBigIntValue(Collections.emptyList(), nextOutput)
                    }
                    Expression.EndOfExpansion -> {
                        thisExpansion.tapeIndex = delegate.tapeIndex
                        return Expression.EndOfExpansion
                    }
                    else -> throw IonException("delta arguments must be integers")
                }
            }
        },
        Repeat {
            private val COUNT_ARG = Expression.VariableRef(0)
            private val THING_TO_REPEAT = Expression.VariableRef(1)

            override fun produceNext(thisExpansion: ExpansionInfo): Expression.ExpansionOutputExpressionOrContinue {
                var n = thisExpansion.additionalState as Long?
                if (n == null) {
                    n = thisExpansion.readExactlyOneArgument<Expression.IntValue>(COUNT_ARG).longValue
                    if (n < 0) throw IonException("invalid argument; 'n' must be non-negative")
                    thisExpansion.additionalState = n
                }

                if (thisExpansion.childExpansion == null) {
                    if (n > 0) {
                        thisExpansion.childExpansion = thisExpansion.readArgument(THING_TO_REPEAT)
                        thisExpansion.additionalState = n - 1
                    } else {
                        return Expression.EndOfExpansion
                    }
                }

                val repeated = thisExpansion.childExpansion!!
                return when (val maybeNext = repeated.produceNext()) {
                    is Expression.DataModelExpression -> maybeNext
                    Expression.EndOfExpansion -> thisExpansion.closeDelegateAndContinue()
                }
            }
        },
        ;

        /**
         * Produces the next value, [EndOfExpansion], or [ContinueExpansion].
         * Each enum variant must implement this method.
         */
        abstract fun produceNext(thisExpansion: ExpansionInfo): Expression.ExpansionOutputExpressionOrContinue

        /** Helper function for the `if_*` macros */
        inline fun ExpansionInfo.branchIf(condition: (Int) -> Boolean): Expression.ContinueExpansion {
            val argToTest = Expression.VariableRef(0)
            val trueBranch = Expression.VariableRef(1)
            val falseBranch = Expression.VariableRef(2)

            val testArg = readArgument(argToTest)
            var n = 0
            while (n < 2) {
                if (testArg.produceNext() is Expression.EndOfExpansion) break
                n++
            }
            testArg.close()

            val branch = if (condition(n)) trueBranch else falseBranch

            tailCall(readArgument(branch))
            if (branch === falseBranch) {
                // The false branch comes second; skip the tape past its expression(s) if the branch is not taken.
                additionalState = environment.arguments!!.findIndexAfterEndEExpressionFrom(tapeIndex)
            }
            return Expression.ContinueExpansion
        }

        /**
         * Returns an expansion for the given variable.
         */
        fun ExpansionInfo.readArgument(variableRef: Expression.VariableRef): ExpansionInfo {
            if (!environment.useTape && environment.expressions.isNotEmpty()) {
                //val materializedEnvironment = environment as Environment
                val argIndex = environment.argumentIndices[variableRef.signatureIndex]
                if (argIndex < 0) {
                    // Argument was elided.
                    return session.getExpander(Empty, emptyList(), 0, 0, tapeIndex, LazyEnvironment.EMPTY)
                }
                val firstArgExpression = environment.expressions[argIndex]
                if (environment.parentEnvironment!!.expressions.isEmpty()) {
                    environment.parentEnvironment!!.startVariableEvaluation()
                }
                return session.getExpander(
                    expansionKind = Variable,
                    expressions = environment.expressions,
                    startInclusive = if (firstArgExpression is Expression.ExpressionGroup) firstArgExpression.startInclusive else argIndex,
                    endExclusive = if (firstArgExpression is Expression.HasStartAndEnd) firstArgExpression.endExclusive else argIndex + 1,
                    tapeIndex = tapeIndex,
                    environment = environment.parentEnvironment!!
                )
            } else {
                    val tapeStartIndex = environment.firstArgumentStartIndex
                    val variableIndex = variableRef.signatureIndex
                    // TODO iterate through the tape to find the index of the requested variable
                    // TODO we probably should cache the arguments found so far to avoid iterating from the start for each arg
                    var argIndex = 0
                    var relativeDepth = 0
                    var i = tapeStartIndex
                    while (i < environment.arguments!!.size()) {
                        when (environment.arguments!!.typeAt(i)) {
                            ExpressionType.FIELD_NAME -> {
                                // Skip the field name; advance to the following expression.
                            }
                            ExpressionType.ANNOTATION -> {
                                if (relativeDepth == 0) {
                                    if (argIndex == variableIndex) {
                                        break;
                                    }
                                    argIndex++
                                }
                            }
                            ExpressionType.E_EXPRESSION -> {
                                if (relativeDepth == 0) {
                                    if (argIndex == variableIndex) {
                                        break;
                                    }
                                    argIndex++
                                }
                                relativeDepth++
                            }
                            ExpressionType.EXPRESSION_GROUP -> {
                                if (relativeDepth == 0) {
                                    if (argIndex == variableIndex) {
                                        break;
                                    }
                                    argIndex++
                                }
                                relativeDepth++
                            }
                            ExpressionType.EXPRESSION_GROUP_END -> {
                                if (--relativeDepth < 0) {
                                    i++
                                    break;
                                }
                            }
                            ExpressionType.DATA_MODEL_SCALAR -> {
                                if (relativeDepth == 0) {
                                    if (argIndex == variableIndex) {
                                        break;
                                    }
                                    argIndex++
                                }
                            }
                            ExpressionType.DATA_MODEL_CONTAINER -> {
                                if (relativeDepth == 0) {
                                    if (argIndex == variableIndex) {
                                        break;
                                    }
                                    argIndex++
                                }
                                relativeDepth++
                            }
                            ExpressionType.DATA_MODEL_CONTAINER_END -> {
                                if (--relativeDepth < 0) {
                                    i++
                                    break;
                                }
                            }
                            ExpressionType.E_EXPRESSION_END -> {
                                if (--relativeDepth < 0) {
                                    i++
                                    break;
                                }
                            }
                        }
                        i++
                    }
                    if (argIndex < variableIndex) {
                        // Argument was elided.
                        return session.getExpander(Empty, emptyList(), 0, 0, i, LazyEnvironment.EMPTY)
                    }
                    environment.startVariableEvaluation()
                    return session.getExpander(
                        expansionKind = Variable,
                        expressions = Collections.emptyList(), // The expressions will come from the tape, not the macro body
                        startInclusive = 0,
                        endExclusive = 0,
                        tapeIndex = i,
                        environment = environment.createLazyChild(environment.arguments!!, i, true)
                    )
                }
        }

        /**
         * Performs the given [action] for each value produced by the expansion of [variableRef].
         */
        inline fun ExpansionInfo.forEach(variableRef: Expression.VariableRef, action: (Expression.DataModelExpression) -> Unit) {
            val variableExpansion = readArgument(variableRef)
            while (true) {
                when (val next = variableExpansion.produceNext()) {
                    Expression.EndOfExpansion -> return
                    is Expression.DataModelExpression -> action(next)
                }
            }
        }

        /**
         * Performs the given [transform] on each value produced by the expansion of [variableRef], returning a list
         * of the results.
         */
        inline fun <T> ExpansionInfo.map(variableRef: Expression.VariableRef, transform: (Expression.DataModelExpression) -> T): List<T> {
            val variableExpansion = readArgument(variableRef)
            val result = mutableListOf<T>()
            while (true) {
                when (val next = variableExpansion.produceNext()) {
                    Expression.EndOfExpansion -> return result
                    is Expression.DataModelExpression -> result.add(transform(next))
                }
            }
        }

        /**
         * Reads and returns zero or one values from the expansion of the given [variableRef].
         * Throws an [IonException] if more than one value is present in the variable expansion.
         * Throws an [IonException] if the value is not the expected type [T].
         */
        inline fun <reified T : Expression.DataModelValue> ExpansionInfo.readZeroOrOneArgument(variableRef: Expression.VariableRef): T? {
            val argExpansion = readArgument(variableRef)
            var argValue: T? = null
            while (true) {
                when (val it = argExpansion.produceNext()) {
                    is T -> if (argValue == null) {
                        argValue = it
                    } else {
                        throw IonException("invalid argument; too many values")
                    }
                    is Expression.DataModelValue -> throw IonException("invalid argument; found ${it.type}")
                    Expression.EndOfExpansion -> break
                    is Expression.FieldName -> unreachable("Unreachable without stepping into a container")
                }
            }
            tapeIndex = argExpansion.tapeIndex
            argExpansion.close()
            return argValue
        }

        /**
         * Reads and returns exactly one value from the expansion of the given [variableRef].
         * Throws an [IonException] if the expansion of [variableRef] does not produce exactly one value.
         * Throws an [IonException] if the value is not the expected type [T].
         */
        inline fun <reified T : Expression.DataModelValue> ExpansionInfo.readExactlyOneArgument(variableRef: Expression.VariableRef): T {
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
     *       Alternately, consider ExpansionOperator to reflect the fact that these are
     *       like operators in an expression tree.
     */
    internal class ExpansionInfo(@JvmField val session: Session) {

        /** The [ExpansionKind]. */
        @JvmField var expansionKind: ExpansionKind = ExpansionKind.Uninitialized
        /**
         * The evaluation [Environment]—i.e. variable bindings.
         */
        @JvmField var environment: LazyEnvironment = LazyEnvironment.EMPTY
        /**
         * The [Expression]s being expanded. This MUST be the original list, not a sublist because
         * (a) we don't want to be allocating new sublists all the time, and (b) the
         * start and end indices of the expressions may be incorrect if a sublist is taken.
         */
        @JvmField var expressions: List<Expression> = emptyList()
        //@JvmField var expressionTape: ExpressionTape? = null
        /** End of [expressions] that are applicable for this [ExpansionInfo] */
        @JvmField var endExclusive: Int = 0
        /** Current position within [expressions] of this expansion */
        @JvmField var i: Int = 0
        /** Current position within [expressions] of this expansion */
        @JvmField var tapeIndex: Int = 0

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
        fun closeDelegateAndContinue(): Expression.ContinueExpansion {
            if (childExpansion != null) {
                tapeIndex = childExpansion!!.tapeIndex
                childExpansion!!.close()
                childExpansion = null
            }
            return Expression.ContinueExpansion
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
            environment = LazyEnvironment.EMPTY
            additionalState?.let { if (it is ExpansionInfo) it.close() }
            additionalState = null
            childExpansion?.close()
            childExpansion = null
        }

        /**
         * Replaces the state of `this` [ExpansionInfo] with the state of [other]—effectively a tail-call optimization.
         * After transferring the state, `other` is returned to the expansion pool.
         */
        fun tailCall(other: ExpansionInfo) {
            this.expansionKind = other.expansionKind
            this.i = other.i
            this.tapeIndex = other.tapeIndex
            this.endExclusive = other.endExclusive
            this.childExpansion = other.childExpansion
            this.additionalState = other.additionalState
            this.environment = other.environment
            if (!this.environment.useTape) {
                this.expressions = environment.expressions
            }
            // Close `other`
            other.childExpansion = null
            other.close()
        }

        /**
         * Produces the next value from this expansion.
         */
        fun produceNext(): Expression.ExpansionOutputExpression {
            while (true) {
                val next = expansionKind.produceNext(this)
                if (next is Expression.ContinueExpansion) continue
                // This the only place where we count the expansion steps.
                // It is theoretically possible to have macro expansions that are millions of levels deep because this
                // only counts macro invocations at the end of their expansion, but this will still work to catch things
                // like a  billion laughs attack because it does place a limit on the number of _values_ produced.
                // This counts every value _at every level_, so most values will be counted multiple times. If possible
                // without impacting performance, count values only once in order to have more predictable behavior.
                session.incrementStepCounter()
                return next as Expression.ExpansionOutputExpression
            }
        }

        // TODO
        override fun toString() = """
        |ExpansionInfo(
        |    expansionKind: $expansionKind,
        |    environment: ${environment.toString().lines().joinToString("\n|        ")},
        |    i: $i,
        |    child: ${childExpansion?.expansionKind}
        |    additionalState: $additionalState,
        |)
        """.trimMargin()
    }

    private val session = Session(expansionLimit = 1_000_000)
    private val containerStack = _Private_RecyclingStack(8) { ContainerInfo() }
    private var currentExpr: Expression.DataModelExpression? = null

    /**
     * Returns the e-expression argument expressions that this MacroEvaluator would evaluate.
     */
    fun getArguments(): List<Expression> {
        // TODO avoid eager materialization; change call site to iterate
        materializeExpressions(containerStack.iterator().next().expansion.environment.arguments!!)
        return session.expressions.toList()
    }


    // TODO modify the evaluator to operate on the "tape" instead of the materialized expressions. Remove
    //  the materialization
    private fun materializeExpressions(expressionTape: ExpressionTape) {
        expressionTape.rewind()
        var expression: EExpressionBodyExpression?
        while ((expressionTape.dequeue(session.expressions).also { expression = it }) != null) {
            session.expressions.add(expression!!)
        }
    }

    /**
     * Initialize the macro evaluator with an E-Expression.
     */
    fun initExpansion(encodingExpressions: ExpressionTape) {
        session.reset()
        val ci = containerStack.push { _ -> }
        ci.type = ContainerInfo.Type.TopLevel

        ci.expansion = session.getExpander(ExpansionKind.Stream, Collections.emptyList(), 0, 0, 0, LazyEnvironment.create(encodingExpressions, 0))
    }

    /**
     * Evaluate the macro expansion until the next [DataModelExpression] can be returned.
     * Returns null if at the end of a container or at the end of the expansion.
     */
    fun expandNext(): Expression.DataModelExpression? {
        currentExpr = null
        val currentContainer = containerStack.peek()
        when (val nextExpansionOutput = currentContainer.produceNext()) {
            is Expression.DataModelExpression -> currentExpr = nextExpansionOutput
            Expression.EndOfExpansion -> {
                if (currentContainer.type == ContainerInfo.Type.TopLevel) {
                    currentContainer.close()
                    containerStack.pop()
                }
            }
        }
        return currentExpr
    }

    private fun seekTapePastEndOfCurrentContainer(currentContainer: ContainerInfo) {
        if (currentContainer.expansion.tapeIndex >= 0) { // TODO why will tapeIndex sometimes be < 0? TRY REMOVING. The fix in findIndex... should prevent this
            val parentContainer = containerStack.peek()
            parentContainer.expansion.tapeIndex = currentContainer.expansion.environment.arguments!!.findIndexAfterEndContainerFrom(currentContainer.expansion.tapeIndex)
            var childExpansion = parentContainer.expansion.childExpansion
            while (childExpansion != null) {
                childExpansion.tapeIndex = parentContainer.expansion.tapeIndex
                childExpansion = childExpansion.childExpansion
            }
        }
    }

    /**
     * Steps out of the current [DataModelContainer].
     */
    fun stepOut() {
        // TODO: We should be able to step out of a "TopLevel" container and/or we need some way to close the evaluation early.
        if (containerStack.size() <= 1) throw IonException("Nothing to step out of.")
        val popped = containerStack.pop()
        seekTapePastEndOfCurrentContainer(popped)
        popped.close()
    }

    /**
     * Steps in to the current [DataModelContainer].
     * Throws [IonException] if not positioned on a container.
     */
    fun stepIn() {
        val expression = requireNotNull(currentExpr) { "Not positioned on a value" }
        if (expression is Expression.DataModelContainer) {
            val currentContainer = containerStack.peek()
            val topExpansion = currentContainer.expansion.top()
            val ci = containerStack.push { _ -> }
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
                tapeIndex = currentContainer.expansion.tapeIndex,
                environment = topExpansion.environment,
            )
            currentExpr = null
        } else {
            throw IonException("Not positioned on a container.")
        }
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
