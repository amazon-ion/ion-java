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

        private val tapePool: ArrayList<ExpressionTape> = ArrayList(64)
        private var tapePoolIndex = 0

        val environment: LazyEnvironment = LazyEnvironment.create()
        var sideEffectExpander: ExpansionInfo? = null
        var currentExpander: ExpansionInfo? = null
        var currentFieldName: SymbolToken? = null
        var currentAnnotations: List<SymbolToken>? = null

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
        fun getExpander(expansionKind: Byte, environmentContext: LazyEnvironment.NestedContext): ExpansionInfo {
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

        fun getTape(core: ExpressionTape.Core): ExpressionTape {
            val tape: ExpressionTape
            if (tapePoolIndex >= tapePool.size) {
                tape = ExpressionTape(core)
                tapePool.add(tape)
            } else {
                tape = tapePool[tapePoolIndex]
                tape.reset(core)
            }
            tapePoolIndex++
            return tape
        }

        fun finishVariable() {
            val variable = currentExpander!!
            currentExpander = variable.parentExpansion
            environment.finishChildEnvironment()
            variable.close()
        }

        fun produceNext(): Byte {
            while (true) {
                currentExpander!!.environmentContext.tape?.next() // TODO if Empty doesn't exist and CONTINUE_EXPANSION isn't used with it, then this could be tape!!
                val next = ExpansionKind.produceNext(currentExpander!!.expansionKind, currentExpander!!)
                if (next == ExpressionType.CONTINUE_EXPANSION_ORDINAL) continue
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

        fun reset(fieldName: SymbolToken?, arguments: ExpressionTape) {
            numExpandedExpressions = 0
            expanderPoolIndex = 0
            tapePoolIndex = 0
            environment.reset(arguments)
            sideEffectExpander = getExpander(ExpansionKind.EMPTY, this.environment.sideEffectContext)
            sideEffectExpander!!.keepAlive = true
            currentExpander = null
            currentFieldName = fieldName
            currentAnnotations = null
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
    internal object ExpansionKind {
        const val UNINITIALIZED: Byte = 0
        const val EMPTY: Byte = 1
        const val STREAM: Byte = 2
        const val VARIABLE: Byte = 3
        const val TEMPLATE_BODY: Byte = 4
        const val EXPR_GROUP: Byte = 5
        const val EXACTLY_ONE_VALUE_STREAM: Byte = 6
        const val IF_NONE: Byte = 7
        const val IF_SOME: Byte = 8
        const val IF_SINGLE: Byte = 9
        const val IF_MULTI: Byte = 10
        const val ANNOTATE: Byte = 11
        const val MAKE_STRING: Byte = 12
        const val MAKE_SYMBOL: Byte = 13
        const val MAKE_BLOB: Byte = 14
        const val MAKE_DECIMAL: Byte = 15
        const val MAKE_TIMESTAMP: Byte = 16
        const val PRIVATE_MAKE_FIELD_NAME_AND_VALUE: Byte = 17
        const val PRIVATE_FLATTEN_STRUCT: Byte = 18
        const val FLATTEN: Byte = 19
        const val SUM: Byte = 20
        const val DELTA: Byte = 21
        const val REPEAT: Byte = 22

        /**
         * Gets the [ExpansionKind] for the given [macro].
         */
        @JvmStatic
        fun forMacro(macro: Macro): Byte {
            return if (macro.body != null) {
                TEMPLATE_BODY
            } else when (macro as SystemMacro) {
                SystemMacro.IfNone -> IF_NONE
                SystemMacro.IfSome -> IF_SOME
                SystemMacro.IfSingle -> IF_SINGLE
                SystemMacro.IfMulti -> IF_MULTI
                SystemMacro.Annotate -> ANNOTATE
                SystemMacro.MakeString -> MAKE_STRING
                SystemMacro.MakeSymbol -> MAKE_SYMBOL
                SystemMacro.MakeDecimal -> MAKE_DECIMAL
                SystemMacro.MakeTimestamp -> MAKE_TIMESTAMP
                SystemMacro.MakeBlob -> MAKE_BLOB
                SystemMacro.Repeat -> REPEAT
                SystemMacro.Sum -> SUM
                SystemMacro.Delta -> DELTA
                SystemMacro.Flatten -> FLATTEN
                SystemMacro._Private_FlattenStruct -> PRIVATE_FLATTEN_STRUCT
                SystemMacro._Private_MakeFieldNameAndValue -> PRIVATE_MAKE_FIELD_NAME_AND_VALUE
                else -> TODO("Not implemented yet: ${macro.name}")
            }
        }

        fun produceNext(kind: Byte, thisExpansion: ExpansionInfo): Byte {
            return when (kind) {
                UNINITIALIZED -> throw IllegalStateException("ExpansionInfo not initialized.")
                EMPTY -> ExpressionType.END_OF_EXPANSION_ORDINAL
                STREAM -> handleStream(thisExpansion)
                VARIABLE -> handleVariable(thisExpansion)
                TEMPLATE_BODY -> handleTemplateBody(thisExpansion)
                EXPR_GROUP -> handleStream(thisExpansion)
                EXACTLY_ONE_VALUE_STREAM -> handleExactlyOneValueStream(thisExpansion)
                IF_NONE -> handleIfNone(thisExpansion)
                IF_SOME -> handleIfSome(thisExpansion)
                IF_SINGLE -> handleIfSingle(thisExpansion)
                IF_MULTI -> handleIfMulti(thisExpansion)
                ANNOTATE -> handleAnnotate(thisExpansion)
                MAKE_STRING -> handleMakeString(thisExpansion)
                MAKE_SYMBOL -> handleMakeSymbol(thisExpansion)
                MAKE_BLOB -> handleMakeBlob(thisExpansion)
                MAKE_DECIMAL -> handleMakeDecimal(thisExpansion)
                MAKE_TIMESTAMP -> handleMakeTimestamp(thisExpansion)
                PRIVATE_MAKE_FIELD_NAME_AND_VALUE -> handlePrivateMakeFieldNameAndValue(thisExpansion)
                PRIVATE_FLATTEN_STRUCT -> handlePrivateFlattenStruct(thisExpansion)
                FLATTEN -> handleFlatten(thisExpansion)
                SUM -> handleSum(thisExpansion)
                DELTA -> handleDelta(thisExpansion)
                REPEAT -> handleRepeat(thisExpansion)
                else -> throw IllegalStateException("Unknown expansion kind: $kind")
            }
        }

        private fun handleVariable(thisExpansion: ExpansionInfo): Byte {
            if (thisExpansion.reachedEndOfExpression) {
                thisExpansion.session.environment.finishChildEnvironment()
                return ExpressionType.END_OF_EXPANSION_ORDINAL
            }
            val expression = handleStream(thisExpansion)
            if (thisExpansion.childExpansion == null) {
                if (expression == ExpressionType.DATA_MODEL_SCALAR_ORDINAL) {
                    thisExpansion.reachedEndOfExpression = true
                }
            }
            return expression
        }

        private fun handleTemplateBody(thisExpansion: ExpansionInfo): Byte {
            val expression = handleStream(thisExpansion)
            if (expression == ExpressionType.END_OF_EXPANSION_ORDINAL) {
                thisExpansion.session.environment.currentContext.tape!!.seekPastFinalArgument()
                thisExpansion.session.environment.finishChildEnvironment()
            }
            return expression
        }

        private fun handleExactlyOneValueStream(thisExpansion: ExpansionInfo): Byte {
            if (thisExpansion.additionalState != 1) {
                val firstValue = handleStream(thisExpansion)
                return when {
                    ExpressionType.isDataModelExpression(firstValue) -> {
                        thisExpansion.additionalState = 1
                        firstValue
                    }

                    firstValue == ExpressionType.CONTINUE_EXPANSION_ORDINAL -> ExpressionType.CONTINUE_EXPANSION_ORDINAL
                    firstValue == ExpressionType.END_OF_EXPANSION_ORDINAL -> throw IonException("Expected one value, found 0")
                    else -> unreachable()
                }
            } else {
                val secondValue = handleStream(thisExpansion)
                return when {
                    ExpressionType.isDataModelExpression(secondValue) -> throw IonException("Expected one value, found multiple")
                    secondValue == ExpressionType.CONTINUE_EXPANSION_ORDINAL -> ExpressionType.CONTINUE_EXPANSION_ORDINAL
                    secondValue == ExpressionType.END_OF_EXPANSION_ORDINAL -> secondValue
                    else -> unreachable()
                }
            }
        }

        private fun handleIfNone(thisExpansion: ExpansionInfo): Byte = handleBranchIf(thisExpansion) { it == 0 }
        private fun handleIfSome(thisExpansion: ExpansionInfo): Byte = handleBranchIf(thisExpansion) { it > 0 }
        private fun handleIfSingle(thisExpansion: ExpansionInfo): Byte = handleBranchIf(thisExpansion) { it == 1 }
        private fun handleIfMulti(thisExpansion: ExpansionInfo): Byte = handleBranchIf(thisExpansion) { it > 1 }

        private fun handleAnnotate(thisExpansion: ExpansionInfo): Byte {
            val annotations = thisExpansion.map(0) {
                when (it) {
                    ExpressionType.DATA_MODEL_SCALAR_ORDINAL -> asSymbol(thisExpansion)
                    else -> unreachable("Unreachable without stepping in to a container")
                }
            }
            thisExpansion.session.currentAnnotations = annotations
            val valueToAnnotateExpansion = thisExpansion.readArgument(1)

            val type = valueToAnnotateExpansion.session.produceNext()
            if (type == ExpressionType.END_OF_EXPANSION_ORDINAL) {
                throw IonException("Can only annotate exactly one value")
            }
            thisExpansion.tailCall(valueToAnnotateExpansion)
            return type
        }

        private fun handleMakeString(thisExpansion: ExpansionInfo): Byte {
            val sb = StringBuilder()
            thisExpansion.forEach(0) {
                when {
                    IonType.isText(it.type()) -> sb.append(it.textValue())
                    else -> throw IonException("Invalid argument type for 'make_string': ${it.type()}")
                }
            }
            thisExpansion.environmentContext.tape!!.advanceToAfterEndEExpression()
            thisExpansion.expansionKind = EMPTY
            thisExpansion.produceValueSideEffect(IonType.STRING, sb.toString())
            return ExpressionType.DATA_MODEL_SCALAR_ORDINAL
        }

        private fun handleMakeSymbol(thisExpansion: ExpansionInfo): Byte {
            val sb = StringBuilder()
            thisExpansion.forEach(0) {
                when {
                    IonType.isText(it.type()) -> sb.append(it.textValue())
                    else -> throw IonException("Invalid argument type for 'make_symbol': ${it.type()}")
                }
            }
            thisExpansion.environmentContext.tape!!.advanceToAfterEndEExpression()
            thisExpansion.expansionKind = EMPTY
            thisExpansion.produceValueSideEffect(IonType.SYMBOL, sb.toString())
            return ExpressionType.DATA_MODEL_SCALAR_ORDINAL
        }

        private fun handleMakeBlob(thisExpansion: ExpansionInfo): Byte {
            val baos = ByteArrayOutputStream()
            thisExpansion.forEach(0) {
                baos.write(it.lobValue())
            }
            thisExpansion.expansionKind = EMPTY
            thisExpansion.produceValueSideEffect(IonType.BLOB, baos.toByteArray())
            return ExpressionType.DATA_MODEL_SCALAR_ORDINAL
        }

        private fun handleMakeDecimal(thisExpansion: ExpansionInfo): Byte {
            val coefficient = thisExpansion.readExactlyOneArgument(0, ::asBigInteger)
            val exponent = thisExpansion.readExactlyOneArgument(1, ::asBigInteger)
            thisExpansion.expansionKind = EMPTY
            thisExpansion.produceValueSideEffect(
                IonType.DECIMAL,
                BigDecimal(coefficient, -1 * exponent.intValueExact())
            )
            return ExpressionType.DATA_MODEL_SCALAR_ORDINAL
        }

        private fun handleMakeTimestamp(thisExpansion: ExpansionInfo): Byte {
            val year = thisExpansion.readExactlyOneArgument(0, ::asLong).toInt()
            val month = thisExpansion.readZeroOrOneArgument(1, ::asLong)?.toInt()
            val day = thisExpansion.readZeroOrOneArgument(2, ::asLong)?.toInt()
            val hour = thisExpansion.readZeroOrOneArgument(3, ::asLong)?.toInt()
            val minute = thisExpansion.readZeroOrOneArgument(4, ::asLong)?.toInt()
            val second = thisExpansion.readZeroOrOneArgument(5, ::asBigDecimal)
            val offsetMinutes = thisExpansion.readZeroOrOneArgument(6, ::asLong)?.toInt()

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
                thisExpansion.expansionKind = EMPTY
                thisExpansion.produceValueSideEffect(IonType.TIMESTAMP, ts)
                return ExpressionType.DATA_MODEL_SCALAR_ORDINAL
            } catch (e: IllegalArgumentException) {
                throw IonException(e.message)
            }
        }

        private fun handleSum(thisExpansion: ExpansionInfo): Byte {
            val a = thisExpansion.readExactlyOneArgument(0, ::asBigInteger)
            val b = thisExpansion.readExactlyOneArgument(1, ::asBigInteger)
            thisExpansion.expansionKind = EMPTY
            thisExpansion.produceValueSideEffect(IonType.INT, a + b)
            return ExpressionType.DATA_MODEL_SCALAR_ORDINAL
        }

        private fun handleDelta(thisExpansion: ExpansionInfo): Byte {
            var delegate = thisExpansion.childExpansion
            val runningTotal = thisExpansion.additionalState as? BigInteger ?: BigInteger.ZERO
            if (delegate == null) {
                delegate = thisExpansion.readArgument(0)
                thisExpansion.childExpansion = delegate
                delegate.parentExpansion = thisExpansion
            }
            thisExpansion.session.currentExpander = delegate
            val nextExpandedArg = delegate.session.produceNext()
            when {
                ExpressionType.isDataModelValue(nextExpandedArg) -> {
                    val nextDelta = asBigInteger(delegate)
                    val nextOutput = runningTotal + nextDelta
                    delegate.childExpansion!!.produceValueSideEffect(IonType.INT, nextOutput)
                    thisExpansion.session.currentExpander!!.parentExpansion = thisExpansion
                    thisExpansion.additionalState = nextOutput
                    return nextExpandedArg
                }

                nextExpandedArg == ExpressionType.END_OF_EXPANSION_ORDINAL -> {
                    delegate.close()
                    thisExpansion.childExpansion = null
                    thisExpansion.session.currentExpander = thisExpansion
                    return ExpressionType.END_OF_EXPANSION_ORDINAL
                }

                else -> throw IonException("delta arguments must be integers")
            }
        }

        private fun handleRepeat(thisExpansion: ExpansionInfo): Byte {
            var n = thisExpansion.additionalState as Long?
            if (n == null) {
                n = thisExpansion.readExactlyOneArgument(0, ::asLong)
                if (n < 0) throw IonException("invalid argument; 'n' must be non-negative")
                thisExpansion.additionalState = n
            }

            if (thisExpansion.childExpansion == null) {
                if (n > 0) {
                    thisExpansion.childExpansion = thisExpansion.readArgument(1)
                    thisExpansion.childExpansion!!.parentExpansion = thisExpansion
                    thisExpansion.additionalState = n - 1
                } else {
                    return ExpressionType.END_OF_EXPANSION_ORDINAL
                }
            }

            val repeated = thisExpansion.childExpansion!!
            val maybeNext = repeated.session.produceNext()
            thisExpansion.childExpansion = null
            return when {
                ExpressionType.isDataModelExpression(maybeNext) -> maybeNext
                maybeNext == ExpressionType.END_OF_EXPANSION_ORDINAL -> ExpressionType.END_OF_EXPANSION_ORDINAL
                else -> unreachable()
            }
        }

        private fun handlePrivateMakeFieldNameAndValue(thisExpansion: ExpansionInfo): Byte {
            thisExpansion.session.currentFieldName = thisExpansion.readExactlyOneArgument(0, ::asSymbol)
            val valueExpansion = thisExpansion.readArgument(1)
            thisExpansion.tailCall(valueExpansion)
            thisExpansion.expansionKind = EXACTLY_ONE_VALUE_STREAM
            return ExpressionType.CONTINUE_EXPANSION_ORDINAL
        }

        private fun handlePrivateFlattenStruct(thisExpansion: ExpansionInfo): Byte {
            var argumentExpansion: ExpansionInfo? = thisExpansion.additionalState as ExpansionInfo?
            if (argumentExpansion == null) {
                argumentExpansion = thisExpansion.readArgument(0)
                thisExpansion.additionalState = argumentExpansion
            }

            val currentChildExpansion = thisExpansion.childExpansion
            val next = currentChildExpansion?.session?.produceNext()
            if (next == null) {
                thisExpansion.session.currentExpander = argumentExpansion
                val nextSequence = argumentExpansion.session.produceNext()
                thisExpansion.session.currentExpander = thisExpansion
                return when {
                    nextSequence == ExpressionType.DATA_MODEL_CONTAINER_ORDINAL -> {
                        thisExpansion.childExpansion = thisExpansion.session.getExpander(
                            expansionKind = STREAM,
                            environmentContext = argumentExpansion.top().environmentContext,
                        )
                        thisExpansion.childExpansion!!.parentExpansion = thisExpansion
                        ExpressionType.CONTINUE_EXPANSION_ORDINAL
                    }

                    nextSequence == ExpressionType.END_OF_EXPANSION_ORDINAL -> ExpressionType.END_OF_EXPANSION_ORDINAL
                    ExpressionType.isDataModelExpression(nextSequence) -> throw IonException("invalid argument; make_struct expects structs")
                    else -> unreachable()
                }
            }
            thisExpansion.session.currentExpander = thisExpansion
            return when {
                ExpressionType.isDataModelExpression(next) -> next
                next == ExpressionType.END_OF_EXPANSION_ORDINAL -> ExpressionType.END_OF_EXPANSION_ORDINAL
                else -> unreachable()
            }
        }

        private fun handleFlatten(thisExpansion: ExpansionInfo): Byte {
            var argumentExpansion: ExpansionInfo? = thisExpansion.additionalState as ExpansionInfo?
            if (argumentExpansion == null) {
                argumentExpansion = thisExpansion.readArgument(0)
                thisExpansion.additionalState = argumentExpansion
            }

            val currentChildExpansion = thisExpansion.childExpansion
            val next = currentChildExpansion?.session?.produceNext()
            if (next == null) {
                thisExpansion.session.currentExpander = argumentExpansion
                val nextSequence = argumentExpansion.session.produceNext()
                thisExpansion.session.currentExpander = thisExpansion
                return when {
                    nextSequence == ExpressionType.DATA_MODEL_CONTAINER_ORDINAL -> {
                        thisExpansion.childExpansion = thisExpansion.session.getExpander(
                            expansionKind = STREAM,
                            environmentContext = argumentExpansion.top().environmentContext,
                        )
                        thisExpansion.childExpansion!!.parentExpansion = thisExpansion
                        ExpressionType.CONTINUE_EXPANSION_ORDINAL
                    }

                    nextSequence == ExpressionType.END_OF_EXPANSION_ORDINAL -> ExpressionType.END_OF_EXPANSION_ORDINAL
                    ExpressionType.isDataModelExpression(nextSequence) -> throw IonException("invalid argument; flatten expects sequences")
                    else -> unreachable()
                }
            }
            thisExpansion.session.currentExpander = thisExpansion
            return when {
                ExpressionType.isDataModelExpression(next) -> next
                next == ExpressionType.END_OF_EXPANSION_ORDINAL -> ExpressionType.END_OF_EXPANSION_ORDINAL
                else -> unreachable()
            }
        }

        private fun handleBranchIf(thisExpansion: ExpansionInfo, condition: (Int) -> Boolean): Byte {
            val testArg = thisExpansion.readArgument(0)
            var n = 0
            while (n < 2) {
                if (testArg.session.produceNext() == ExpressionType.END_OF_EXPANSION_ORDINAL) break
                n++
            }
            var isVariable = testArg.expansionKind == VARIABLE
            testArg.close()

            val branch = if (condition(n)) 1 else 2
            if (branch == 1 && isVariable) {
                thisExpansion.session.environment.finishChildEnvironment()
            }
            thisExpansion.environmentContext.arguments!!.setNextAfterEndOfEExpression()
            thisExpansion.tailCall(thisExpansion.readArgument(branch))
            return ExpressionType.CONTINUE_EXPANSION_ORDINAL
        }

        private fun handleStream(thisExpansion: ExpansionInfo): Byte {
            val expressionTape = thisExpansion.environmentContext.tape!!
            if (expressionTape.isExhausted) {
                return ExpressionType.END_OF_EXPANSION_ORDINAL
            }
            val nextType = expressionTape.type()
            if (ExpressionType.isEnd(nextType)) {
                if (nextType == ExpressionType.EXPRESSION_GROUP_END_ORDINAL || nextType == ExpressionType.E_EXPRESSION_END_ORDINAL) {
                    // Expressions and expression groups do not rely on stepIn/stepOut for navigation, so the tape must be advanced
                    // here.
                    expressionTape.prepareNext()
                }
                if (nextType != ExpressionType.E_EXPRESSION_END_ORDINAL) { // TODO why the special case?
                    return ExpressionType.END_OF_EXPANSION_ORDINAL
                }
                // thisExpansion.session.environment.finishChildEnvironment() // TODO is this missing? Causes things to break.
                return ExpressionType.CONTINUE_EXPANSION_ORDINAL // TODO should end of expansion be conveyed in any case here?
            }

            return when (nextType) {
                ExpressionType.FIELD_NAME_ORDINAL -> {
                    thisExpansion.session.currentFieldName = expressionTape.context() as SymbolToken
                    expressionTape.prepareNext()
                    ExpressionType.CONTINUE_EXPANSION_ORDINAL
                }

                ExpressionType.ANNOTATION_ORDINAL -> {
                    thisExpansion.session.currentAnnotations = expressionTape.annotations()
                    expressionTape.prepareNext()
                    ExpressionType.CONTINUE_EXPANSION_ORDINAL
                }

                ExpressionType.E_EXPRESSION_ORDINAL -> {
                    val macro = expressionTape.context() as Macro
                    val macroBodyTape = thisExpansion.session.getTape(macro.bodyTape)
                    macroBodyTape.cacheExpressionPointers(expressionTape, expressionTape.currentIndex())
                    // TODO do these three in one step? OR, change setNextAfterEndOfEExpression to skip the eexp i currently points at and then remove the next two lines
                    expressionTape.prepareNext()
                    expressionTape.next()
                    expressionTape.setNextAfterEndOfEExpression() // TODO expressionStarts[] can be changed to include the index of the e_expression_end
                    val newEnvironment =
                        thisExpansion.session.environment.startChildEnvironment(macroBodyTape, expressionTape)
                    val expansionKind = forMacro(macro)
                    thisExpansion.childExpansion = thisExpansion.session.getExpander(
                        expansionKind = expansionKind,
                        environmentContext = newEnvironment,
                    )
                    thisExpansion.childExpansion!!.parentExpansion = thisExpansion
                    ExpressionType.CONTINUE_EXPANSION_ORDINAL
                }

                ExpressionType.E_EXPRESSION_END_ORDINAL -> unreachable()
                ExpressionType.EXPRESSION_GROUP_ORDINAL -> {
                    expressionTape.prepareNext()
                    thisExpansion.childExpansion = thisExpansion.session.getExpander(
                        expansionKind = ExpansionKind.EXPR_GROUP,
                        environmentContext = thisExpansion.environmentContext,
                    )
                    thisExpansion.childExpansion!!.parentExpansion = thisExpansion
                    ExpressionType.CONTINUE_EXPANSION_ORDINAL
                }

                ExpressionType.EXPRESSION_GROUP_END_ORDINAL -> unreachable()
                ExpressionType.DATA_MODEL_SCALAR_ORDINAL -> {
                    expressionTape.prepareNext()
                    ExpressionType.DATA_MODEL_SCALAR_ORDINAL
                }

                ExpressionType.DATA_MODEL_CONTAINER_ORDINAL -> {
                    expressionTape.prepareNext()
                    ExpressionType.DATA_MODEL_CONTAINER_ORDINAL
                }

                ExpressionType.VARIABLE_ORDINAL -> {
                    expressionTape.prepareNext()
                    // TODO determine if this is a pass-through variable that has already been consumed. If so, skip it.
                    thisExpansion.childExpansion = thisExpansion.readArgument(expressionTape.context() as Int)
                    thisExpansion.childExpansion!!.parentExpansion = thisExpansion
                    ExpressionType.CONTINUE_EXPANSION_ORDINAL
                }

                ExpressionType.DATA_MODEL_CONTAINER_END_ORDINAL -> unreachable()
                ExpressionType.END_OF_EXPANSION_ORDINAL -> unreachable()
                ExpressionType.CONTINUE_EXPANSION_ORDINAL -> unreachable()
                else -> unreachable()
            }
        }

        /**
         * Returns an expansion for the given variable.
         */
        fun ExpansionInfo.readArgument(variableRef: Int): ExpansionInfo {
            val argumentTape = environmentContext.tape!!.seekToArgument(variableRef)
                ?: return session.getExpander(
                    ExpansionKind.EMPTY,
                    LazyEnvironment.EMPTY.currentContext
                ) // Argument was elided.
            return session.getExpander(
                expansionKind = ExpansionKind.VARIABLE,
                environmentContext = session.environment.startChildEnvironment(argumentTape, argumentTape)
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

        /**
         * Performs the given [action] for each value produced by the expansion of [variableRef].
         */
        inline fun ExpansionInfo.forEach(variableRef: Int, action: (LazyEnvironment.NestedContext) -> Unit) {
            val variableExpansion = readArgument(variableRef)
            while (true) {
                val next = session.produceNext()
                when {
                    next == ExpressionType.END_OF_EXPANSION_ORDINAL -> return
                    ExpressionType.isDataModelExpression(next) -> action(variableExpansion.environmentContext)
                }
            }
        }

        /**
         * Performs the given [transform] on each value produced by the expansion of [variableRef], returning a list
         * of the results.
         */
        inline fun <T> ExpansionInfo.map(variableRef: Int, transform: (Byte) -> T): List<T> {
            val variableExpansion = readArgument(variableRef)
            val result = mutableListOf<T>()
            while (true) {
                val next = variableExpansion.session.produceNext()
                when {
                    next == ExpressionType.END_OF_EXPANSION_ORDINAL -> return result
                    ExpressionType.isDataModelExpression(next) -> result.add(transform(next))
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
                    ExpressionType.isDataModelValue(it) -> {
                        if (argValue == null) {
                            argValue = converter(argExpansion)
                        } else {
                            throw IonException("invalid argument; too many values")
                        }
                    }

                    it == ExpressionType.END_OF_EXPANSION_ORDINAL -> break
                    it == ExpressionType.FIELD_NAME_ORDINAL -> unreachable("Unreachable without stepping into a container")
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
            return readZeroOrOneArgument(variableRef, converter)
                ?: throw IonException("invalid argument; no value when one is expected")
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

        /** The expansion kind as a byte constant. */
        @JvmField var expansionKind: Byte = ExpansionKind.UNINITIALIZED
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
                expansionKind = ExpansionKind.UNINITIALIZED
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
    private var currentExpr: Byte? = null
    private var currentValueType: IonType? = null

    /**
     * Initialize the macro evaluator with an E-Expression.
     */
    fun initExpansion(fieldName: SymbolToken?, encodingExpressions: ExpressionTape) {
        session.reset(fieldName, encodingExpressions)
        val ci = containerStack.push { _ -> }
        ci.type = ContainerInfo.Type.TopLevel

        ci.expansion = session.getExpander(ExpansionKind.STREAM, session.environment.currentContext)
    }

    override fun next(): IonType? {
        currentValueType = null
        while (currentValueType == null) {
            currentExpr = session.produceNext()
            when {
                ExpressionType.isDataModelValue(currentExpr!!) -> currentValueType = session.currentExpander!!.environmentContext.type()
                currentExpr == ExpressionType.END_OF_EXPANSION_ORDINAL -> { // TODO should this go in ExpansionInfo.produceNext?
                    val currentExpander = session.currentExpander!!
                    if (currentExpander.parentExpansion != null) {
                        session.currentExpander = currentExpander.parentExpansion
                        if (session.currentExpander!!.expansionKind != ExpansionKind.DELTA) {
                            session.currentExpander!!.childExpansion = null // TODO temporary. Fix Delta so this is not necessary.
                        }
                        if (session.currentExpander!!.expansionKind == ExpansionKind.VARIABLE) {
                            // The variable has been satisfied
                            session.finishVariable()
                        }
                        currentExpander.close()
                        // TODO removing the following line makes no functional difference; ensure removing it does
                        //  not result in a memory leak, then remove.
                        session.environment.finishChildEnvironments(session.currentExpander!!.environmentContext)
                        // TODO reset field name and annotations here?
                        continue
                    }
                    if (containerStack.peek().type == Type.TopLevel) {
                        containerStack.pop().close()
                    }
                    session.currentFieldName = null
                    session.currentAnnotations = null
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
        if (session.currentExpander!!.expansionKind == ExpansionKind.VARIABLE) {
            // The container being stepped out of was an argument to a variable.
            session.currentExpander!!.reachedEndOfExpression = true
        }
        popped.expansion.environmentContext.tape!!.advanceToAfterEndContainer() // TODO what if this is a logical container?
        // TODO removing the following line makes no functional difference; ensure removing it does
        //  not result in a memory leak, then remove.
        popped.expansion.session.environment.finishChildEnvironments(popped.expansion.environmentContext)
        popped.close()
        session.currentFieldName = currentContainer.currentFieldName
        currentValueType = null // Must call `next()` to get the next value
        session.currentAnnotations = null
    }

    /**
     * Steps in to the current [DataModelContainer].
     * Throws [IonException] if not positioned on a container.
     */
    override fun stepIn() {
        val expressionType = requireNotNull(currentExpr) { "Not positioned on a value" }
        if (expressionType == ExpressionType.DATA_MODEL_CONTAINER_ORDINAL) {
            val currentContainer = containerStack.peek()
            currentContainer.currentFieldName = session.currentFieldName
            val ci = containerStack.push { _ -> }
            ci.container = currentValueType
            ci.type = when (currentValueType) {
                IonType.LIST -> ContainerInfo.Type.List
                IonType.SEXP -> ContainerInfo.Type.Sexp
                IonType.STRUCT -> ContainerInfo.Type.Struct
                else -> unreachable()
            }
            ci.expansion = session.getExpander(
                expansionKind = ExpansionKind.STREAM,
                environmentContext = session.currentExpander!!.environmentContext,
            )
            ci.currentFieldName = null
            currentExpr = null
            session.currentFieldName = null
            currentValueType = null
            session.currentAnnotations = null
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
        session.currentAnnotations = null // Annotations are written only via Annotation expressions
        session.currentFieldName = null // Field names are written only via FieldName expressions
        while (index < arguments.size()) {
            currentValueType = null
            arguments.next()
            arguments.prepareNext()
            val argument = arguments.type()
            if (ExpressionType.isEnd(argument)) {
                session.currentAnnotations = null
                session.currentFieldName = null
                writer.stepOut()
                index++;
                continue
            }
            when (argument) {
                ExpressionType.ANNOTATION_ORDINAL -> {
                    session.currentAnnotations = arguments.context() as List<SymbolToken>
                    writer.setTypeAnnotationSymbols(*session.currentAnnotations!!.toTypedArray())
                }
                ExpressionType.DATA_MODEL_CONTAINER_ORDINAL -> {
                    currentValueType = arguments.ionType()
                    writer.stepIn(currentValueType)
                }
                ExpressionType.DATA_MODEL_SCALAR_ORDINAL -> {
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
                ExpressionType.FIELD_NAME_ORDINAL -> {
                    session.currentFieldName = arguments.context() as SymbolToken
                    writer.setFieldNameSymbol(session.currentFieldName)
                }
                ExpressionType.E_EXPRESSION_ORDINAL -> writer.startMacro(arguments.context() as Macro)
                ExpressionType.EXPRESSION_GROUP_ORDINAL -> writer.startExpressionGroup()
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

    fun hasAnnotations(): Boolean = session.currentAnnotations != null && session.currentAnnotations!!.isNotEmpty()

    override fun getTypeAnnotations(): Array<String> = session.currentAnnotations?.let { Array(it.size) { i -> it[i].assumeText() } } ?: emptyArray()
    override fun getTypeAnnotationSymbols(): Array<SymbolToken> = session.currentAnnotations?.toTypedArray() ?: emptyArray()

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
        return if (session.currentAnnotations?.isNotEmpty() == true) {
            SymbolTokenAsStringIterator(session.currentAnnotations!!)
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
