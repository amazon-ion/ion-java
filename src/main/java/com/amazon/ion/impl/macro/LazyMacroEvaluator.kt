package com.amazon.ion.impl.macro

import com.amazon.ion.*
import com.amazon.ion.impl.*
import com.amazon.ion.impl.macro.ExpansionKinds.*
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

        val emptyExpansion: ExpansionInfo = ExpansionInfo(this)
        var expressionTape: ExpressionTape? = null
        val sideEffects: ExpressionTape = ExpressionTape(null, 4)
        var sideEffectExpander: ExpansionInfo? = null
        var currentExpander: ExpansionInfo? = null
        var currentFieldName: SymbolToken? = null
        var currentAnnotations: List<SymbolToken>? = null

        var eExpressionIndex = -1;

        init {
            emptyExpansion.expansionKind = EMPTY
            emptyExpansion.tape = null
        }

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
        fun getExpander(expansionKind: Byte, tape: ExpressionTape?): ExpansionInfo {
            val expansion = getExpansion()
            expansion.expansionKind = expansionKind
            expansion.tape = tape
            expansion.additionalState = null
            expansion.childExpansion = null
            expansion.parentExpansion = null
            expansion.reachedEndOfExpression = false
            currentExpander = expansion
            return expansion
        }

        fun produceNext(): Byte {
            while (true) {
                expressionTape?.next() // TODO if Empty doesn't exist and CONTINUE_EXPANSION isn't used with it, then this could be tape!!
                val next = when (currentExpander!!.expansionKind) {
                    UNINITIALIZED -> throw IllegalStateException("ExpansionInfo not initialized.")
                    EMPTY -> ExpressionType.END_OF_EXPANSION_ORDINAL
                    STREAM -> handleStream(currentExpander!!)
                    VARIABLE -> handleVariable(currentExpander!!)
                    EXPR_GROUP -> handleStream(currentExpander!!)
                    EXACTLY_ONE_VALUE_STREAM -> handleExactlyOneValueStream(currentExpander!!)
                    IF_NONE -> handleIfNone(currentExpander!!)
                    IF_SOME -> handleIfSome(currentExpander!!)
                    IF_SINGLE -> handleIfSingle(currentExpander!!)
                    IF_MULTI -> handleIfMulti(currentExpander!!)
                    ANNOTATE -> handleAnnotate(currentExpander!!)
                    MAKE_STRING -> handleMakeStringOrSymbol(IonType.STRING, currentExpander!!)
                    MAKE_SYMBOL -> handleMakeStringOrSymbol(IonType.SYMBOL, currentExpander!!)
                    MAKE_BLOB -> handleMakeBlob(currentExpander!!)
                    MAKE_DECIMAL -> handleMakeDecimal(currentExpander!!)
                    MAKE_TIMESTAMP -> handleMakeTimestamp(currentExpander!!)
                    PRIVATE_MAKE_FIELD_NAME_AND_VALUE -> handlePrivateMakeFieldNameAndValue(currentExpander!!)
                    PRIVATE_FLATTEN_STRUCT -> handleFlatten(currentExpander!!)
                    FLATTEN -> handleFlatten(currentExpander!!)
                    SUM -> handleSum(currentExpander!!)
                    DELTA -> handleDelta(currentExpander!!)
                    REPEAT -> handleRepeat(currentExpander!!)
                    else -> throw IllegalStateException("Unknown expansion kind: ${currentExpander!!.expansionKind}")
                }
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

        private fun handleVariable(thisExpansion: ExpansionInfo): Byte {
            if (thisExpansion.reachedEndOfExpression) {
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
            currentAnnotations = annotations
            thisExpansion.readArgument(1)

            val type = produceNext()
            if (type == ExpressionType.END_OF_EXPANSION_ORDINAL) {
                throw IonException("Can only annotate exactly one value")
            }
            return type
        }

        private fun handleMakeStringOrSymbol(ionType: IonType, thisExpansion: ExpansionInfo): Byte {
            thisExpansion.expansionKind = STREAM
            val sb = StringBuilder()
            thisExpansion.forEach(0) {
                when {
                    IonType.isText(it.ionType()) -> sb.append(it.readText())
                    else -> throw IonException("Invalid argument type for 'make_string': ${it.ionType()}")
                }
            }
            thisExpansion.tape!!.advanceToAfterEndEExpression(thisExpansion.session.eExpressionIndex)
            thisExpansion.expansionKind = EMPTY
            thisExpansion.produceValueSideEffect(ionType, sb.toString())
            return ExpressionType.DATA_MODEL_SCALAR_ORDINAL
        }

        private fun handleMakeBlob(thisExpansion: ExpansionInfo): Byte {
            thisExpansion.expansionKind = STREAM
            val baos = ByteArrayOutputStream()
            thisExpansion.forEach(0) {
                baos.write(it.readLob())
            }
            thisExpansion.expansionKind = EMPTY
            thisExpansion.produceValueSideEffect(IonType.BLOB, baos.toByteArray())
            return ExpressionType.DATA_MODEL_SCALAR_ORDINAL
        }

        private fun handleMakeDecimal(thisExpansion: ExpansionInfo): Byte {
            thisExpansion.expansionKind = STREAM
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
            thisExpansion.expansionKind = STREAM
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
            thisExpansion.expansionKind = STREAM
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
                thisExpansion.readArgument(0)
                delegate = getExpander(
                    expansionKind = VARIABLE,
                    tape = expressionTape,
                )
                thisExpansion.childExpansion = delegate
                delegate.parentExpansion = thisExpansion
            }
            currentExpander = delegate
            val nextExpandedArg = produceNext()
            when {
                ExpressionType.isDataModelValue(nextExpandedArg) -> {
                    val nextDelta = asBigInteger(delegate)
                    val nextOutput = runningTotal + nextDelta
                    delegate.produceValueSideEffect(IonType.INT, nextOutput)
                    currentExpander!!.parentExpansion = thisExpansion
                    thisExpansion.additionalState = nextOutput
                    return nextExpandedArg
                }

                nextExpandedArg == ExpressionType.END_OF_EXPANSION_ORDINAL -> {
                    delegate.close()
                    thisExpansion.childExpansion = null
                    currentExpander = thisExpansion
                    return ExpressionType.END_OF_EXPANSION_ORDINAL
                }

                else -> throw IonException("delta arguments must be integers")
            }
        }

        private fun handleRepeat(thisExpansion: ExpansionInfo): Byte {
            thisExpansion.expansionKind = STREAM
            var n = thisExpansion.additionalState as Long?
            if (n == null) {
                n = thisExpansion.readExactlyOneArgument(0, ::asLong)
                if (n < 0) throw IonException("invalid argument; 'n' must be non-negative")
                thisExpansion.additionalState = n
            }

            if (n > 0) {
                thisExpansion.readArgument(1)
                thisExpansion.additionalState = n - 1
            } else {
                return ExpressionType.END_OF_EXPANSION_ORDINAL
            }
            val maybeNext = produceNext()
            thisExpansion.childExpansion = null
            return when {
                ExpressionType.isDataModelExpression(maybeNext) -> maybeNext
                maybeNext == ExpressionType.END_OF_EXPANSION_ORDINAL -> ExpressionType.END_OF_EXPANSION_ORDINAL
                else -> unreachable()
            }
        }

        private fun handlePrivateMakeFieldNameAndValue(thisExpansion: ExpansionInfo): Byte {
            currentFieldName = thisExpansion.readExactlyOneArgument(0, ::asSymbol)
            thisExpansion.readArgument(1)
            thisExpansion.expansionKind = EXACTLY_ONE_VALUE_STREAM
            return ExpressionType.CONTINUE_EXPANSION_ORDINAL
        }

        private fun handleFlatten(thisExpansion: ExpansionInfo): Byte {
            thisExpansion.expansionKind = STREAM
            var argumentExpansion: ExpansionInfo? = thisExpansion.additionalState as ExpansionInfo?
            if (argumentExpansion == null) {
                argumentExpansion = thisExpansion.readArgument(0)
                thisExpansion.additionalState = argumentExpansion
            }

            val currentChildExpansion = thisExpansion.childExpansion
            val next = currentChildExpansion?.session?.produceNext()
            if (next == null) {
                currentExpander = argumentExpansion
                val nextSequence = produceNext()
                currentExpander = thisExpansion
                return when {
                    nextSequence == ExpressionType.DATA_MODEL_CONTAINER_ORDINAL -> {
                        thisExpansion.childExpansion = getExpander(
                            expansionKind = STREAM,
                            tape = expressionTape,
                        )
                        thisExpansion.childExpansion!!.parentExpansion = thisExpansion
                        ExpressionType.CONTINUE_EXPANSION_ORDINAL
                    }

                    nextSequence == ExpressionType.END_OF_EXPANSION_ORDINAL -> ExpressionType.END_OF_EXPANSION_ORDINAL
                    ExpressionType.isDataModelExpression(nextSequence) -> throw IonException("invalid argument; flatten expects sequences")
                    else -> unreachable()
                }
            }
            currentExpander = thisExpansion
            return when {
                ExpressionType.isDataModelExpression(next) -> next
                next == ExpressionType.END_OF_EXPANSION_ORDINAL -> ExpressionType.END_OF_EXPANSION_ORDINAL
                else -> unreachable()
            }
        }

        private fun handleBranchIf(thisExpansion: ExpansionInfo, condition: (Int) -> Boolean): Byte {
            thisExpansion.readArgument(0)
            thisExpansion.expansionKind = STREAM
            var n = 0
            while (n < 2) {
                if (produceNext() == ExpressionType.END_OF_EXPANSION_ORDINAL) break
                n++
            }
            val branch = if (condition(n)) 1 else 2
            thisExpansion.tape!!.setNextAfterEndOfEExpression(thisExpansion.session.eExpressionIndex)
            thisExpansion.readArgument(branch)
            return ExpressionType.CONTINUE_EXPANSION_ORDINAL
        }

        private fun fieldName(expressionTape: ExpressionTape): Byte {
            currentFieldName = expressionTape.context() as SymbolToken
            expressionTape.prepareNext()
            return ExpressionType.CONTINUE_EXPANSION_ORDINAL
        }

        private fun annotation(expressionTape: ExpressionTape): Byte {
            currentAnnotations = expressionTape.annotations()
            expressionTape.prepareNext()
            return ExpressionType.CONTINUE_EXPANSION_ORDINAL
        }

        private fun eExpression(expressionTape: ExpressionTape, thisExpansion: ExpansionInfo): Byte {
            val macro = expressionTape.context() as SystemMacro
            eExpressionIndex++
            // TODO do these three in one step? OR, change setNextAfterEndOfEExpression to skip the eexp i currently points at and then remove the next two lines
            expressionTape.prepareNext()
            expressionTape.next()
            expressionTape.setNextAfterEndOfEExpression(eExpressionIndex)
            thisExpansion.childExpansion = getExpander(
                expansionKind = macro.expansionKind,
                tape = expressionTape,
            )
            thisExpansion.childExpansion!!.parentExpansion = thisExpansion
            return ExpressionType.CONTINUE_EXPANSION_ORDINAL
        }

        private fun expressionGroup(expressionTape: ExpressionTape, thisExpansion: ExpansionInfo): Byte {
            expressionTape.prepareNext()
            thisExpansion.childExpansion = getExpander(
                expansionKind = EXPR_GROUP,
                tape = expressionTape,
            )
            thisExpansion.childExpansion!!.parentExpansion = thisExpansion
            return ExpressionType.CONTINUE_EXPANSION_ORDINAL
        }

        private fun dataModelScalar(expressionTape: ExpressionTape): Byte {
            expressionTape.prepareNext()
            return ExpressionType.DATA_MODEL_SCALAR_ORDINAL
        }

        private fun dataModelContainer(expressionTape: ExpressionTape): Byte {
            expressionTape.prepareNext()
            return ExpressionType.DATA_MODEL_CONTAINER_ORDINAL
        }

        private fun expressionEnd(nextType: Byte, expressionTape: ExpressionTape): Byte {
            if (nextType == ExpressionType.EXPRESSION_GROUP_END_ORDINAL || nextType == ExpressionType.E_EXPRESSION_END_ORDINAL) {
                // Expressions and expression groups do not rely on stepIn/stepOut for navigation, so the tape must be advanced
                // here.
                expressionTape.prepareNext()
            }
            if (nextType != ExpressionType.E_EXPRESSION_END_ORDINAL) { // TODO why the special case?
                return ExpressionType.END_OF_EXPANSION_ORDINAL
            }
            return ExpressionType.CONTINUE_EXPANSION_ORDINAL // TODO should end of expansion be conveyed in any case here?
        }

        private fun handleStream(thisExpansion: ExpansionInfo): Byte {
            val expressionTape = thisExpansion.tape!!
            if (expressionTape.isExhausted) {
                return ExpressionType.END_OF_EXPANSION_ORDINAL
            }
            val nextType = expressionTape.type()
            if (ExpressionType.isEnd(nextType)) {
                return expressionEnd(nextType, expressionTape)
            }
            return when (nextType) {
                ExpressionType.FIELD_NAME_ORDINAL -> fieldName(expressionTape)
                ExpressionType.ANNOTATION_ORDINAL -> annotation(expressionTape)
                ExpressionType.E_EXPRESSION_ORDINAL -> eExpression(expressionTape, thisExpansion)
                ExpressionType.EXPRESSION_GROUP_ORDINAL -> expressionGroup(expressionTape, thisExpansion)
                ExpressionType.DATA_MODEL_SCALAR_ORDINAL -> dataModelScalar(expressionTape)
                ExpressionType.DATA_MODEL_CONTAINER_ORDINAL -> dataModelContainer(expressionTape)
                else -> unreachable()
            }
        }

        private fun asLong(expansion: ExpansionInfo): Long {
            return expansion.tape!!.readLong()
        }

        private fun asBigInteger(expansion: ExpansionInfo): BigInteger {
            return expansion.tape!!.readBigInteger()
        }

        private fun asText(expansion: ExpansionInfo): String {
            return expansion.tape!!.readText()
        }

        private fun asSymbol(expansion: ExpansionInfo): SymbolToken {
            return expansion.tape!!.readSymbol()
        }

        private fun asBigDecimal(expansion: ExpansionInfo): BigDecimal {
            return expansion.tape!!.readBigDecimal()
        }

        private fun asLob(expansion: ExpansionInfo): ByteArray {
            return expansion.tape!!.readLob()
        }

        fun reset(fieldName: SymbolToken?, arguments: ExpressionTape) {
            numExpandedExpressions = 0
            expanderPoolIndex = 0
            expressionTape = arguments
            sideEffectExpander = getExpander(EMPTY, sideEffects)
            sideEffectExpander!!.keepAlive = true
            currentExpander = null
            currentFieldName = fieldName
            currentAnnotations = null
            eExpressionIndex = -1;
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
     * Represents a frame in the expansion stack for a particular container.
     *
     * TODO: "info" is very non-specific; rename to ExpansionFrame next time there's a
     *       non-functional refactoring in this class.
     *       Alternately, consider ExpansionOperator to reflect the fact that these are
     *       like operators in an expression tree.
     */
    internal class ExpansionInfo(@JvmField val session: Session) {

        /** The expansion kind as a byte constant. */
        @JvmField var expansionKind: Byte = UNINITIALIZED
        /**
         * The evaluation [Environment]—i.e. variable bindings.
         */
        @JvmField var tape: ExpressionTape? = null

        @JvmField var reachedEndOfExpression: Boolean = false

        /**
         * Field for storing any additional state required by an ExpansionKind.
         */
        @JvmField
        var additionalState: Any? = null

        @JvmField
        var keepAlive: Boolean = false

        // TODO remove child and parent expansion?

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
         * Returns an expansion for the given variable.
         */
        fun readArgument(variableRef: Int): ExpansionInfo {
            tape!!.seekToArgument(session.eExpressionIndex, variableRef)
                ?: return session.emptyExpansion // Argument was elided.
            return this
        }

        fun produceValueSideEffect(type: IonType, value: Any) {
            val sideEffectTape = session.sideEffects
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
        inline fun forEach(variableRef: Int, action: (ExpressionTape) -> Unit) {
            val variableExpansion = readArgument(variableRef)
            while (true) {
                val next = session.produceNext()
                when {
                    next == ExpressionType.END_OF_EXPANSION_ORDINAL -> return
                    ExpressionType.isDataModelExpression(next) -> action(variableExpansion.tape!!)
                }
            }
        }

        /**
         * Performs the given [transform] on each value produced by the expansion of [variableRef], returning a list
         * of the results.
         */
        inline fun <T> map(variableRef: Int, transform: (Byte) -> T): List<T> {
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

        /**
         * Reads and returns zero or one values from the expansion of the given [variableRef].
         * Throws an [IonException] if more than one value is present in the variable expansion.
         * Throws an [IonException] if the value is not the expected type [T].
         */
        inline fun <T> readZeroOrOneArgument(
            variableRef: Int,
            converter: (ExpansionInfo) -> T
        ): T? {
            readArgument(variableRef)
            // TODO possible to detect ahead of time whether this argument can return more than one value? It can't
            //  unless it's an invocation of certain system macros or it's an expression group
            val argExpansion = session.getExpander(
                expansionKind = VARIABLE,
                tape = session.expressionTape
            )
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
            session.currentExpander = this
            return argValue
        }

        /**
         * Reads and returns exactly one value from the expansion of the given [variableRef].
         * Throws an [IonException] if the expansion of [variableRef] does not produce exactly one value.
         * Throws an [IonException] if the value is not the expected type [T].
         */
        inline fun <T> readExactlyOneArgument(variableRef: Int, converter: (ExpansionInfo) -> T): T {
            return readZeroOrOneArgument(variableRef, converter)
                ?: throw IonException("invalid argument; no value when one is expected")
        }

        /**
         * Returns this [ExpansionInfo] to the expander pool, recursively closing [childExpansion]s in the process.
         * Could also be thought of as a `free` function.
         */
        fun close() {
            if (!keepAlive) {
                expansionKind = UNINITIALIZED
                additionalState?.let { if (it is ExpansionInfo) it.close() }
                additionalState = null
                childExpansion?.close()
                childExpansion = null
                parentExpansion = null
                reachedEndOfExpression = false
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
    private var currentExpr: Byte? = null
    private var currentValueType: IonType? = null

    /**
     * Initialize the macro evaluator with an E-Expression.
     */
    fun initExpansion(fieldName: SymbolToken?, encodingExpressions: ExpressionTape) {
        session.reset(fieldName, encodingExpressions)
        val ci = containerStack.push { _ -> }
        ci.type = ContainerInfo.Type.TopLevel

        ci.expansion = session.getExpander(STREAM, session.expressionTape)
    }

    override fun next(): IonType? {
        currentValueType = null
        while (currentValueType == null) {
            currentExpr = session.produceNext()
            when {
                ExpressionType.isDataModelValue(currentExpr!!) -> currentValueType = session.currentExpander!!.tape!!.ionType()
                currentExpr == ExpressionType.END_OF_EXPANSION_ORDINAL -> { // TODO should this go in produceNext?
                    val currentExpander = session.currentExpander!!
                    if (currentExpander.parentExpansion != null) {
                        session.currentExpander = currentExpander.parentExpansion
                        if (session.currentExpander!!.expansionKind != DELTA) {
                            session.currentExpander!!.childExpansion = null // TODO temporary. Fix Delta so this is not necessary.
                        }
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
        popped.expansion.tape!!.advanceToAfterEndContainer()
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
            // TODO can this expander be eliminated?
            ci.expansion = session.getExpander(
                expansionKind = STREAM,
                tape = session.expressionTape
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
    override fun longValue(): Long  = session.currentExpander!!.tape!!.readLong()
    override fun bigIntegerValue(): BigInteger = session.currentExpander!!.tape!!.readBigInteger()
    override fun getIntegerSize(): IntegerSize = session.currentExpander!!.tape!!.readIntegerSize()
    override fun stringValue(): String = session.currentExpander!!.tape!!.readText()
    override fun symbolValue(): SymbolToken = session.currentExpander!!.tape!!.readSymbol()
    override fun bigDecimalValue(): BigDecimal = session.currentExpander!!.tape!!.readBigDecimal()
    override fun byteSize(): Int = session.currentExpander!!.tape!!.lobSize()
    override fun newBytes(): ByteArray = session.currentExpander!!.tape!!.readLob().copyOf()
    override fun doubleValue(): Double = session.currentExpander!!.tape!!.readFloat()
    override fun timestampValue(): Timestamp = session.currentExpander!!.tape!!.readTimestamp()
    override fun isNullValue(): Boolean = session.currentExpander!!.tape!!.isNullValue
    override fun booleanValue(): Boolean = session.currentExpander!!.tape!!.readBoolean()
}
