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
    internal class Session( // TODO eliminate; move internals into LazyMacroEvaluator?
        /** Number of expansion steps at which the evaluation session should be aborted. */
        private val expansionLimit: Int = 1_000_000
    ) {
        /** Internal state for tracking the number of expansion steps. */
        private var numExpandedExpressions = 0

        val emptyExpansion: ExpansionInfo = ExpansionInfo(this)
        val sideEffects: ExpressionTape = ExpressionTape(null, 4)
        val sideEffectExpander: ExpansionInfo = ExpansionInfo(this) // TODO remove this; make currentExpander constant. Only swap in side effect tape for expressionTape. Change scalar getters to access expressionTape
        val invocationExpander: ExpansionInfo = ExpansionInfo(this)
        var currentExpander: ExpansionInfo = invocationExpander
        var expressionTape: ExpressionTape = invocationExpander.tape
        var currentFieldName: SymbolToken? = null
        var currentAnnotations: List<SymbolToken>? = null

        var eExpressionIndex = -1;

        init {
            emptyExpansion.expansionKind = EMPTY
        }

        /** Gets an [ExpansionInfo] from the pool (or allocates a new one if necessary), initializing it with the provided values. */
        private fun initializeExpander(expansion: ExpansionInfo, expansionKind: Byte, tape: ExpressionTape) {
            expansion.expansionKind = expansionKind
            expansion.tape = tape
            expansion.additionalState = null
            expansion.expansionKindStack[0] = expansionKind
            expansion.expansionKindStackTop = 1
            expansion.expansionKindStackIndex = 1
            expansion.reachedEndOfExpression = false
        }

        fun produceNext(): Byte {
            currentExpander = invocationExpander
            while (true) {
                expressionTape.next()
                val next = when (currentExpander.expansionKind) {
                    UNINITIALIZED -> throw IllegalStateException("ExpansionInfo not initialized.")
                    EMPTY -> ExpressionType.END_OF_EXPANSION_ORDINAL
                    STREAM -> handleStream(currentExpander)
                    VARIABLE -> handleVariable(currentExpander)
                    EXPR_GROUP -> handleStream(currentExpander)
                    EXACTLY_ONE_VALUE_STREAM -> handleExactlyOneValueStream(currentExpander)
                    IF_NONE -> handleIfNone(currentExpander)
                    IF_SOME -> handleIfSome(currentExpander)
                    IF_SINGLE -> handleIfSingle(currentExpander)
                    IF_MULTI -> handleIfMulti(currentExpander)
                    ANNOTATE -> handleAnnotate(currentExpander)
                    MAKE_STRING -> handleMakeStringOrSymbol(IonType.STRING, currentExpander)
                    MAKE_SYMBOL -> handleMakeStringOrSymbol(IonType.SYMBOL, currentExpander)
                    MAKE_BLOB -> handleMakeBlob(currentExpander)
                    MAKE_DECIMAL -> handleMakeDecimal(currentExpander)
                    MAKE_TIMESTAMP -> handleMakeTimestamp(currentExpander)
                    PRIVATE_MAKE_FIELD_NAME_AND_VALUE -> handlePrivateMakeFieldNameAndValue(currentExpander)
                    PRIVATE_FLATTEN_STRUCT -> handleFlatten(currentExpander)
                    FLATTEN -> handleFlatten(currentExpander)
                    SUM -> handleSum(currentExpander)
                    DELTA -> handleDelta(currentExpander)
                    REPEAT -> handleRepeat(currentExpander)
                    else -> throw IllegalStateException("Unknown expansion kind: ${currentExpander.expansionKind}")
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
            if (thisExpansion.reachedEndOfExpression) { // TODO  remove reachedEndOfExpression, just pop immediately from the expansion stack when this would be set to true?
                return ExpressionType.END_OF_EXPANSION_ORDINAL
            }
            // TODO visit the child. Currently this isn't correct (though it may end up in the right spot in some tests) if the variable is an expression group (the child)
            val expression = handleStream(thisExpansion)
            if (!thisExpansion.hasChild()) {
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
            thisExpansion.tape.advanceToAfterEndEExpression(thisExpansion.session.eExpressionIndex)
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
            val a = thisExpansion.readExactlyOneArgument(0, ::asBigInteger)
            val b = thisExpansion.readExactlyOneArgument(1, ::asBigInteger)
            thisExpansion.produceValueSideEffect(IonType.INT, a + b)
            thisExpansion.finishChildExpansion() // Finishes the sum expansion
            return ExpressionType.DATA_MODEL_SCALAR_ORDINAL
        }

        private fun handleDelta(thisExpansion: ExpansionInfo): Byte {
            val expansionIndex = thisExpansion.expansionKindStackIndex
            val runningTotal = thisExpansion.additionalState as? BigInteger ?: BigInteger.ZERO
            if (!thisExpansion.visitChild()) {
                thisExpansion.readArgument(0)
                thisExpansion.expandChild(VARIABLE)
            }
            val nextExpandedArg = produceNext()
            thisExpansion.returnToExpansion(expansionIndex)
            when {
                ExpressionType.isDataModelValue(nextExpandedArg) -> {
                    val nextDelta = asBigInteger(thisExpansion)
                    val nextOutput = runningTotal + nextDelta
                    thisExpansion.produceValueSideEffect(IonType.INT, nextOutput)
                    thisExpansion.additionalState = nextOutput
                    return nextExpandedArg
                }

                nextExpandedArg == ExpressionType.END_OF_EXPANSION_ORDINAL -> {
                    thisExpansion.dropChildren(expansionIndex)
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
            // TODO this needs more testing; probably needs corrections.
            thisExpansion.expansionKind = STREAM
            if (thisExpansion.additionalState == null) {
                thisExpansion.readArgument(0)
                thisExpansion.additionalState = 1
            }
            var next: Byte?
            if (!thisExpansion.hasChild()) {
                val nextSequence = produceNext()
                return when {
                    nextSequence == ExpressionType.DATA_MODEL_CONTAINER_ORDINAL -> {
                        thisExpansion.expandChild(STREAM)
                        ExpressionType.CONTINUE_EXPANSION_ORDINAL
                    }

                    nextSequence == ExpressionType.END_OF_EXPANSION_ORDINAL -> ExpressionType.END_OF_EXPANSION_ORDINAL
                    ExpressionType.isDataModelExpression(nextSequence) -> throw IonException("invalid argument; flatten expects sequences")
                    else -> unreachable()
                }
            } else {
                next = produceNext()
            }
            return when {
                ExpressionType.isDataModelExpression(next) -> next
                next == ExpressionType.END_OF_EXPANSION_ORDINAL -> ExpressionType.END_OF_EXPANSION_ORDINAL
                else -> unreachable()
            }
        }

        private fun handleBranchIf(thisExpansion: ExpansionInfo, condition: (Int) -> Boolean): Byte {
            val expansionIndex = thisExpansion.expansionKindStackIndex
            thisExpansion.readArgument(0)
            thisExpansion.expansionKind = STREAM // TODO should not have to do this, but makes tests pass. Probably need to push a child expansion
            var n = 0
            while (n < 2) {
                if (produceNext() == ExpressionType.END_OF_EXPANSION_ORDINAL) break
                n++
            }
            val branch = if (condition(n)) 1 else 2
            thisExpansion.tape.setNextAfterEndOfEExpression(thisExpansion.session.eExpressionIndex)
            thisExpansion.readArgument(branch)
            thisExpansion.dropChildren(expansionIndex)
            thisExpansion.finishChildExpansion() // This finishes the IfNone
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
            currentExpander.expandChild(macro.expansionKind)
            return ExpressionType.CONTINUE_EXPANSION_ORDINAL
        }

        private fun expressionGroup(expressionTape: ExpressionTape, thisExpansion: ExpansionInfo): Byte {
            expressionTape.prepareNext()
            // TODO short-circuit if empty?
            currentExpander.expandChild(EXPR_GROUP)
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
            val expressionTape = thisExpansion.tape
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

        // TODO remove all the as* methods
        private fun asLong(expansion: ExpansionInfo): Long {
            return expansion.tape.readLong()
        }

        private fun asBigInteger(expansion: ExpansionInfo): BigInteger {
            return expansion.tape.readBigInteger()
        }

        private fun asText(expansion: ExpansionInfo): String {
            return expansion.tape.readText()
        }

        private fun asSymbol(expansion: ExpansionInfo): SymbolToken {
            return expansion.tape.readSymbol()
        }

        private fun asBigDecimal(expansion: ExpansionInfo): BigDecimal {
            return expansion.tape.readBigDecimal()
        }

        private fun asLob(expansion: ExpansionInfo): ByteArray {
            return expansion.tape.readLob()
        }

        fun reset(fieldName: SymbolToken?, arguments: ExpressionTape) {
            numExpandedExpressions = 0
            //expanderPoolIndex = 0
            expressionTape = arguments
            initializeExpander(sideEffectExpander, EMPTY, sideEffects)
            initializeExpander(invocationExpander, STREAM, expressionTape)
            currentFieldName = fieldName
            currentAnnotations = null
            eExpressionIndex = -1;
        }
    }

    // TODO possible to just access the parent reader's container stack rather than reproducing it here?
    /**
     * A container in the macro evaluator's [containerStack].
     */
    private data class ContainerInfo(var type: Type = Type.Uninitialized) {
        enum class Type { TopLevel, List, Sexp, Struct, Uninitialized }

        fun close() {
            currentFieldName = null
            container = null
            type = Type.Uninitialized
        }

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
    internal class ExpansionInfo(@JvmField val session: Session) { // TODO eliminate; move to upper level?

        /** The expansion kind as a byte constant. */
        @JvmField var expansionKind: Byte = UNINITIALIZED
        /**
         * The evaluation [Environment]—i.e. variable bindings.
         */
        @JvmField var tape: ExpressionTape = ExpressionTape.EMPTY

        @JvmField var reachedEndOfExpression: Boolean = false

        /**
         * Field for storing any additional state required by an ExpansionKind.
         */
        @JvmField
        var additionalState: Any? = null

        @JvmField var expansionKindStack: ByteArray = ByteArray(16)
        @JvmField var expansionKindStackTop: Int = 0 // The index where the next child expansion would be stored
        @JvmField var expansionKindStackIndex: Int = 0 // The index after the expansion currently being evaluated

        fun expandChild(expansionKind: Byte) {
            if (expansionKindStackTop == expansionKindStack.size) {
                expansionKindStack = expansionKindStack.copyOf(expansionKindStack.size * 2)
            }
            expansionKindStack[expansionKindStackTop++] = expansionKind //this.expansionKind
            expansionKindStackIndex++
            this.expansionKind = expansionKind
        }

        fun hasChild(): Boolean {
            return expansionKindStackIndex < expansionKindStackTop
        }

        fun visitChild(): Boolean {
            if (expansionKindStackIndex < expansionKindStackTop) {
                expansionKind = expansionKindStack[expansionKindStackIndex++]
                return true
            }
            return false
        }

        fun dropChildren(index: Int) {
            expansionKindStackIndex = index
            expansionKindStackTop = index
            expansionKind = expansionKindStack[index - 1]
        }

        fun returnToExpansion(index: Int) {
            expansionKindStackIndex = index
            expansionKind = expansionKindStack[index - 1]
        }

        fun finishChildExpansion() {
            if (expansionKindStackTop == 1) {
                return
            }
            this.expansionKind = expansionKindStack[--expansionKindStackTop - 1]
            expansionKindStackIndex = Math.min(expansionKindStackIndex, expansionKindStackTop)
        }

        /**
         * Returns an expansion for the given variable.
         */
        fun readArgument(variableRef: Int): ExpansionInfo {
            tape.seekToArgument(session.eExpressionIndex, variableRef)
                ?: return session.emptyExpansion // Argument was elided.
            return this
        }

        fun produceValueSideEffect(type: IonType, value: Any) {
            val sideEffectTape = session.sideEffects
            sideEffectTape.clear()
            sideEffectTape.addScalar(type, value)
            sideEffectTape.rewindTo(0)
            session.currentExpander = session.sideEffectExpander
        }

        /**
         * Performs the given [action] for each value produced by the expansion of [variableRef].
         */
        inline fun forEach(variableRef: Int, action: (ExpressionTape) -> Unit) {
            val expansionIndex = session.currentExpander.expansionKindStackIndex
            val variableExpansion = readArgument(variableRef)
            while (true) {
                val next = session.produceNext()
                when {
                    next == ExpressionType.END_OF_EXPANSION_ORDINAL -> break
                    ExpressionType.isDataModelExpression(next) -> action(variableExpansion.tape)
                }
            }
            session.currentExpander.dropChildren(expansionIndex)
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
            val expansionIndex = session.currentExpander.expansionKindStackIndex
            readArgument(variableRef)
            // TODO possible to detect ahead of time whether this argument can return more than one value? It can't
            //  unless it's an invocation of certain system macros or it's an expression group
            session.currentExpander.expandChild(VARIABLE)
            var argValue: T? = null
            while (true) {
                val it = session.produceNext()
                when {
                    ExpressionType.isDataModelValue(it) -> {
                        if (argValue == null) {
                            argValue = converter(session.currentExpander)
                        } else {
                            throw IonException("invalid argument; too many values")
                        }
                    }

                    it == ExpressionType.END_OF_EXPANSION_ORDINAL -> break
                    it == ExpressionType.FIELD_NAME_ORDINAL -> unreachable("Unreachable without stepping into a container")
                }
            }
            session.currentExpander.dropChildren(expansionIndex)
            session.currentExpander.reachedEndOfExpression = false
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

        // TODO
        override fun toString() = """
        |ExpansionInfo(
        |    expansionKind: $expansionKind,
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
    }

    override fun next(): IonType? {
        currentValueType = null
        while (currentValueType == null) {
            currentExpr = session.produceNext()
            when {
                ExpressionType.isDataModelValue(currentExpr!!) -> currentValueType = session.currentExpander.tape.ionType()
                currentExpr == ExpressionType.END_OF_EXPANSION_ORDINAL -> { // TODO should this go in produceNext?
                    if (session.currentExpander.expansionKindStackTop > 1) {
                        session.currentExpander.finishChildExpansion()
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
        session.expressionTape.advanceToAfterEndContainer()
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
    override fun longValue(): Long  = session.currentExpander.tape.readLong()
    override fun bigIntegerValue(): BigInteger = session.currentExpander.tape.readBigInteger()
    override fun getIntegerSize(): IntegerSize = session.currentExpander.tape.readIntegerSize()
    override fun stringValue(): String = session.currentExpander.tape.readText()
    override fun symbolValue(): SymbolToken = session.currentExpander.tape.readSymbol()
    override fun bigDecimalValue(): BigDecimal = session.currentExpander.tape.readBigDecimal()
    override fun byteSize(): Int = session.currentExpander.tape.lobSize()
    override fun newBytes(): ByteArray = session.currentExpander.tape.readLob().copyOf()
    override fun doubleValue(): Double = session.currentExpander.tape.readFloat()
    override fun timestampValue(): Timestamp = session.currentExpander.tape.readTimestamp()
    override fun isNullValue(): Boolean = session.currentExpander.tape.isNullValue
    override fun booleanValue(): Boolean = session.currentExpander.tape.readBoolean()
}
