package com.amazon.ion.impl.macro

import com.amazon.ion.*
import com.amazon.ion.impl.*
import com.amazon.ion.impl.macro.ExpansionKinds.*
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

        val sideEffects: ExpressionTape = ExpressionTape(null, 4)
        var expander: ExpansionInfo = ExpansionInfo(this)
        var expressionTape: ExpressionTape = expander.tape
        var currentFieldName: SymbolToken? = null
        var currentAnnotations: List<SymbolToken>? = null

        var eExpressionIndex = -1;

        fun produceNext(): Byte {
            expressionTape = expander.tape
            while (true) {
                expressionTape.next()
                val next = when (expander.expansionKind) {
                    UNINITIALIZED -> throw IllegalStateException("ExpansionInfo not initialized.")
                    EMPTY -> ExpressionType.END_OF_EXPANSION_ORDINAL
                    STREAM -> handleStream()
                    VARIABLE -> handleVariable()
                    EXPR_GROUP -> handleStream()
                    EXACTLY_ONE_VALUE_STREAM -> handleExactlyOneValueStream()
                    IF_NONE -> handleIfNone()
                    IF_SOME -> handleIfSome()
                    IF_SINGLE -> handleIfSingle()
                    IF_MULTI -> handleIfMulti()
                    ANNOTATE -> handleAnnotate()
                    MAKE_STRING -> handleMakeStringOrSymbol(IonType.STRING)
                    MAKE_SYMBOL -> handleMakeStringOrSymbol(IonType.SYMBOL)
                    MAKE_BLOB -> handleMakeBlob()
                    MAKE_DECIMAL -> handleMakeDecimal()
                    MAKE_TIMESTAMP -> handleMakeTimestamp()
                    PRIVATE_MAKE_FIELD_NAME_AND_VALUE -> handlePrivateMakeFieldNameAndValue()
                    PRIVATE_FLATTEN_STRUCT -> handleFlatten()
                    FLATTEN -> handleFlatten()
                    SUM -> handleSum()
                    DELTA -> handleDelta()
                    REPEAT -> handleRepeat()
                    else -> throw IllegalStateException("Unknown expansion kind: ${expander.expansionKind}")
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

        private fun handleVariable(): Byte {
            if (expander.reachedEndOfExpression) { // TODO  remove reachedEndOfExpression, just pop immediately from the expansion stack when this would be set to true?
                return ExpressionType.END_OF_EXPANSION_ORDINAL
            }
            // TODO visit the child. Currently this isn't correct (though it may end up in the right spot in some tests) if the variable is an expression group (the child)
            val expression = handleStream()
            if (!expander.hasChild()) {
                if (expression == ExpressionType.DATA_MODEL_SCALAR_ORDINAL) {
                    expander.reachedEndOfExpression = true
                }
            }
            return expression
        }

        private fun handleExactlyOneValueStream(): Byte {
            if (expander.additionalState != 1) {
                val firstValue = handleStream()
                return when {
                    ExpressionType.isDataModelExpression(firstValue) -> {
                        expander.additionalState = 1
                        firstValue
                    }

                    firstValue == ExpressionType.CONTINUE_EXPANSION_ORDINAL -> ExpressionType.CONTINUE_EXPANSION_ORDINAL
                    firstValue == ExpressionType.END_OF_EXPANSION_ORDINAL -> throw IonException("Expected one value, found 0")
                    else -> unreachable()
                }
            } else {
                val secondValue = handleStream()
                return when {
                    ExpressionType.isDataModelExpression(secondValue) -> throw IonException("Expected one value, found multiple")
                    secondValue == ExpressionType.CONTINUE_EXPANSION_ORDINAL -> ExpressionType.CONTINUE_EXPANSION_ORDINAL
                    secondValue == ExpressionType.END_OF_EXPANSION_ORDINAL -> secondValue
                    else -> unreachable()
                }
            }
        }

        private fun handleIfNone(): Byte = handleBranchIf() { it == 0 }
        private fun handleIfSome(): Byte = handleBranchIf() { it > 0 }
        private fun handleIfSingle(): Byte = handleBranchIf() { it == 1 }
        private fun handleIfMulti(): Byte = handleBranchIf() { it > 1 }

        private fun handleAnnotate(): Byte {
            val annotations = expander.map(0) {
                when (it) {
                    ExpressionType.DATA_MODEL_SCALAR_ORDINAL -> asSymbol(expander)
                    else -> unreachable("Unreachable without stepping in to a container")
                }
            }
            currentAnnotations = annotations
            expander.readArgument(1)

            val type = produceNext()
            if (type == ExpressionType.END_OF_EXPANSION_ORDINAL) {
                throw IonException("Can only annotate exactly one value")
            }
            return type
        }

        private fun handleMakeStringOrSymbol(ionType: IonType): Byte {
            expander.expansionKind = STREAM
            val sb = StringBuilder()
            expander.forEach(0) {
                when {
                    IonType.isText(it.ionType()) -> sb.append(it.readText())
                    else -> throw IonException("Invalid argument type for 'make_string': ${it.ionType()}")
                }
            }
            expander.tape.advanceToAfterEndEExpression(eExpressionIndex)
            expander.expansionKind = EMPTY
            expander.produceValueSideEffect(ionType, sb.toString())
            return ExpressionType.DATA_MODEL_SCALAR_ORDINAL
        }

        private fun handleMakeBlob(): Byte {
            expander.expansionKind = STREAM
            val baos = ByteArrayOutputStream()
            expander.forEach(0) {
                baos.write(it.readLob())
            }
            expander.expansionKind = EMPTY
            expander.produceValueSideEffect(IonType.BLOB, baos.toByteArray())
            return ExpressionType.DATA_MODEL_SCALAR_ORDINAL
        }

        private fun handleMakeDecimal(): Byte {
            expander.expansionKind = STREAM
            val coefficient = expander.readExactlyOneArgument(0, ::asBigInteger)
            val exponent = expander.readExactlyOneArgument(1, ::asBigInteger)
            expander.expansionKind = EMPTY
            expander.produceValueSideEffect(
                IonType.DECIMAL,
                BigDecimal(coefficient, -1 * exponent.intValueExact())
            )
            return ExpressionType.DATA_MODEL_SCALAR_ORDINAL
        }

        private fun handleMakeTimestamp(): Byte {
            expander.expansionKind = STREAM
            val year = expander.readExactlyOneArgument(0, ::asLong).toInt()
            val month = expander.readZeroOrOneArgument(1, ::asLong)?.toInt()
            val day = expander.readZeroOrOneArgument(2, ::asLong)?.toInt()
            val hour = expander.readZeroOrOneArgument(3, ::asLong)?.toInt()
            val minute = expander.readZeroOrOneArgument(4, ::asLong)?.toInt()
            val second = expander.readZeroOrOneArgument(5, ::asBigDecimal)
            val offsetMinutes = expander.readZeroOrOneArgument(6, ::asLong)?.toInt()

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
                expander.expansionKind = EMPTY
                expander.produceValueSideEffect(IonType.TIMESTAMP, ts)
                return ExpressionType.DATA_MODEL_SCALAR_ORDINAL
            } catch (e: IllegalArgumentException) {
                throw IonException(e.message)
            }
        }

        private fun handleSum(): Byte {
            val a = expander.readExactlyOneArgument(0, ::asBigInteger)
            val b = expander.readExactlyOneArgument(1, ::asBigInteger)
            expander.produceValueSideEffect(IonType.INT, a + b)
            expander.finishChildExpansion() // Finishes the sum expansion
            return ExpressionType.DATA_MODEL_SCALAR_ORDINAL
        }

        private fun handleDelta(): Byte {
            val expansionIndex = expander.expansionKindStackIndex
            val runningTotal = expander.additionalState as? BigInteger ?: BigInteger.ZERO
            if (!expander.visitChild()) {
                expander.readArgument(0)
                expander.expandChild(VARIABLE)
            }
            val nextExpandedArg = produceNext()
            expander.returnToExpansion(expansionIndex)
            when {
                ExpressionType.isDataModelValue(nextExpandedArg) -> {
                    val nextDelta = asBigInteger(expander)
                    val nextOutput = runningTotal + nextDelta
                    expander.produceValueSideEffect(IonType.INT, nextOutput)
                    expander.additionalState = nextOutput
                    return nextExpandedArg
                }

                nextExpandedArg == ExpressionType.END_OF_EXPANSION_ORDINAL -> {
                    expander.dropChildren(expansionIndex)
                    return ExpressionType.END_OF_EXPANSION_ORDINAL
                }

                else -> throw IonException("delta arguments must be integers")
            }
        }

        private fun handleRepeat(): Byte {
            expander.expansionKind = STREAM
            var n = expander.additionalState as Long?
            if (n == null) {
                n = expander.readExactlyOneArgument(0, ::asLong)
                if (n < 0) throw IonException("invalid argument; 'n' must be non-negative")
                expander.additionalState = n
            }

            if (n > 0) {
                expander.readArgument(1)
                expander.additionalState = n - 1
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

        private fun handlePrivateMakeFieldNameAndValue(): Byte {
            currentFieldName = expander.readExactlyOneArgument(0, ::asSymbol)
            expander.readArgument(1)
            expander.expansionKind = EXACTLY_ONE_VALUE_STREAM
            return ExpressionType.CONTINUE_EXPANSION_ORDINAL
        }

        private fun handleFlatten(): Byte {
            // TODO this needs more testing; probably needs corrections.
            expander.expansionKind = STREAM
            if (expander.additionalState == null) {
                expander.readArgument(0)
                expander.additionalState = 1
            }
            var next: Byte?
            if (!expander.hasChild()) {
                val nextSequence = produceNext()
                return when {
                    nextSequence == ExpressionType.DATA_MODEL_CONTAINER_ORDINAL -> {
                        expander.expandChild(STREAM)
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

        private fun handleBranchIf(condition: (Int) -> Boolean): Byte {
            val expansionIndex = expander.expansionKindStackIndex
            expander.readArgument(0)
            expander.expansionKind = STREAM // TODO should not have to do this, but makes tests pass. Probably need to push a child expansion
            var n = 0
            while (n < 2) {
                if (produceNext() == ExpressionType.END_OF_EXPANSION_ORDINAL) break
                n++
            }
            val branch = if (condition(n)) 1 else 2
            expander.tape.setNextAfterEndOfEExpression(expander.session.eExpressionIndex)
            expander.readArgument(branch)
            expander.dropChildren(expansionIndex)
            expander.finishChildExpansion() // This finishes the IfNone
            return ExpressionType.CONTINUE_EXPANSION_ORDINAL
        }

        private fun fieldName(): Byte {
            currentFieldName = expressionTape.context() as SymbolToken
            expressionTape.prepareNext()
            return ExpressionType.CONTINUE_EXPANSION_ORDINAL
        }

        private fun annotation(): Byte {
            currentAnnotations = expressionTape.annotations()
            expressionTape.prepareNext()
            return ExpressionType.CONTINUE_EXPANSION_ORDINAL
        }

        private fun eExpression(): Byte {
            val macro = expressionTape.context() as SystemMacro
            eExpressionIndex++
            // TODO do these three in one step? OR, change setNextAfterEndOfEExpression to skip the eexp i currently points at and then remove the next two lines
            expressionTape.prepareNext()
            expressionTape.next()
            expressionTape.setNextAfterEndOfEExpression(eExpressionIndex)
            expander.expandChild(macro.expansionKind)
            return ExpressionType.CONTINUE_EXPANSION_ORDINAL
        }

        private fun expressionGroup(): Byte {
            expressionTape.prepareNext()
            // TODO short-circuit if empty?
            expander.expandChild(EXPR_GROUP)
            return ExpressionType.CONTINUE_EXPANSION_ORDINAL
        }

        private fun dataModelScalar(): Byte {
            expressionTape.prepareNext()
            return ExpressionType.DATA_MODEL_SCALAR_ORDINAL
        }

        private fun dataModelContainer(): Byte {
            expressionTape.prepareNext()
            return ExpressionType.DATA_MODEL_CONTAINER_ORDINAL
        }

        private fun expressionEnd(nextType: Byte): Byte {
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

        private fun handleStream(): Byte {
            if (expressionTape.isExhausted) {
                return ExpressionType.END_OF_EXPANSION_ORDINAL
            }
            val nextType = expressionTape.type()
            if (ExpressionType.isEnd(nextType)) {
                return expressionEnd(nextType)
            }
            return when (nextType) {
                ExpressionType.FIELD_NAME_ORDINAL -> fieldName()
                ExpressionType.ANNOTATION_ORDINAL -> annotation()
                ExpressionType.E_EXPRESSION_ORDINAL -> eExpression()
                ExpressionType.EXPRESSION_GROUP_ORDINAL -> expressionGroup()
                ExpressionType.DATA_MODEL_SCALAR_ORDINAL -> dataModelScalar()
                ExpressionType.DATA_MODEL_CONTAINER_ORDINAL -> dataModelContainer()
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
            expressionTape = arguments
            expander.expansionKind = STREAM
            expander.tape = expressionTape
            expander.additionalState = null
            expander.expansionKindStack[0] = STREAM
            expander.expansionKindStackTop = 1
            expander.expansionKindStackIndex = 1
            expander.reachedEndOfExpression = false
            currentFieldName = fieldName
            currentAnnotations = null
            eExpressionIndex = -1;
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
            expansionKindStack[expansionKindStackTop++] = expansionKind
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
        fun readArgument(variableRef: Int) {
            tape.seekToArgument(session.eExpressionIndex, variableRef)
        }

        fun produceValueSideEffect(type: IonType, value: Any) {
            val sideEffectTape = session.sideEffects
            sideEffectTape.clear()
            sideEffectTape.addScalar(type, value)
            sideEffectTape.rewindTo(0)
            session.expressionTape = sideEffectTape
        }

        /**
         * Performs the given [action] for each value produced by the expansion of [variableRef].
         */
        inline fun forEach(variableRef: Int, action: (ExpressionTape) -> Unit) {
            val expansionIndex = session.expander.expansionKindStackIndex
            readArgument(variableRef)
            while (true) {
                val next = session.produceNext()
                when {
                    next == ExpressionType.END_OF_EXPANSION_ORDINAL -> break
                    ExpressionType.isDataModelExpression(next) -> action(tape)
                }
            }
            session.expander.dropChildren(expansionIndex)
        }

        /**
         * Performs the given [transform] on each value produced by the expansion of [variableRef], returning a list
         * of the results.
         */
        inline fun <T> map(variableRef: Int, transform: (Byte) -> T): List<T> {
            readArgument(variableRef)
            val result = mutableListOf<T>()
            while (true) {
                val next = session.produceNext()
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
            val expansionIndex = session.expander.expansionKindStackIndex
            readArgument(variableRef)
            // TODO possible to detect ahead of time whether this argument can return more than one value? It can't
            //  unless it's an invocation of certain system macros or it's an expression group
            session.expander.expandChild(VARIABLE)
            var argValue: T? = null
            while (true) {
                val it = session.produceNext()
                when {
                    ExpressionType.isDataModelValue(it) -> {
                        if (argValue == null) {
                            argValue = converter(session.expander)
                        } else {
                            throw IonException("invalid argument; too many values")
                        }
                    }

                    it == ExpressionType.END_OF_EXPANSION_ORDINAL -> break
                    it == ExpressionType.FIELD_NAME_ORDINAL -> unreachable("Unreachable without stepping into a container")
                }
            }
            session.expander.dropChildren(expansionIndex)
            session.expander.reachedEndOfExpression = false
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
    private val containerStack = Array<IonType?>(8) { null }
    private var depth = 0
    private var currentExpr: Byte? = null
    private var currentValueType: IonType? = null

    /**
     * Initialize the macro evaluator with an E-Expression.
     */
    fun initExpansion(fieldName: SymbolToken?, encodingExpressions: ExpressionTape) {
        session.reset(fieldName, encodingExpressions)
    }

    override fun next(): IonType? {
        currentValueType = null
        while (currentValueType == null) {
            currentExpr = session.produceNext()
            when {
                ExpressionType.isDataModelValue(currentExpr!!) -> currentValueType = session.expressionTape.ionType()
                currentExpr == ExpressionType.END_OF_EXPANSION_ORDINAL -> { // TODO should this go in produceNext?
                    if (session.expander.expansionKindStackTop > 1) {
                        session.expander.finishChildExpansion()
                        continue
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
        if (depth <= 0) throw IonException("Nothing to step out of.")
        depth--
        session.expressionTape.advanceToAfterEndContainer()
        session.currentFieldName = null
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
            if (++depth >= containerStack.size) {
                containerStack.copyOf(containerStack.size * 2)
            }
            containerStack[depth] = currentValueType
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
    override fun getDepth(): Int = depth
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

    override fun isInStruct(): Boolean = containerStack[depth] == IonType.STRUCT

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
    override fun longValue(): Long  = session.expressionTape.readLong()
    override fun bigIntegerValue(): BigInteger = session.expressionTape.readBigInteger()
    override fun getIntegerSize(): IntegerSize = session.expressionTape.readIntegerSize()
    override fun stringValue(): String = session.expressionTape.readText()
    override fun symbolValue(): SymbolToken = session.expressionTape.readSymbol()
    override fun bigDecimalValue(): BigDecimal = session.expressionTape.readBigDecimal()
    override fun byteSize(): Int = session.expressionTape.lobSize()
    override fun newBytes(): ByteArray = session.expressionTape.readLob().copyOf()
    override fun doubleValue(): Double = session.expressionTape.readFloat()
    override fun timestampValue(): Timestamp = session.expressionTape.readTimestamp()
    override fun isNullValue(): Boolean = session.expressionTape.isNullValue
    override fun booleanValue(): Boolean = session.expressionTape.readBoolean()
}
