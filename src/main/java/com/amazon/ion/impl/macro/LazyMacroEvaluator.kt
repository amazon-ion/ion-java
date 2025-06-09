package com.amazon.ion.impl.macro

import com.amazon.ion.*
import com.amazon.ion.impl.*
import com.amazon.ion.impl.macro.ExpansionKinds.*
import com.amazon.ion.impl.macro.Expression.*
import com.amazon.ion.util.*
import java.io.ByteArrayOutputStream
import java.math.BigDecimal
import java.math.BigInteger
import java.util.*

/**
 * Evaluates an EExpression from an [ExpressionTape].
 *
 * General Usage:
 *  - To start evaluating an e-expression, call [initExpansion]
 *  - Call [next] to get the next field name or value, or null
 *    if the end of the container or end of expansion has been reached.
 *  - Call [stepIn] when positioned on a container to step into that container.
 *  - Call [stepOut] to step out of the current container.
 *
 * TODO: Make expansion limit configurable.
 */
class LazyMacroEvaluator : IonReader {

    private val expansionLimit: Int = 1_000_000
    /** Internal state for tracking the number of expansion steps. */
    private var numExpandedExpressions = 0

    private val sideEffects: ExpressionTape = ExpressionTape(null, 4)
    private var invocationTape: ExpressionTape = ExpressionTape.EMPTY
    private var expressionTape: ExpressionTape = ExpressionTape.EMPTY
    private var initialFieldName: String? = null
    private var currentFieldName: String? = null
    private var currentAnnotations: List<SymbolToken>? = null

    private var eExpressionIndex = -1;

    private val containerStack = Array<IonType?>(8) { null }
    private var depth = 0
    private var currentExpr: Byte? = null
    private var currentValueType: IonType? = null

    /** The expansion kind as a byte constant. */
    private var expansionKind: Byte = UNINITIALIZED

    private var reachedEndOfExpression: Boolean = false

    /**
     * Field for storing any additional state required by an ExpansionKind.
     */
    private var additionalState: Any? = null

    private var expansionKindStack: ByteArray = ByteArray(16)
    private var expansionKindStackTop: Int = 0 // The index where the next child expansion would be stored
    private var expansionKindStackIndex: Int = 0 // The index after the expansion currently being evaluated

    /**
     * Initialize the macro evaluator with an E-Expression.
     */
    fun initExpansion(fieldName: String?, encodingExpressions: ExpressionTape) {
        numExpandedExpressions = 0
        invocationTape = encodingExpressions
        expressionTape = invocationTape
        expansionKind = STREAM
        additionalState = null
        expansionKindStack[0] = STREAM
        expansionKindStackTop = 1
        expansionKindStackIndex = 1
        reachedEndOfExpression = false
        initialFieldName = fieldName
        currentFieldName = null
        currentAnnotations = null
        eExpressionIndex = -1;
    }

    private fun handleVariable(): Byte {
        if (reachedEndOfExpression) { // TODO  remove reachedEndOfExpression, just pop immediately from the expansion stack when this would be set to true?
            return ExpressionType.END_OF_EXPANSION_ORDINAL
        }
        // TODO visit the child. Currently this isn't correct (though it may end up in the right spot in some tests) if the variable is an expression group (the child)
        val expression = handleStream()
        if (!hasChild()) {
            if (expression == ExpressionType.DATA_MODEL_SCALAR_ORDINAL) {
                reachedEndOfExpression = true
            }
        }
        return expression
    }

    private fun handleExactlyOneValueStream(): Byte {
        if (additionalState != 1) {
            val firstValue = handleStream()
            return when {
                ExpressionType.isDataModelExpression(firstValue) -> {
                    additionalState = 1
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

    private fun handleDefault(): Byte {
        // TODO not tested; previously, Default compiled down to IfNone. Invocation flattening optimizes away most
        //  Default invocations. The ones that make it here will have an un-flattenable system macro or non-empty
        //  expression group as the first argument.
        val expansionIndex = expansionKindStackIndex
        readArgument(0)
        expansionKind = STREAM // TODO should not have to do this, but makes tests pass. Probably need to push a child expansion
        var n = 0
        while (n < 1) {
            if (produceNext() == ExpressionType.END_OF_EXPANSION_ORDINAL) break
            n++
        }
        expressionTape.setNextAfterEndOfEExpression(eExpressionIndex)
        if (n == 0) readArgument(1) else readArgument(0)
        dropChildren(expansionIndex)
        finishChildExpansion() // This finishes the Default
        return ExpressionType.CONTINUE_EXPANSION_ORDINAL
    }

    private fun handleIfNone(): Byte = handleBranchIf() { it == 0 }
    private fun handleIfSome(): Byte = handleBranchIf() { it > 0 }
    private fun handleIfSingle(): Byte = handleBranchIf() { it == 1 }
    private fun handleIfMulti(): Byte = handleBranchIf() { it > 1 }

    private fun handleAnnotate(): Byte {
        val annotations = map(0) {
            when (it) {
                ExpressionType.DATA_MODEL_SCALAR_ORDINAL -> symbolValue()
                else -> unreachable("Unreachable without stepping in to a container")
            }
        }
        currentAnnotations = annotations
        readArgument(1)

        val type = produceNext()
        if (type == ExpressionType.END_OF_EXPANSION_ORDINAL) {
            throw IonException("Can only annotate exactly one value")
        }
        return type
    }

    private fun handleMakeStringOrSymbol(ionType: IonType): Byte {
        expansionKind = STREAM
        val sb = StringBuilder()
        forEach(0) {
            when {
                IonType.isText(it.ionType()) -> sb.append(it.readText())
                else -> throw IonException("Invalid argument type for 'make_string': ${it.ionType()}")
            }
        }
        expressionTape.advanceToAfterEndEExpression(eExpressionIndex)
        expansionKind = EMPTY
        produceValueSideEffect(ionType, sb.toString())
        return ExpressionType.DATA_MODEL_SCALAR_ORDINAL
    }

    private fun handleMakeBlob(): Byte {
        expansionKind = STREAM
        val baos = ByteArrayOutputStream()
        forEach(0) {
            baos.write(it.readLob())
        }
        expansionKind = EMPTY
        produceValueSideEffect(IonType.BLOB, baos.toByteArray())
        return ExpressionType.DATA_MODEL_SCALAR_ORDINAL
    }

    private fun handleMakeDecimal(): Byte {
        expansionKind = STREAM
        val coefficient = readExactlyOneArgument(0, ::bigIntegerValue)
        val exponent = readExactlyOneArgument(1, ::bigIntegerValue)
        expansionKind = EMPTY
        produceValueSideEffect(
            IonType.DECIMAL,
            BigDecimal(coefficient, -1 * exponent.intValueExact())
        )
        return ExpressionType.DATA_MODEL_SCALAR_ORDINAL
    }

    private fun handleMakeTimestamp(): Byte {
        expansionKind = STREAM
        val year = readExactlyOneArgument(0, ::longValue).toInt()
        val month = readZeroOrOneArgument(1, ::longValue)?.toInt()
        val day = readZeroOrOneArgument(2, ::longValue)?.toInt()
        val hour = readZeroOrOneArgument(3, ::longValue)?.toInt()
        val minute = readZeroOrOneArgument(4, ::longValue)?.toInt()
        val second = readZeroOrOneArgument(5, ::bigDecimalValue)
        val offsetMinutes = readZeroOrOneArgument(6, ::longValue)?.toInt()

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
            expansionKind = EMPTY
            produceValueSideEffect(IonType.TIMESTAMP, ts)
            return ExpressionType.DATA_MODEL_SCALAR_ORDINAL
        } catch (e: IllegalArgumentException) {
            throw IonException(e.message)
        }
    }

    private fun handleSum(): Byte {
        val a = readExactlyOneArgument(0, ::bigIntegerValue)
        val b = readExactlyOneArgument(1, ::bigIntegerValue)
        produceValueSideEffect(IonType.INT, a + b)
        finishChildExpansion() // Finishes the sum expansion
        return ExpressionType.DATA_MODEL_SCALAR_ORDINAL
    }

    private fun handleDelta(): Byte {
        val expansionIndex = expansionKindStackIndex
        val runningTotal = additionalState as? BigInteger ?: BigInteger.ZERO
        if (!visitChild()) {
            readArgument(0)
            expandChild(VARIABLE)
        }
        val nextExpandedArg = produceNext()
        returnToExpansion(expansionIndex)
        when {
            ExpressionType.isDataModelValue(nextExpandedArg) -> {
                val nextDelta = bigIntegerValue()
                val nextOutput = runningTotal + nextDelta
                produceValueSideEffect(IonType.INT, nextOutput)
                additionalState = nextOutput
                return nextExpandedArg
            }

            nextExpandedArg == ExpressionType.END_OF_EXPANSION_ORDINAL -> {
                dropChildren(expansionIndex)
                return ExpressionType.END_OF_EXPANSION_ORDINAL
            }

            else -> throw IonException("delta arguments must be integers")
        }
    }

    private fun handleRepeat(): Byte {
        expansionKind = STREAM
        var n = additionalState as Long?
        if (n == null) {
            n = readExactlyOneArgument(0, ::longValue)
            if (n < 0) throw IonException("invalid argument; 'n' must be non-negative")
            additionalState = n
        }

        if (n > 0) {
            readArgument(1)
            additionalState = n - 1
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
        currentFieldName = readExactlyOneArgument(0, ::stringValue)
        readArgument(1)
        expansionKind = EXACTLY_ONE_VALUE_STREAM
        return ExpressionType.CONTINUE_EXPANSION_ORDINAL
    }

    private fun handleFlatten(): Byte {
        // TODO this needs more testing; probably needs corrections.
        expansionKind = STREAM
        if (additionalState == null) {
            readArgument(0)
            additionalState = 1
        }
        var next: Byte?
        if (!hasChild()) {
            val nextSequence = produceNext()
            return when {
                nextSequence == ExpressionType.DATA_MODEL_CONTAINER_ORDINAL -> {
                    expandChild(STREAM)
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
        val expansionIndex = expansionKindStackIndex
        readArgument(0)
        expansionKind = STREAM // TODO should not have to do this, but makes tests pass. Probably need to push a child expansion
        var n = 0
        while (n < 2) {
            if (produceNext() == ExpressionType.END_OF_EXPANSION_ORDINAL) break
            n++
        }
        val branch = if (condition(n)) 1 else 2
        expressionTape.setNextAfterEndOfEExpression(eExpressionIndex)
        readArgument(branch)
        dropChildren(expansionIndex)
        finishChildExpansion() // This finishes the IfNone
        return ExpressionType.CONTINUE_EXPANSION_ORDINAL
    }

    private fun handleNone(): Byte {
        expressionTape.prepareNext()
        return ExpressionType.CONTINUE_EXPANSION_ORDINAL;
    }

    private fun skipTombstone(): Byte {
        expressionTape.skipTombstone()
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
        expandChild(macro.expansionKind)
        return ExpressionType.CONTINUE_EXPANSION_ORDINAL
    }

    private fun expressionGroup(): Byte {
        expressionTape.prepareNext()
        // TODO short-circuit if empty?
        expandChild(EXPR_GROUP)
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
        currentFieldName = initialFieldName ?: expressionTape.fieldName() ?: currentFieldName
        initialFieldName = null
        return when (nextType) {
            ExpressionType.NONE_ORDINAL -> handleNone()
            ExpressionType.TOMBSTONE_ORDINAL -> skipTombstone()
            ExpressionType.ANNOTATION_ORDINAL -> annotation()
            ExpressionType.E_EXPRESSION_ORDINAL -> eExpression()
            ExpressionType.EXPRESSION_GROUP_ORDINAL -> expressionGroup()
            ExpressionType.DATA_MODEL_SCALAR_ORDINAL -> dataModelScalar()
            ExpressionType.DATA_MODEL_CONTAINER_ORDINAL -> dataModelContainer()
            else -> unreachable()
        }
    }

    private fun expandChild(expansionKind: Byte) {
        if (expansionKindStackTop == expansionKindStack.size) {
            expansionKindStack = expansionKindStack.copyOf(expansionKindStack.size * 2)
        }
        expansionKindStack[expansionKindStackTop++] = expansionKind
        expansionKindStackIndex++
        this.expansionKind = expansionKind
    }

    private fun hasChild(): Boolean {
        return expansionKindStackIndex < expansionKindStackTop
    }

    private fun visitChild(): Boolean {
        if (expansionKindStackIndex < expansionKindStackTop) {
            expansionKind = expansionKindStack[expansionKindStackIndex++]
            return true
        }
        return false
    }

    private fun dropChildren(index: Int) {
        expansionKindStackIndex = index
        expansionKindStackTop = index
        expansionKind = expansionKindStack[index - 1]
    }

    private fun returnToExpansion(index: Int) {
        expansionKindStackIndex = index
        expansionKind = expansionKindStack[index - 1]
    }

    private fun finishChildExpansion() {
        if (expansionKindStackTop == 1) {
            return
        }
        this.expansionKind = expansionKindStack[--expansionKindStackTop - 1]
        expansionKindStackIndex = Math.min(expansionKindStackIndex, expansionKindStackTop)
    }

    /**
     * Returns an expansion for the given variable.
     */
    private fun readArgument(variableRef: Int) {
        expressionTape.seekToArgument(eExpressionIndex, variableRef)
    }

    private fun produceValueSideEffect(type: IonType, value: Any) {
        sideEffects.clear()
        // TODO try something lighter-weight than this
        sideEffects.addScalar(type, value, currentFieldName)
        sideEffects.rewindTo(0)
        expressionTape = sideEffects
    }

    /**
     * Performs the given [action] for each value produced by the expansion of [variableRef].
     */
    private inline fun forEach(variableRef: Int, action: (ExpressionTape) -> Unit) {
        val expansionIndex = expansionKindStackIndex
        readArgument(variableRef)
        while (true) {
            val next = produceNext()
            when {
                next == ExpressionType.END_OF_EXPANSION_ORDINAL -> break
                ExpressionType.isDataModelExpression(next) -> action(expressionTape)
            }
        }
        dropChildren(expansionIndex)
    }

    /**
     * Performs the given [transform] on each value produced by the expansion of [variableRef], returning a list
     * of the results.
     */
    private inline fun <T> map(variableRef: Int, transform: (Byte) -> T): List<T> {
        readArgument(variableRef)
        val result = mutableListOf<T>()
        while (true) {
            val next = produceNext()
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
    private inline fun <T> readZeroOrOneArgument(
        variableRef: Int,
        converter: () -> T
    ): T? {
        val expansionIndex = expansionKindStackIndex
        readArgument(variableRef)
        // TODO possible to detect ahead of time whether this argument can return more than one value? It can't
        //  unless it's an invocation of certain system macros or it's an expression group
        expandChild(VARIABLE)
        var argValue: T? = null
        while (true) {
            val it = produceNext()
            when {
                ExpressionType.isDataModelValue(it) -> {
                    if (argValue == null) {
                        argValue = converter()
                    } else {
                        throw IonException("invalid argument; too many values")
                    }
                }

                it == ExpressionType.END_OF_EXPANSION_ORDINAL -> break
            }
        }
        dropChildren(expansionIndex)
        reachedEndOfExpression = false
        return argValue
    }

    /**
     * Reads and returns exactly one value from the expansion of the given [variableRef].
     * Throws an [IonException] if the expansion of [variableRef] does not produce exactly one value.
     * Throws an [IonException] if the value is not the expected type [T].
     */
    private inline fun <T> readExactlyOneArgument(variableRef: Int, converter: () -> T): T {
        return readZeroOrOneArgument(variableRef, converter)
            ?: throw IonException("invalid argument; no value when one is expected")
    }

    private fun produceNext(): Byte {
        expressionTape = invocationTape
        while (true) {
            expressionTape.next()
            val next = when (expansionKind) {
                UNINITIALIZED -> throw IllegalStateException("ExpansionInfo not initialized.")
                EMPTY -> ExpressionType.END_OF_EXPANSION_ORDINAL
                STREAM -> handleStream()
                VARIABLE -> handleVariable()
                EXPR_GROUP -> handleStream()
                EXACTLY_ONE_VALUE_STREAM -> handleExactlyOneValueStream()
                DEFAULT -> handleDefault()
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
                else -> throw IllegalStateException("Unknown expansion kind: $expansionKind")
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

    override fun next(): IonType? {
        currentValueType = null
        while (currentValueType == null) {
            currentExpr = produceNext()
            when {
                ExpressionType.isDataModelValue(currentExpr!!) -> currentValueType = expressionTape.ionType()
                currentExpr == ExpressionType.END_OF_EXPANSION_ORDINAL -> { // TODO should this go in produceNext?
                    if (expansionKindStackTop > 1) {
                        finishChildExpansion()
                        continue
                    }
                    currentFieldName = null
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
        if (depth <= 0) throw IonException("Nothing to step out of.")
        depth--
        expressionTape.advanceToAfterEndContainer()
        currentFieldName = null
        currentValueType = null // Must call `next()` to get the next value
        currentAnnotations = null
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
            currentFieldName = null
            currentValueType = null
            currentAnnotations = null
        } else {
            throw IonException("Not positioned on a container.")
        }
    }

    override fun close() { /* Nothing to do (yet) */ }
    override fun <T : Any?> asFacet(facetType: Class<T>?): Nothing? = null
    override fun getDepth(): Int = depth
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

    override fun isInStruct(): Boolean = containerStack[depth] == IonType.STRUCT

    override fun getFieldId(): Int = -1
    override fun getFieldName(): String? = currentFieldName
    override fun getFieldNameSymbol(): SymbolToken? = if (currentFieldName == null) null else _Private_Utils.newSymbolToken(currentFieldName)

    /** TODO: Throw on data loss */
    override fun intValue(): Int = longValue().toInt()
    override fun decimalValue(): Decimal = Decimal.valueOf(bigDecimalValue())
    override fun dateValue(): Date = timestampValue().dateValue()

    override fun getBytes(buffer: ByteArray?, offset: Int, len: Int): Int {
        TODO("Not yet implemented")
    }
    override fun longValue(): Long  = expressionTape.readLong()
    override fun bigIntegerValue(): BigInteger = expressionTape.readBigInteger()
    override fun getIntegerSize(): IntegerSize = expressionTape.readIntegerSize()
    override fun stringValue(): String = expressionTape.readText()
    override fun symbolValue(): SymbolToken = expressionTape.readSymbol()
    override fun bigDecimalValue(): BigDecimal = expressionTape.readBigDecimal()
    override fun byteSize(): Int = expressionTape.lobSize()
    override fun newBytes(): ByteArray = expressionTape.readLob().copyOf()
    override fun doubleValue(): Double = expressionTape.readFloat()
    override fun timestampValue(): Timestamp = expressionTape.readTimestamp()
    override fun isNullValue(): Boolean = expressionTape.isNullValue
    override fun booleanValue(): Boolean = expressionTape.readBoolean()
}
