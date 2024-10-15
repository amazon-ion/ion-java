// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.macro

import com.amazon.ion.Decimal
import com.amazon.ion.IntegerSize
import com.amazon.ion.IonReader
import com.amazon.ion.IonType
import com.amazon.ion.SymbolTable
import com.amazon.ion.SymbolToken
import com.amazon.ion.Timestamp
import com.amazon.ion.impl._Private_RecyclingStack
import java.math.BigDecimal
import java.math.BigInteger
import java.util.*

/**
 * This class is an example of how we might wrap the macro evaluator's [Expression] model, adapting it to an [IonReader].
 *
 * TODO:
 *   - Consider merging this with [MacroEvaluator].
 *   - Error handling is inconsistent with other [IonReader] implementations
 *   - Testing
 */
class MacroEvaluatorAsIonReader(
    private val evaluator: MacroEvaluator,
) : IonReader {

    private class ContainerInfo {
        @JvmField var currentFieldName: Expression.FieldName? = null
        @JvmField var container: Expression.DataModelContainer? = null
    }
    private val containerStack = _Private_RecyclingStack(8) { ContainerInfo() }

    private var currentFieldName: Expression.FieldName? = null
    private var currentValueExpression: Expression.DataModelValue? = null

    private var queuedFieldName: Expression.FieldName? = null
    private var queuedValueExpression: Expression.DataModelValue? = null

    private fun queueNext() {
        queuedValueExpression = null
        while (queuedValueExpression == null) {
            val nextCandidate = evaluator.expandNext() ?: return
            when (nextCandidate) {
                is Expression.FieldName -> queuedFieldName = nextCandidate
                is Expression.DataModelValue -> queuedValueExpression = nextCandidate
            }
        }
    }

    @Deprecated("Deprecated in Java")
    override fun hasNext(): Boolean {
        if (queuedValueExpression == null) queueNext()
        return queuedValueExpression != null
    }

    override fun next(): IonType? {
        if (!hasNext()) {
            currentValueExpression = null
            return null
        }
        currentValueExpression = queuedValueExpression
        currentFieldName = queuedFieldName
        queuedValueExpression = null
        return getType()
    }

    override fun stepIn() {
        // This is essentially a no-op for Lists and SExps
        containerStack.peek()?.currentFieldName = this.currentFieldName

        val containerToStepInto = currentValueExpression
        evaluator.stepIn()
        containerStack.push {
            it.container = containerToStepInto as Expression.DataModelContainer
            it.currentFieldName = null
        }
        currentFieldName = null
        currentValueExpression = null
        queuedFieldName = null
        queuedValueExpression = null
    }

    override fun stepOut() {
        evaluator.stepOut()
        containerStack.pop()
        // This is essentially a no-op for Lists and SExps
        currentFieldName = containerStack.peek()?.currentFieldName
        currentValueExpression = null // Must call `next()` to get the next value
        queuedFieldName = null
        queuedValueExpression = null
    }

    override fun close() { /* Nothing to do (yet) */ }
    override fun <T : Any?> asFacet(facetType: Class<T>?): Nothing? = null
    override fun getDepth(): Int = containerStack.size()
    override fun getSymbolTable(): SymbolTable? = null

    override fun getType(): IonType? = currentValueExpression?.type

    fun hasAnnotations(): Boolean = currentValueExpression != null && currentValueExpression!!.annotations.isNotEmpty()

    override fun getTypeAnnotations(): Array<String>? = currentValueExpression?.annotations?.let { Array(it.size) { i -> it[i].assumeText() } }
    override fun getTypeAnnotationSymbols(): Array<SymbolToken>? = currentValueExpression?.annotations?.toTypedArray()
    // TODO: Make this into an iterator that unwraps the SymbolTokens as it goes instead of allocating a new list
    override fun iterateTypeAnnotations(): MutableIterator<String> {
        return currentValueExpression?.annotations?.mapTo(mutableListOf()) { it.assumeText() }?.iterator()
            ?: return Collections.emptyIterator()
    }

    override fun isInStruct(): Boolean = TODO("Not yet implemented")

    override fun getFieldId(): Int = currentFieldName?.value?.sid ?: 0
    override fun getFieldName(): String? = currentFieldName?.value?.text
    override fun getFieldNameSymbol(): SymbolToken? = currentFieldName?.value

    override fun isNullValue(): Boolean = currentValueExpression is Expression.NullValue
    override fun booleanValue(): Boolean = (currentValueExpression as Expression.BoolValue).value

    override fun getIntegerSize(): IntegerSize {
        // TODO: Make this more efficient, more precise
        return when (val intExpression = currentValueExpression as Expression.IntValue) {
            is Expression.LongIntValue -> if (intExpression.value.toInt().toLong() == intExpression.value) {
                IntegerSize.INT
            } else {
                IntegerSize.LONG
            }
            is Expression.BigIntValue -> IntegerSize.BIG_INTEGER
        }
    }

    /** TODO: Throw on data loss */
    override fun intValue(): Int = longValue().toInt()

    override fun longValue(): Long = when (val intExpression = currentValueExpression as Expression.IntValue) {
        is Expression.LongIntValue -> intExpression.value
        is Expression.BigIntValue -> intExpression.value.longValueExact()
    }

    override fun bigIntegerValue(): BigInteger = when (val intExpression = currentValueExpression as Expression.IntValue) {
        is Expression.LongIntValue -> intExpression.value.toBigInteger()
        is Expression.BigIntValue -> intExpression.value
    }

    override fun doubleValue(): Double = (currentValueExpression as Expression.FloatValue).value
    override fun bigDecimalValue(): BigDecimal = (currentValueExpression as Expression.DecimalValue).value
    override fun decimalValue(): Decimal = Decimal.valueOf(bigDecimalValue())
    override fun timestampValue(): Timestamp = (currentValueExpression as Expression.TimestampValue).value
    override fun dateValue(): Date = timestampValue().dateValue()
    override fun stringValue(): String = (currentValueExpression as Expression.TextValue).stringValue
    override fun symbolValue(): SymbolToken = (currentValueExpression as Expression.SymbolValue).value
    override fun byteSize(): Int = (currentValueExpression as Expression.LobValue).value.size
    override fun newBytes(): ByteArray = (currentValueExpression as Expression.LobValue).value.copyOf()

    override fun getBytes(buffer: ByteArray?, offset: Int, len: Int): Int {
        TODO("Not yet implemented")
    }
}
