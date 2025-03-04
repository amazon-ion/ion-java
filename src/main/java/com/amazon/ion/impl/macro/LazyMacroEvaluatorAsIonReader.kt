// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.macro

import com.amazon.ion.*
import com.amazon.ion.impl.*
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
class LazyMacroEvaluatorAsIonReader(
    private val evaluator: LazyMacroEvaluator,
) : IonReader {

    private class ContainerInfo {
        @JvmField var currentFieldName: SymbolToken? = null
        @JvmField var container: IonType? = null
    }
    private val containerStack = _Private_RecyclingStack(8) { ContainerInfo() }

    private var currentFieldName: SymbolToken? = null
    private var currentAnnotations: List<SymbolToken>? = null
    private var currentValueType: IonType? = null

    private var queuedFieldName: SymbolToken? = null
    private var queuedAnnotations: List<SymbolToken>? = null
    private var queuedValueType: IonType? = null

    private fun queueNext() {
        queuedValueType = null
        while (queuedValueType == null) {
            val nextCandidate = evaluator.expandNext()
            when {
                nextCandidate == null -> {
                    queuedFieldName = null
                    queuedAnnotations = null
                    return
                }
                nextCandidate == ExpressionType.FIELD_NAME -> queuedFieldName = evaluator.currentFieldName()
                nextCandidate == ExpressionType.ANNOTATION -> queuedAnnotations = evaluator.currentAnnotations()
                nextCandidate.isDataModelValue -> queuedValueType = evaluator.currentValueType()
            }
        }
    }

    @Deprecated("Deprecated in Java")
    override fun hasNext(): Boolean {
        if (queuedValueType == null) queueNext()
        return queuedValueType != null
    }

    override fun next(): IonType? {
        if (!hasNext()) {
            currentValueType = null
            return null
        }
        currentValueType = queuedValueType
        currentFieldName = queuedFieldName
        currentAnnotations = queuedAnnotations
        queuedValueType = null
        return currentValueType
    }

    /**
     * Transcodes the e-expression argument expressions provided to this MacroEvaluator
     * without evaluation.
     * @param writer the writer to which the expressions will be transcoded.
     */
    fun transcodeArgumentsTo(writer: MacroAwareIonWriter) {
        var index = 0
        val arguments: ExpressionTape = evaluator.getArgumentTape()
        arguments.rewindTo(0)

        // TODO all the null-setting within the when branches might not be necessary

        currentAnnotations = null // Annotations are written only via Annotation expressions
        currentFieldName = null // Field names are written only via FieldName expressions
        while (index < arguments.size()) {
            currentValueType = null
            arguments.next()
            arguments.prepareNext()
            val argument = arguments.type()
            if (argument.isEnd) {
                currentAnnotations = null
                currentFieldName = null
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
                    currentFieldName = null
                    currentAnnotations = null
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
                    currentFieldName = null
                    currentAnnotations = null
                }
                ExpressionType.FIELD_NAME -> {
                    currentFieldName = arguments.context() as SymbolToken
                    writer.setFieldNameSymbol(currentFieldName)
                }
                ExpressionType.E_EXPRESSION -> {
                    writer.startMacro(arguments.context() as Macro)
                    currentAnnotations = null
                    currentFieldName = null
                }
                ExpressionType.EXPRESSION_GROUP -> {
                    writer.startExpressionGroup()
                    currentAnnotations = null
                    currentFieldName = null
                }
                else -> throw IllegalStateException("Unexpected branch")
            }
            index++
        }
        arguments.rewindTo(0)
    }

    override fun stepIn() {
        // This is essentially a no-op for Lists and SExps
        containerStack.peek()?.currentFieldName = this.currentFieldName

        val containerToStepInto = currentValueType
        evaluator.stepIn()
        val it = containerStack.push { _ -> }
        it.container = containerToStepInto
        it.currentFieldName = null
        currentFieldName = null
        currentValueType = null
        currentAnnotations = null
        queuedFieldName = null
        queuedValueType = null
        queuedAnnotations = null
    }

    override fun stepOut() {
        evaluator.stepOut()
        containerStack.pop()
        // This is essentially a no-op for Lists and SExps
        currentFieldName = containerStack.peek()?.currentFieldName
        currentValueType = null // Must call `next()` to get the next value
        currentAnnotations = null
        queuedFieldName = null
        queuedValueType = null
        queuedAnnotations = null
    }

    override fun close() { /* Nothing to do (yet) */ }
    override fun <T : Any?> asFacet(facetType: Class<T>?): Nothing? = null
    override fun getDepth(): Int = containerStack.size()
    override fun getSymbolTable(): SymbolTable? = null

    override fun getType(): IonType? = currentValueType

    fun hasAnnotations(): Boolean = currentAnnotations != null && currentAnnotations!!.isNotEmpty()

    override fun getTypeAnnotations(): Array<String>? = currentAnnotations?.let { Array(it.size) { i -> it[i].assumeText() } }
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

    override fun getFieldId(): Int = currentFieldName?.sid ?: 0
    override fun getFieldName(): String? = currentFieldName?.text
    override fun getFieldNameSymbol(): SymbolToken? = currentFieldName

    override fun isNullValue(): Boolean = evaluator.isNullValue()
    override fun booleanValue(): Boolean = evaluator.booleanValue()

    override fun getIntegerSize(): IntegerSize = evaluator.getIntegerSize()

    /** TODO: Throw on data loss */
    override fun intValue(): Int = longValue().toInt()

    override fun longValue(): Long = evaluator.longValue()

    override fun bigIntegerValue(): BigInteger = evaluator.bigIntegerValue()

    override fun doubleValue(): Double = evaluator.doubleValue()
    override fun bigDecimalValue(): BigDecimal = evaluator.bigDecimalValue()
    override fun decimalValue(): Decimal = Decimal.valueOf(bigDecimalValue())
    override fun timestampValue(): Timestamp = evaluator.timestampValue()
    override fun dateValue(): Date = timestampValue().dateValue()
    override fun stringValue(): String = evaluator.stringValue()
    override fun symbolValue(): SymbolToken = evaluator.symbolValue()
    override fun byteSize(): Int = evaluator.lobSize()
    override fun newBytes(): ByteArray = evaluator.lobValue().copyOf()

    override fun getBytes(buffer: ByteArray?, offset: Int, len: Int): Int {
        TODO("Not yet implemented")
    }
}
