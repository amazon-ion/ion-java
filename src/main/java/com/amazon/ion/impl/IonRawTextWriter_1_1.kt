// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl

import com.amazon.ion.*
import com.amazon.ion.impl.IonRawTextWriter_1_1.ContainerType.*
import com.amazon.ion.impl.macro.*
import com.amazon.ion.util.*
import java.math.BigDecimal
import java.math.BigInteger

/**
 * A raw writer for Ion 1.1 text. This should be combined with managed writer to handle concerns such as macros and
 * possible symbol interning.
 *
 * Notes:
 *  - Never writes using "long string" syntax in order to simplify the writer.
 *  - Does not try to resolve symbol tokens. That is the concern of the managed writer.
 *  - To make it easier to concatenate streams, this eagerly emits a top-level separator after each top-level syntax item.
 */
class IonRawTextWriter_1_1 internal constructor(
    private val options: _Private_IonTextWriterBuilder_1_1,
    private val output: _Private_IonTextAppender,
) : IonRawWriter_1_1, `PrivateIonRawWriter_1_1` {

    companion object {
        const val IVM = "\$ion_1_1"
    }

    enum class ContainerType {
        List,
        SExp,
        Struct,
        EExpression,
        ExpressionGroup,
        Top,
    }

    private var closed = false

    private val ancestorContainersStack: ArrayList<ContainerType> = ArrayList()
    private var currentContainer: ContainerType = Top
    private var currentContainerHasValues = false

    private var isPendingSeparator = false
    private var isPendingLeadingWhitespace = false

    private var fieldNameText: CharSequence? = null
    private var fieldNameId: Int = -1
    private var hasFieldName = false

    private var annotationsTextBuffer = arrayOfNulls<CharSequence>(8)
    private var annotationsIdBuffer = IntArray(8)
    private var numAnnotations = 0

    private inline fun openValue(valueWriterExpression: () -> Unit) {
        if (currentContainer == Struct) {
            confirm(hasFieldName) { "Values in a struct require a field name." }
        }
        val separatorCharacter = when (currentContainer) {
            List, Struct -> ","
            EExpression, SExp, ExpressionGroup -> " "
            Top -> options.topLevelSeparator()
        }

        if (options.isPrettyPrintOn && !forceNoNewlines) {
            if (isPendingSeparator && !IonTextUtils.isAllWhitespace(separatorCharacter)) {
                // Only bother if the separator is non-whitespace.
                output.appendAscii(separatorCharacter)
            }
            if (isPendingSeparator || isPendingLeadingWhitespace) {
                output.appendAscii(options.lineSeparator())
                output.appendAscii(" ".repeat(ancestorContainersStack.size * 2))
            }
        } else if (isPendingSeparator) {
            output.appendAscii(separatorCharacter)
        }

        isPendingSeparator = false

        if (hasFieldName) {
            if (fieldNameText != null) {
                output.printSymbol(fieldNameText)
                output.appendAscii(':')
                if (options.isPrettyPrintOn) output.appendAscii(" ")
                fieldNameText = null
            } else {
                output.appendAscii("$")
                output.printInt(fieldNameId.toLong())
                output.appendAscii(":")
                if (options.isPrettyPrintOn) output.appendAscii(" ")
                fieldNameId = -1
            }
        }

        for (i in 0 until numAnnotations) {
            if (annotationsTextBuffer[i] != null) {
                output.printSymbol(annotationsTextBuffer[i])
                annotationsTextBuffer[i] = null
            } else {
                output.appendAscii("$")
                output.printInt(annotationsIdBuffer[i].toLong())
                annotationsIdBuffer[i] = -1
            }
            output.appendAscii("::")
        }

        hasFieldName = false
        numAnnotations = 0
        valueWriterExpression()
    }

    private inline fun closeValue(valueWriterExpression: () -> Unit) {
        valueWriterExpression()
        if (currentContainer == Top) {
            output.appendAscii(options.topLevelSeparator())
            isPendingSeparator = false
        } else {
            isPendingSeparator = true
        }
        isPendingLeadingWhitespace = false
        currentContainerHasValues = true
    }

    private inline fun writeScalar(valueWriterExpression: () -> Unit) {
        // Note—it doesn't matter which order we combine these. The result will be the same because of where
        // valueWriterExpression is called in openValue and closeValue.
        openValue { closeValue(valueWriterExpression) }
    }

    override fun close() {
        if (closed) return
        flush()
        output.close()
        closed = true
    }

    override fun flush() {
        if (closed) return
        confirm(depth() == 0) { "Cannot call finish() while in a container" }
        confirm(numAnnotations == 0) { "Cannot call finish with dangling annotations" }
        output.flush()
    }

    override fun writeIVM() {
        confirm(currentContainer == Top) { "IVM can only be written at the top level of an Ion stream." }
        confirm(numAnnotations == 0) { "Cannot write an IVM with annotations" }
        output.appendAscii(IVM)
        output.appendAscii(options.topLevelSeparator())
        isPendingSeparator = false
    }

    override fun isInStruct(): Boolean = currentContainer == Struct

    override fun depth(): Int = ancestorContainersStack.size

    /**
     * Ensures that there is enough space in the annotation buffers for [n] annotations.
     * If more space is needed, it over-allocates by 8 to ensure that we're not continually allocating when annotations
     * are being added one by one.
     */
    private inline fun ensureAnnotationSpace(n: Int) {
        // We only need to check the size of one of the arrays because we always keep them the same size.
        if (annotationsIdBuffer.size < n) {
            val oldIds = annotationsIdBuffer
            annotationsIdBuffer = IntArray(n + 8)
            oldIds.copyInto(annotationsIdBuffer)
            val oldText = annotationsTextBuffer
            annotationsTextBuffer = arrayOfNulls(n + 8)
            oldText.copyInto(annotationsTextBuffer)
        }
    }

    override fun writeAnnotations(annotation0: Int) {
        ensureAnnotationSpace(numAnnotations + 1)
        annotationsIdBuffer[numAnnotations++] = annotation0
    }

    override fun writeAnnotations(annotation0: Int, annotation1: Int) {
        ensureAnnotationSpace(numAnnotations + 2)
        annotationsIdBuffer[numAnnotations++] = annotation0
        annotationsIdBuffer[numAnnotations++] = annotation1
    }

    override fun writeAnnotations(annotations: IntArray) {
        ensureAnnotationSpace(numAnnotations + annotations.size)
        annotations.copyInto(annotationsIdBuffer, numAnnotations)
        numAnnotations += annotations.size
    }

    override fun writeAnnotations(annotation0: CharSequence) {
        ensureAnnotationSpace(numAnnotations + 1)
        annotationsTextBuffer[numAnnotations++] = annotation0
    }

    override fun writeAnnotations(annotation0: CharSequence, annotation1: CharSequence) {
        ensureAnnotationSpace(numAnnotations + 2)
        annotationsTextBuffer[numAnnotations++] = annotation0
        annotationsTextBuffer[numAnnotations++] = annotation1
    }

    override fun writeAnnotations(annotations: Array<CharSequence>) {
        if (annotations.isEmpty()) return
        ensureAnnotationSpace(numAnnotations + annotations.size)
        annotations.copyInto(annotationsTextBuffer, numAnnotations)
        numAnnotations += annotations.size
    }

    override fun _private_clearAnnotations() {
        numAnnotations = 0
    }

    override fun _private_hasFirstAnnotation(sid: Int, text: String?): Boolean {
        if (numAnnotations == 0) return false
        if (sid >= 0 && annotationsIdBuffer[0] == sid) {
            return true
        }
        if (text != null && annotationsTextBuffer[0] == text) {
            return true
        }
        return false
    }

    override fun _private_hasFieldName(): Boolean = hasFieldName

    override fun writeFieldName(sid: Int) {
        confirm(currentContainer == Struct) { "Cannot write field name outside of a struct." }
        confirm(!hasFieldName) { "Field name already set." }
        fieldNameId = sid
        hasFieldName = true
    }

    override fun writeFieldName(text: CharSequence) {
        confirm(currentContainer == Struct) { "Cannot write field name outside of a struct." }
        confirm(!hasFieldName) { "Field name already set." }
        fieldNameText = text
        hasFieldName = true
    }

    override fun writeNull() = writeScalar {
        output.appendAscii("null")
    }

    override fun writeNull(type: IonType) = writeScalar {
        val nullimage = if (options._untyped_nulls) { "null" } else {
            when (type) {
                IonType.NULL -> "null"
                IonType.BOOL -> "null.bool"
                IonType.INT -> "null.int"
                IonType.FLOAT -> "null.float"
                IonType.DECIMAL -> "null.decimal"
                IonType.TIMESTAMP -> "null.timestamp"
                IonType.SYMBOL -> "null.symbol"
                IonType.STRING -> "null.string"
                IonType.BLOB -> "null.blob"
                IonType.CLOB -> "null.clob"
                IonType.SEXP -> "null.sexp"
                IonType.LIST -> "null.list"
                IonType.STRUCT -> "null.struct"
                else -> throw IllegalStateException("unexpected type $type")
            }
        }
        output.appendAscii(nullimage)
    }

    override fun writeBool(value: Boolean) = writeScalar { output.appendAscii(if (value) "true" else "false") }

    override fun writeInt(value: Long) = writeScalar { output.printInt(value) }
    override fun writeInt(value: BigInteger) = writeScalar { output.printInt(value) }

    override fun writeFloat(value: Float) = writeFloat(value.toDouble())
    override fun writeFloat(value: Double) = writeScalar { output.printFloat(options, value) }

    override fun writeDecimal(value: BigDecimal) = writeScalar { output.printDecimal(options, value) }

    override fun writeTimestamp(value: Timestamp) = writeScalar {
        writeTimestampHelper(
            toMillis = { value.millis },
            toString = { value.toString() },
        )
    }

    private inline fun writeTimestampHelper(toMillis: () -> Long, toString: () -> String) {
        if (options._timestamp_as_millis) {
            output.appendAscii("${toMillis()}")
        } else if (options._timestamp_as_string) {
            // Timestamp is ASCII-safe so this is easy
            output.appendAscii('"')
            output.appendAscii(toString())
            output.appendAscii('"')
        } else {
            output.appendAscii(toString())
        }
    }

    override fun writeSymbol(id: Int) = writeScalar {
        output.appendAscii('$')
        output.printInt(id.toLong())
    }

    override fun writeSymbol(text: CharSequence) = writeScalar {
        when (IonTextUtils.symbolVariant(text)) {
            IonTextUtils.SymbolVariant.IDENTIFIER -> output.appendAscii(text)
            IonTextUtils.SymbolVariant.OPERATOR -> if (currentContainer == SExp) output.appendAscii(text) else output.printQuotedSymbol(text)
            IonTextUtils.SymbolVariant.QUOTED -> output.printQuotedSymbol(text)
        }
    }

    override fun writeString(value: CharSequence) = writeScalar { output.printString(value) }

    override fun writeBlob(value: ByteArray, start: Int, length: Int) = writeScalar { output.printBlob(options, value, start, length) }

    override fun writeClob(value: ByteArray, start: Int, length: Int) = writeScalar { output.printClob(options, value, start, length) }

    override fun stepInList(usingLengthPrefix: Boolean) {
        openValue { output.appendAscii("[") }
        ancestorContainersStack.add(currentContainer)
        currentContainer = List
        currentContainerHasValues = false
        isPendingLeadingWhitespace = true
    }

    override fun stepInSExp(usingLengthPrefix: Boolean) {
        openValue { output.appendAscii("(") }
        ancestorContainersStack.add(currentContainer)
        currentContainer = SExp
        currentContainerHasValues = false
        isPendingLeadingWhitespace = true
    }

    override fun stepInStruct(usingLengthPrefix: Boolean) {
        openValue { output.appendAscii("{") }
        ancestorContainersStack.add(currentContainer)
        currentContainer = Struct
        currentContainerHasValues = false
        isPendingLeadingWhitespace = true
    }

    override fun stepInEExp(name: CharSequence) {
        confirm(numAnnotations == 0) { "Cannot annotate a macro invocation" }
        openValue {
            output.appendAscii("(:")
            output.printSymbol(name)
        }
        ancestorContainersStack.add(currentContainer)
        currentContainer = EExpression
        currentContainerHasValues = false
        isPendingSeparator = true // Treat the macro name as if it is a value that needs a separator.
    }

    override fun stepInEExp(id: Int, usingLengthPrefix: Boolean, macro: Macro) {
        confirm(numAnnotations == 0) { "Cannot annotate a macro invocation" }
        openValue {
            output.appendAscii("(:")
            output.printInt(id.toLong())
        }
        ancestorContainersStack.add(currentContainer)
        currentContainer = EExpression
        currentContainerHasValues = false
        isPendingSeparator = true // Treat the macro id as if it is a value that needs a separator.
    }

    override fun stepInExpressionGroup(usingLengthPrefix: Boolean) {
        confirm(numAnnotations == 0) { "Cannot annotate an expression group" }
        confirm(currentContainer == EExpression) { "Can only create an expression group in a macro invocation" }
        openValue { output.appendAscii("(:") }
        ancestorContainersStack.add(currentContainer)
        currentContainer = ExpressionGroup
        currentContainerHasValues = false
        isPendingLeadingWhitespace = true
        isPendingSeparator = true
    }

    override fun stepOut() {
        confirm(numAnnotations == 0) { "Cannot step out with a dangling annotation" }
        confirm(!hasFieldName) { "Cannot step out with a dangling field name" }
        val endChar = when (currentContainer) {
            Struct -> '}'
            SExp, EExpression, ExpressionGroup -> ')'
            List -> ']'
            Top -> throw IonException("Nothing to step out of.")
        }

        currentContainer = ancestorContainersStack.removeLast()

        closeValue {
            if (options.isPrettyPrintOn && currentContainerHasValues && !forceNoNewlines) {
                output.appendAscii(options.lineSeparator())
                output.appendAscii(" ".repeat(ancestorContainersStack.size * 2))
            }
            output.appendAscii(endChar)
        }
    }

    private var forceNoNewlines: Boolean = false
    override fun forceNoNewlines(boolean: Boolean) { forceNoNewlines = boolean }

    override fun writeMacroParameterCardinality(cardinality: Macro.ParameterCardinality) {
        output.appendAscii(cardinality.sigil)
    }
}
