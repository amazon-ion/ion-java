// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl

import com.amazon.ion.IonException
import com.amazon.ion.IonType
import com.amazon.ion.Timestamp
import com.amazon.ion.bytecode.bin11.OpCode
import com.amazon.ion.impl.IonRawTextWriter_1_1.ContainerType.EExpression
import com.amazon.ion.impl.IonRawTextWriter_1_1.ContainerType.List
import com.amazon.ion.impl.IonRawTextWriter_1_1.ContainerType.SExp
import com.amazon.ion.impl.IonRawTextWriter_1_1.ContainerType.Struct
import com.amazon.ion.impl.IonRawTextWriter_1_1.ContainerType.Top
import com.amazon.ion.impl.bin.BlockAllocatorProviders
import com.amazon.ion.ion_1_1.IonRawWriter_1_1
import com.amazon.ion.ion_1_1.TaglessScalarType
import com.amazon.ion.printTimestamp
import com.amazon.ion.system.IonTextWriterBuilder
import com.amazon.ion.util.IonTextUtils
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import java.io.OutputStream
import java.math.BigDecimal
import java.math.BigInteger
import java.util.function.Consumer

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
    options: _Private_IonTextWriterBuilder,
    @SuppressFBWarnings("EI_EXPOSE_REP2", justification = "We're intentionally storing a reference to a mutable object because we need to write to it.")
    private val output: _Private_IonTextAppender,
) : IonRawWriter_1_1 {

    private val options = options.immutable()

    private inline fun confirm(condition: Boolean, lazyMessage: () -> String) {
        if (!condition) {
            throw IonException(lazyMessage())
        }
    }

    companion object {
        const val IVM = "\$ion_1_1"

        @JvmStatic
        fun from(output: OutputStream, blockSize: Int, options: IonTextWriterBuilder): IonRawTextWriter_1_1 {
            val bufferedOutput = BufferedOutputStreamFastAppendable(
                output,
                BlockAllocatorProviders.basicProvider().vendAllocator(blockSize)
            )
            return IonRawTextWriter_1_1(
                options as _Private_IonTextWriterBuilder,
                _Private_IonTextAppender.forFastAppendable(bufferedOutput, Charsets.UTF_8)
            )
        }
    }

    enum class ContainerType {
        List,
        SExp,
        Struct,
        EExpression,
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
            EExpression, SExp -> " "
            Top -> options.topLevelSeparator()
        }

        if (options.isPrettyPrintOn) {
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
                fieldNameText = null
            } else {
                output.appendAscii("$")
                output.printInt(fieldNameId.toLong())
                output.appendAscii(":")
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
        if (options._timestamp_as_millis) {
            output.appendAscii(value.millis.toString())
        } else if (options._timestamp_as_string) {
            // Timestamp is ASCII-safe so this is easy
            output.appendAscii('"')
            output.appendAscii(printTimestamp(value, options.maximumTimestampPrecisionDigits))
            output.appendAscii('"')
        } else {
            output.appendAscii(printTimestamp(value, options.maximumTimestampPrecisionDigits))
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
        startSexp { output.appendAscii("(") }
    }

    private inline fun startSexp(openingTokens: () -> Unit) {
        openValue(openingTokens)
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
        openValue {
            output.appendAscii("(:")
            output.printSymbol(name)
        }
        ancestorContainersStack.add(currentContainer)
        currentContainer = EExpression
        currentContainerHasValues = false
        isPendingSeparator = true // Treat the macro name as if it is a value that needs a separator.
    }

    override fun stepInEExp(id: Int, usingLengthPrefix: Boolean) {
        openValue {
            output.appendAscii("(:")
            output.printInt(id.toLong())
        }
        ancestorContainersStack.add(currentContainer)
        currentContainer = EExpression
        currentContainerHasValues = false
        isPendingSeparator = true // Treat the macro id as if it is a value that needs a separator.
    }

    override fun writeAbsentArgument() {
        writeScalar {
            output.appendAscii("(:)")
        }
    }

    override fun stepOut() {
        confirm(numAnnotations == 0) { "Cannot step out with a dangling annotation" }
        confirm(!hasFieldName) { "Cannot step out with a dangling field name" }
        val endChar = when (currentContainer) {
            Struct -> '}'
            SExp, EExpression -> ')'
            List -> ']'
            Top -> throw IonException("Nothing to step out of.")
        }

        currentContainer = ancestorContainersStack.removeAt(ancestorContainersStack.size - 1)

        closeValue {
            if (options.isPrettyPrintOn && currentContainerHasValues) {
                output.appendAscii(options.lineSeparator())
                output.appendAscii(" ".repeat(ancestorContainersStack.size * 2))
            }
            output.appendAscii(endChar)
        }
    }

    override fun writeTaggedPlaceholder() {
        writeScalar { output.appendAscii("(:?)") }
    }

    override fun writeTaggedPlaceholderWithDefault(default: Consumer<IonRawWriter_1_1>) {
        writeScalar {
            stepInSExp(false)
            output.appendAscii(":?")
            isPendingSeparator = true
            default.accept(this)
            stepOut()
        }
    }

    override fun writeTaglessPlaceholder(taglessEncodingOpcode: Int) {
        writeScalar {
            output.appendAscii("(:? ")
            val tag = TaglessScalarType.getTaglessScalarTypeForOpcode(taglessEncodingOpcode)!!
            writePrimitiveEncodingTag(tag)
            output.appendAscii(")")
        }
    }

    override fun stepInDirective(directiveOpcode: Int) {
        val directiveName = when (directiveOpcode) {
            OpCode.DIRECTIVE_SET_SYMBOLS -> "set_symbols"
            OpCode.DIRECTIVE_ADD_SYMBOLS -> "add_symbols"
            OpCode.DIRECTIVE_SET_MACROS -> "set_macros"
            OpCode.DIRECTIVE_ADD_MACROS -> "add_macros"
            OpCode.DIRECTIVE_USE -> "use"
            OpCode.DIRECTIVE_MODULE -> "module"
            OpCode.DIRECTIVE_IMPORT -> "import"
            OpCode.DIRECTIVE_ENCODING -> "encoding"
            else -> throw IonException("Not a directive $directiveOpcode")
        }
        stepInSExp(false)
        output.appendAscii(":\$ion ")
        output.appendAscii(directiveName)
        isPendingSeparator = true
    }

    private fun writeMacroEncodingTag(macroName: String) {
        output.appendAscii("{:")
        output.appendAscii(macroName)
        output.appendAscii("}")
    }

    private fun writePrimitiveEncodingTag(taglessScalarType: TaglessScalarType) {
        output.appendAscii("{#")
        output.appendAscii(taglessScalarType.textEncodingName)
        output.appendAscii("}")
    }

    override fun stepInTaglessElementList(taglessEncodingOpcode: Int) {
        stepInList(usingLengthPrefix = false) // Arg here doesn't actually matter.
        val tag = TaglessScalarType.getTaglessScalarTypeForOpcode(taglessEncodingOpcode)!!
        writePrimitiveEncodingTag(tag)
        output.appendAscii(" ")
    }

    override fun stepInTaglessElementList(macroId: Int, macroName: String?, lengthPrefixed: Boolean) {
        stepInList(usingLengthPrefix = false) // Arg here doesn't actually matter.
        writeMacroEncodingTag(macroName ?: macroId.toString())
        output.appendAscii(" ")
    }

    override fun stepInTaglessElementSExp(taglessEncodingOpcode: Int) {
        stepInSExp(usingLengthPrefix = false) // Arg here doesn't actually matter.
        val tag = TaglessScalarType.getTaglessScalarTypeForOpcode(taglessEncodingOpcode)!!
        writePrimitiveEncodingTag(tag)
        output.appendAscii(" ")
    }

    override fun stepInTaglessElementSExp(macroId: Int, macroName: String?, lengthPrefixed: Boolean) {
        stepInSExp(usingLengthPrefix = false) // Arg here doesn't actually matter.
        writeMacroEncodingTag(macroName ?: macroId.toString())
        output.appendAscii(" ")
    }

    override fun stepInTaglessEExp() {
        // Looks like a SExp, so we'll start this way and switch it to EExp.
        stepInSExp(usingLengthPrefix = false) // Arg here doesn't actually matter.
        currentContainer = EExpression
    }

    override fun writeTaglessInt(implicitOpcode: Int, value: Long) {
        writeInt(value)
    }

    override fun writeTaglessInt(implicitOpcode: Int, value: BigInteger) {
        writeInt(value)
    }

    override fun writeTaglessFloat(implicitOpcode: Int, value: Float) {
        writeFloat(value)
    }

    override fun writeTaglessFloat(implicitOpcode: Int, value: Double) {
        writeFloat(value)
    }

    override fun writeTaglessDecimal(implicitOpcode: Int, value: BigDecimal) {
        writeDecimal(value)
    }

    override fun writeTaglessTimestamp(implicitOpcode: Int, value: Timestamp) {
        writeTimestamp(value)
    }

    override fun writeTaglessSymbol(implicitOpcode: Int, id: Int) {
        writeSymbol(id)
    }

    override fun writeTaglessSymbol(implicitOpcode: Int, text: CharSequence) {
        writeSymbol(text)
    }
}
