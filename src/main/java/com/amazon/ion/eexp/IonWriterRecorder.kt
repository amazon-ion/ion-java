// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.eexp

import com.amazon.ion.*
import com.amazon.ion.impl.*
import java.math.BigDecimal
import java.math.BigInteger
import java.util.ArrayDeque
import java.util.Date
import java.util.Deque

/**
 * An Ion Writer that records write operations for later replay.
 *
 * This class captures Ion write operations without immediately executing them,
 * storing them for later replay on another IonWriter.
 */
class IonWriterRecorder : IonWriter {

    override fun close() {}
    override fun flush() {}
    override fun finish() {}
    override fun <T : Any?> asFacet(facetType: Class<T>?): T? = null
    override fun getSymbolTable(): SymbolTable? = null

    private var depth = 0

    private val containerStack: Deque<IonType> = ArrayDeque()

    private val actions = ArrayList<IonWriter.() -> Unit>()

    private var isFieldNameSet = false

    fun replay(target: IonWriter) {
        actions.forEach { it(target) }
    }

    override fun setFieldName(name: String?) {
        if (!isInStruct) throw IonException("Cannot set field name when not in struct")
        actions.add { setFieldName(name) }
        isFieldNameSet = true
    }

    override fun setFieldNameSymbol(name: SymbolToken?) {
        if (!isInStruct) throw IonException("Cannot set field name when not in struct")
        actions.add { setFieldNameSymbol(name) }
        isFieldNameSet = true
    }

    override fun isFieldNameSet(): Boolean = isFieldNameSet

    override fun setTypeAnnotations(vararg annotations: String?) {
        actions.add { setTypeAnnotations(*annotations) }
    }

    override fun setTypeAnnotationSymbols(vararg annotations: SymbolToken?) {
        actions.add { setTypeAnnotationSymbols(*annotations) }
    }

    override fun addTypeAnnotation(annotation: String?) {
        actions.add { addTypeAnnotation(annotation) }
    }

    override fun stepIn(containerType: IonType?) {
        when (containerType) {
            IonType.LIST, IonType.SEXP, IonType.STRUCT -> {
                actions.add { stepIn(containerType) }
                containerStack.push(containerType)
                depth++
            }
            else -> throw IonException("Not a container type: $containerType")
        }
        isFieldNameSet = false
    }

    override fun stepOut() {
        if (depth == 0) throw IonException("Nothing to step out from")
        containerStack
        depth--
        isFieldNameSet = false
    }

    override fun getDepth(): Int = depth

    override fun isInStruct(): Boolean = containerStack.peek() == IonType.STRUCT

    @Deprecated("Deprecated in IonWriter", ReplaceWith("value.writeTo(this)"))
    override fun writeValue(value: IonValue?) {
        value?.writeTo(this)
    }

    override fun writeValue(reader: IonReader) {
        DefaultReaderToWriterTransfer.writeValue(reader, this)
    }

    override fun writeValues(reader: IonReader) {
        if (reader.type == null) reader.next()
        while (reader.type != null) {
            writeValue(reader)
            reader.next()
        }
    }

    override fun writeNull() {
        actions.add { writeNull() }
        isFieldNameSet = false
    }

    override fun writeNull(type: IonType?) {
        actions.add { writeNull(type) }
        isFieldNameSet = false
    }

    override fun writeBool(value: Boolean) {
        actions.add { writeBool(value) }
        isFieldNameSet = false
    }

    override fun writeInt(value: Long) {
        actions.add { writeInt(value) }
        isFieldNameSet = false
    }

    override fun writeInt(value: BigInteger?) {
        actions.add { writeInt(value) }
        isFieldNameSet = false
    }

    override fun writeFloat(value: Double) {
        actions.add { writeFloat(value) }
        isFieldNameSet = false
    }

    override fun writeDecimal(value: BigDecimal?) {
        actions.add { writeDecimal(value) }
        isFieldNameSet = false
    }

    override fun writeTimestamp(value: Timestamp?) {
        actions.add { writeTimestamp(value) }
        isFieldNameSet = false
    }

    override fun writeTimestampUTC(value: Date?) {
        actions.add { writeTimestampUTC(value) }
        isFieldNameSet = false
    }

    override fun writeSymbol(content: String?) {
        actions.add { writeSymbol(content) }
        isFieldNameSet = false
    }

    override fun writeSymbolToken(content: SymbolToken?) {
        actions.add { writeSymbolToken(content) }
        isFieldNameSet = false
    }

    override fun writeString(value: String?) {
        actions.add { writeString(value) }
        isFieldNameSet = false
    }

    override fun writeClob(value: ByteArray?) {
        actions.add { writeClob(value) }
        isFieldNameSet = false
    }

    override fun writeClob(value: ByteArray?, start: Int, len: Int) {
        actions.add { writeClob(value, start, len) }
        isFieldNameSet = false
    }

    override fun writeBlob(value: ByteArray?) {
        actions.add { writeBlob(value) }
        isFieldNameSet = false
    }

    override fun writeBlob(value: ByteArray?, start: Int, len: Int) {
        actions.add { writeBlob(value, start, len) }
        isFieldNameSet = false
    }

    override fun writeObject(obj: WriteAsIon) {
        actions.add { writeObject(obj) }
        isFieldNameSet = false
    }
}
