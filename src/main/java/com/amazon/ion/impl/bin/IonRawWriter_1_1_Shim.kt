// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.bin

import com.amazon.ion.*
import com.amazon.ion.impl.*
import java.math.BigDecimal
import java.math.BigInteger

internal class IonRawWriter_1_1_Shim(
    val delegate: IonRawWriter_1_1,
    optimization: WriteValueOptimization
) : AbstractIonWriter(optimization), _Private_IonRawWriter {

    /** Always returns [Symbols.systemSymbolTable].  */
    override fun getSymbolTable(): SymbolTable? {
        return Symbols.systemSymbolTable()
    }

    override fun addTypeAnnotation(annotation: String?) = annotation?.let { delegate.writeAnnotations(it) } ?: delegate.writeAnnotations(0)
    override fun addTypeAnnotationSymbol(sid: Int) = delegate.writeAnnotations(sid)

    override fun setTypeAnnotations(vararg annotations: String?) {
        delegate._private_clearAnnotations()
        annotations.forEach { addTypeAnnotation(it) }
    }
    override fun setTypeAnnotationSymbols(vararg sids: Int) {
        delegate._private_clearAnnotations()
        delegate.writeAnnotations(sids)
    }

    override fun setTypeAnnotationSymbols(vararg annotations: SymbolToken?) {
        delegate._private_clearAnnotations()
        annotations.forEach {
            if (it == null) return@forEach
            if (it.sid == SymbolTable.UNKNOWN_SYMBOL_ID) {
                delegate.writeAnnotations(it.assumeText())
            } else {
                delegate.writeAnnotations(it.sid)
            }
        }
    }

    override fun setFieldName(name: String?) { delegate.writeFieldName(name!!) }

    override fun setFieldNameSymbol(sid: Int) { delegate.writeFieldName(sid) }

    override fun setFieldNameSymbol(name: SymbolToken?) {
        if (name!!.sid == SymbolTable.UNKNOWN_SYMBOL_ID) {
            delegate.writeFieldName(name.assumeText())
        } else {
            delegate.writeFieldName(name.sid)
        }
    }

    // No facets supported right now
    override fun <T : Any?> asFacet(facetType: Class<T>?): T? = null

    override fun flush() {
        // No-op
    }

    override fun stepIn(containerType: IonType?) {
        val delimited = false
        when (containerType) {
            IonType.LIST -> delegate.stepInList(delimited)
            IonType.SEXP -> delegate.stepInSExp(delimited)
            IonType.STRUCT -> delegate.stepInStruct(delimited)
            else -> throw IllegalArgumentException("Not a container type: $containerType")
        }
    }

    override fun writeString(data: ByteArray?, offset: Int, length: Int) {
        if (data == null) {
            delegate.writeNull(IonType.STRING)
        } else {
            // TODO: We should probably plumb this through to the Ion 1.1 raw writer rather than decoding it here
            delegate.writeString(data.decodeToString(offset, length + offset, throwOnInvalidSequence = true))
        }
    }

    override fun writeString(value: String?) {
        value?.let { delegate.writeString(it as CharSequence) } ?: writeNull(IonType.STRING)
    }

    override fun writeSymbol(content: String?) {
        if (content == null) {
            delegate.writeNull(IonType.SYMBOL)
        } else {
            delegate.writeSymbol(content)
        }
    }

    override fun writeSymbolToken(content: SymbolToken?) {
        if (content == null) {
            delegate.writeNull(IonType.SYMBOL)
        } else if (content.sid != SymbolTable.UNKNOWN_SYMBOL_ID) {
            delegate.writeSymbol(content.sid)
        } else {
            delegate.writeSymbol(content.assumeText())
        }
    }

    override fun writeSymbolToken(sid: Int) = delegate.writeSymbol(sid)

    override fun writeTimestamp(value: Timestamp?) {
        if (value == null) {
            delegate.writeNull(IonType.TIMESTAMP)
        } else {
            delegate.writeTimestamp(value)
        }
    }

    override fun writeDecimal(value: BigDecimal?) {
        if (value == null) {
            delegate.writeNull(IonType.DECIMAL)
        } else {
            delegate.writeDecimal(value)
        }
    }

    override fun writeInt(value: BigInteger?) {
        if (value == null) {
            delegate.writeNull(IonType.INT)
        } else {
            delegate.writeInt(value)
        }
    }

    override fun finish() = delegate.finish()
    override fun close() = delegate.close()
    override fun stepOut() = delegate.stepOut()
    override fun isInStruct(): Boolean = delegate.isInStruct()
    override fun writeNull() = delegate.writeNull()
    override fun writeNull(type: IonType?) = delegate.writeNull(type ?: IonType.NULL)
    override fun writeBool(value: Boolean) = delegate.writeBool(value)
    override fun writeInt(value: Long) = delegate.writeInt(value)
    override fun writeFloat(value: Double) = delegate.writeFloat(value)
    override fun writeClob(value: ByteArray?) = value?.let { delegate.writeClob(it) } ?: delegate.writeNull(IonType.CLOB)
    override fun writeClob(value: ByteArray?, start: Int, len: Int) {
        value?.let { delegate.writeClob(it, start, len) } ?: delegate.writeNull(IonType.CLOB)
    }
    override fun writeBlob(value: ByteArray?) = value?.let { delegate.writeBlob(it) } ?: delegate.writeNull(IonType.BLOB)
    override fun writeBlob(value: ByteArray?, start: Int, len: Int) {
        value?.let { delegate.writeBlob(it, start, len) } ?: delegate.writeNull(IonType.BLOB)
    }

    override fun getCatalog(): IonCatalog {
        TODO("Not supported, just like the Ion 1.0 raw writer.")
    }

    override fun isFieldNameSet(): Boolean = delegate._private_hasFieldName()

    override fun getDepth(): Int = delegate.depth()

    override fun writeIonVersionMarker() {
        delegate.writeIVM()
    }

    override fun writeBytes(data: ByteArray?, off: Int, len: Int) {
        TODO("Should not be necessary. Use type-specific write methods instead.")
    }
}
