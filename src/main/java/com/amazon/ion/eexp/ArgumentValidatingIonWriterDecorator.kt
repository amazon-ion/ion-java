// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.eexp

import com.amazon.ion.*
import com.amazon.ion.impl.*
import com.amazon.ion.impl.macro.*
import java.math.BigDecimal
import java.math.BigInteger
import java.util.Date

/**
 * A decorator for IonWriter that validates arguments against macro parameter specifications.
 *
 * This class wraps an IonWriter and ensures that all values written through it conform to
 * the constraints specified by a macro parameter.
 */
class ArgumentValidatingIonWriterDecorator(
    private val parameter: Macro.Parameter,
    private val delegate: IonWriter,
) : IonWriter {

    private val encoding: Macro.ParameterEncoding = parameter.type

    override fun close() {}
    override fun flush() {}
    override fun finish() {}
    override fun <T : Any?> asFacet(facetType: Class<T>?): T? = null
    override fun getSymbolTable(): SymbolTable? = delegate.symbolTable

    var numberOfExpressions = 0
        private set

    private var depth = 0

    override fun setFieldName(name: String?) = delegate.setFieldName(name)
    override fun setFieldNameSymbol(name: SymbolToken?) = delegate.setFieldNameSymbol(name)
    override fun isFieldNameSet(): Boolean = delegate.isFieldNameSet

    override fun setTypeAnnotations(vararg annotations: String?) {
        if (depth == 0 && annotations.isNotEmpty()) require(encoding == Macro.ParameterEncoding.Tagged) { "Parameter with encoding $encoding cannot be annotated." }
        delegate.setTypeAnnotations(*annotations)
    }

    override fun setTypeAnnotationSymbols(vararg annotations: SymbolToken?) {
        if (depth == 0 && annotations.isNotEmpty()) require(encoding == Macro.ParameterEncoding.Tagged) { "Parameter with encoding $encoding cannot be annotated." }
        delegate.setTypeAnnotationSymbols(*annotations)
    }

    override fun addTypeAnnotation(annotation: String?) {
        if (depth == 0) require(encoding == Macro.ParameterEncoding.Tagged) { "Parameter with encoding $encoding cannot be annotated." }
        delegate.addTypeAnnotation(annotation)
    }

    override fun stepIn(containerType: IonType?) {
        when (containerType) {
            IonType.LIST, IonType.SEXP, IonType.STRUCT -> {
                if (depth == 0) {
                    require(encoding == Macro.ParameterEncoding.Tagged) { "Type $containerType is not valid for parameter encoding $encoding." }
                    numberOfExpressions++
                }
                delegate.stepIn(containerType)
                depth++
            }
            else -> throw IonException("Not a container type: $containerType")
        }
    }

    override fun stepOut() {
        if (depth == 0) throw IonException("Nothing to step out from")
        depth--
    }

    override fun getDepth(): Int = depth

    override fun isInStruct(): Boolean = delegate.isInStruct()

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
        if (depth == 0) {
            require(encoding == Macro.ParameterEncoding.Tagged) { "null value is not valid for parameter encoding $encoding." }
            numberOfExpressions++
        }
        delegate.writeNull()
    }

    override fun writeNull(type: IonType?) {
        if (depth == 0) {
            require(encoding == Macro.ParameterEncoding.Tagged) { "null.${type.toString().lowercase()} value is not valid for parameter encoding $encoding." }
            numberOfExpressions++
        }
        delegate.writeNull(type)
    }

    override fun writeBool(value: Boolean) {
        if (depth == 0) {
            require(encoding == Macro.ParameterEncoding.Tagged) { "bool value is not valid for parameter encoding $encoding." }
            numberOfExpressions++
        }
        delegate.writeBool(value)
    }

    override fun writeInt(value: Long) {
        if (depth == 0) {
            checkArgumentEncodingCompatibility(parameter, value)
            numberOfExpressions++
        }
        delegate.writeInt(value)
    }

    override fun writeInt(value: BigInteger?) {
        if (depth == 0) {
            checkArgumentEncodingCompatibility(parameter, value)
            numberOfExpressions++
        }
        delegate.writeInt(value)
    }

    override fun writeFloat(value: Double) {
        if (depth == 0) {
            checkArgumentEncodingCompatibility(parameter, value)
            numberOfExpressions++
        }
        delegate.writeFloat(value)
    }

    override fun writeDecimal(value: BigDecimal?) {
        if (depth == 0) {
            require(encoding == Macro.ParameterEncoding.Tagged) { "decimal value is not valid for parameter encoding $encoding." }
            numberOfExpressions++
        }
        delegate.writeDecimal(value)
    }

    override fun writeTimestamp(value: Timestamp?) {
        if (depth == 0) {
            require(encoding == Macro.ParameterEncoding.Tagged) { "timestamp value is not valid for parameter encoding $encoding." }
            numberOfExpressions++
        }
        delegate.writeTimestamp(value)
    }

    override fun writeTimestampUTC(value: Date?) {
        if (depth == 0) {
            require(encoding == Macro.ParameterEncoding.Tagged) { "timestamp value is not valid for parameter encoding $encoding." }
            numberOfExpressions++
        }
        delegate.writeTimestampUTC(value)
    }

    override fun writeSymbol(content: String?) {
        if (depth == 0) {
            when (encoding) {
                Macro.ParameterEncoding.Tagged -> delegate.writeSymbol(content)
                Macro.ParameterEncoding.FlexString -> {
                    if (content == null) throw IllegalArgumentException("null.symbol is not valid for parameter encoding $encoding")
                    delegate.writeSymbol(content)
                }
                else -> throw IllegalArgumentException("symbol value is not valid for parameter encoding $encoding")
            }
            numberOfExpressions++
        }
        delegate.writeSymbol(content)
    }

    override fun writeSymbolToken(content: SymbolToken?) {
        if (depth == 0) {
            when (encoding) {
                Macro.ParameterEncoding.Tagged -> delegate.writeSymbolToken(content)
                Macro.ParameterEncoding.FlexString -> {
                    if (content == null) throw IllegalArgumentException("null.symbol is not valid for parameter encoding $encoding")
                    TODO()
                }
                else -> throw IllegalArgumentException("symbol value is not valid for parameter encoding $encoding")
            }
            numberOfExpressions++
        }
        delegate.writeSymbolToken(content)
    }

    override fun writeString(value: String?) {
        if (depth == 0) {
            checkArgumentEncodingCompatibility(parameter, value)
            numberOfExpressions++
        }
        delegate.writeString(value)
    }

    override fun writeClob(value: ByteArray?) {
        if (depth == 0) {
            require(encoding == Macro.ParameterEncoding.Tagged) { "clob value is not valid for parameter encoding $encoding." }
            numberOfExpressions++
        }
        delegate.writeClob(value)
    }

    override fun writeClob(value: ByteArray?, start: Int, len: Int) {
        if (depth == 0) {
            require(encoding == Macro.ParameterEncoding.Tagged) { "clob value is not valid for parameter encoding $encoding." }
            numberOfExpressions++
        }
        delegate.writeClob(value, start, len)
    }

    override fun writeBlob(value: ByteArray?) {
        if (depth == 0) {
            require(encoding == Macro.ParameterEncoding.Tagged) { "blob value is not valid for parameter encoding $encoding." }
            numberOfExpressions++
        }
        delegate.writeBlob(value)
    }

    override fun writeBlob(value: ByteArray?, start: Int, len: Int) {
        if (depth == 0) {
            require(encoding == Macro.ParameterEncoding.Tagged) { "blob value is not valid for parameter encoding $encoding." }
            numberOfExpressions++
        }
        delegate.writeBlob(value, start, len)
    }

    override fun writeObject(obj: WriteAsIon?) {
        if (depth == 0) {
            // TODO: How to correctly validate this? Can we defer it until we implement macro-shaped parameters?
//            require(encoding == Macro.ParameterEncoding.Tagged) { "e-expression is not valid for parameter encoding $encoding." }
            numberOfExpressions++
        }
        delegate.writeObject(obj)
    }
}
