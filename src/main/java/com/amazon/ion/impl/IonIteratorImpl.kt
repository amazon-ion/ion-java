/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazon.ion.impl

import com.amazon.ion.IonReader
import com.amazon.ion.IonType
import com.amazon.ion.IonValue
import com.amazon.ion.ValueFactory

/**
 * An [Iterator] that wraps an [IonReader] and emits [IonValue]s.
 */
internal class IonIteratorImpl(private val _valueFactory: ValueFactory, private val _reader: IonReader) : MutableIterator<IonValue> {

    private var _at_eof = false
    private var _next: IonValue? = null

    /**
     * Returns true if the iteration has more elements.
     * here we actually walk ahead and get the next value (it's
     * the only way we know if there are more and clear out the
     * various $ion noise out of the way
     */
    override fun hasNext(): Boolean {
        if (_at_eof) return false
        return if (_next != null) true else prefetch() != null
    }

    private fun prefetch(): IonValue? {
        assert(!_at_eof && _next == null)
        val type = _reader.next()
        if (type == null) {
            _at_eof = true
        } else {
            _next = readValue()
        }
        return _next
    }

    private fun readValue(): IonValue {
        val annotations = _reader.typeAnnotationSymbols
        val v = if (_reader.isNullValue) {
            _valueFactory.newNull(_reader.type)
        } else {
            when (_reader.type) {
                IonType.NULL -> throw IllegalStateException()
                IonType.BOOL -> _valueFactory.newBool(_reader.booleanValue())
                IonType.INT -> _valueFactory.newInt(_reader.bigIntegerValue())
                IonType.FLOAT -> _valueFactory.newFloat(_reader.doubleValue())
                IonType.DECIMAL -> _valueFactory.newDecimal(_reader.decimalValue())
                IonType.TIMESTAMP -> _valueFactory.newTimestamp(_reader.timestampValue())
                IonType.STRING -> _valueFactory.newString(_reader.stringValue())
                // TODO always pass the SID?  Is it correct?
                IonType.SYMBOL -> _valueFactory.newSymbol(_reader.symbolValue())
                IonType.BLOB -> _valueFactory.newNullBlob().apply { bytes = _reader.newBytes() }
                IonType.CLOB -> _valueFactory.newNullClob().apply { bytes = _reader.newBytes() }
                IonType.STRUCT -> _valueFactory.newEmptyStruct().apply { _reader.forEachInContainer { add(fieldNameSymbol, readValue()) } }
                IonType.LIST -> _valueFactory.newEmptyList().apply { _reader.forEachInContainer { add(readValue()) } }
                IonType.SEXP -> _valueFactory.newEmptySexp().apply { _reader.forEachInContainer { add(readValue()) } }
                else -> throw IllegalStateException()
            }
        }

        // TODO this is too late in the case of system reading
        // when v is a local symtab (it will get itself, not the prior symtab)
        (v as _Private_IonValue).symbolTable = _reader.symbolTable
        if (annotations.isNotEmpty()) v.setTypeAnnotationSymbols(*annotations)
        return v
    }

    override fun next(): IonValue {
        if (_at_eof) throw NoSuchElementException()
        val value = _next ?: prefetch()
        _next = null
        return value ?: throw NoSuchElementException()
    }

    override fun remove() {
        throw UnsupportedOperationException()
    }

    private inline fun IonReader.forEachInContainer(block: IonReader.() -> Unit) {
        stepIn()
        while (next() != null) { block() }
        stepOut()
    }
}
