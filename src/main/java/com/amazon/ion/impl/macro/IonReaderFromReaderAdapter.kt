// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.macro

import com.amazon.ion.*
import java.lang.UnsupportedOperationException
import java.math.BigDecimal
import java.math.BigInteger
import java.util.*

/**
 * An [IonReader] that delegates to a [ReaderAdapter].
 */
internal class IonReaderFromReaderAdapter(val reader: ReaderAdapter) : IonReader {

    override fun close() {
        // Do nothing. ReaderAdapter does not implement close().
    }

    override fun <T : Any?> asFacet(facetType: Class<T>?): T {
        throw UnsupportedOperationException()
    }

    override fun hasNext(): Boolean {
        throw UnsupportedOperationException()
    }

    override fun next(): IonType? = if (reader.nextValue()) reader.encodingType()!! else null

    override fun stringValue(): String = reader.stringValue()

    override fun intValue(): Int = reader.intValue()

    override fun bigDecimalValue(): BigDecimal = reader.decimalValue()

    override fun decimalValue(): Decimal = reader.ionDecimalValue()

    override fun dateValue(): Date {
        TODO("Not yet implemented")
    }

    override fun doubleValue(): Double = reader.doubleValue()

    override fun stepIn() = reader.stepIntoContainer()

    override fun stepOut() = reader.stepOutOfContainer()

    override fun getDepth(): Int = reader.getDepth()

    override fun getSymbolTable(): SymbolTable {
        TODO("Not yet implemented")
    }

    override fun getType(): IonType? = reader.encodingType()

    override fun getTypeAnnotationSymbols(): Array<SymbolToken> = reader.getTypeAnnotationSymbols().toTypedArray()

    override fun iterateTypeAnnotations(): MutableIterator<String> {
        TODO("Not yet implemented")
    }

    override fun getFieldId(): Int {
        TODO("Not yet implemented")
    }

    override fun getFieldName(): String {
        TODO("Not yet implemented")
    }

    override fun booleanValue(): Boolean = reader.booleanValue()

    override fun isNullValue(): Boolean = reader.isNullValue()

    override fun longValue(): Long = reader.longValue()

    override fun bigIntegerValue(): BigInteger = reader.bigIntegerValue()

    override fun timestampValue(): Timestamp = reader.timestampValue()

    override fun newBytes(): ByteArray = reader.newBytes()

    override fun getBytes(buffer: ByteArray?, offset: Int, len: Int): Int {
        TODO("Not yet implemented")
    }

    override fun symbolValue(): SymbolToken = reader.symbolValue()

    override fun byteSize(): Int {
        TODO("Not yet implemented")
    }

    override fun getIntegerSize(): IntegerSize = reader.getIntegerSize()

    override fun getTypeAnnotations(): Array<String> {
        TODO("Not yet implemented")
    }

    override fun getFieldNameSymbol(): SymbolToken = reader.getFieldNameSymbol()

    override fun isInStruct(): Boolean = reader.isInStruct()
}
