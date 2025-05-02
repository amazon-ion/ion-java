// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.macro

import com.amazon.ion.*
import com.amazon.ion.impl.*
import java.math.BigDecimal
import java.math.BigInteger

/**
 * A [ReaderAdapter] that wraps an [IonReaderContinuableCore].
 */
internal class ReaderAdapterContinuable(val reader: IonReaderContinuableCore) : ReaderAdapter {

    // TODO: Make sure that we can throw exceptions if there's an over-sized value.

    override fun hasAnnotations(): Boolean = reader.hasAnnotations()

    override fun fieldNameSymbol(): SymbolToken = reader.fieldNameSymbol

    override fun encodingType(): IonType? = reader.encodingType

    override fun nextValue(): Boolean {
        val event = reader.nextValue()
        return event != IonCursor.Event.NEEDS_DATA_ORDINAL && event != IonCursor.Event.END_CONTAINER_ORDINAL
    }

    override fun getDepth(): Int = reader.depth

    /**
     * Ensures that the value on which the reader is positioned is fully buffered.
     */
    private fun prepareValue() {
        // TODO performance: fill entire expression groups up-front so that the reader will usually not be in slow
        //  mode when this is called.
        if (reader.fillValue() != IonCursor.Event.VALUE_READY_ORDINAL) {
            throw IonException("TODO: support continuable reading and oversize value handling via this adapter.")
        }
    }

    override fun stringValue(): String {
        prepareValue()
        return reader.stringValue()
    }

    override fun intValue(): Int {
        prepareValue()
        return reader.intValue()
    }

    override fun decimalValue(): BigDecimal {
        prepareValue()
        return reader.decimalValue()
    }

    override fun ionDecimalValue(): Decimal {
        prepareValue()
        return reader.decimalValue()
    }

    override fun doubleValue(): Double {
        prepareValue()
        return reader.doubleValue()
    }

    override fun stepIntoContainer() {
        // Note: the following line ensures the entire container is buffered. This improves performance when reading the
        // container's elements because there is less work to do per element. However, very large containers would
        // increase memory usage. The current implementation already assumes this risk by eagerly materializing
        // macro invocation arguments. However, if that is changed, then removing the following line should also be
        // considered.
        prepareValue()
        reader.stepIntoContainer()
    }

    override fun stepOutOfContainer() {
        reader.stepOutOfContainer()
    }

    override fun getTypeAnnotationSymbols(): List<SymbolToken> {
        if (!reader.hasAnnotations()) {
            return emptyList()
        }
        val annotations = arrayListOf<SymbolToken>()
        reader.consumeAnnotationTokens { annotations += it }
        return annotations
    }

    override fun symbolValue(): SymbolToken {
        prepareValue()
        return reader.symbolValue()
    }

    override fun getIntegerSize(): IntegerSize {
        prepareValue()
        return reader.integerSize
    }

    override fun getFieldNameSymbol(): SymbolToken = reader.fieldNameSymbol

    override fun isInStruct(): Boolean = reader.isInStruct

    override fun newBytes(): ByteArray {
        prepareValue()
        return reader.newBytes()
    }

    override fun timestampValue(): Timestamp {
        prepareValue()
        return reader.timestampValue()
    }

    override fun bigIntegerValue(): BigInteger {
        prepareValue()
        return reader.bigIntegerValue()
    }

    override fun longValue(): Long {
        prepareValue()
        return reader.longValue()
    }

    override fun isNullValue(): Boolean = reader.isNullValue

    override fun booleanValue(): Boolean {
        prepareValue()
        return reader.booleanValue()
    }

    override fun integerSize(): IntegerSize? {
        prepareValue()
        return reader.integerSize
    }
}
