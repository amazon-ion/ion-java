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
        return event != IonCursor.Event.NEEDS_DATA && event != IonCursor.Event.END_CONTAINER
    }

    override fun stringValue(): String = reader.stringValue()

    override fun intValue(): Int = reader.intValue()

    override fun decimalValue(): BigDecimal = reader.decimalValue()

    override fun doubleValue(): Double = reader.doubleValue()

    override fun stepIntoContainer() {
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

    override fun symbolValue(): SymbolToken = reader.symbolValue()

    override fun getIntegerSize(): IntegerSize = reader.integerSize

    override fun getFieldNameSymbol(): SymbolToken = reader.fieldNameSymbol

    override fun isInStruct(): Boolean = reader.isInStruct

    override fun newBytes(): ByteArray = reader.newBytes()

    override fun timestampValue(): Timestamp = reader.timestampValue()

    override fun bigIntegerValue(): BigInteger = reader.bigIntegerValue()

    override fun longValue(): Long = reader.longValue()

    override fun isNullValue(): Boolean = reader.isNullValue

    override fun booleanValue(): Boolean = reader.booleanValue()

    override fun integerSize(): IntegerSize? = reader.integerSize
}
