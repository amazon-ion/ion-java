// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.macro

import com.amazon.ion.*
import java.math.BigDecimal
import java.math.BigInteger

/**
 * A [ReaderAdapter] that wraps an [IonReader].
 */
internal class ReaderAdapterIonReader(val reader: IonReader) : ReaderAdapter {

    // TODO performance: when there are annotations, this causes a redundant allocation if the allocations are
    //  later consumed.
    override fun hasAnnotations(): Boolean = reader.typeAnnotations.isNotEmpty()

    override fun fieldNameSymbol(): SymbolToken = reader.fieldNameSymbol

    override fun encodingType(): IonType? = reader.type

    override fun nextValue(): Boolean = reader.next() != null
    override fun getDepth(): Int = reader.depth

    override fun stringValue(): String = reader.stringValue()

    override fun intValue(): Int = reader.intValue()

    override fun decimalValue(): BigDecimal = reader.bigDecimalValue()
    override fun ionDecimalValue(): Decimal = reader.decimalValue()

    override fun doubleValue(): Double = reader.doubleValue()

    override fun stepIntoContainer() = reader.stepIn()

    override fun stepOutOfContainer() = reader.stepOut()

    override fun getTypeAnnotationSymbols(): List<SymbolToken> = reader.typeAnnotationSymbols.asList()

    override fun integerSize(): IntegerSize? = reader.integerSize

    override fun booleanValue(): Boolean = reader.booleanValue()

    override fun isNullValue(): Boolean = reader.isNullValue

    override fun longValue(): Long = reader.longValue()

    override fun bigIntegerValue(): BigInteger = reader.bigIntegerValue()

    override fun timestampValue(): Timestamp = reader.timestampValue()

    override fun newBytes(): ByteArray = reader.newBytes()

    override fun symbolValue(): SymbolToken = reader.symbolValue()

    override fun getIntegerSize(): IntegerSize = reader.integerSize

    override fun getFieldNameSymbol(): SymbolToken = reader.fieldNameSymbol

    override fun isInStruct(): Boolean = reader.isInStruct
}
