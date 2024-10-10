package com.amazon.ion.impl.macro

import com.amazon.ion.*
import java.math.BigDecimal
import java.math.BigInteger

/**
 * Provides a single abstraction over any Ion reader, e.g. [IonReader] or IonReaderContinuableCore.
 * @see ReaderAdapterIonReader
 * @see ReaderAdapterContinuable
 */
interface ReaderAdapter {

    fun hasAnnotations(): Boolean
    fun fieldNameSymbol(): SymbolToken
    fun encodingType(): IonType?

    /** Returns true if positioned on a value; false if at container or stream end. */
    fun nextValue(): Boolean
    fun stringValue(): String
    fun intValue(): Int
    fun decimalValue(): BigDecimal
    fun doubleValue(): Double
    fun stepIntoContainer()
    fun stepOutOfContainer()
    fun getTypeAnnotationSymbols(): List<SymbolToken>
    fun integerSize(): IntegerSize?
    fun booleanValue(): Boolean
    fun isNullValue(): Boolean
    fun longValue(): Long
    fun bigIntegerValue(): BigInteger
    fun timestampValue(): Timestamp
    fun newBytes(): ByteArray
    fun symbolValue(): SymbolToken
    fun getIntegerSize(): IntegerSize
    fun getFieldNameSymbol(): SymbolToken
    fun isInStruct(): Boolean
}
