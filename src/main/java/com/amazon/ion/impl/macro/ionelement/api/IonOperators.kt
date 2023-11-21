package com.amazon.ion.impl.macro.ionelement.api

import com.amazon.ion.Decimal
import com.amazon.ion.impl.macro.ionelement.api.IntElementSize.*
import java.math.BigDecimal
import java.math.BigInteger

@RequiresOptIn(message = "This API is experimental and subject to change.")
@Retention(AnnotationRetention.BINARY)
@Target(AnnotationTarget.CLASS, AnnotationTarget.FUNCTION)
public annotation class IonOperators

/* Type aliases for readability */
internal typealias BE = BoolElement
internal typealias IE = IntElement
internal typealias DE = DecimalElement
internal typealias FE = FloatElement

/* BoolElement unary operators */
@IonOperators
public operator fun BE.not(): BE = ionBool(!this.booleanValue, this.annotations, this.metas)

/* BoolElement with Boolean operators */
@IonOperators
public infix fun BE.and(other: Boolean): BE = ionBool(this.booleanValue && other, this.annotations, this.metas)
@IonOperators
public infix fun BE.or(other: Boolean): BE = ionBool(this.booleanValue || other, this.annotations, this.metas)

/* Boolean with BoolElement operators */
@IonOperators
public infix fun Boolean.and(other: BE): BE = ionBool(this && other.booleanValue, other.annotations, other.metas)
@IonOperators
public infix fun Boolean.or(other: BE): BE = ionBool(this || other.booleanValue, other.annotations, other.metas)

/* IntElement unary operators */
@IonOperators
public operator fun IE.unaryMinus(): IE =
    if (useLong(this)) {
        ionInt(-this.longValue, this.annotations, this.metas)
    } else {
        ionInt(-this.bigIntegerValue, this.annotations, this.metas)
    }

@IonOperators
public operator fun IE.inc(): IE =
    if (useLong(this) && this.longValue != Long.MAX_VALUE) {
        ionInt(this.longValue + 1, this.annotations, this.metas)
    } else {
        ionInt(this.bigIntegerValue.inc(), this.annotations, this.metas)
    }

@IonOperators
public operator fun IE.dec(): IE =
    if (useLong(this) && this.longValue != Long.MIN_VALUE) {
        ionInt(this.longValue - 1, this.annotations, this.metas)
    } else {
        ionInt(this.bigIntegerValue.dec(), this.annotations, this.metas)
    }

/* IntElement with IntElement binary operators */
@IonOperators
public infix fun IE.eq(other: IE): Boolean = intCmp(this, other) == 0
@IonOperators
public infix fun IE.lt(other: IE): Boolean = intCmp(this, other) < 0
@IonOperators
public infix fun IE.gt(other: IE): Boolean = intCmp(this, other) > 0
@IonOperators
public infix fun IE.lte(other: IE): Boolean = intCmp(this, other) <= 0
@IonOperators
public infix fun IE.gte(other: IE): Boolean = intCmp(this, other) >= 0

/* IntElement with Int binary operators */
@IonOperators
public operator fun IE.plus(other: Int): IE = intOp(this, other, Math::addExact, BigInteger::plus)
@IonOperators
public operator fun IE.minus(other: Int): IE = intOp(this, other, Math::subtractExact, BigInteger::minus)
@IonOperators
public operator fun IE.times(other: Int): IE = intOp(this, other, Math::multiplyExact, BigInteger::times)
@IonOperators
public operator fun IE.div(other: Int): IE = intOp(this, other, Long::div, BigInteger::div)
@IonOperators
public operator fun IE.rem(other: Int): IE = intOp(this, other, Long::rem, BigInteger::rem)
@IonOperators
public infix fun IE.eq(other: Int): Boolean = intCmp(this, other) == 0
@IonOperators
public infix fun IE.lt(other: Int): Boolean = intCmp(this, other) < 0
@IonOperators
public infix fun IE.gt(other: Int): Boolean = intCmp(this, other) > 0
@IonOperators
public infix fun IE.lte(other: Int): Boolean = intCmp(this, other) <= 0
@IonOperators
public infix fun IE.gte(other: Int): Boolean = intCmp(this, other) >= 0

/* IntElement with Long binary operators */
@IonOperators
public operator fun IE.plus(other: Long): IE = intOp(this, other, Math::addExact, BigInteger::plus)
@IonOperators
public operator fun IE.minus(other: Long): IE = intOp(this, other, Math::subtractExact, BigInteger::minus)
@IonOperators
public operator fun IE.times(other: Long): IE = intOp(this, other, Math::multiplyExact, BigInteger::times)
@IonOperators
public operator fun IE.div(other: Long): IE = intOp(this, other, Long::div, BigInteger::div)
@IonOperators
public operator fun IE.rem(other: Long): IE = intOp(this, other, Long::rem, BigInteger::rem)
@IonOperators
public infix fun IE.eq(other: Long): Boolean = intCmp(this, other) == 0
@IonOperators
public infix fun IE.lt(other: Long): Boolean = intCmp(this, other) < 0
@IonOperators
public infix fun IE.gt(other: Long): Boolean = intCmp(this, other) > 0
@IonOperators
public infix fun IE.lte(other: Long): Boolean = intCmp(this, other) <= 0
@IonOperators
public infix fun IE.gte(other: Long): Boolean = intCmp(this, other) >= 0

/* IntElement with BigInteger binary operators */
@IonOperators
public operator fun IE.plus(other: BigInteger): IE = ionInt(this.bigIntegerValue + other, this.annotations, this.metas)
@IonOperators
public operator fun IE.minus(other: BigInteger): IE = ionInt(this.bigIntegerValue - other, this.annotations, this.metas)
@IonOperators
public operator fun IE.times(other: BigInteger): IE = ionInt(this.bigIntegerValue * other, this.annotations, this.metas)
@IonOperators
public operator fun IE.div(other: BigInteger): IE = ionInt(this.bigIntegerValue / other, this.annotations, this.metas)
@IonOperators
public operator fun IE.rem(other: BigInteger): IE = ionInt(this.bigIntegerValue % other, this.annotations, this.metas)
@IonOperators
public infix fun IE.eq(other: BigInteger): Boolean = this.bigIntegerValue == other
@IonOperators
public infix fun IE.lt(other: BigInteger): Boolean = this.bigIntegerValue < other
@IonOperators
public infix fun IE.gt(other: BigInteger): Boolean = this.bigIntegerValue > other
@IonOperators
public infix fun IE.lte(other: BigInteger): Boolean = this.bigIntegerValue <= other
@IonOperators
public infix fun IE.gte(other: BigInteger): Boolean = this.bigIntegerValue >= other

/* Int with IntElement binary operators */
@IonOperators
public operator fun Int.plus(other: IE): IE = intOp(this, other, Math::addExact, BigInteger::plus)
@IonOperators
public operator fun Int.minus(other: IE): IE = intOp(this, other, Math::subtractExact, BigInteger::minus)
@IonOperators
public operator fun Int.times(other: IE): IE = intOp(this, other, Math::multiplyExact, BigInteger::times)
@IonOperators
public operator fun Int.div(other: IE): IE = intOp(this, other, Long::div, BigInteger::div)
@IonOperators
public operator fun Int.rem(other: IE): IE = intOp(this, other, Long::rem, BigInteger::rem)
@IonOperators
public infix fun Int.eq(other: IE): Boolean = intCmp(this, other) == 0
@IonOperators
public infix fun Int.lt(other: IE): Boolean = intCmp(this, other) < 0
@IonOperators
public infix fun Int.gt(other: IE): Boolean = intCmp(this, other) > 0
@IonOperators
public infix fun Int.lte(other: IE): Boolean = intCmp(this, other) <= 0
@IonOperators
public infix fun Int.gte(other: IE): Boolean = intCmp(this, other) >= 0

/* Long with IntElement binary operators */
@IonOperators
public operator fun Long.plus(other: IE): IE = intOp(this, other, Math::addExact, BigInteger::plus)
@IonOperators
public operator fun Long.minus(other: IE): IE = intOp(this, other, Math::subtractExact, BigInteger::minus)
@IonOperators
public operator fun Long.times(other: IE): IE = intOp(this, other, Math::multiplyExact, BigInteger::times)
@IonOperators
public operator fun Long.div(other: IE): IE = intOp(this, other, Long::div, BigInteger::div)
@IonOperators
public operator fun Long.rem(other: IE): IE = intOp(this, other, Long::rem, BigInteger::rem)
@IonOperators
public infix fun Long.eq(other: IE): Boolean = intCmp(this, other) == 0
@IonOperators
public infix fun Long.lt(other: IE): Boolean = intCmp(this, other) < 0
@IonOperators
public infix fun Long.gt(other: IE): Boolean = intCmp(this, other) > 0
@IonOperators
public infix fun Long.lte(other: IE): Boolean = intCmp(this, other) <= 0
@IonOperators
public infix fun Long.gte(other: IE): Boolean = intCmp(this, other) >= 0

/* BigInteger with IntElement binary operators */
@IonOperators
public operator fun BigInteger.plus(other: IE): IE = ionInt(this + other.bigIntegerValue, other.annotations, other.metas)
@IonOperators
public operator fun BigInteger.minus(other: IE): IE = ionInt(this - other.bigIntegerValue, other.annotations, other.metas)
@IonOperators
public operator fun BigInteger.times(other: IE): IE = ionInt(this * other.bigIntegerValue, other.annotations, other.metas)
@IonOperators
public operator fun BigInteger.div(other: IE): IE = ionInt(this / other.bigIntegerValue, other.annotations, other.metas)
@IonOperators
public operator fun BigInteger.rem(other: IE): IE = ionInt(this % other.bigIntegerValue, other.annotations, other.metas)
@IonOperators
public infix fun BigInteger.eq(other: IE): Boolean = this == other.bigIntegerValue
@IonOperators
public infix fun BigInteger.lt(other: IE): Boolean = this < other.bigIntegerValue
@IonOperators
public infix fun BigInteger.gt(other: IE): Boolean = this > other.bigIntegerValue
@IonOperators
public infix fun BigInteger.lte(other: IE): Boolean = this <= other.bigIntegerValue
@IonOperators
public infix fun BigInteger.gte(other: IE): Boolean = this >= other.bigIntegerValue

/* DecimalElement unary operators */
@IonOperators
public operator fun DE.unaryMinus(): DE = ionDecimal(Decimal.valueOf(-this.decimalValue), this.annotations, this.metas)
@IonOperators
public operator fun DE.inc(): DE = ionDecimal(Decimal.valueOf(this.decimalValue.inc()), this.annotations, this.metas)
@IonOperators
public operator fun DE.dec(): DE = ionDecimal(Decimal.valueOf(this.decimalValue.dec()), this.annotations, this.metas)

/* DecimalElement with DecimalElement operators */
@IonOperators
public infix fun DE.eq(other: DE): Boolean = this.decimalValue == other.decimalValue
@IonOperators
public infix fun DE.lt(other: DE): Boolean = this.decimalValue < other.decimalValue
@IonOperators
public infix fun DE.gt(other: DE): Boolean = this.decimalValue > other.decimalValue
@IonOperators
public infix fun DE.lte(other: DE): Boolean = this.decimalValue <= other.decimalValue
@IonOperators
public infix fun DE.gte(other: DE): Boolean = this.decimalValue >= other.decimalValue

/* DecimalElement with BigDecimal operators */
@IonOperators
public operator fun DE.plus(other: BigDecimal): DE = ionDecimal(Decimal.valueOf(this.decimalValue + other), this.annotations, this.metas)
@IonOperators
public operator fun DE.minus(other: BigDecimal): DE = ionDecimal(Decimal.valueOf(this.decimalValue - other), this.annotations, this.metas)
@IonOperators
public operator fun DE.times(other: BigDecimal): DE = ionDecimal(Decimal.valueOf(this.decimalValue * other), this.annotations, this.metas)
@IonOperators
public operator fun DE.div(other: BigDecimal): DE = ionDecimal(Decimal.valueOf(this.decimalValue / other), this.annotations, this.metas)
@IonOperators
public operator fun DE.rem(other: BigDecimal): DE = ionDecimal(Decimal.valueOf(this.decimalValue % other), this.annotations, this.metas)
@IonOperators
public infix fun DE.eq(other: BigDecimal): Boolean = this.decimalValue == other
@IonOperators
public infix fun DE.lt(other: BigDecimal): Boolean = this.decimalValue < other
@IonOperators
public infix fun DE.gt(other: BigDecimal): Boolean = this.decimalValue > other
@IonOperators
public infix fun DE.lte(other: BigDecimal): Boolean = this.decimalValue <= other
@IonOperators
public infix fun DE.gte(other: BigDecimal): Boolean = this.decimalValue >= other

/* BigDecimal with DecimalElement operators */
@IonOperators
public operator fun BigDecimal.plus(other: DE): DE = ionDecimal(Decimal.valueOf(this + other.decimalValue), other.annotations, other.metas)
@IonOperators
public operator fun BigDecimal.minus(other: DE): DE = ionDecimal(Decimal.valueOf(this - other.decimalValue), other.annotations, other.metas)
@IonOperators
public operator fun BigDecimal.times(other: DE): DE = ionDecimal(Decimal.valueOf(this * other.decimalValue), other.annotations, other.metas)
@IonOperators
public operator fun BigDecimal.div(other: DE): DE = ionDecimal(Decimal.valueOf(this / other.decimalValue), other.annotations, other.metas)
@IonOperators
public operator fun BigDecimal.rem(other: DE): DE = ionDecimal(Decimal.valueOf(this % other.decimalValue), other.annotations, other.metas)
@IonOperators
public infix fun BigDecimal.eq(other: DE): Boolean = this == other.decimalValue
@IonOperators
public infix fun BigDecimal.lt(other: DE): Boolean = this < other.decimalValue
@IonOperators
public infix fun BigDecimal.gt(other: DE): Boolean = this > other.decimalValue
@IonOperators
public infix fun BigDecimal.lte(other: DE): Boolean = this <= other.decimalValue
@IonOperators
public infix fun BigDecimal.gte(other: DE): Boolean = this >= other.decimalValue

/* FloatElement unary operators */
@IonOperators
public operator fun FE.unaryMinus(): FE = ionFloat(-this.doubleValue, this.annotations, this.metas)
@IonOperators
public operator fun FE.inc(): FE = ionFloat(this.doubleValue.inc(), this.annotations, this.metas)
@IonOperators
public operator fun FE.dec(): FE = ionFloat(this.doubleValue.dec(), this.annotations, this.metas)

/* FloatElement with FloatElement operators */
@IonOperators
public infix fun FE.eq(other: FE): Boolean = this.doubleValue == other.doubleValue
@IonOperators
public infix fun FE.lt(other: FE): Boolean = this.doubleValue < other.doubleValue
@IonOperators
public infix fun FE.gt(other: FE): Boolean = this.doubleValue > other.doubleValue
@IonOperators
public infix fun FE.lte(other: FE): Boolean = this.doubleValue <= other.doubleValue
@IonOperators
public infix fun FE.gte(other: FE): Boolean = this.doubleValue >= other.doubleValue

/* FloatElement with Double operators */
@IonOperators
public operator fun FE.plus(other: Double): FE = ionFloat(this.doubleValue + other, this.annotations, this.metas)
@IonOperators
public operator fun FE.minus(other: Double): FE = ionFloat(this.doubleValue - other, this.annotations, this.metas)
@IonOperators
public operator fun FE.times(other: Double): FE = ionFloat(this.doubleValue * other, this.annotations, this.metas)
@IonOperators
public operator fun FE.div(other: Double): FE = ionFloat(this.doubleValue / other, this.annotations, this.metas)
@IonOperators
public operator fun FE.rem(other: Double): FE = ionFloat(this.doubleValue % other, this.annotations, this.metas)
@IonOperators
public infix fun FE.eq(other: Double): Boolean = this.doubleValue == other
@IonOperators
public infix fun FE.lt(other: Double): Boolean = this.doubleValue < other
@IonOperators
public infix fun FE.gt(other: Double): Boolean = this.doubleValue > other
@IonOperators
public infix fun FE.lte(other: Double): Boolean = this.doubleValue <= other
@IonOperators
public infix fun FE.gte(other: Double): Boolean = this.doubleValue >= other

/* Double with FloatElement operators */
@IonOperators
public operator fun Double.plus(other: FloatElement): FE = ionFloat(this + other.doubleValue, other.annotations, other.metas)
@IonOperators
public operator fun Double.minus(other: FloatElement): FE = ionFloat(this - other.doubleValue, other.annotations, other.metas)
@IonOperators
public operator fun Double.times(other: FloatElement): FE = ionFloat(this * other.doubleValue, other.annotations, other.metas)
@IonOperators
public operator fun Double.div(other: FloatElement): FE = ionFloat(this / other.doubleValue, other.annotations, other.metas)
@IonOperators
public operator fun Double.rem(other: FloatElement): FE = ionFloat(this % other.doubleValue, other.annotations, other.metas)
@IonOperators
public infix fun Double.eq(other: FloatElement): Boolean = this == other.doubleValue
@IonOperators
public infix fun Double.lt(other: FloatElement): Boolean = this < other.doubleValue
@IonOperators
public infix fun Double.gt(other: FloatElement): Boolean = this > other.doubleValue
@IonOperators
public infix fun Double.lte(other: FloatElement): Boolean = this <= other.doubleValue
@IonOperators
public infix fun Double.gte(other: FloatElement): Boolean = this >= other.doubleValue

/* Text Element operators */
@IonOperators
public operator fun StringElement.plus(other: CharSequence): StringElement = ionString(this.textValue + other, this.annotations, this.metas)
@IonOperators
public operator fun SymbolElement.plus(other: CharSequence): SymbolElement = ionSymbol(this.textValue + other, this.annotations, this.metas)

/* Integer operator helpers */
private fun useLong(x: IE): Boolean = x.integerSize == LONG
private fun useLong(x: IE, y: IE): Boolean = x.integerSize == LONG && y.integerSize == LONG

private fun intOp(x: IE, y: Long, longOp: (Long, Long) -> Long, bigOp: (BigInteger, BigInteger) -> BigInteger): IE {
    return if (useLong(x)) {
        try {
            ionInt(longOp(x.longValue, y), x.annotations, x.metas)
        } catch (e: ArithmeticException) {
            ionInt(bigOp(x.bigIntegerValue, y.toBigInteger()), x.annotations, x.metas)
        }
    } else {
        ionInt(bigOp(x.bigIntegerValue, y.toBigInteger()), x.annotations, x.metas)
    }
}

private fun intOp(x: Long, y: IE, longOp: (Long, Long) -> Long, bigOp: (BigInteger, BigInteger) -> BigInteger): IE {
    return if (useLong(y)) {
        try {
            ionInt(longOp(x, y.longValue), y.annotations, y.metas)
        } catch (e: ArithmeticException) {
            ionInt(bigOp(x.toBigInteger(), y.bigIntegerValue), y.annotations, y.metas)
        }
    } else {
        ionInt(bigOp(x.toBigInteger(), y.bigIntegerValue), y.annotations, y.metas)
    }
}

private fun intOp(x: IE, y: Int, longOp: (Long, Long) -> Long, bigOp: (BigInteger, BigInteger) -> BigInteger): IE {
    return intOp(x, y.toLong(), longOp, bigOp)
}

private fun intOp(x: Int, y: IE, longOp: (Long, Long) -> Long, bigOp: (BigInteger, BigInteger) -> BigInteger): IE {
    return intOp(x.toLong(), y, longOp, bigOp)
}

private fun intCmp(x: IE, y: IE): Int {
    return if (useLong(x, y)) {
        x.longValue.compareTo(y.longValue)
    } else {
        x.bigIntegerValue.compareTo(y.bigIntegerValue)
    }
}

private fun intCmp(x: IE, y: Long): Int {
    return if (useLong(x)) {
        x.longValue.compareTo(y)
    } else {
        x.bigIntegerValue.compareTo(y.toBigInteger())
    }
}

private fun intCmp(x: IE, y: Int) = intCmp(x, y.toLong())

private fun intCmp(x: Long, y: IE): Int {
    return if (useLong(y)) {
        x.compareTo(y.longValue)
    } else {
        x.toBigInteger().compareTo(y.bigIntegerValue)
    }
}

private fun intCmp(x: Int, y: IE) = intCmp(x.toLong(), y)
