// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin10

import com.amazon.ion.Decimal
import com.amazon.ion.IonException
import com.amazon.ion.Timestamp
import java.math.BigDecimal
import java.math.BigInteger
import kotlin.experimental.and

/**
 * Given a typeId in the range 0x20..0x3F, returns either -1 or 1.
 * This uses some clever bit twiddling to avoid any branching.
 *
 * (Yes, it's obtuse looking, but it works.)
 */
internal fun signForIntTypeId(typeId: Int): Int = (((typeId shr 4) shl 31) shr 31) or 1

/**
 * Return either -1 or 1 based on the sign bit of the given byte. This uses some bit manipulation to avoid any branching.
 */
internal fun getSignumValueFromLeadingSignBit(byte: Byte): Int = byte.toInt().shr(7).shl(1) + 1

/**
 * Reads a timestamp value from the given byte array.
 */
internal fun readTimestampReference(valueBytes: ByteArray, position: Int, length: Int): Timestamp {
    var p = position
    val end = position + length

    val offset: Int? = if (valueBytes[p].toInt() and 0xFF == 0xC0) {
        p++
        null
    } else {
        val offsetValueAndLength = VarIntHelper.readVarIntValueAndLength(valueBytes, p)
        p += offsetValueAndLength.toInt() and 0xFF
        (offsetValueAndLength shr 8).toInt()
    }
    val yearValueAndLength = VarIntHelper.readVarUIntValueAndLength(valueBytes, p)
    p += yearValueAndLength.toInt() and 0xFF
    val year = (yearValueAndLength shr 8).toInt()
    var month = 0
    var day = 0
    var hour = 0
    var minute = 0
    var second = 0
    var fractionalSecond: BigDecimal? = null
    var precision = Timestamp.Precision.YEAR
    if (p < end) {
        val monthValueAndLength = VarIntHelper.readVarUIntValueAndLength(valueBytes, p)
        p += monthValueAndLength.toInt() and 0xFF
        month = (monthValueAndLength shr 8).toInt()
        precision = Timestamp.Precision.MONTH
        if (p < end) {
            val dayValueAndLength = VarIntHelper.readVarUIntValueAndLength(valueBytes, p)
            p += dayValueAndLength.toInt() and 0xFF
            day = (dayValueAndLength shr 8).toInt()
            precision = Timestamp.Precision.DAY
            if (p < end) {
                val hourValueAndLength = VarIntHelper.readVarUIntValueAndLength(valueBytes, p)
                p += hourValueAndLength.toInt() and 0xFF
                hour = (hourValueAndLength shr 8).toInt()
                if (p >= end) {
                    throw IonException("Timestamps may not specify hour without specifying minute.")
                }

                val minuteValueAndLength = VarIntHelper.readVarUIntValueAndLength(valueBytes, p)
                p += minuteValueAndLength.toInt() and 0xFF
                minute = (minuteValueAndLength shr 8).toInt()
                precision = Timestamp.Precision.MINUTE
                if (p < end) {
                    val secondValueAndLength = VarIntHelper.readVarUIntValueAndLength(valueBytes, p)
                    p += secondValueAndLength.toInt() and 0xFF
                    second = (secondValueAndLength shr 8).toInt()
                    precision = Timestamp.Precision.SECOND
                    if (p < end) {
                        fractionalSecond = readDecimalReference(valueBytes, p, end)
                        if (fractionalSecond.scale() < 0) {
                            fractionalSecond = fractionalSecond.setScale(0)
                        }
                    }
                }
            }
        }
    }
    try {
        return Timestamp.createFromUtcFields(
            precision,
            year,
            month,
            day,
            hour,
            minute,
            second,
            fractionalSecond,
            offset
        )
    } catch (e: IllegalArgumentException) {
        println("Timestamp starting at $position")
        throw IonException("Illegal timestamp encoding. ", e)
    }
}

/**
 * Reads a Decimal value from the given byte array.
 */
internal fun readDecimalReference(valueBytes: ByteArray, position: Int, end: Int): Decimal {
    var p = position
    val exponentValueAndLength = VarIntHelper.readVarIntValueAndLength(valueBytes, p)
    p += exponentValueAndLength.toInt() and 0xFF
    val scale = -(exponentValueAndLength shr 8).toInt()

    val coefficientLength = end - p
    return if (coefficientLength > 0) {
        // TODO: See if we can have a shared set of reusable buffers for this instead of allocating a copy.
        val bytes = valueBytes.copyOfRange(p, p + coefficientLength)

        // Get the signum
        val signum = getSignumValueFromLeadingSignBit(bytes[0])
        // Clear the sign bit
        bytes[0] = bytes[0] and 0x7F
        // Construct the BigInteger
        val coefficient = BigInteger(signum, bytes)
        if (coefficient == BigInteger.ZERO && signum == -1) {
            Decimal.negativeZero(scale)
        } else {
            Decimal.valueOf(BigInteger(signum, bytes), scale)
        }
    } else {
        Decimal.valueOf(BigInteger.ZERO, scale)
    }
}
