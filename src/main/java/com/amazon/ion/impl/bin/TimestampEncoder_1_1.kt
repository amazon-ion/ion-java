// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.bin

import com.amazon.ion.Timestamp
import com.amazon.ion.bytecode.bin11.OpCode

/**
 * Helper for writing Ion 1.1 binary timestamps.
 */
internal object TimestampEncoder_1_1 {
    /**
     * Writes a Timestamp to the given WriteBuffer using the Ion 1.1 encoding for Ion Timestamps.
     * @return the number of bytes written
     */
    @JvmStatic
    fun writeTimestampValue(buffer: WriteBuffer, value: Timestamp): Int {

        // Timestamps may be encoded using the short form if they meet certain conditions.
        // Condition 1: The year is between 1970 and 2097.
        if (value.year < 1970 || value.year > 2097) {
            buffer.writeByte(OpCode.VARIABLE_LENGTH_TIMESTAMP.toByte())
            val bodyLength = writeLongFormTimestampBody(buffer, value)
            return 1 + bodyLength
        }

        var shortOpcode = OpCode.TIMESTAMP_YEAR_PRECISION + value.precision.ordinal

        // If the precision is year, month, or day, we can skip the remaining checks.
        if (!value.precision.includes(Timestamp.Precision.MINUTE)) {
            buffer.writeByte(shortOpcode.toByte())
            return 1 + writeTaglessTimestampValue(buffer, shortOpcode, value)
        }

        // Condition 2: The fractional seconds are a common precision.
        if (value.zFractionalSecond != null) {
            val secondsScale = value.zFractionalSecond.scale()
            if (secondsScale != 0 && secondsScale != 3 && secondsScale != 6 && secondsScale != 9) {
                buffer.writeByte(OpCode.VARIABLE_LENGTH_TIMESTAMP.toByte())
                val bodyLength = writeLongFormTimestampBody(buffer, value)
                return 1 + bodyLength
            } else {
                shortOpcode += secondsScale / 3
            }
        }

        // Condition 3: The local offset is either UTC, unknown, or falls between -14:00 to +14:00 and is divisible by 15 minutes.
        val offset = value.localOffset
        if (offset == null || offset == 0) {
            buffer.writeByte(shortOpcode.toByte())
            return 1 + writeTaglessTimestampValue(buffer, shortOpcode, value)
        } else if (offset >= -14 * 60 && offset <= 14 * 60 && offset % 15 == 0) {
            shortOpcode += OpCode.TIMESTAMP_MINUTE_PRECISION_WITH_OFFSET - OpCode.TIMESTAMP_MINUTE_PRECISION
            buffer.writeByte(shortOpcode.toByte())
            return 1 + writeShortTimestampWithOffsetBody(buffer, shortOpcode, value)
        } else {
            buffer.writeByte(OpCode.VARIABLE_LENGTH_TIMESTAMP.toByte())
            val bodyLength = writeLongFormTimestampBody(buffer, value)
            return 1 + bodyLength
        }
    }

    @JvmStatic
    fun writeTaglessTimestampValue(buffer: WriteBuffer, implicitOpcode: Int, value: Timestamp): Int {
        // Rather than have a lot of early-escape branching points, we'll just fill the bits with all the
        // timestamp fields and then have one branch at the end to write the correct number of bits.
        // This also keeps the code a little shorter.

        // TODO(perf) revisit this to see if this is slower than branching earlier.

        var bits = (value.year - 1970L)
        bits = bits or ((value.month.toLong()) shl Ion_1_1_Constants.S_TIMESTAMP_MONTH_BIT_OFFSET)
        bits = bits or ((value.day.toLong()) shl Ion_1_1_Constants.S_TIMESTAMP_DAY_BIT_OFFSET)
        bits = bits or ((value.hour.toLong()) shl Ion_1_1_Constants.S_TIMESTAMP_HOUR_BIT_OFFSET)
        bits = bits or ((value.minute.toLong()) shl Ion_1_1_Constants.S_TIMESTAMP_MINUTE_BIT_OFFSET)
        if (value.localOffset != null) {
            bits = bits or Ion_1_1_Constants.S_U_TIMESTAMP_UTC_FLAG.toLong()
        }
        bits = bits or ((value.second.toLong()) shl Ion_1_1_Constants.S_U_TIMESTAMP_SECOND_BIT_OFFSET)

        val size = when (implicitOpcode) {
            OpCode.TIMESTAMP_YEAR_PRECISION -> {
                // Chop off the month and day bits, if there are any.
                bits = bits and ((1L shl Ion_1_1_Constants.S_TIMESTAMP_MONTH_BIT_OFFSET) - 1)
                1
            }
            OpCode.TIMESTAMP_MONTH_PRECISION -> {
                // Chop off the day bits, if there are any.
                bits = bits and ((1L shl Ion_1_1_Constants.S_TIMESTAMP_DAY_BIT_OFFSET) - 1)
                2
            }
            OpCode.TIMESTAMP_DAY_PRECISION -> 2
            OpCode.TIMESTAMP_MINUTE_PRECISION -> 4
            OpCode.TIMESTAMP_SECOND_PRECISION -> 5
            OpCode.TIMESTAMP_MILLIS_PRECISION -> {
                val fractionalSeconds = value.zFractionalSecond.unscaledValue().toLong()
                bits = bits or (fractionalSeconds shl Ion_1_1_Constants.S_U_TIMESTAMP_FRACTION_BIT_OFFSET)
                6
            }
            OpCode.TIMESTAMP_MICROS_PRECISION -> {
                val fractionalSeconds = value.zFractionalSecond.unscaledValue().toLong()
                bits = bits or (fractionalSeconds shl Ion_1_1_Constants.S_U_TIMESTAMP_FRACTION_BIT_OFFSET)
                7
            }
            OpCode.TIMESTAMP_NANOS_PRECISION -> {
                val fractionalSeconds = value.zFractionalSecond.unscaledValue().toLong()
                bits = bits or (fractionalSeconds shl Ion_1_1_Constants.S_U_TIMESTAMP_FRACTION_BIT_OFFSET)
                8
            }
            else -> throw IllegalStateException("This is unreachable!")
        }
        buffer.writeFixedIntOrUInt(bits, size)
        return size
    }

    @JvmStatic
    private fun writeShortTimestampWithOffsetBody(buffer: WriteBuffer, implicitOpcode: Int, value: Timestamp): Int {
        // Rather than have a lot of early-escape branching points, we'll just fill the bits with all the
        // timestamp fields and then have one branch at the end to write the correct number of bits.
        // This also keeps the code a little shorter.

        // TODO(perf) revisit this to see if this is slower than branching earlier.

        var bits = (value.year - 1970L)
        bits = bits or ((value.month.toLong()) shl Ion_1_1_Constants.S_TIMESTAMP_MONTH_BIT_OFFSET)
        bits = bits or ((value.day.toLong()) shl Ion_1_1_Constants.S_TIMESTAMP_DAY_BIT_OFFSET)
        bits = bits or ((value.hour.toLong()) shl Ion_1_1_Constants.S_TIMESTAMP_HOUR_BIT_OFFSET)
        bits = bits or ((value.minute.toLong()) shl Ion_1_1_Constants.S_TIMESTAMP_MINUTE_BIT_OFFSET)
        val localOffset = (value.localOffset.toLong() / 15) + (14 * 4)
        bits = bits or ((localOffset and Ion_1_1_Constants.LEAST_SIGNIFICANT_7_BITS) shl Ion_1_1_Constants.S_O_TIMESTAMP_OFFSET_BIT_OFFSET)
        bits = bits or ((value.second.toLong()) shl Ion_1_1_Constants.S_O_TIMESTAMP_SECOND_BIT_OFFSET)

        buffer.writeFixedIntOrUInt(bits, 5)

        // The fractional seconds bits will be put into a separate long because we need nine bytes total
        // if there are nanoseconds (which is too much for one long) and the boundary between the seconds
        // and fractional seconds subfields conveniently aligns with a byte boundary.
        var fractionBits = 0L

        val size = when (implicitOpcode) {
            OpCode.TIMESTAMP_MINUTE_PRECISION_WITH_OFFSET,
            OpCode.TIMESTAMP_SECOND_PRECISION_WITH_OFFSET -> {
                5
            }
            OpCode.TIMESTAMP_MILLIS_PRECISION_WITH_OFFSET -> {
                fractionBits = value.zFractionalSecond.unscaledValue().toLong()
                buffer.writeFixedIntOrUInt(fractionBits, 2)
                7
            }
            OpCode.TIMESTAMP_MICROS_PRECISION_WITH_OFFSET -> {
                fractionBits = value.zFractionalSecond.unscaledValue().toLong()
                buffer.writeFixedIntOrUInt(fractionBits, 3)
                8
            }
            OpCode.TIMESTAMP_NANOS_PRECISION_WITH_OFFSET -> {
                fractionBits = value.zFractionalSecond.unscaledValue().toLong()
                buffer.writeFixedIntOrUInt(fractionBits, 4)
                9
            }
            else -> throw IllegalStateException("This is unreachable!")
        }
        return size
    }

    /**
     * Writes a long-form timestamp.
     * Value may not be null.
     * Only visible for testing. If calling from outside this class, use writeTimestampValue instead.
     */
    @JvmStatic
    internal fun writeLongFormTimestampBody(buffer: WriteBuffer, value: Timestamp): Int {
        var bits = value.year.toLong()
        if (value.precision == Timestamp.Precision.YEAR) {
            buffer.writeFlexUInt(2)
            buffer.writeFixedIntOrUInt(bits, 2)
            return 3 // FlexUInt + 2 bytes data
        }

        bits = bits or ((value.month.toLong()) shl Ion_1_1_Constants.L_TIMESTAMP_MONTH_BIT_OFFSET)
        if (value.precision == Timestamp.Precision.MONTH) {
            buffer.writeFlexUInt(3)
            buffer.writeFixedIntOrUInt(bits, 3)
            return 4 // FlexUInt + 3 bytes data
        }

        bits = bits or ((value.day.toLong()) shl Ion_1_1_Constants.L_TIMESTAMP_DAY_BIT_OFFSET)
        if (value.precision == Timestamp.Precision.DAY) {
            buffer.writeFlexUInt(3)
            buffer.writeFixedIntOrUInt(bits, 3)
            return 4 // FlexUInt + 3 bytes data
        }

        bits = bits or ((value.hour.toLong()) shl Ion_1_1_Constants.L_TIMESTAMP_HOUR_BIT_OFFSET)
        bits = bits or ((value.minute.toLong()) shl Ion_1_1_Constants.L_TIMESTAMP_MINUTE_BIT_OFFSET)
        var localOffsetValue = Ion_1_1_Constants.L_TIMESTAMP_UNKNOWN_OFFSET_VALUE.toLong()
        if (value.localOffset != null) {
            localOffsetValue = (value.localOffset + (24 * 60)).toLong()
        }
        bits = bits or (localOffsetValue shl Ion_1_1_Constants.L_TIMESTAMP_OFFSET_BIT_OFFSET)

        if (value.precision == Timestamp.Precision.MINUTE) {
            buffer.writeFlexUInt(6)
            buffer.writeFixedIntOrUInt(bits, 6)
            return 7 // FlexUInt + 6 bytes data
        }

        bits = bits or ((value.second.toLong()) shl Ion_1_1_Constants.L_TIMESTAMP_SECOND_BIT_OFFSET)
        var secondsScale = 0
        if (value.zFractionalSecond != null) {
            secondsScale = value.zFractionalSecond.scale()
        }
        if (secondsScale == 0) {
            buffer.writeFlexUInt(7)
            buffer.writeFixedIntOrUInt(bits, 7)
            return 8 // FlexUInt + 7 bytes data
        }

        val fractionalSeconds = value.zFractionalSecond
        val coefficient = fractionalSeconds.unscaledValue()
        val exponent = fractionalSeconds.scale().toLong()
        val numCoefficientBytes = PrimitiveEncoder.flexUIntLength(coefficient)
        val numExponentBytes = WriteBuffer.fixedUIntLength(exponent)
        // Years-seconds data (7 bytes) + fraction coefficient + fraction exponent
        val dataLength = 7 + numCoefficientBytes + numExponentBytes

        val lengthOfLength = buffer.writeFlexUInt(dataLength)
        buffer.writeFixedIntOrUInt(bits, 7)
        buffer.writeFlexUInt(coefficient)
        buffer.writeFixedUInt(exponent)

        return lengthOfLength + dataLength
    }
}
