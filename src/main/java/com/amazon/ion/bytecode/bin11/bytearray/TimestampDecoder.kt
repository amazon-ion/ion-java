// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11.bytearray

import com.amazon.ion.IonException
import com.amazon.ion.Timestamp
import com.amazon.ion.bytecode.bin11.bytearray.PrimitiveDecoder.readFixedInt16
import com.amazon.ion.bytecode.bin11.bytearray.PrimitiveDecoder.readFixedInt32
import com.amazon.ion.bytecode.bin11.bytearray.PrimitiveDecoder.readFixedInt8AsShort
import com.amazon.ion.bytecode.bin11.bytearray.PrimitiveDecoder.readFixedIntAsLong
import com.amazon.ion.impl.bin.Ion_1_1_Constants.*
import java.math.BigDecimal

/**
 * Helper class for decoding the various timestamp encoding variants from a [ByteArray].
 *
 * TODO(perf): avoid auto-boxing the `0` integer for the offset when constructing the Timestamp instance.
 */
internal object TimestampDecoder {
    private const val MASK_4 = 0b1111
    private const val MASK_5 = 0b11111
    private const val MASK_6 = 0b111111
    private const val MASK_7 = 0b1111111
    private const val MASK_4L = 0b1111L
    private const val MASK_5L = 0b11111L
    private const val MASK_6L = 0b111111L
    private const val MASK_7L = 0b1111111L
    private const val MASK_10L = 0b1111111111L
    private const val MASK_20L = 0b11111111111111111111L
    private const val MASK_30L = 0b111111111111111111111111111111L

    fun readTimestampToYear(source: ByteArray, position: Int): Timestamp {
        val year = readFixedInt8AsShort(source, position).toInt()
        return Timestamp.forYear(year + 1970)
    }

    fun readTimestampToMonth(source: ByteArray, position: Int): Timestamp {
        val yearAndMonth = readFixedInt16(source, position).toInt()
        val year = yearAndMonth.and(MASK_7)
        val month = yearAndMonth.shr(S_TIMESTAMP_MONTH_BIT_OFFSET)

        return Timestamp.forMonth(year + 1970, month)
    }

    fun readTimestampToDay(source: ByteArray, position: Int): Timestamp {
        val yearMonthAndDay = readFixedInt16(source, position).toInt()
        val year = yearMonthAndDay.and(MASK_7)
        val month = yearMonthAndDay.shr(S_TIMESTAMP_MONTH_BIT_OFFSET).and(MASK_4)
        val day = yearMonthAndDay.shr(S_TIMESTAMP_DAY_BIT_OFFSET)

        return Timestamp.forDay(year + 1970, month, day)
    }

    fun readTimestampToMinuteUTCOrUnknown(source: ByteArray, position: Int): Timestamp {
        val data = readFixedInt32(source, position)
        val year = data.and(MASK_7)
        val month = data.shr(S_TIMESTAMP_MONTH_BIT_OFFSET).and(MASK_4)
        val day = data.shr(S_TIMESTAMP_DAY_BIT_OFFSET).and(MASK_5)
        val hour = data.shr(S_TIMESTAMP_HOUR_BIT_OFFSET).and(MASK_5)
        val minute = data.shr(S_TIMESTAMP_MINUTE_BIT_OFFSET).and(MASK_6)
        val isUTC = data.and(S_U_TIMESTAMP_UTC_FLAG) != 0

        return Timestamp.forMinute(year + 1970, month, day, hour, minute, if (isUTC) 0 else null)
    }

    fun readTimestampToSecondUTCOrUnknown(source: ByteArray, position: Int): Timestamp {
        val data = readFixedIntAsLong(source, position, 5)
        val year = data.and(MASK_7L).toInt()
        val month = data.shr(S_TIMESTAMP_MONTH_BIT_OFFSET).and(MASK_4L).toInt()
        val day = data.shr(S_TIMESTAMP_DAY_BIT_OFFSET).and(MASK_5L).toInt()
        val hour = data.shr(S_TIMESTAMP_HOUR_BIT_OFFSET).and(MASK_5L).toInt()
        val minute = data.shr(S_TIMESTAMP_MINUTE_BIT_OFFSET).and(MASK_6L).toInt()
        val second = data.shr(S_U_TIMESTAMP_SECOND_BIT_OFFSET).and(MASK_6L).toInt()
        val isUTC = data.and(S_U_TIMESTAMP_UTC_FLAG_L) != 0L

        return Timestamp.forSecond(year + 1970, month, day, hour, minute, second, if (isUTC) 0 else null)
    }

    fun readTimestampToMillisecondUTCOrUnknown(source: ByteArray, position: Int): Timestamp {
        val data = readFixedIntAsLong(source, position, 6)
        val year = data.and(MASK_7L).toInt()
        val month = data.shr(S_TIMESTAMP_MONTH_BIT_OFFSET).and(MASK_4L).toInt()
        val day = data.shr(S_TIMESTAMP_DAY_BIT_OFFSET).and(MASK_5L).toInt()
        val hour = data.shr(S_TIMESTAMP_HOUR_BIT_OFFSET).and(MASK_5L).toInt()
        val minute = data.shr(S_TIMESTAMP_MINUTE_BIT_OFFSET).and(MASK_6L).toInt()
        val second = data.shr(S_U_TIMESTAMP_SECOND_BIT_OFFSET).and(MASK_6L)
        val fractionalSecond = data.shr(S_U_TIMESTAMP_FRACTION_BIT_OFFSET).and(MASK_10L)
        val isUTC = data.and(S_U_TIMESTAMP_UTC_FLAG_L) != 0L

        val secondBigDecimal = BigDecimal.valueOf(second)
        val fractionalSecondBigDecimal = BigDecimal.valueOf(fractionalSecond, 3)
        return Timestamp.forSecond(year + 1970, month, day, hour, minute, secondBigDecimal.add(fractionalSecondBigDecimal), if (isUTC) 0 else null)
    }

    fun readTimestampToMicrosecondUTCOrUnknown(source: ByteArray, position: Int): Timestamp {
        val data = readFixedIntAsLong(source, position, 7)
        val year = data.and(MASK_7L).toInt()
        val month = data.shr(S_TIMESTAMP_MONTH_BIT_OFFSET).and(MASK_4L).toInt()
        val day = data.shr(S_TIMESTAMP_DAY_BIT_OFFSET).and(MASK_5L).toInt()
        val hour = data.shr(S_TIMESTAMP_HOUR_BIT_OFFSET).and(MASK_5L).toInt()
        val minute = data.shr(S_TIMESTAMP_MINUTE_BIT_OFFSET).and(MASK_6L).toInt()
        val second = data.shr(S_U_TIMESTAMP_SECOND_BIT_OFFSET).and(MASK_6L)
        val fractionalSecond = data.shr(S_U_TIMESTAMP_FRACTION_BIT_OFFSET).and(MASK_20L)
        val isUTC = data.and(S_U_TIMESTAMP_UTC_FLAG_L) != 0L

        val secondBigDecimal = BigDecimal.valueOf(second)
        val fractionalSecondBigDecimal = BigDecimal.valueOf(fractionalSecond, 6)
        return Timestamp.forSecond(year + 1970, month, day, hour, minute, secondBigDecimal.add(fractionalSecondBigDecimal), if (isUTC) 0 else null)
    }

    fun readTimestampToNanosecondUTCOrUnknown(source: ByteArray, position: Int): Timestamp {
        val data = readFixedIntAsLong(source, position, 8)
        val year = data.and(MASK_7L).toInt()
        val month = data.shr(S_TIMESTAMP_MONTH_BIT_OFFSET).and(MASK_4L).toInt()
        val day = data.shr(S_TIMESTAMP_DAY_BIT_OFFSET).and(MASK_5L).toInt()
        val hour = data.shr(S_TIMESTAMP_HOUR_BIT_OFFSET).and(MASK_5L).toInt()
        val minute = data.shr(S_TIMESTAMP_MINUTE_BIT_OFFSET).and(MASK_6L).toInt()
        val second = data.shr(S_U_TIMESTAMP_SECOND_BIT_OFFSET).and(MASK_6L)
        val fractionalSecond = data.ushr(S_U_TIMESTAMP_FRACTION_BIT_OFFSET).and(MASK_30L)
        val isUTC = data.and(S_U_TIMESTAMP_UTC_FLAG_L) != 0L

        val secondBigDecimal = BigDecimal.valueOf(second)
        val fractionalSecondBigDecimal = BigDecimal.valueOf(fractionalSecond, 9)
        return Timestamp.forSecond(year + 1970, month, day, hour, minute, secondBigDecimal.add(fractionalSecondBigDecimal), if (isUTC) 0 else null)
    }

    fun readTimestampToMinuteWithOffset(source: ByteArray, position: Int): Timestamp {
        val data = readFixedIntAsLong(source, position, 5)
        val year = data.and(MASK_7L).toInt()
        val month = data.shr(S_TIMESTAMP_MONTH_BIT_OFFSET).and(MASK_4L).toInt()
        val day = data.shr(S_TIMESTAMP_DAY_BIT_OFFSET).and(MASK_5L).toInt()
        val hour = data.shr(S_TIMESTAMP_HOUR_BIT_OFFSET).and(MASK_5L).toInt()
        val minute = data.shr(S_TIMESTAMP_MINUTE_BIT_OFFSET).and(MASK_6L).toInt()
        val offset = data.shr(S_O_TIMESTAMP_OFFSET_BIT_OFFSET).and(MASK_7L).toInt()

        return Timestamp.forMinute(year + 1970, month, day, hour, minute, (offset - 56) * 15)
    }

    fun readTimestampToSecondWithOffset(source: ByteArray, position: Int): Timestamp {
        val data = readFixedIntAsLong(source, position, 5)
        val year = data.and(MASK_7L).toInt()
        val month = data.shr(S_TIMESTAMP_MONTH_BIT_OFFSET).and(MASK_4L).toInt()
        val day = data.shr(S_TIMESTAMP_DAY_BIT_OFFSET).and(MASK_5L).toInt()
        val hour = data.shr(S_TIMESTAMP_HOUR_BIT_OFFSET).and(MASK_5L).toInt()
        val minute = data.shr(S_TIMESTAMP_MINUTE_BIT_OFFSET).and(MASK_6L).toInt()
        val offset = data.shr(S_O_TIMESTAMP_OFFSET_BIT_OFFSET).and(MASK_7L).toInt()
        val second = data.shr(S_O_TIMESTAMP_SECOND_BIT_OFFSET).and(MASK_6L).toInt()

        return Timestamp.forSecond(year + 1970, month, day, hour, minute, second, (offset - 56) * 15)
    }

    fun readTimestampToMillisecondWithOffset(source: ByteArray, position: Int): Timestamp {
        val data = readFixedIntAsLong(source, position, 7)
        val year = data.and(MASK_7L).toInt()
        val month = data.shr(S_TIMESTAMP_MONTH_BIT_OFFSET).and(MASK_4L).toInt()
        val day = data.shr(S_TIMESTAMP_DAY_BIT_OFFSET).and(MASK_5L).toInt()
        val hour = data.shr(S_TIMESTAMP_HOUR_BIT_OFFSET).and(MASK_5L).toInt()
        val minute = data.shr(S_TIMESTAMP_MINUTE_BIT_OFFSET).and(MASK_6L).toInt()
        val offset = data.shr(S_O_TIMESTAMP_OFFSET_BIT_OFFSET).and(MASK_7L).toInt()
        val second = data.shr(S_O_TIMESTAMP_SECOND_BIT_OFFSET).and(MASK_6L)
        val fractionalSecond = data.shr(S_O_TIMESTAMP_FRACTION_BIT_OFFSET).and(MASK_10L)

        val secondBigDecimal = BigDecimal.valueOf(second)
        val fractionalSecondBigDecimal = BigDecimal.valueOf(fractionalSecond, 3)
        return Timestamp.forSecond(year + 1970, month, day, hour, minute, secondBigDecimal.add(fractionalSecondBigDecimal), (offset - 56) * 15)
    }

    fun readTimestampToMicrosecondWithOffset(source: ByteArray, position: Int): Timestamp {
        val data = readFixedIntAsLong(source, position, 8)
        val year = data.and(MASK_7L).toInt()
        val month = data.shr(S_TIMESTAMP_MONTH_BIT_OFFSET).and(MASK_4L).toInt()
        val day = data.shr(S_TIMESTAMP_DAY_BIT_OFFSET).and(MASK_5L).toInt()
        val hour = data.shr(S_TIMESTAMP_HOUR_BIT_OFFSET).and(MASK_5L).toInt()
        val minute = data.shr(S_TIMESTAMP_MINUTE_BIT_OFFSET).and(MASK_6L).toInt()
        val offset = data.shr(S_O_TIMESTAMP_OFFSET_BIT_OFFSET).and(MASK_7L).toInt()
        val second = data.shr(S_O_TIMESTAMP_SECOND_BIT_OFFSET).and(MASK_6L)
        val fractionalSecond = data.shr(S_O_TIMESTAMP_FRACTION_BIT_OFFSET).and(MASK_20L)

        val secondBigDecimal = BigDecimal.valueOf(second)
        val fractionalSecondBigDecimal = BigDecimal.valueOf(fractionalSecond, 6)
        return Timestamp.forSecond(year + 1970, month, day, hour, minute, secondBigDecimal.add(fractionalSecondBigDecimal), (offset - 56) * 15)
    }

    fun readTimestampToNanosecondWithOffset(source: ByteArray, position: Int): Timestamp {
        val data = readFixedIntAsLong(source, position, 8)
        val highFractionalSecondByte = readFixedInt8AsShort(source, position + 8).toLong().and(MASK_6L)
        val year = data.and(MASK_7L).toInt()
        val month = data.shr(S_TIMESTAMP_MONTH_BIT_OFFSET).and(MASK_4L).toInt()
        val day = data.shr(S_TIMESTAMP_DAY_BIT_OFFSET).and(MASK_5L).toInt()
        val hour = data.shr(S_TIMESTAMP_HOUR_BIT_OFFSET).and(MASK_5L).toInt()
        val minute = data.shr(S_TIMESTAMP_MINUTE_BIT_OFFSET).and(MASK_6L).toInt()
        val offset = data.shr(S_O_TIMESTAMP_OFFSET_BIT_OFFSET).and(MASK_7L).toInt()
        val second = data.shr(S_O_TIMESTAMP_SECOND_BIT_OFFSET).and(MASK_6L)
        val fractionalSecond = data.ushr(S_O_TIMESTAMP_FRACTION_BIT_OFFSET).or(highFractionalSecondByte.shl(24))

        val secondBigDecimal = BigDecimal.valueOf(second)
        val fractionalSecondBigDecimal = BigDecimal.valueOf(fractionalSecond, 9)
        return Timestamp.forSecond(year + 1970, month, day, hour, minute, secondBigDecimal.add(fractionalSecondBigDecimal), (offset - 56) * 15)
    }

    @OptIn(ExperimentalStdlibApi::class) // for Byte.toHexString()
    fun readShortTimestamp(source: ByteArray, position: Int, opcode: Int): Timestamp {
        return when (opcode) {
            0x80 -> readTimestampToYear(source, position)
            0x81 -> readTimestampToMonth(source, position)
            0x82 -> readTimestampToDay(source, position)
            0x83 -> readTimestampToMinuteUTCOrUnknown(source, position)
            0x84 -> readTimestampToSecondUTCOrUnknown(source, position)
            0x85 -> readTimestampToMillisecondUTCOrUnknown(source, position)
            0x86 -> readTimestampToMicrosecondUTCOrUnknown(source, position)
            0x87 -> readTimestampToNanosecondUTCOrUnknown(source, position)
            0x88 -> readTimestampToMinuteWithOffset(source, position)
            0x89 -> readTimestampToSecondWithOffset(source, position)
            0x8a -> readTimestampToMillisecondWithOffset(source, position)
            0x8b -> readTimestampToMicrosecondWithOffset(source, position)
            0x8c -> readTimestampToNanosecondWithOffset(source, position)
            else -> throw IonException("Unrecognized short timestamp opcode ${opcode.toByte().toHexString()}")
        }
    }
}
