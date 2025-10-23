// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11.bytearray

import com.amazon.ion.Timestamp
import com.amazon.ion.bytecode.bin11.bytearray.PrimitiveDecoder.readFixedInt16
import com.amazon.ion.bytecode.bin11.bytearray.PrimitiveDecoder.readFixedInt32
import com.amazon.ion.bytecode.bin11.bytearray.PrimitiveDecoder.readFixedInt8AsShort
import com.amazon.ion.bytecode.bin11.bytearray.PrimitiveDecoder.readFixedIntAsLong
import java.math.BigDecimal

/**
 * Helper class for decoding the various short timestamp encoding variants from a [ByteArray].
 */
internal object ShortTimestampDecoder {
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
    private const val MASK_UTC_OR_UNKNOWN_BIT = 0b1000_00000000_00000000_00000000
    private const val MASK_UTC_OR_UNKNOWN_BITL = 0b1000_00000000_00000000_00000000L

    private val opcodeToDecoderFunctionTable = arrayOf(
        ShortTimestampDecoder::readTimestampToYear,
        ShortTimestampDecoder::readTimestampToMonth,
        ShortTimestampDecoder::readTimestampToDay,
        ShortTimestampDecoder::readTimestampToMinuteUTCOrUnknown,
        ShortTimestampDecoder::readTimestampToSecondUTCOrUnknown,
        ShortTimestampDecoder::readTimestampToMillisecondUTCOrUnknown,
        ShortTimestampDecoder::readTimestampToMicrosecondUTCOrUnknown,
        ShortTimestampDecoder::readTimestampToNanosecondUTCOrUnknown,
        ShortTimestampDecoder::readTimestampToMinuteWithOffset,
        ShortTimestampDecoder::readTimestampToSecondWithOffset,
        ShortTimestampDecoder::readTimestampToMillisecondWithOffset,
        ShortTimestampDecoder::readTimestampToMicrosecondWithOffset,
        ShortTimestampDecoder::readTimestampToNanosecondWithOffset,
    )

    fun readTimestampToYear(source: ByteArray, position: Int): Timestamp {
        val year = readFixedInt8AsShort(source, position).toInt()
        return Timestamp.forYear(year + 1970)
    }

    fun readTimestampToMonth(source: ByteArray, position: Int): Timestamp {
        val yearAndMonth = readFixedInt16(source, position).toInt()
        val year = yearAndMonth.and(MASK_7)
        val month = yearAndMonth.shr(7)

        return Timestamp.forMonth(year + 1970, month)
    }

    fun readTimestampToDay(source: ByteArray, position: Int): Timestamp {
        val yearMonthAndDay = readFixedInt16(source, position).toInt()
        val year = yearMonthAndDay.and(MASK_7)
        val month = yearMonthAndDay.shr(7).and(MASK_4)
        val day = yearMonthAndDay.shr(11)

        return Timestamp.forDay(year + 1970, month, day)
    }

    fun readTimestampToMinuteUTCOrUnknown(source: ByteArray, position: Int): Timestamp {
        val data = readFixedInt32(source, position)
        val year = data.and(MASK_7)
        val month = data.shr(7).and(MASK_4)
        val day = data.shr(11).and(MASK_5)
        val hour = data.shr(16).and(MASK_5)
        val minute = data.shr(21).and(MASK_6)
        val isUTC = data.and(MASK_UTC_OR_UNKNOWN_BIT) != 0

        return Timestamp.forMinute(year + 1970, month, day, hour, minute, if (isUTC) 0 else null)
    }

    fun readTimestampToSecondUTCOrUnknown(source: ByteArray, position: Int): Timestamp {
        val data = readFixedIntAsLong(source, position, 5)
        val year = data.and(MASK_7L).toInt()
        val month = data.shr(7).and(MASK_4L).toInt()
        val day = data.shr(11).and(MASK_5L).toInt()
        val hour = data.shr(16).and(MASK_5L).toInt()
        val minute = data.shr(21).and(MASK_6L).toInt()
        val second = data.shr(28).and(MASK_6L).toInt()
        val isUTC = data.and(MASK_UTC_OR_UNKNOWN_BITL) != 0L

        return Timestamp.forSecond(year + 1970, month, day, hour, minute, second, if (isUTC) 0 else null)
    }

    fun readTimestampToMillisecondUTCOrUnknown(source: ByteArray, position: Int): Timestamp {
        val data = readFixedIntAsLong(source, position, 6)
        val year = data.and(MASK_7L).toInt()
        val month = data.shr(7).and(MASK_4L).toInt()
        val day = data.shr(11).and(MASK_5L).toInt()
        val hour = data.shr(16).and(MASK_5L).toInt()
        val minute = data.shr(21).and(MASK_6L).toInt()
        val second = data.shr(28).and(MASK_6L)
        val fractionalSecond = data.shr(34).and(MASK_10L)
        val isUTC = data.and(MASK_UTC_OR_UNKNOWN_BITL) != 0L

        val secondBigDecimal = BigDecimal.valueOf(second)
        val fractionalSecondBigDecimal = BigDecimal.valueOf(fractionalSecond, 3)
        return Timestamp.forSecond(year + 1970, month, day, hour, minute, secondBigDecimal.add(fractionalSecondBigDecimal), if (isUTC) 0 else null)
    }

    fun readTimestampToMicrosecondUTCOrUnknown(source: ByteArray, position: Int): Timestamp {
        val data = readFixedIntAsLong(source, position, 7)
        val year = data.and(MASK_7L).toInt()
        val month = data.shr(7).and(MASK_4L).toInt()
        val day = data.shr(11).and(MASK_5L).toInt()
        val hour = data.shr(16).and(MASK_5L).toInt()
        val minute = data.shr(21).and(MASK_6L).toInt()
        val second = data.shr(28).and(MASK_6L)
        val fractionalSecond = data.shr(34).and(MASK_20L)
        val isUTC = data.and(MASK_UTC_OR_UNKNOWN_BITL) != 0L

        val secondBigDecimal = BigDecimal.valueOf(second)
        val fractionalSecondBigDecimal = BigDecimal.valueOf(fractionalSecond, 6)
        return Timestamp.forSecond(year + 1970, month, day, hour, minute, secondBigDecimal.add(fractionalSecondBigDecimal), if (isUTC) 0 else null)
    }

    fun readTimestampToNanosecondUTCOrUnknown(source: ByteArray, position: Int): Timestamp {
        val data = readFixedIntAsLong(source, position, 8)
        val year = data.and(MASK_7L).toInt()
        val month = data.shr(7).and(MASK_4L).toInt()
        val day = data.shr(11).and(MASK_5L).toInt()
        val hour = data.shr(16).and(MASK_5L).toInt()
        val minute = data.shr(21).and(MASK_6L).toInt()
        val second = data.shr(28).and(MASK_6L)
        val fractionalSecond = data.ushr(34).and(MASK_30L)
        val isUTC = data.and(MASK_UTC_OR_UNKNOWN_BITL) != 0L

        val secondBigDecimal = BigDecimal.valueOf(second)
        val fractionalSecondBigDecimal = BigDecimal.valueOf(fractionalSecond, 9)
        return Timestamp.forSecond(year + 1970, month, day, hour, minute, secondBigDecimal.add(fractionalSecondBigDecimal), if (isUTC) 0 else null)
    }

    fun readTimestampToMinuteWithOffset(source: ByteArray, position: Int): Timestamp {
        val data = readFixedIntAsLong(source, position, 5)
        val year = data.and(MASK_7L).toInt()
        val month = data.shr(7).and(MASK_4L).toInt()
        val day = data.shr(11).and(MASK_5L).toInt()
        val hour = data.shr(16).and(MASK_5L).toInt()
        val minute = data.shr(21).and(MASK_6L).toInt()
        val offset = data.shr(27).and(MASK_7L).toInt()

        return Timestamp.forMinute(year + 1970, month, day, hour, minute, (offset - 56) * 15)
    }

    fun readTimestampToSecondWithOffset(source: ByteArray, position: Int): Timestamp {
        val data = readFixedIntAsLong(source, position, 5)
        val year = data.and(MASK_7L).toInt()
        val month = data.shr(7).and(MASK_4L).toInt()
        val day = data.shr(11).and(MASK_5L).toInt()
        val hour = data.shr(16).and(MASK_5L).toInt()
        val minute = data.shr(21).and(MASK_6L).toInt()
        val offset = data.shr(27).and(MASK_7L).toInt()
        val second = data.shr(34).and(MASK_6L).toInt()

        return Timestamp.forSecond(year + 1970, month, day, hour, minute, second, (offset - 56) * 15)
    }

    fun readTimestampToMillisecondWithOffset(source: ByteArray, position: Int): Timestamp {
        val data = readFixedIntAsLong(source, position, 7)
        val year = data.and(MASK_7L).toInt()
        val month = data.shr(7).and(MASK_4L).toInt()
        val day = data.shr(11).and(MASK_5L).toInt()
        val hour = data.shr(16).and(MASK_5L).toInt()
        val minute = data.shr(21).and(MASK_6L).toInt()
        val offset = data.shr(27).and(MASK_7L).toInt()
        val second = data.shr(34).and(MASK_6L)
        val fractionalSecond = data.shr(40).and(MASK_10L)

        val secondBigDecimal = BigDecimal.valueOf(second)
        val fractionalSecondBigDecimal = BigDecimal.valueOf(fractionalSecond, 3)
        return Timestamp.forSecond(year + 1970, month, day, hour, minute, secondBigDecimal.add(fractionalSecondBigDecimal), (offset - 56) * 15)
    }

    fun readTimestampToMicrosecondWithOffset(source: ByteArray, position: Int): Timestamp {
        val data = readFixedIntAsLong(source, position, 8)
        val year = data.and(MASK_7L).toInt()
        val month = data.shr(7).and(MASK_4L).toInt()
        val day = data.shr(11).and(MASK_5L).toInt()
        val hour = data.shr(16).and(MASK_5L).toInt()
        val minute = data.shr(21).and(MASK_6L).toInt()
        val offset = data.shr(27).and(MASK_7L).toInt()
        val second = data.shr(34).and(MASK_6L)
        val fractionalSecond = data.shr(40).and(MASK_20L)

        val secondBigDecimal = BigDecimal.valueOf(second)
        val fractionalSecondBigDecimal = BigDecimal.valueOf(fractionalSecond, 6)
        return Timestamp.forSecond(year + 1970, month, day, hour, minute, secondBigDecimal.add(fractionalSecondBigDecimal), (offset - 56) * 15)
    }

    fun readTimestampToNanosecondWithOffset(source: ByteArray, position: Int): Timestamp {
        val data = readFixedIntAsLong(source, position, 8)
        val highFractionalSecondByte = readFixedInt8AsShort(source, position + 8).toLong().and(MASK_6L)
        val year = data.and(MASK_7L).toInt()
        val month = data.shr(7).and(MASK_4L).toInt()
        val day = data.shr(11).and(MASK_5L).toInt()
        val hour = data.shr(16).and(MASK_5L).toInt()
        val minute = data.shr(21).and(MASK_6L).toInt()
        val offset = data.shr(27).and(MASK_7L).toInt()
        val second = data.shr(34).and(MASK_6L)
        val fractionalSecond = data.ushr(40).or(highFractionalSecondByte.shl(24))

        val secondBigDecimal = BigDecimal.valueOf(second)
        val fractionalSecondBigDecimal = BigDecimal.valueOf(fractionalSecond, 9)
        return Timestamp.forSecond(year + 1970, month, day, hour, minute, secondBigDecimal.add(fractionalSecondBigDecimal), (offset - 56) * 15)
    }

    fun readTimestamp(source: ByteArray, position: Int, precisionAndOffsetMode: Int): Timestamp {
        // TODO: calling function references like this might be slower than just using a conditional or other solutions.
        //  Might be worth looking into.
        val decoder = opcodeToDecoderFunctionTable[precisionAndOffsetMode]
        return decoder(source, position)
    }
}
