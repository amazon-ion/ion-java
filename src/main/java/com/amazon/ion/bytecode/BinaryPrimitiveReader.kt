// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode

import com.amazon.ion.IonException
import com.amazon.ion.bytecode.NumericReader.getInt
import com.amazon.ion.bytecode.NumericReader.getInt24
import com.amazon.ion.bytecode.NumericReader.getLong
import com.amazon.ion.bytecode.NumericReader.getShort
import java.nio.ByteBuffer

/**
 * Helper class containing methods for reading FixedInts, and (TODO) FlexInts, FixedUInts, and FlexUInts.
 *
 * Some methods in this class use a technique to reduce the number of branches and minimize the number of calls to
 * get data from the ByteBuffer. We know that any FlexInt or FlexUint must have at least 4 bytes preceding it (because
 * the IVM is 4 bytes), so rather than reading bytes one at a time, we'll read one byte to figure out how many bytes to
 * read, and then we'll read the entire FlexUInt (plus zero or more preceding bytes) in one call to [ByteBuffer.getInt]
 * or [ByteBuffer.getLong]. This puts all the bytes we want into the _most_ significant bits of the `int` or `long`.
 * Then we can remove the extra bytes and the continuation bits by using a single right-shift operation (signed for
 * FlexInt or unsigned for FlexUInt). This technique significantly reduces the number of operations required to read a
 * Flex(U)Int as compared to reading bytes one at a time.
 *
 * A similar technique is also used for reading FixedInts and FixedUInts.
 *
 * Examples:
 * ```
 * aaaa_aaaa bbbb_bbbb cccc_cccc dddd_dddd eeee_eee1 ffff_ffff gggg_gggg hhhh_hhhh iiii_iiii
 * ```
 * - read B-E... `eeee_eee1 dddd_dddd cccc_cccc bbbb_bbbb`
 * - shift right by (8 * 3 + 1) = 25 = 4 + 7 * 3
 * - unsigned shift right for FlexUint; signed shift right for FlexInt.
 *
 * ```
 * aaaa_aaaa bbbb_bbbb cccc_cccc dddd_dddd eeee_ee10 ffff_ffff gggg_gggg hhhh_hhhh iiii_iiii
 * ```
 * - read C-F... `ffff_ffff eeee_ee10 dddd_dddd cccc_cccc`
 * - shift right by (8 * 2 + 2) = 18 = 4 + 7 * 2
 *
 * ```
 * aaaa_aaaa bbbb_bbbb cccc_cccc dddd_dddd eeee_e100 ffff_ffff gggg_gggg hhhh_hhhh iiii_iiii
 * ```
 * - read D-G... `gggg_gggg ffff_ffff eeee_e100 dddd_dddd`
 * - shift right by (8 * 1 + 3) = 11 = 4 + 7 * 1
 *
 * ```
 * aaaa_aaaa bbbb_bbbb cccc_cccc dddd_dddd eeee_1000 ffff_ffff gggg_gggg hhhh_hhhh iiii_iiii
 * ```
 * - read E-H... `hhhh_hhhh gggg_gggg ffff_ffff eeee_1000`
 * - shift right by (8 * 0 + 4) = 4
 */
object BinaryPrimitiveReader {

    @JvmStatic
    fun ByteArray.readFixedInt8AsShort(start: Int): Short {
        if (this.size < start + 1) throw IonException("Incomplete data: start=$start, length=${1}, limit=${this.size}")
        return this[start].toShort()
    }

    @JvmStatic
    fun ByteArray.readFixedInt16AsShort(start: Int): Short {
        if (this.size < start + 2) throw IonException("Incomplete data: start=$start, length=${2}, limit=${this.size}")
        return this.getShort(start)
    }

    @JvmStatic
    fun ByteArray.readFixedInt24AsInt(start: Int): Int {
        if (this.size < start + 3) throw IonException("Incomplete data: start=$start, length=${3}, limit=${this.size}")
        return this.getInt24(start)
    }

    @JvmStatic
    fun ByteArray.readFixedInt32AsInt(start: Int): Int {
        if (this.size < start + 4) throw IonException("Incomplete data: start=$start, length=${4}, limit=${this.size}")
        return this.getInt(start)
    }

    @JvmStatic
    fun ByteArray.readFixedIntAsInt(start: Int, length: Int): Int {
        if (this.size < start + length) throw IonException("Incomplete data: start=$start, length=$length, limit=${this.size}")
        return (this.getInt(start - 4 + length) shr ((4 - length) * 8))
    }

    @JvmStatic
    fun ByteArray.readFixedIntAsLong(start: Int, length: Int): Long {
        if (this.size < start + length) throw IonException("Incomplete data: start=$start, length=$length, limit=${this.size}")
        if (length > 4) {
            // TODO: See if we can simplify some of the calculations
            return this.getLong(start - 8 + length) shr ((8 - length) * 8)
        } else {
            return (this.getInt(start - 4 + length) shr ((4 - length) * 8)).toLong()
        }
    }

    // TODO: add helpers for reading FixedUInts, FlexInts, and FlexUInts once needed. These needed more cleanup
    //  and did not have test coverage so I did not include them yet.
}
