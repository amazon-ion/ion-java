// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11.bytearray

import com.amazon.ion.bytecode.ir.Instructions
import com.amazon.ion.bytecode.ir.Instructions.packInstructionData
import com.amazon.ion.bytecode.util.unsignedToInt
import java.math.BigInteger

/** Helper for constructing BigIntegers from ULongs that are larger than Long.MAX_VALUE */
private val LONG_MAX_PLUS_1 = BigInteger.valueOf(Long.MAX_VALUE).plus(BigInteger.ONE)

/** Handles tagless opcode E0 */
internal val TAGLESS_FLEX_UINT = OpcodeToBytecodeHandler { _, src, pos, dest, cp, _, _, _ ->
    val numBytes = PrimitiveDecoder.lengthOfFlexIntOrUIntAt(src, pos)
    // Ten or more bytes should be unlikely to occur (and therefore easy for the JVM to predict this branch).
    if (numBytes < 10) {
        // No more than 9 bytes. Will fit in a long.
        val value = PrimitiveDecoder.readFlexUIntAsULong(src, pos)

        // TODO(perf): See if it's faster to just eliminate the branch and always emit `I_INT_64` for this case.
        if (value.countLeadingZeroBits() > Int.SIZE_BITS) {
            dest.add2(Instructions.I_INT_I32, value.toInt())
        } else {
            dest.add3(Instructions.I_INT_I64, value.shr(Int.SIZE_BITS).toInt(), value.toInt())
        }
    } else {
        // NOTE: This is very inefficient for over-padded small values,
        //       but we're not worried about optimizing that case.
        val bigInt = PrimitiveDecoder.readFlexUIntAsBigInteger(src, pos)
        val cpIndex = cp.add(bigInt)
        dest.add(Instructions.I_INT_CP.packInstructionData(cpIndex))
    }
    numBytes
}

/** Handles tagless opcode E1 */
internal val TAGLESS_FIXED_UINT_8 = OpcodeToBytecodeHandler { _, src, pos, dest, _, _, _, _ ->
    val value = src[pos].unsignedToInt()
    dest.add(Instructions.I_INT_I16.packInstructionData(value))
    1
}

/** Handles tagless opcode E2 */
internal val TAGLESS_FIXED_UINT_16 = OpcodeToBytecodeHandler { _, src, pos, dest, _, _, _, _ ->
    val value = PrimitiveDecoder.readFixedUInt16(src, pos)
    dest.add2(Instructions.I_INT_I32, value.toInt())
    2
}

/** Handles tagless opcode E4 */
internal val TAGLESS_FIXED_UINT_32 = OpcodeToBytecodeHandler { _, src, pos, dest, _, _, _, _ ->
    val value = PrimitiveDecoder.readFixedUInt32(src, pos)
    if (value > Int.MAX_VALUE.toUInt()) {
        // The high-order bits are all 0 because we just need sign extension here.
        dest.add3(Instructions.I_INT_I64, 0, value.toInt())
    } else {
        dest.add2(Instructions.I_INT_I32, value.toInt())
    }
    4
}

/** Handles tagless opcode E8 */
internal val TAGLESS_FIXED_UINT_64 = OpcodeToBytecodeHandler { _, src, pos, dest, cp, _, _, _ ->
    val value: ULong = PrimitiveDecoder.readFixedUInt64(src, pos)
    if (value > Long.MAX_VALUE.toULong()) {
        // It would be more efficient if BigInteger could interpret a Long as if it were ULong,
        // but this should be a relatively infrequent code path since this only occurs for ULongs
        // that are larger than Long.MAX_VALUE.
        val bigInt = BigInteger.valueOf(value.toLong().and(Long.MAX_VALUE)).plus(LONG_MAX_PLUS_1)
        val cpIndex = cp.add(bigInt)
        dest.add(Instructions.I_INT_CP.packInstructionData(cpIndex))
    } else {
        dest.add3(Instructions.I_INT_I64, value.shr(32).toInt(), value.toInt())
    }
    8
}
