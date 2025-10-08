// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.ion_1_1

import com.amazon.ion.bytecode.bin11.OpCode

/**
 * Type-safe representation of the opcodes that are valid tagless scalar types.
 */
enum class TaglessScalarType(val textEncodingName: String, private val binaryEncodingOpcode: Int) {
    INT("int", OpCode.TE_FLEX_INT),
    INT_8("int8", OpCode.INT_8),
    INT_16("int16", OpCode.INT_16),
    INT_32("int32", OpCode.INT_32),
    INT_64("int64", OpCode.INT_64),
    UINT("uint", OpCode.TE_FLEX_UINT),
    UINT_8("uint8", OpCode.TE_UINT_8),
    UINT_16("uint16", OpCode.TE_UINT_16),
    UINT_32("uint32", OpCode.TE_UINT_32),
    UINT_64("uint64", OpCode.TE_UINT_64),
    FLOAT_16("float16", OpCode.FLOAT_16),
    FLOAT_32("float32", OpCode.FLOAT_32),
    FLOAT_64("float64", OpCode.FLOAT_64),
    SMALL_DECIMAL("small_decimal", OpCode.TE_SMALL_DECIMAL),
    TIMESTAMP_DAY("timestamp_day", OpCode.TIMESTAMP_DAY_PRECISION),
    TIMESTAMP_MIN("timestamp_min", OpCode.TIMESTAMP_MINUTE_PRECISION),
    TIMESTAMP_S("timestamp_s", OpCode.TIMESTAMP_SECOND_PRECISION),
    TIMESTAMP_MS("timestamp_ms", OpCode.TIMESTAMP_MILLIS_PRECISION),
    TIMESTAMP_US("timestamp_us", OpCode.TIMESTAMP_MICROS_PRECISION),
    TIMESTAMP_NS("timestamp_ns", OpCode.TIMESTAMP_NANOS_PRECISION),
    SYMBOL("symbol", OpCode.TE_SYMBOL_FS),
    ;

    fun getOpcode(): Int = binaryEncodingOpcode
}
