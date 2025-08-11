// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.eexp

import com.amazon.ion.impl.macro.*
import java.math.BigInteger

/**
 * Checks that the encoding of [parameter] can represent the given [Double] value.
 */
internal fun checkArgumentEncodingCompatibility(parameter: Macro.Parameter, value: Double) {
    val encoding = parameter.type
    val isInRangeForEncoding = when (encoding) {
        Macro.ParameterEncoding.Tagged -> true
        Macro.ParameterEncoding.Float16 -> TODO("Writing float16 values is not implemented yet")
        Macro.ParameterEncoding.Float32 -> value.toFloat().toDouble() == value
        Macro.ParameterEncoding.Float64 -> true
        else -> throw IllegalArgumentException("Parameter ${parameter.variableName} must be a ${encoding.ionTextName}")
    }
    require(isInRangeForEncoding) { "Parameter ${parameter.variableName} must be a ${encoding.ionTextName}; value is out of range for encoding type: $value" }
}

/**
 * Checks that the encoding of [parameter] can represent the given [Long] value.
 */
internal fun checkArgumentEncodingCompatibility(parameter: Macro.Parameter, value: Long) {
    val encoding = parameter.type
    val isInRangeForEncoding = when (encoding) {
        Macro.ParameterEncoding.Tagged -> true
        Macro.ParameterEncoding.Int8 -> value.toByte().toLong() == value
        Macro.ParameterEncoding.Int16 -> value.toShort().toLong() == value
        Macro.ParameterEncoding.Int32 -> value.toInt().toLong() == value
        Macro.ParameterEncoding.Int64 -> true
        Macro.ParameterEncoding.FlexInt -> true
        Macro.ParameterEncoding.Uint8 -> value >= 0 && value.toByte().toLong() == value
        Macro.ParameterEncoding.Uint16 -> value >= 0 && value.toShort().toLong() == value
        Macro.ParameterEncoding.Uint32 -> value >= 0 && value.toInt().toLong() == value
        Macro.ParameterEncoding.Uint64 -> value >= 0
        Macro.ParameterEncoding.FlexUint -> value >= 0
        else -> throw IllegalArgumentException("Parameter ${parameter.variableName} must be a ${parameter.type.ionTextName}")
    }
    require(isInRangeForEncoding) { "Parameter ${parameter.variableName} must be a ${parameter.type.ionTextName}; value is out of range for encoding type: $value" }
}

/**
 * Checks that the encoding of [parameter] can represent the given [BigInteger] value.
 */
internal fun checkArgumentEncodingCompatibility(parameter: Macro.Parameter, value: BigInteger?) {
    val encoding = parameter.type

    if (value == null) {
        if (encoding == Macro.ParameterEncoding.Tagged) return
        throw IllegalArgumentException("Parameter ${parameter.variableName} must be a ${encoding.ionTextName}; value may not be null")
    }

    val isInRangeForEncoding = when (encoding) {
        Macro.ParameterEncoding.Tagged -> true
        Macro.ParameterEncoding.Int8 -> value.bitLength() <= 8
        Macro.ParameterEncoding.Int16 -> value.bitLength() <= 16
        Macro.ParameterEncoding.Int32 -> value.bitLength() <= 32
        Macro.ParameterEncoding.Int64 -> value.bitLength() <= 64
        Macro.ParameterEncoding.FlexInt -> true
        Macro.ParameterEncoding.Uint8 -> value.signum() >= 0 && value.bitLength() <= 8
        Macro.ParameterEncoding.Uint16 -> value.signum() >= 0 && value.bitLength() <= 16
        Macro.ParameterEncoding.Uint32 -> value.signum() >= 0 && value.bitLength() <= 32
        Macro.ParameterEncoding.Uint64 -> value.signum() >= 0 && value.bitLength() <= 64
        Macro.ParameterEncoding.FlexUint -> value.signum() >= 0
        else -> throw IllegalArgumentException("Parameter ${parameter.variableName} must be a ${parameter.type.ionTextName}")
    }
    require(isInRangeForEncoding) { "Parameter ${parameter.variableName} must be a ${parameter.type.ionTextName}; value is out of range for encoding type: $value" }
}

/**
 * Checks that the encoding of [parameter] can represent the given [String] value as an Ion String.
 */
internal fun checkArgumentEncodingCompatibility(parameter: Macro.Parameter, value: String?) {
    when (val encoding = parameter.type) {
        Macro.ParameterEncoding.Tagged -> {}
        Macro.ParameterEncoding.FlexString -> {
            if (value == null) {
                throw IllegalArgumentException("Parameter ${parameter.variableName} must be a ${encoding.ionTextName}; value may not be null")
            }
        }
        else -> throw IllegalArgumentException("Parameter ${parameter.variableName} must be a ${parameter.type.ionTextName}")
    }
}
