// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode

import com.amazon.ion.Decimal
import com.amazon.ion.impl._Private_ScalarConversions
import com.amazon.ion.impl._Private_ScalarConversions.ValueVariant
import java.math.BigInteger

/**
 * Wraps [_Private_ScalarConversions] to cut down on repeated code in [BytecodeIonReader].
 *
 * This abstraction probably adds overhead, but it should only be used in the non-ideal paths—performing **lossy**
 * conversions on scalar values.
 */
internal class ScalarConversionHelper {
    private val scalarConverter = ValueVariant()
    private val preparedConverter = ThisPreparedConverter()

    private inline fun initConversion(startType: Int, addValueFn: ValueVariant.() -> Unit): PreparedConverter {
        val converter = scalarConverter
        converter.clear()
        converter.addValueFn()
        converter.authoritativeType = startType
        return preparedConverter
    }

    fun from(value: Int) = initConversion(_Private_ScalarConversions.AS_TYPE.int_value) { addValue(value) }
    fun from(value: Long) = initConversion(_Private_ScalarConversions.AS_TYPE.long_value) { addValue(value) }
    fun from(value: BigInteger?) = initConversion(_Private_ScalarConversions.AS_TYPE.bigInteger_value) { addValue(value) }
    fun from(value: Double) = initConversion(_Private_ScalarConversions.AS_TYPE.double_value) { addValue(value) }
    fun from(value: Decimal?) = initConversion(_Private_ScalarConversions.AS_TYPE.decimal_value) { addValue(value) }

    sealed class PreparedConverter(private val converter: ValueVariant) {

        private fun doConversion(toType: Int): ValueVariant = converter.apply { cast(get_conversion_fnid(toType)) }

        fun intoInt(): Int = doConversion(_Private_ScalarConversions.AS_TYPE.int_value).int
        fun intoLong(): Long = doConversion(_Private_ScalarConversions.AS_TYPE.long_value).long
        fun intoBigInteger(): BigInteger = doConversion(_Private_ScalarConversions.AS_TYPE.bigInteger_value).bigInteger
        fun intoDecimal(): Decimal = doConversion(_Private_ScalarConversions.AS_TYPE.decimal_value).decimal
        fun intoDouble(): Double = doConversion(_Private_ScalarConversions.AS_TYPE.double_value).double
    }

    private inner class ThisPreparedConverter : PreparedConverter(this@ScalarConversionHelper.scalarConverter)
}
