// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.eexp

import com.amazon.ion.IonWriter
import java.math.BigInteger
import java.util.function.Consumer

interface EExpressionArgumentBuilder<T : EExpression> {

    fun withAbsentArgument(): EExpressionArgumentBuilder<T>

    fun withIntArgument(value: Byte): EExpressionArgumentBuilder<T> = withIntArgument(value.toLong())
    fun withIntArgument(value: Short): EExpressionArgumentBuilder<T> = withIntArgument(value.toLong())
    fun withIntArgument(value: Int): EExpressionArgumentBuilder<T> = withIntArgument(value.toLong())
    fun withIntArgument(value: Long): EExpressionArgumentBuilder<T>
    fun withIntArgument(value: BigInteger): EExpressionArgumentBuilder<T>

    fun withFloatArgument(value: Float): EExpressionArgumentBuilder<T> = withFloatArgument(value.toDouble())
    fun withFloatArgument(value: Double): EExpressionArgumentBuilder<T>

    /*
     TODO: Methods that are optimized for writing tagless groups.
           Eg:
            fun withIntArgGroup(values: ByteArray): ArgumentBuilder
            fun withIntArgGroup(values: ShortArray): ArgumentBuilder
            fun withIntArgGroup(values: IntArray): ArgumentBuilder
            fun withIntArgGroup(values: LongArray): ArgumentBuilder
            fun withFloatArgGroup(values: FloatArray): ArgumentBuilder
            fun withFloatArgGroup(values: DoubleArray): ArgumentBuilder
    */

    // TODO:
    //  fun withSymbolArgument(content: String): ArgumentBuilder
    //  ... but we need to support SymbolTokens as well.

    fun withStringArgument(value: String): EExpressionArgumentBuilder<T>

    fun withArgument(values: Consumer<IonWriter>): EExpressionArgumentBuilder<T>

    fun build(): T
}
