// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion

import com.amazon.ion.eexp.*
import com.amazon.ion.impl.macro.*

/**
 * Indicates that the implementing class has a standardized/built-in way to serialize as Ion.
 *
 * Example implementation:
 *
 * ```kotlin
 * data class Point2D(val x: Long, val y: Long) : WriteAsIon {
 *     companion object {
 *         // This is a very long macro name, but by using the qualified class name,
 *         // there is almost no risk of having a name conflict with another macro.
 *         private val MACRO_NAME = Point2D::class.simpleName!!.replace(".", "_")
 *         private val MACRO = TemplateMacro(
 *             signature = listOf(exactlyOneTagged("x"), exactlyOneTagged("y")),
 *             templateBody {
 *                 struct {
 *                     fieldName("x"); variable(0)
 *                     fieldName("y"); variable(1)
 *                 }
 *             }
 *         )
 *     }
 *
 *     override fun writeWithEExpression(builder: EExpressionBuilder): EExpression? {
 *         return builder.withName(MACRO_NAME)
 *             .withMacro(MACRO)
 *             .withIntArgument(x)
 *             .withIntArgument(y)
 *             .build()
 *     }
 *
 *     override fun writeTo(writer: IonWriter) {
 *         with(writer) {
 *             stepIn(IonType.STRUCT)
 *             setFieldName("x"); writeInt(x)
 *             setFieldName("x"); writeInt(y)
 *             stepOut()
 *         }
 *     }
 * }
 * ```
 *
 * TODO: There is a significant weakness in this API—if someone calls `myObject.writeTo(myWriter)`, then the check for
 *       e-expression support is completely bypassed.
 */
interface WriteAsIon {

    /**
     * Writes this value to an IonWriter using an E-Expression.
     *
     * Implementations must return an instance of [EExpression] to indicate that it can be written using an e-expression.
     * Returning `null` indicates that this implementation or instance should not be serialized
     *
     * [EExpression] instances can be obtained from an [EExpressionArgumentBuilder], which in turn, can be obtained by
     * calling [Macro.createInvocation] or by using the supplied [builder] in this method.
     *
     * If you call any methods on [builder], but do not return the [EExpression] produced by [builder], something bad
     * will happen. If you're lucky, an exception will be thrown. If you're unlucky, your application will continue
     * running and produce invalid or incorrect data.
     */
    fun writeWithEExpression(builder: EExpressionBuilder): EExpression? = null

    /**
     * Writes this object to a standard [IonWriter].
     */
    fun writeTo(writer: IonWriter)
}
