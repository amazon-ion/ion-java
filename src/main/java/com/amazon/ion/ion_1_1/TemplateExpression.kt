// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.ion_1_1

import com.amazon.ion.bytecode.ir.OperationKind
import java.lang.StringBuilder

/**
 * This class can be used to model templates.
 *
 * When expression kind is...
 *  - NULL: objectValue is the [IonType]
 *  - BOOL: primitiveValue is 0 (false) or 1 (true)
 *  - INT: value is [BigInteger] or [primitiveValue] is the [Long] value. These are mutually exclusive.
 *  - FLOAT: primitiveValue is double as raw bits
 *  - DECIMAL: objectValue is [Decimal]
 *  - TIMESTAMP: objectValue is [Timestamp]
 *  - STRING: objectValue is [String]
 *  - SYMBOL: objectValue is `String?`
 *  - CLOB: objectValue is [ByteSlice]
 *  - BLOB: objectValue is [ByteSlice]
 *  - LIST: list content is in [childValues]
 *  - SEXP: children are in [childValues]
 *  - STRUCT: children are in [childValues]
 *  - ANNOTATIONS: objectValue is `Array<String?>`
 *  - FIELD_NAME: objectValue is `String?`
 *  - PLACEHOLDER:
 *    - When tagged, [primitiveValue] is 0 and default value (if any) is in [childValues]
 *    - When tagless, tagless opcode type is in [primitiveValue].
 *
 *  Any other expression kind is illegal.
 */
internal class TemplateExpression(
    @JvmField var expressionKind: Int,
    @JvmField var primitiveValue: Long = 0,
    @JvmField var objectValue: Any? = null,
    @JvmField var childValues: Array<TemplateExpression> = EMPTY_EXPRESSION_ARRAY,
) {
    companion object {
        @JvmField val EMPTY_EXPRESSION_ARRAY = emptyArray<TemplateExpression>()
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null || javaClass != other.javaClass) return false
        val that = other as TemplateExpression
        if (expressionKind != that.expressionKind) return false
        if (primitiveValue != that.primitiveValue) return false

        val thisObjectValue = this.objectValue
        val thatObjectValue = that.objectValue
        if (thisObjectValue is Array<*> && thatObjectValue is Array<*>) {
            if (!thisObjectValue.contentEquals(thatObjectValue)) return false
        } else {
            if (thisObjectValue != thatObjectValue) return false
        }

        if (childValues.size != that.childValues.size) return false
        if (!childValues.contentEquals(that.childValues)) return false
        return true
    }

    override fun hashCode(): Int {
        var h = expressionKind
        h = h * 31 + primitiveValue.hashCode()
        h = h * 31 + objectValue.hashCode()
        h = h * 31 + childValues.contentDeepHashCode()
        return h
    }

    override fun toString(): String {
        val sb = StringBuilder()
        sb.append("TemplateExpression(kind=${OperationKind.nameOf(expressionKind)}")
        sb.append(",primitiveValue=$primitiveValue")
        sb.append(",objectValue=$objectValue")
        sb.append(",childExpressions=[")
        for (child in childValues) {
            sb.append(child.toString())
            sb.append(",")
        }
        sb.append("])")
        return sb.toString()
    }
}
