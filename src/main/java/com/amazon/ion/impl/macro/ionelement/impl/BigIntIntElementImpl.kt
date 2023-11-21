/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.ion.impl.macro.ionelement.impl

import com.amazon.ion.IonWriter
import com.amazon.ion.impl.macro.ionelement.api.*
import com.amazon.ion.impl.macro.ionelement.api.PersistentMetaContainer
import com.amazon.ion.impl.macro.ionelement.api.constraintError
import java.math.BigInteger
import kotlinx.collections.immutable.PersistentList
import kotlinx.collections.immutable.toPersistentList
import kotlinx.collections.immutable.toPersistentMap

internal class BigIntIntElementImpl(
    override val bigIntegerValue: BigInteger,
    override val annotations: PersistentList<String>,
    override val metas: PersistentMetaContainer
) : AnyElementBase(), IntElement {

    override val type: ElementType get() = ElementType.INT

    override val integerSize: IntElementSize get() = IntElementSize.BIG_INTEGER

    override val longValue: Long get() {
        if (bigIntegerValue > MAX_LONG_AS_BIG_INT || bigIntegerValue < MIN_LONG_AS_BIG_INT) {
            constraintError(this, "Ion integer value outside of range of 64 bit signed integer, use bigIntegerValue instead.")
        }
        return bigIntegerValue.longValueExact()
    }

    override fun copy(annotations: List<String>, metas: MetaContainer): BigIntIntElementImpl =
        BigIntIntElementImpl(bigIntegerValue, annotations.toPersistentList(), metas.toPersistentMap())

    override fun withAnnotations(vararg additionalAnnotations: String): BigIntIntElementImpl = _withAnnotations(*additionalAnnotations)
    override fun withAnnotations(additionalAnnotations: Iterable<String>): BigIntIntElementImpl = _withAnnotations(additionalAnnotations)
    override fun withoutAnnotations(): BigIntIntElementImpl = _withoutAnnotations()
    override fun withMetas(additionalMetas: MetaContainer): BigIntIntElementImpl = _withMetas(additionalMetas)
    override fun withMeta(key: String, value: Any): BigIntIntElementImpl = _withMeta(key, value)
    override fun withoutMetas(): BigIntIntElementImpl = _withoutMetas()

    override fun writeContentTo(writer: IonWriter) = writer.writeInt(bigIntegerValue)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as BigIntIntElementImpl

        if (bigIntegerValue != other.bigIntegerValue) return false
        if (annotations != other.annotations) return false
        // Note: metas intentionally omitted!

        return true
    }

    override fun hashCode(): Int {
        var result = bigIntegerValue.hashCode()
        result = 31 * result + annotations.hashCode()
        // Note: metas intentionally omitted!
        return result
    }
}

internal val MAX_LONG_AS_BIG_INT = BigInteger.valueOf(Long.MAX_VALUE)
internal val MIN_LONG_AS_BIG_INT = BigInteger.valueOf(Long.MIN_VALUE)
