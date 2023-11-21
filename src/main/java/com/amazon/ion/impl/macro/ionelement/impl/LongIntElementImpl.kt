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
import java.math.BigInteger
import kotlinx.collections.immutable.PersistentList
import kotlinx.collections.immutable.toPersistentList
import kotlinx.collections.immutable.toPersistentMap

internal class LongIntElementImpl(
    override val longValue: Long,
    override val annotations: PersistentList<String>,
    override val metas: PersistentMetaContainer
) : AnyElementBase(), IntElement {
    override val integerSize: IntElementSize get() = IntElementSize.LONG
    override val type: ElementType get() = ElementType.INT

    override val bigIntegerValue: BigInteger get() = BigInteger.valueOf(longValue)

    override fun copy(annotations: List<String>, metas: MetaContainer): LongIntElementImpl =
        LongIntElementImpl(longValue, annotations.toPersistentList(), metas.toPersistentMap())

    override fun withAnnotations(vararg additionalAnnotations: String): LongIntElementImpl = _withAnnotations(*additionalAnnotations)
    override fun withAnnotations(additionalAnnotations: Iterable<String>): LongIntElementImpl = _withAnnotations(additionalAnnotations)
    override fun withoutAnnotations(): LongIntElementImpl = _withoutAnnotations()
    override fun withMetas(additionalMetas: MetaContainer): LongIntElementImpl = _withMetas(additionalMetas)
    override fun withMeta(key: String, value: Any): LongIntElementImpl = _withMeta(key, value)
    override fun withoutMetas(): LongIntElementImpl = _withoutMetas()

    override fun writeContentTo(writer: IonWriter) = writer.writeInt(longValue)
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as LongIntElementImpl

        if (longValue != other.longValue) return false
        if (annotations != other.annotations) return false
        // Note: metas intentionally omitted!

        return true
    }

    override fun hashCode(): Int {
        var result = longValue.hashCode()
        result = 31 * result + annotations.hashCode()
        // Note: metas intentionally omitted!
        return result
    }
}
