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

import com.amazon.ion.impl.macro.ionelement.api.*
import com.amazon.ion.impl.macro.ionelement.api.PersistentMetaContainer
import kotlinx.collections.immutable.PersistentList
import kotlinx.collections.immutable.toPersistentList
import kotlinx.collections.immutable.toPersistentMap

internal class SexpElementImpl(
    values: PersistentList<AnyElement>,
    override val annotations: PersistentList<String>,
    override val metas: PersistentMetaContainer
) : SeqElementBase(values), SexpElement {
    override val type: ElementType get() = ElementType.SEXP

    override val sexpValues: List<AnyElement> get() = seqValues

    override fun copy(annotations: List<String>, metas: MetaContainer): SexpElementImpl =
        SexpElementImpl(values, annotations.toPersistentList(), metas.toPersistentMap())

    override fun withAnnotations(vararg additionalAnnotations: String): SexpElementImpl = _withAnnotations(*additionalAnnotations)
    override fun withAnnotations(additionalAnnotations: Iterable<String>): SexpElementImpl = _withAnnotations(additionalAnnotations)
    override fun withoutAnnotations(): SexpElementImpl = _withoutAnnotations()
    override fun withMetas(additionalMetas: MetaContainer): SexpElementImpl = _withMetas(additionalMetas)
    override fun withMeta(key: String, value: Any): SexpElementImpl = _withMeta(key, value)
    override fun withoutMetas(): SexpElementImpl = _withoutMetas()

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as SexpElementImpl

        if (values != other.values) return false
        if (annotations != other.annotations) return false
        // Note: [metas] intentionally omitted!

        return true
    }

    override fun hashCode(): Int {
        var result = values.hashCode()
        result = 31 * result + annotations.hashCode()
        // Note: [metas] intentionally omitted!
        return result
    }
}
