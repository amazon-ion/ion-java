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
import com.amazon.ion.impl.macro.ionelement.api.AnyElement
import com.amazon.ion.impl.macro.ionelement.api.MetaContainer
import com.amazon.ion.impl.macro.ionelement.api.SeqElement
import kotlinx.collections.immutable.PersistentList

internal abstract class SeqElementBase(
    override val values: PersistentList<AnyElement>
) : AnyElementBase(), SeqElement {

    override val containerValues: List<AnyElement> get() = values
    override val seqValues: List<AnyElement> get() = values

    override val size: Int
        get() = values.size

    override fun writeContentTo(writer: IonWriter) {
        writer.stepIn(type.toIonType())
        values.forEach {
            it.writeTo(writer)
        }
        writer.stepOut()
    }

    abstract override fun copy(annotations: List<String>, metas: MetaContainer): SeqElementBase
    abstract override fun withAnnotations(vararg additionalAnnotations: String): SeqElementBase
    abstract override fun withAnnotations(additionalAnnotations: Iterable<String>): SeqElementBase
    abstract override fun withoutAnnotations(): SeqElementBase
    abstract override fun withMetas(additionalMetas: MetaContainer): SeqElementBase
    abstract override fun withMeta(key: String, value: Any): SeqElementBase
    abstract override fun withoutMetas(): SeqElementBase
}
