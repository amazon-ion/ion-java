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

import com.amazon.ion.impl.macro.ionelement.api.ByteArrayView
import com.amazon.ion.impl.macro.ionelement.api.LobElement
import com.amazon.ion.impl.macro.ionelement.api.MetaContainer

internal abstract class LobElementBase(
    protected val bytes: ByteArray
) : AnyElementBase(), LobElement {

    override val bytesValue: ByteArrayView = ByteArrayViewImpl(bytes)

    abstract override fun copy(annotations: List<String>, metas: MetaContainer): LobElementBase
    abstract override fun withAnnotations(vararg additionalAnnotations: String): LobElementBase
    abstract override fun withAnnotations(additionalAnnotations: Iterable<String>): LobElementBase
    abstract override fun withoutAnnotations(): LobElementBase
    abstract override fun withMetas(additionalMetas: MetaContainer): LobElementBase
    abstract override fun withMeta(key: String, value: Any): LobElementBase
    abstract override fun withoutMetas(): LobElementBase

    override fun equals(other: Any?): Boolean {
        return when {
            this === other -> true
            other !is LobElementBase -> false
            type != other.type -> false
            !bytes.contentEquals(other.bytes) -> false
            annotations != other.annotations -> false
            // Metas are intentionally omitted here.
            else -> true
        }
    }

    override fun hashCode(): Int {
        var result = bytes.contentHashCode()
        result = 31 * result + type.hashCode()
        result = 31 * result + annotations.hashCode()
        // Metas are intentionally omitted here.
        return result
    }
}
