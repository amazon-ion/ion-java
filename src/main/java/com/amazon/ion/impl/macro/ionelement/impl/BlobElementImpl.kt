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
import kotlinx.collections.immutable.PersistentList
import kotlinx.collections.immutable.toPersistentList
import kotlinx.collections.immutable.toPersistentMap

internal class BlobElementImpl(
    bytes: ByteArray,
    override val annotations: PersistentList<String>,
    override val metas: PersistentMetaContainer
) : LobElementBase(bytes), BlobElement {

    override val blobValue: ByteArrayView get() = bytesValue

    override fun writeContentTo(writer: IonWriter) = writer.writeBlob(bytes)

    override fun copy(annotations: List<String>, metas: MetaContainer): BlobElementImpl =
        BlobElementImpl(bytes, annotations.toPersistentList(), metas.toPersistentMap())

    override fun withAnnotations(vararg additionalAnnotations: String): BlobElementImpl = _withAnnotations(*additionalAnnotations)
    override fun withAnnotations(additionalAnnotations: Iterable<String>): BlobElementImpl = _withAnnotations(additionalAnnotations)
    override fun withoutAnnotations(): BlobElementImpl = _withoutAnnotations()
    override fun withMetas(additionalMetas: MetaContainer): BlobElementImpl = _withMetas(additionalMetas)
    override fun withMeta(key: String, value: Any): BlobElementImpl = _withMeta(key, value)
    override fun withoutMetas(): BlobElementImpl = _withoutMetas()

    override val type: ElementType get() = ElementType.BLOB
}
