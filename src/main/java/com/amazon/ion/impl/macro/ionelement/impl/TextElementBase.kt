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

import com.amazon.ion.impl.macro.ionelement.api.MetaContainer
import com.amazon.ion.impl.macro.ionelement.api.TextElement

internal abstract class TextElementBase(
    override val textValue: String
) : AnyElementBase(), TextElement {

    abstract override fun copy(annotations: List<String>, metas: MetaContainer): TextElementBase
    abstract override fun withAnnotations(vararg additionalAnnotations: String): TextElementBase
    abstract override fun withAnnotations(additionalAnnotations: Iterable<String>): TextElementBase
    abstract override fun withoutAnnotations(): TextElementBase
    abstract override fun withMetas(additionalMetas: MetaContainer): TextElementBase
    abstract override fun withMeta(key: String, value: Any): TextElementBase
    abstract override fun withoutMetas(): TextElementBase
}
