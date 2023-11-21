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

/** Returns a shallow copy of the current node with the specified additional annotations. */
internal inline fun <reified T : IonElement> T._withAnnotations(vararg additionalAnnotations: String): T =
    when {
        additionalAnnotations.isEmpty() -> this
        else -> copy(annotations = this.annotations + additionalAnnotations) as T
    }

/** Returns a shallow copy of the current node with the specified additional annotations. */
internal inline fun <reified T : IonElement> T._withAnnotations(additionalAnnotations: Iterable<String>): T =
    _withAnnotations(*additionalAnnotations.toList().toTypedArray())

/** Returns a shallow copy of the current node with all annotations removed. */
internal inline fun <reified T : IonElement> T._withoutAnnotations(): T =
    when {
        this.annotations.isNotEmpty() -> copy(annotations = emptyList()) as T
        else -> this
    }

/**
 * Returns a shallow copy of the current node with the specified additional metadata, overwriting any metas
 * that already exist with the same keys.
 */
internal inline fun <reified T : IonElement> T._withMetas(additionalMetas: MetaContainer): T =
    when {
        additionalMetas.isEmpty() -> this
        else -> copy(metas = metaContainerOf(metas.toList().union(additionalMetas.toList()).toList())) as T
    }

/**
 * Returns a shallow copy of the current node with the specified additional meta, overwriting any meta
 * that previously existed with the same key.
 *
 * When adding multiple metas, consider [withMetas] instead.
 */
internal inline fun <reified T : IonElement> T._withMeta(key: String, value: Any): T =
    _withMetas(metaContainerOf(key to value))

/** Returns a shallow copy of the current node without any metadata. */
internal inline fun <reified T : IonElement> T._withoutMetas(): T =
    when {
        metas.isEmpty() -> this
        else -> copy(metas = emptyMetaContainer(), annotations = annotations) as T
    }
