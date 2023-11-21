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

@file: JvmName("IonMeta")
package com.amazon.ion.impl.macro.ionelement.api

import kotlinx.collections.immutable.PersistentMap
import kotlinx.collections.immutable.toPersistentHashMap

public typealias MetaContainer = Map<String, Any>
internal typealias PersistentMetaContainer = PersistentMap<String, Any>

internal val EMPTY_METAS = HashMap<String, Any>().toPersistentHashMap()

public fun emptyMetaContainer(): MetaContainer = EMPTY_METAS

public inline fun <reified T> MetaContainer.metaOrNull(key: String): T? = this[key] as T
public inline fun <reified T> MetaContainer.meta(key: String): T =
    metaOrNull(key) ?: error("Meta with key '$key' and type ${T::class.java} not found in MetaContainer")

public fun metaContainerOf(kvps: List<Pair<String, Any>>): MetaContainer =
    metaContainerOf(*kvps.toTypedArray())

public fun metaContainerOf(vararg kvps: Pair<String, Any>): MetaContainer =
    when {
        kvps.none() -> EMPTY_METAS
        else -> HashMap(mapOf(*kvps))
    }

/**
 * Merges two meta containers.  Any keys present in the receiver will be replaced by any keys in with the same
 * name in [other].
 */
public operator fun MetaContainer.plus(other: MetaContainer): MetaContainer =
    HashMap<String, Any>(this.toList().union(other.toList()).toMap())

/**
 * Merges two meta containers.  Any keys present in the receiver will be replaced by any keys in with the same
 * name in [other].
 */
public operator fun MetaContainer.plus(other: Iterable<Pair<String, Any>>): MetaContainer =
    HashMap<String, Any>(this.toList().union(other).toMap())
