// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.macro

/**
 * A reference to a particular macro, either by name or by template id.
 *
 * INVARIANT: Only one of `name` and `id` should be set, otherwise the equality semantics are unclear.
 *
 * TODO: Since system macros have an independent address space, do we need to have a `SystemById` variant,
 *       or is it enough to add the system module name?
 */
data class MacroRef private constructor(
    // Null indicates no module qualifier
    val module: String?,
    // Must be null if unknown
    val name: String?,
    // Must be -1 if unknown
    val id: Int
) {
    fun isQualified() = module != null
    fun isUnqualified() = module == null
    fun isSystemMacro() = module == "\$ion"
    fun hasName() = name != null
    fun hasId() = id >= 0

    companion object {
        @JvmStatic
        fun byId(id: Int): MacroRef = MacroRef(null, null, id)

        @JvmStatic
        fun byId(module: String?, id: Int): MacroRef = MacroRef(module, null, id)

        @JvmStatic
        fun byName(name: String): MacroRef = MacroRef(null, name, -1)

        @JvmStatic
        fun byName(module: String?, name: String): MacroRef = MacroRef(module, name, -1)
    }
}
