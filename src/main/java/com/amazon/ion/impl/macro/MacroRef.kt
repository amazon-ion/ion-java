// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.macro

/**
 * A reference to a particular macro, either by name or by template id.
 */
sealed interface MacroRef {
    // TODO: See if these could be inline value classes
    @JvmInline value class ByName(val name: String) : MacroRef
    // Ion is not limited to Int.MAX_VALUE macro addresses, but Java collection sizes are limited by Int.MAX_VALUE
    @JvmInline value class ById(val id: Int) : MacroRef
    // TODO: Since system macros have an independent address space, do we need to have a `SystemById` variant?

    companion object {

        private val LOW_IDS = Array(128) { i -> ById(i) }

        @JvmStatic
        fun byId(id: Int): MacroRef = if (id < 128) LOW_IDS[id] else ById(id)

        @JvmStatic
        fun byName(name: String): MacroRef = ByName(name)
    }
}
