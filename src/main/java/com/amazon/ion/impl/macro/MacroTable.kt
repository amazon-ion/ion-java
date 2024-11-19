// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.macro

interface MacroTable {
    fun get(address: MacroRef): Macro?
    fun putAll(mappings: Map<MacroRef, Macro>): Unit = throw UnsupportedOperationException()

    companion object {
        @JvmStatic
        @get:JvmName("empty")
        val EMPTY = object : MacroTable {
            override fun get(address: MacroRef): Macro? = null
        }
    }
}
