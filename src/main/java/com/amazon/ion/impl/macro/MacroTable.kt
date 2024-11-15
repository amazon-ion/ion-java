// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.macro

fun interface MacroTable {
    fun get(macroRef: MacroRef): Macro?
    fun putAll(mappings: Map<MacroRef, Macro>): Unit = throw UnsupportedOperationException()

    companion object {
        @JvmStatic
        val EMPTY = MacroTable { null }
    }
}
