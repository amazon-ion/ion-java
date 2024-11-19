// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.macro

/**
 * When we implement modules, this will likely need to be replaced.
 * For now, it is a placeholder for what is to come and a container for the macro table.
 */
class EncodingContext {

    val macroTable: MacroTable
    val isMutable: Boolean

    @JvmOverloads
    constructor(macroTable: MacroTable, isMutable: Boolean = true) {
        this.macroTable = macroTable
        this.isMutable = isMutable
    }

    companion object {
        @JvmStatic
        @get:JvmName("getDefault")
        val DEFAULT = EncodingContext(SystemMacro.SYSTEM_MACRO_TABLE, false)
    }
}
