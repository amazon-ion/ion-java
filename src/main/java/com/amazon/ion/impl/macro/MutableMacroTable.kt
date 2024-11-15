// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.macro

class MutableMacroTable(private val antecedent: MacroTable) : MacroTable {
    private val macroTable = HashMap<MacroRef, Macro>()

    override fun get(macroRef: MacroRef): Macro? {
        return macroTable[macroRef] ?: antecedent.get(macroRef)
    }
    override fun putAll(mappings: Map<MacroRef, Macro>) = macroTable.putAll(mappings)
}
