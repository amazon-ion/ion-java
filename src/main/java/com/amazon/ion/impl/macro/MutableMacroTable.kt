// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.macro

// This needs modeling attention.
// - do we want to model an antecedent chain, or have a flat mutable table?
// - antecedent allows cheap reference to immutable system table or empty table
// - flat mutable table allows simpler implementation, GC of unneeded values, constant number of lookups
// - at some point we'll need the capability to communicate immutable encoding contexts to interpret Ion bytes, but
//   this is neither here nor there
class MutableMacroTable(private val antecedent: MacroTable) : MacroTable {
    private val macroTable = HashMap<MacroRef, Macro>()

    override fun get(address: MacroRef): Macro? {
        return macroTable[address] ?: antecedent.get(address)
    }
    override fun putAll(mappings: Map<MacroRef, Macro>) = macroTable.putAll(mappings)
}
