// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl

import com.amazon.ion.IonWriter
import com.amazon.ion.SymbolTable
import com.amazon.ion.bytecode.bin11.OpCode
import com.amazon.ion.ion_1_1.IonRawWriter_1_1
import com.amazon.ion.ion_1_1.MacroImpl
import java.util.ArrayList
import java.util.HashMap
import java.util.LinkedHashMap

/**
 * TODO: Testing that is distinct from [IonManagedWriter_1_1] tests.
 * TODO(perf): See if there is a meaningful effect on performance if we move all of this into [IonManagedWriter_1_1].
 */
internal class IonManagedWriterEncodingContext_1_1 {

    companion object {
        private const val NUMBER_OF_SYSTEM_SIDS = 9

        private val SYSTEM_SYMBOLS = mapOf(
            "\$ion" to 1,
            "\$ion_1_0" to 2,
            "\$ion_symbol_table" to 3,
            "name" to 4,
            "version" to 5,
            "imports" to 6,
            "symbols" to 7,
            "max_id" to 8,
            "\$ion_shared_symbol_table" to 9,
        )
    }

    // We take a slightly different approach here by handling the encoding context as a prior encoding context
    // plus a list of symbols added by the current encoding context.
    /** The symbol table for the prior encoding context */
    private var symbolTable: HashMap<String, Int> = HashMap<String, Int>().also { it.putAll(SYSTEM_SYMBOLS) }

    /** Symbols to be interned since the prior encoding context. */
    private var newSymbols: HashMap<String, Int> = LinkedHashMap() // Preserves insertion order.

    /** The macro table of the prior encoding context. Map value is the user-space address. */
    private var macroTable: HashMap<MacroImpl, Int> = LinkedHashMap()
    /** Macros to be added since the last encoding directive was flushed. Map value is the user-space address. */
    private var newMacros: HashMap<MacroImpl, Int> = LinkedHashMap()
    /** Macro names by user-space address, including new macros. */
    private var macroNames = ArrayList<String?>()
    /** Macro definitions by user-space address, including new macros. */
    private var macrosById = ArrayList<MacroImpl>()

    /**
     * Adds a new symbol to the table for this writer, or finds an existing definition of it. This writer does not
     * implement [IonWriter.getSymbolTable], so this method supplies some of that functionality.
     *
     * @return an SID for the given symbol text
     * @see SymbolTable.intern
     */
    fun intern(text: String): Int {
        // Check the current symbol table
        var sid = symbolTable[text]
        if (sid != null) return sid
        // Check the to-be-appended symbols
        sid = newSymbols[text]
        if (sid != null) return sid
        // Add to the to-be-appended symbols
        sid = symbolTable.size + newSymbols.size + 1
        newSymbols[text] = sid
        return sid
    }

    /**
     * Adds a named macro to the macro table
     *
     * Steps:
     * - If the name is not already in use...
     *    - And the macro is already in `newMacros`...
     *      1. Get the address of the macro in `newMacros`
     *      2. Add the name to `macroNames` for the that address
     *      3. return the address
     *    - Else...
     *      1. Add a new entry for the macro to `newMacros` and get a new address
     *      2. Add the name to `macroNames` for the new address
     *      3. Return the new address
     * - If the name is already in use...
     *   - And it is associated with the same macro...
     *      1. Return the address associated with the name
     *   - And it is associated with a different macro...
     *      - This is where the managed writer take an opinion. (Or be configurable.)
     *        - It could mangle the name
     *        - It could remove the name from a macro in macroTable, but then it would have to immediately flush to
     *          make sure that any prior e-expressions are still valid. In addition, we would need to re-export all
     *          the other macros from `_` (the default module).
     *        - For now, we're just throwing an Exception.
     */
    private fun getOrAssignMacroAddressAndName(name: String, macro: MacroImpl): Int {
        // TODO: This is O(n), but could be O(1).
        var existingAddress = macroNames.indexOf(name)
        if (existingAddress < 0) {
            // Name is not already in use
            existingAddress = newMacros.getOrDefault(macro, -1)

            val address = if (existingAddress < 0) {
                // Macro is not in newMacros
                // Add to newMacros and get a macro address
                assignMacroAddress(macro)
            } else {
                // Macro already exists in newMacros, but doesn't have a name
                existingAddress
            }
            // Set the name of the macro
            macroNames[address] = name
            return address
        } else if (macrosById[existingAddress] == macro) {
            // Macro already in table, and already using the same name
            return existingAddress
        } else {
            // Name is already in use for a different macro.
            // This macro may or may not be in the table under a different name, but that's
            // not particularly relevant unless we want to try to fall back to a different name.
            TODO("Name shadowing is not supported yet. Call finish() before attempting to shadow an existing macro.")
        }
    }

    /**
     * Steps for adding an anonymous macro to the macro table
     *    1. Check macroTable, if found, return that address
     *    2. Check newMacros, if found, return that address
     *    3. Add to newMacros, return new address
     */
    private fun getOrAssignMacroAddress(macro: MacroImpl): Int {
        var address = macroTable.getOrDefault(macro, -1)
        if (address >= 0) return address
        address = newMacros.getOrDefault(macro, -1)
        if (address >= 0) return address

        return assignMacroAddress(macro)
    }

    fun getOrAssignMacroAddress(macro: MacroImpl, name: String?): Int {
        return if (name == null)
            getOrAssignMacroAddress(macro)
        else
            getOrAssignMacroAddressAndName(name, macro)
    }

    fun getMacroNameForId(id: Int): String? = macroNames[id]

    /** Unconditionally adds a macro to the macro table data structures and returns the new address. */
    private fun assignMacroAddress(macro: MacroImpl): Int {
        val address = macrosById.size
        macrosById.add(macro)
        macroNames.add(null)
        newMacros[macro] = address
        return address
    }

    fun reset() {
        symbolTable.clear()
        macroNames.clear()
        macrosById.clear()
        macroTable.clear()
        newMacros.clear()
        symbolTable.putAll(SYSTEM_SYMBOLS)
    }

    /**
     * Writes an encoding directive for the current encoding context, and updates internal state accordingly.
     * This always appends to the current encoding context. If there is nothing to append, calling this function
     * is a no-op.
     */
    fun writeEncodingDirective(systemData: IonRawWriter_1_1) {
        if (newSymbols.isEmpty() && newMacros.isEmpty()) return

        writeSymbolTableDirective(systemData)
        symbolTable.putAll(newSymbols)
        newSymbols.clear()
        // NOTE: Once we have emitted the symbol table update with set/add_symbols those symbols become available
        //       for use in set/add_macros (if relevant)

        writeMacroTableDirective(systemData)
        macroTable.putAll(newMacros)
        newMacros.clear()
    }

    /**
     * Updates the symbols in the encoding context by invoking
     * the `add_symbols` or `set_symbols` system macro.
     * If the symbol table would be empty, writes nothing, which is equivalent
     * to an empty symbol table.
     */
    private fun writeSymbolTableDirective(systemData: IonRawWriter_1_1) {
        val hasSymbolsToAdd = newSymbols.isNotEmpty()
        val hasSymbolsToRetain = symbolTable.size > NUMBER_OF_SYSTEM_SIDS
        if (!hasSymbolsToAdd) return
        val directive = if (!hasSymbolsToRetain) OpCode.DIRECTIVE_SET_SYMBOLS else OpCode.DIRECTIVE_ADD_SYMBOLS

        // Add new symbols
        systemData.stepInDirective(directive)
        newSymbols.forEach { (text, _) -> systemData.writeString(text) }
        systemData.stepOut()
    }

    private fun writeMacroTableDirective(systemData: IonRawWriter_1_1) {
        val hasMacrosToAdd = newMacros.isNotEmpty()
        val hasMacrosToRetain = macroTable.isNotEmpty()
        if (!hasMacrosToAdd) return
        val directive = if (!hasMacrosToRetain) OpCode.DIRECTIVE_SET_MACROS else OpCode.DIRECTIVE_ADD_MACROS

        // Add new macros
        systemData.stepInDirective(directive)
        newMacros.forEach { (macro, id) ->
            val macroName = macroNames[id]
            systemData.stepInSExp(usingLengthPrefix = false)
            if (macroName == null) {
                systemData.writeNull()
            } else {
                systemData.writeSymbol(macroName)
            }
            macro.writeTo(systemData)
            systemData.stepOut()
        }
        systemData.stepOut()
    }
}
