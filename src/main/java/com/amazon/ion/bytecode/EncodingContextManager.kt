// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode

import com.amazon.ion.bytecode.util.BytecodeBuffer
import com.amazon.ion.bytecode.util.ConstantPool
import com.amazon.ion.ion_1_1.MacroImpl
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings

/**
 * TODO:
 *   Write more documentation.
 *   Implement stubbed out methods.
 *
 * Notes:
 *
 * It is never safe to remove or modify any existing data in the effective tables. It is safe to append data to those
 * tables for an `add_symbols`, `add_macros`, or `use` directive (as long as the active encoding modules are just `$ion` and `_`).
 */
internal class EncodingContextManager {

    companion object {
        val SYSTEM_SYMBOLS = arrayOf(
            null,
            "\$ion",
            "\$ion_1_0",
            "\$ion_symbol_table",
            "name",
            "version",
            "imports",
            "symbols",
            "max_id",
            "\$ion_shared_symbol_table",
        )
    }

    // These make up the effective macro table and effective symbol table
    private var macroBytecode = BytecodeBuffer()
    private var macroOffsets = BytecodeBuffer()
    private var macroNames = ConstantPool()
    private var symbols = mutableListOf<String?>().apply { SYSTEM_SYMBOLS.forEach { add(it) } }

    // TODO: Do we need the constant pool here?
    private var constants = ConstantPool()

    private class Module(
        val symbols: Array<String>,
        val macros: Array<MacroImpl>,
        val macroNames: Array<String?>
    )

    // Tracks only modules _other_ than the system module and default module
    private val additionalAvailableModules = mutableMapOf<String, Module>()
    // Tracks only modules _other_ than the system module and default module
    private var additionalActiveModules = mutableListOf<Module>()

    @SuppressFBWarnings("IE_EXPOSE_REP", justification = "array is accessible for performance")
    fun getEffectiveMacroTableBytecode(): IntArray = macroBytecode.unsafeGetArray()

    @SuppressFBWarnings("IE_EXPOSE_REP", justification = "array is accessible for performance")
    fun getEffectiveMacroTableOffsets(): IntArray = macroOffsets.unsafeGetArray()

    fun getEffectiveSymbolTable(): Array<String?> = symbols.toTypedArray()

    @SuppressFBWarnings("IE_EXPOSE_REP", justification = "array is accessible for performance")
    fun getEffectiveConstantPool(): Array<Any?> = constants.unsafeGetArray()

    /** Called when encountering an IVM */
    fun reset() {
        additionalActiveModules.clear()
        additionalAvailableModules.clear()
        macroBytecode.clear()
        macroOffsets.clear()
        macroNames.clear()
        symbols.clear()
        SYSTEM_SYMBOLS.forEach { symbols.add(it) }
        constants.clear()
    }

    /**
     * The [BytecodeIonReader] should be positioned in the directive, but not on the first value yet.
     * When this method returns, the [BytecodeIonReader] will be positioned at the end of the directive, but not stepped out.
     */
    fun readSetSymbolsDirective(reader: BytecodeIonReader) {
        TODO()
    }

    /**
     * The [BytecodeIonReader] should be positioned in the directive, but not on the first value yet.
     * When this method returns, the [BytecodeIonReader] will be positioned at the end of the directive, but not stepped out.
     */
    fun readAddSymbols(reader: BytecodeIonReader) {
        TODO()
    }

    /**
     * The [BytecodeIonReader] should be positioned in the directive, but not on the first value yet.
     * When this method returns, the [BytecodeIonReader] will be positioned at the end of the directive, but not stepped out.
     */
    fun readSetMacrosDirective(reader: BytecodeIonReader) {
        TODO()
    }

    /**
     * The [BytecodeIonReader] should be positioned in the directive, but not on the first value yet.
     * When this method returns, the [BytecodeIonReader] will be positioned at the end of the directive, but not stepped out.
     */
    fun readAddMacrosDirective(reader: BytecodeIonReader) {
        TODO()
    }

    /**
     * The [BytecodeIonReader] should be positioned in the directive, but not on the first value yet.
     * When this method returns, the [BytecodeIonReader] will be positioned at the end of the directive, but not stepped out.
     */
    fun readUseDirective(reader: BytecodeIonReader) {
        TODO("Shared symbol tables and shared modules not supported yet.")
    }

    /**
     * The [BytecodeIonReader] should be positioned in the directive, but not on the first value yet.
     * When this method returns, the [BytecodeIonReader] will be positioned at the end of the directive, but not stepped out.
     */
    fun readModuleDirective(reader: BytecodeIonReader) {
        TODO("Module definitions not supported yet.")
    }

    /**
     * The [BytecodeIonReader] should be positioned in the directive, but not on the first value yet.
     * When this method returns, the [BytecodeIonReader] will be positioned at the end of the directive, but not stepped out.
     */
    fun readImportDirective(reader: BytecodeIonReader) {
        TODO("Shared symbol tables and shared modules not supported yet.")
    }

    /**
     * The [BytecodeIonReader] should be positioned in the directive, but not on the first value yet.
     * When this method returns, the [BytecodeIonReader] will be positioned at the end of the directive, but not stepped out.
     *
     * Content should be a list of module names, as symbols.
     */
    fun readEncodingDirective(reader: BytecodeIonReader) {
        TODO()
    }
}
