package com.amazon.ion.impl.macro

/**
 * When we implement modules, this will likely need to be replaced.
 * For now, it is a placeholder for what is to come and a container for the macro table.
 */
class EncodingContext(
    val macroTable: Map<MacroRef, Macro>
) {
    companion object {
        // TODO: Replace this with a DEFAULT encoding context that includes system macros.
        @JvmStatic
        val EMPTY = EncodingContext(emptyMap())
    }
}
