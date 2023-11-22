package com.amazon.ion.impl.macro

/**
 * A reference to a particular macro, either by name or by template id.
 */
sealed interface MacroRef {
    @JvmInline value class ByName(val name: String) : MacroRef

    @JvmInline value class ById(val id: Long) : MacroRef
}
