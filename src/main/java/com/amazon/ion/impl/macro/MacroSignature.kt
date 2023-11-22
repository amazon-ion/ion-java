package com.amazon.ion.impl.macro

/**
 * Represents the signature of a macro.
 */
@JvmInline value class MacroSignature(val parameters: List<Parameter>) {

    data class Parameter(val variableName: String, val type: Encoding, val grouped: Boolean)

    enum class Encoding(val ionTextName: String?) {
        Tagged(null),
        Any("any"),
        Int8("int8"),
        // TODO: List all of the possible tagless encodings
    }
}
