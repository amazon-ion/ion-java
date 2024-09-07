// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.bin

/**
 * A strategy to determine whether a SymbolToken with known text should be encoded by Symbol ID (SID) or as inline text.
 * Symbols with unknown text are always be written as a SID because the text is unknown.
 *
 * Some possible implementation ideas:
 *
 *  - A simple implementation could elect to inline symbols that are less than `N` characters long.
 *  - A domain-specific implementation could choose to inline symbols with specific prefixes. E.g. annotations starting
 *    with `org.example` always get written inline.
 *  - A stateful implementation could keep track of how often a symbol is used, and elect to write the symbol inline
 *    until it has been used at least `N` times.
 *  - A streaming-oriented implementation could keep track of the symbols that are used, and inline any symbols not
 *    already in the symbol table. Once a top-level value is complete, some other component could inspect the list of
 *    new symbols and emit a Local Symbol Table append with those symbols so that they can be interned use in future
 *    top-level values.
 */
fun interface SymbolInliningStrategy {
    /**
     * Represents the different kinds of usage of a symbol token.
     */
    enum class SymbolKind {
        VALUE,
        FIELD_NAME,
        ANNOTATION,
    }

    /**
     * Indicates whether a particular symbol text should be written inline (as opposed to writing as a SID).
     */
    fun shouldWriteInline(symbolKind: SymbolKind, text: String): Boolean

    companion object {
        /**
         * A [SymbolInliningStrategy] that causes all symbols to be written as a SID,
         * interning the text in the Local Symbol Table if necessary.
         *
         * This is equivalent to the behavior of symbols in Ion 1.0.
         */
        @JvmField
        val NEVER_INLINE = object : SymbolInliningStrategy {
            override fun shouldWriteInline(symbolKind: SymbolKind, text: String): Boolean = false
            override fun toString(): String = "NEVER_INLINE"
        }

        /**
         * A [SymbolInliningStrategy] that causes all symbols with known text to have their text written inline.
         */
        @JvmField
        val ALWAYS_INLINE = object : SymbolInliningStrategy {
            override fun shouldWriteInline(symbolKind: SymbolKind, text: String): Boolean = true
            override fun toString(): String = "ALWAYS_INLINE"
        }
    }
}
