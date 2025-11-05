// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings

/**
 * Options that are specific to Ion 1.1 and handled in the managed writer.
 * These are (mostly) generalizable to both text and binary.
 *
 * This should be hidden behind the IonWriteBuilders abstraction.
 */
internal class ManagedWriterOptions_1_1(
    /**
     * Whether the symbols in the encoding directive should be interned or not.
     * For binary, almost certainly want this to be true, and for text, it's
     * more readable if it's false.
     */
    val internEncodingDirectiveSymbols: Boolean,
    val symbolInliningStrategy: SymbolInliningStrategy,
    @get:SuppressFBWarnings("EI_EXPOSE_REP", justification = "LengthPrefixStrategy instances are not mutable.")
    @SuppressFBWarnings("EI_EXPOSE_REP2", justification = "LengthPrefixStrategy instances are not mutable.")
    val lengthPrefixStrategy: LengthPrefixStrategy,
    val eExpressionIdentifierStrategy: EExpressionIdentifierStrategy,
) : SymbolInliningStrategy by symbolInliningStrategy, LengthPrefixStrategy by lengthPrefixStrategy {

    /**
     * Indicates whether e-expressions should be written using macro
     * names or macro addresses (when a choice is available).
     */
    enum class EExpressionIdentifierStrategy {
        BY_NAME,
        BY_ADDRESS,
    }
}

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
 *
 * TODO: remove "sealed" when we are confident of the API.
 */
@SuppressFBWarnings("IC_SUPERCLASS_USES_SUBCLASS_DURING_INITIALIZATION")
sealed interface SymbolInliningStrategy {
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

    private object NeverInline : SymbolInliningStrategy {
        override fun shouldWriteInline(symbolKind: SymbolKind, text: String): Boolean = false
        override fun toString(): String = "NEVER_INLINE"
    }

    private object AlwaysInline : SymbolInliningStrategy {
        override fun shouldWriteInline(symbolKind: SymbolKind, text: String): Boolean = true
        override fun toString(): String = "ALWAYS_INLINE"
    }

    companion object {
        /**
         * A [SymbolInliningStrategy] that causes all symbols to be written as a SID,
         * interning the text in the Local Symbol Table if necessary.
         *
         * This is equivalent to the behavior of symbols in Ion 1.0 binary.
         */
        @JvmField
        val NEVER_INLINE: SymbolInliningStrategy = NeverInline

        /**
         * A [SymbolInliningStrategy] that causes all symbols with known text to have their text written inline.
         */
        @JvmField
        val ALWAYS_INLINE: SymbolInliningStrategy = AlwaysInline
    }
}

/**
 * TODO: Proper documentation.
 *
 * See [SymbolInliningStrategy] for a similar strategy interface.
 *
 * TODO: remove "sealed" when we are confident of the API.
 */
@SuppressFBWarnings("IC_SUPERCLASS_USES_SUBCLASS_DURING_INITIALIZATION")
sealed interface LengthPrefixStrategy {
    /**
     * Indicates whether a container should be written using a length prefix.
     *
     * TODO: See if we can add other context, such as annotations that are going to be added to this container,
     *       the field name (if this container is in a struct), or the delimited/prefixed status of the parent
     *       container.
     *
     * With more context, we could enable strategies like:
     *   - Write lists with annotation `X` as a delimited container.
     */
    fun writeLengthPrefix(containerType: ContainerType, depth: Int): Boolean

    private data object NeverPrefixed : LengthPrefixStrategy {
        override fun writeLengthPrefix(containerType: ContainerType, depth: Int): Boolean = false
    }
    private data object AlwaysPrefixed : LengthPrefixStrategy {
        override fun writeLengthPrefix(containerType: ContainerType, depth: Int): Boolean = true
    }
    private data object ContainersPrefixed : LengthPrefixStrategy {
        override fun writeLengthPrefix(containerType: ContainerType, depth: Int): Boolean = containerType != ContainerType.EEXP
    }

    companion object {
        @JvmField
        val NEVER_PREFIXED: LengthPrefixStrategy = NeverPrefixed
        @JvmField
        val ALWAYS_PREFIXED: LengthPrefixStrategy = AlwaysPrefixed
        /** Containers are prefixed. E-Exps are not. */
        @JvmField
        val CONTAINERS_PREFIXED: LengthPrefixStrategy = ContainersPrefixed
    }

    enum class ContainerType {
        LIST,
        STRUCT,
        SEXP,
        /**
         * These are only containers at an encoding/syntax level.
         * There isn't really a "delimited" option for macros, but there is a length-prefix option.
         */
        EEXP,
    }
}
