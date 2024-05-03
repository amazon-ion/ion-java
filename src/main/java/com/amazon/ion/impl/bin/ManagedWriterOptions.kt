// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.bin

/**
 * Options that are specific to Ion 1.1 and handled in the managed writer.
 * These are (mostly) generalizable to both text and binary.
 */
data class ManagedWriterOptions(
    /**
     * Whether the symbols in the encoding directive should be interned or not.
     * For binary, almost certainly want this to be true, and for text, it's
     * more readable if it's false.
     */
    val internEncodingDirectiveSymbols: Boolean = false,
    val symbolInliningStrategy: SymbolInliningStrategy,
    val delimitedContainerStrategy: DelimitedContainerStrategy,
) : SymbolInliningStrategy by symbolInliningStrategy, DelimitedContainerStrategy by delimitedContainerStrategy {
    companion object {
        @JvmField
        val ION_BINARY_DEFAULT = ManagedWriterOptions(
            internEncodingDirectiveSymbols = true,
            symbolInliningStrategy = SymbolInliningStrategy.NEVER_INLINE,
            delimitedContainerStrategy = DelimitedContainerStrategy.ALWAYS_PREFIXED,
        )
        @JvmField
        val ION_TEXT_DEFAULT = ManagedWriterOptions(
            // It's a little easier to read this way
            internEncodingDirectiveSymbols = false,
            symbolInliningStrategy = SymbolInliningStrategy.ALWAYS_INLINE,
            // This doesn't actually have any effect for Ion Text since there are no length-prefixed containers.
            delimitedContainerStrategy = DelimitedContainerStrategy.ALWAYS_DELIMITED,
        )
    }
}
