// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.bin

/**
 * Options that are specific to Ion 1.1 and handled in the managed writer.
 * These are (mostly) generalizable to both text and binary.
 *
 * TODO: data classes cannot be changed in a backward compatible way because of the auto-generated `copy` method.
 *       See if we can get away with using a non-"data" class here, but if not then replace this with a public
 *       interface, a public builder, and a private/internal implementation class.
 */
data class ManagedWriterOptions_1_1(
    /**
     * Whether the symbols in the encoding directive should be interned or not.
     * For binary, almost certainly want this to be true, and for text, it's
     * more readable if it's false.
     */
    val internEncodingDirectiveSymbols: Boolean,
    val invokeTdlMacrosByName: Boolean,
    val symbolInliningStrategy: SymbolInliningStrategy,
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
