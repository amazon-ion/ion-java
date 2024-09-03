// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.bin

/**
 * TODO: Proper documentation.
 *
 * See [SymbolInliningStrategy] for a similar strategy interface.
 */
fun interface LengthPrefixStrategy {
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

    companion object {
        @JvmField
        val NEVER_PREFIXED = LengthPrefixStrategy { _, _ -> false }
        @JvmField
        val ALWAYS_PREFIXED = LengthPrefixStrategy { _, _ -> true }
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
        EXPRESSION_GROUP,
    }
}
