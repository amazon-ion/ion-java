// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.bin

/**
 * TODO: Proper documentation.
 *
 * TODO: Consider renaming to "ContainerLengthPrefixStrategy" since EExps don't strictly have
 *       a delimited option like the other container types.
 *
 * See [SymbolInliningStrategy] for a similar strategy interface.
 */
fun interface DelimitedContainerStrategy {
    /**
     * Indicates whether a container should be written using a length prefix.
     *
     * TODO: See if we can add other context, such as annotations that are going to be added to this container,
     *       or the field name (if this container is in a struct).
     *
     * With more context, we could enable strategies like:
     *   - Write lists with annotation `X` as a delimited container.
     */
    fun writeDelimited(containerType: ContainerType, depth: Int): Boolean

    companion object {
        @JvmField
        val ALWAYS_DELIMITED = DelimitedContainerStrategy { _, _ -> true }
        @JvmField
        val ALWAYS_PREFIXED = DelimitedContainerStrategy { _, _ -> false }
    }

    enum class ContainerType {
        LIST,
        STRUCT,
        SEXP,
        /**
         * These are only containers at an encoding/syntax level.
         * There isn't really a "delimited" option for these, but there is a length-prefix option.
         */
        EEXP,
    }
}
