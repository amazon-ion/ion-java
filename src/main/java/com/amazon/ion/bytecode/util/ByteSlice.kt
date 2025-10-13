// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.util

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings

/**
 * Light-weight representation of a portion of a [ByteArray].
 *
 * Positions are relative to `bytes`, not the underlying data stream.
 *
 * TODO: Consider adding helper functions that allow
 *   * copying some or all of the bytes into an existing byte array
 *   * creating a new byte array that is a copy of just a subsection of this [ByteSlice].
 *
 * This is not intended to be exposed publicly, but it might end up needing to be exposed in order to expose the
 * [BytecodeGenerator][com.amazon.ion.bytecode.BytecodeGenerator] for use by e.g. an object mapper. If it does get
 * exposed, we need to revisit the location of this class and make [bytes], [startInclusive], and [endExclusive] private.
 */
@SuppressFBWarnings(
    value = ["EI_EXPOSE_REP"],
    justification = "This class is only for internal use. Exposing the internal array is intentional to avoid the performance overhead of copying it."
)
internal class ByteSlice(
    val bytes: ByteArray,
    val startInclusive: Int,
    val endExclusive: Int
) {
    val length = endExclusive - startInclusive

    /**
     * Convenience method to create a new array that is a copy of this data represented by this [ByteSlice].
     */
    fun newByteArray() = bytes.copyOfRange(startInclusive, endExclusive)
}
