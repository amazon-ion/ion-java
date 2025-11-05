// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode

import com.amazon.ion.impl.bin.PrimitiveEncoder

/**
 * Helpers for working with binary primitives in test cases.
 */
internal object PrimitiveUtils {
    /**
     * Helper function for generating FlexUInt bytes from an unsigned integer. Useful for test
     * cases that programmatically generate length-prefixed payloads.
     */
    fun generateFlexUIntBytes(value: Int): ByteArray {
        val asLong = value.toLong()
        val length = PrimitiveEncoder.flexUIntLength(asLong)
        val bytes = ByteArray(length)
        PrimitiveEncoder.writeFlexIntOrUIntInto(bytes, 0, asLong, length)
        return bytes
    }
}
