// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode

import com.amazon.ion.bytecode.ir.Instructions

/**
 * Helpers for generating bytecode in test cases.
 */
internal object BytecodeUtils {
    /**
     * Helper function for generating I_INT_I64 bytecode for a given [Long].
     */
    fun I64(value: Long): IntArray {
        return intArrayOf(
            Instructions.I_INT_I64,
            value.shr(Int.SIZE_BITS).toInt(),
            value.toInt()
        )
    }
}
