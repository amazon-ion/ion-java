// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.util

/**
 * A view of a [ConstantPool] that allows read and append operations.
 */
interface AppendableConstantPoolView {
    /** Adds a value to the constant pool, returning the index assigned to the value. */
    fun add(value: Any?): Int
    /** Retrieves a value from the constant pool. */
    operator fun get(i: Int): Any?

    val size: Int
}
