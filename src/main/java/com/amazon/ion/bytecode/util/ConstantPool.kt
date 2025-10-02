// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.util

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings

/**
 * This is a custom collection that allows unsafe access to the backing array.
 *
 * It has a specialized [add] method that allows adding something to a [ConstantPool] and getting the CP_INDEX all in
 * one method call.
 */
internal class ConstantPool private constructor(
    private var data: Array<Any?>,
    private var numberOfValues: Int,
) : AppendableConstantPoolView {
    companion object {
        const val GROWTH_MULTIPLIER: Int = 2
    }

    constructor(initialCapacity: Int) : this(data = arrayOfNulls(initialCapacity), numberOfValues = 0)

    val size: Int
        get() = numberOfValues

    fun isEmpty(): Boolean = numberOfValues == 0

    /**
     * Empties this `ConstantPool`, allowing items to be inserted at the beginning again.
     * Note that this method does not shrink the size of the backing data store or modify the backing data store in any other way.
     */
    fun clear() {
        numberOfValues = 0
    }

    /**
     * Truncates the constant pool to length of `n`, allowing new items to be inserted starting at `n`.
     * Note that this method does not shrink the size of the backing data store or modify the backing data store in any other way.
     */
    fun truncate(n: Int) {
        require(n <= numberOfValues) { "length exceeds number of values" }
        numberOfValues = n
    }

    /**
     * Returns the `i`th int in the list.
     */
    override fun get(i: Int): Any? {
        if (i < 0 || i >= numberOfValues) {
            throw IndexOutOfBoundsException("Invalid index $i requested from IntList with $numberOfValues values.")
        }
        return data[i]
    }

    /**
     * Appends a value to this `ConstantPool`, returning the index of the newly added item.
     */
    override fun add(value: Any?): Int {
        val n = numberOfValues
        val newNumberOfValues = n + 1
        val data = ensureCapacity(newNumberOfValues)
        data[n] = value
        numberOfValues = newNumberOfValues
        return n
    }

    private fun ensureCapacity(minCapacity: Int): Array<Any?> {
        val data: Array<Any?> = this.data
        val capacity = data.size
        if (minCapacity > capacity) {
            val newCapacity = minCapacity * GROWTH_MULTIPLIER
            val newData: Array<Any?> = arrayOfNulls(newCapacity)
            System.arraycopy(data, 0, newData, 0, capacity)
            this.data = newData
            return newData
        }
        return data
    }

    /**
     * Returns an array that is the same size as this `ConstantPool`. The returned array is a defensive copy, and will
     * not be modified by any mutating methods of this `ConstantPool`.
     */
    fun toArray(): Array<Any?> {
        val thisNumberOfValues = this.numberOfValues
        val copy = arrayOfNulls<Any>(thisNumberOfValues)
        System.arraycopy(data, 0, copy, 0, thisNumberOfValues)
        return copy
    }

    /**
     * Gets the backing array without any safeguards—i.e. defensive copying—to enable faster read-only access to the
     * elements of this ConstantPool.
     *
     * Changes to this `ConstantPool` might be reflected in the returned array, and vice versa, but that behavior cannot
     * be guaranteed because this `ConstantPool` might have to grow (and therefore reallocate) the backing data array.
     * The size of the returned array reflects the `capacity` rather than the `size` of this `ConstantPool`.
     *
     * This is safe to use as long as (a) you don't modify the returned array, (b) you don't modify this `ConstantPool`
     * while you still have a reference to the returned array, and (c) you can ensure that all array access indices
     * are valid given the current `size` of the constant pool.
     */
    @SuppressFBWarnings(
        value = ["EI_EXPOSE_REP"],
        justification = "unsafeGetArray intentionally exposes internal representation as a performance optimization"
    )
    fun unsafeGetArray(): Array<Any?> {
        return data
    }

    override fun toString(): String {
        val numberOfValues = this.numberOfValues

        val builder = StringBuilder()
        builder.append("ConstantPool(data=[")
        if (numberOfValues > 0) {
            for (m in 0 until numberOfValues) {
                builder.append(data[m]).append(",")
            }
        }
        builder.append("])")
        return builder.toString()
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        other as ConstantPool

        val numberOfValues = this.numberOfValues
        val thisData = this.data
        val otherData = other.data

        if (numberOfValues != other.numberOfValues) return false
        for (m in 0 until numberOfValues) {
            if (thisData[m] != otherData[m]) return false
        }
        return true
    }

    override fun hashCode(): Int {
        val numberOfValues = this.numberOfValues
        val data = this.data

        var result = numberOfValues
        for (m in 0 until numberOfValues) {
            result = result * 31 + data[m].hashCode()
        }
        return result
    }
}
