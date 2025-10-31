// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.util

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings

/**
 * This is a custom collection that allows unsafe access to the backing array for storing bytecode instructions.
 *
 * It allows us to build up the bytecode with the convenience of appending to a list, but when it comes time to read
 * the bytecode, we can read it with the access efficiency of an array.
 *
 * It provides specialized methods for adding single values, pairs, triples, and slices of bytecode instructions
 * efficiently. The buffer automatically grows as needed using a growth multiplier strategy.
 *
 * This class looks very similar to [ConstantPool], but this class is backed by an array of primitive integers
 * rather
 *
 * Potential Performance Improvement: Consider exposing raw, `@JvmField`-annotated fields for `size` and `capacity` if it will improve the performance.
 */
internal class BytecodeBuffer private constructor(
    private var data: IntArray,
    private var numberOfValues: Int,
) {
    companion object {
        private const val GROWTH_MULTIPLIER = 2
    }

    /**
     * Creates a new empty BytecodeBuffer with the specified initial capacity.
     */
    constructor(initialCapacity: Int) : this(IntArray(initialCapacity), 0)

    @SuppressFBWarnings(
        value = ["IE_EXPOSE_REP", "IE_EXPOSE_REP2"],
        justification = "unsafeGetArray() intentionally exposes internal representation as a performance optimization"
    )
    constructor() : this(IntArray(16), 0)

    private var capacity: Int = data.size

    /**
     * Returns the current capacity of this `BytecodeBuffer`.
     * The capacity represents the maximum number of elements that can be stored without reallocating the backing array.
     */
    fun capacity() = capacity

    /**
     * Returns the number of bytecode instructions currently stored in this `BytecodeBuffer`.
     */
    fun size(): Int {
        return numberOfValues
    }

    /**
     * Returns `true` if this `BytecodeBuffer` contains no bytecode instructions, `false` otherwise.
     */
    fun isEmpty(): Boolean {
        return numberOfValues == 0
    }

    /**
     * Returns the bytecode instruction at the specified index.
     *
     * @param i the index of the bytecode instruction to retrieve
     * @return the bytecode instruction at the specified index
     * @throws IndexOutOfBoundsException if the index is out of range (index < 0 || index >= size())
     */
    fun get(i: Int): Int {
        if (i < 0 || i >= numberOfValues) {
            throw IndexOutOfBoundsException(
                "Invalid index $i requested from BytecodeBuffer with $numberOfValues values."
            )
        }
        return data[i]
    }

    /**
     * Reserves space for one bytecode instruction without setting its value.
     * This increases the size of the buffer by 1 and returns the index of the reserved position.
     *
     * @return the index of the reserved position that can be set later using [set]
     */
    fun reserve(): Int {
        return numberOfValues++
    }

    /**
     * Appends a single bytecode instruction to this `BytecodeBuffer`.
     * The buffer will automatically grow if necessary to accommodate the new instruction.
     *
     * @param value the bytecode instruction to add
     */
    fun add(value: Int) {
        val n = numberOfValues
        val newNumberOfValues = n + 1
        val data: IntArray = ensureCapacity(newNumberOfValues)
        data[n] = value
        numberOfValues = newNumberOfValues
    }

    /**
     * Appends two bytecode instructions to this `BytecodeBuffer` in a single operation.
     * This is more efficient than calling [add] twice. The buffer will automatically grow if necessary.
     *
     * @param value0 the first bytecode instruction to add
     * @param value1 the second bytecode instruction to add
     */
    fun add2(value0: Int, value1: Int) {
        val n = numberOfValues
        val newNumberOfValues = n + 2
        val data: IntArray = ensureCapacity(newNumberOfValues)
        data[n] = value0
        data[n + 1] = value1
        numberOfValues = newNumberOfValues
    }

    /**
     * Appends three bytecode instructions to this `BytecodeBuffer` in a single operation.
     * This is more efficient than calling [add] three times. The buffer will automatically grow if necessary.
     *
     * @param value0 the first bytecode instruction to add
     * @param value1 the second bytecode instruction to add
     * @param value2 the third bytecode instruction to add
     */
    fun add3(value0: Int, value1: Int, value2: Int) {
        val n = numberOfValues
        val newNumberOfValues = n + 3
        val data: IntArray = ensureCapacity(newNumberOfValues)
        data[n] = value0
        data[n + 1] = value1
        data[n + 2] = value2
        numberOfValues = newNumberOfValues
    }

    /**
     * Appends a slice of bytecode instructions from another `BytecodeBuffer` to this buffer.
     * The buffer will automatically grow if necessary to accommodate the new instructions.
     *
     * @param values the source `BytecodeBuffer` to copy from
     * @param startInclusive the starting index in the source buffer (inclusive)
     * @param length the number of bytecode instructions to copy
     */
    fun addSlice(values: BytecodeBuffer, startInclusive: Int, length: Int) {
        addSlice(values.data, startInclusive, length)
    }

    /**
     * Appends a slice of bytecode instructions from an [IntArray] to this buffer.
     * The buffer will automatically grow if necessary to accommodate the new instructions.
     *
     * @param values the source `IntArray` to copy from
     * @param startInclusive the starting index in the source array (inclusive)
     * @param length the number of bytecode instructions to copy
     */
    fun addSlice(values: IntArray, startInclusive: Int, length: Int) {
        val thisNumberOfValues = this.numberOfValues
        val newNumberOfValues = thisNumberOfValues + length
        val data = ensureCapacity(newNumberOfValues)
        System.arraycopy(values, startInclusive, data, thisNumberOfValues, length)
        this.numberOfValues = newNumberOfValues
    }

    /**
     * Empties this `BytecodeBuffer`, allowing bytecode instructions to be inserted at the beginning again.
     * Note that this method does not shrink the size of the backing data store or modify the backing data store in any other way.
     */
    fun clear() {
        numberOfValues = 0
    }

    /**
     * Truncates the bytecode buffer to the specified length, allowing new instructions to be inserted starting at that position.
     * Note that this method does not shrink the size of the backing data store or modify the backing data store in any other way.
     *
     * @param n the new length of the buffer (must not exceed the current size)
     * @throws IllegalArgumentException if n exceeds the current number of values
     */
    fun truncate(n: Int) {
        require(n <= numberOfValues) { "length exceeds number of values" }
        this.numberOfValues = n
    }

    /**
     * Sets the bytecode instruction at the specified index to the given value.
     * This is typically used in conjunction with [reserve] to set values at previously reserved positions.
     *
     * @param index the index of the bytecode instruction to set
     * @param value the new bytecode instruction value
     * @throws IndexOutOfBoundsException if the index is out of range (index < 0 || index >= size())
     */
    operator fun set(index: Int, value: Int) {
        if (index < 0 || index >= numberOfValues) {
            throw java.lang.IndexOutOfBoundsException()
        }
        data[index] = value
    }

    private fun ensureCapacity(minCapacity: Int): IntArray {
        val capacity: Int = this.capacity
        if (minCapacity > capacity) {
            return grow(minCapacity)
        }
        return data
    }

    private fun grow(minCapacity: Int): IntArray {
        // TODO: Consider making it grow to the next power of 2 instead of just growing to double the required capacity.
        val newCapacity = minCapacity * GROWTH_MULTIPLIER
        val newData = IntArray(newCapacity)
        System.arraycopy(data, 0, newData, 0, capacity)
        this.data = newData
        this.capacity = newCapacity
        return newData
    }

    /**
     * Returns an array that is the same size as this `BytecodeBuffer`. The returned array is a defensive copy, and will
     * not be modified by any mutating methods of this `BytecodeBuffer`.
     */
    fun toArray(): IntArray {
        val thisNumberOfValues = this.numberOfValues
        val copy = IntArray(thisNumberOfValues)
        System.arraycopy(data, 0, copy, 0, thisNumberOfValues)
        return copy
    }

    /**
     * Gets the backing array without any safeguards—i.e. defensive copying—to enable faster read-only access to the
     * bytecode instructions in this BytecodeBuffer.
     *
     * Changes to this `BytecodeBuffer` might be reflected in the returned array, and vice versa, but that behavior cannot
     * be guaranteed because this `BytecodeBuffer` might have to grow (and therefore reallocate) the backing data array.
     * The size of the returned array reflects the `capacity` rather than the `size` of this `BytecodeBuffer`.
     *
     * This is safe to use as long as (a) you don't modify the returned array, (b) you don't modify this `BytecodeBuffer`
     * while you still have a reference to the returned array, and (c) you don't read past the end of the bytecode (which
     * should be denoted by one of the "end" instructions).
     */
    @SuppressFBWarnings(
        value = ["EI_EXPOSE_REP"],
        justification = "unsafeGetArray intentionally exposes internal representation as a performance optimization"
    )
    fun unsafeGetArray(): IntArray {
        return data
    }

    override fun toString(): String {
        val numberOfValues = this.numberOfValues

        val builder = StringBuilder()
        builder.append("BytecodeBuffer(data=[")
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
        other as BytecodeBuffer

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
