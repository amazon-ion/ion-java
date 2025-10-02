// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.util

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

class BytecodeBufferTest {

    @Test
    fun `constructor creates empty buffer with correct initial capacity`() {
        val buffer = BytecodeBuffer()
        Assertions.assertEquals(0, buffer.size())
        Assertions.assertTrue(buffer.isEmpty())
        Assertions.assertTrue(buffer.capacity() > 0)
    }

    @Test
    fun `size returns correct number of elements`() {
        val buffer = BytecodeBuffer()
        Assertions.assertEquals(0, buffer.size())

        buffer.add(42)
        Assertions.assertEquals(1, buffer.size())

        buffer.add(100)
        Assertions.assertEquals(2, buffer.size())
    }

    @Test
    fun `isEmpty returns true for empty buffer and false for non-empty buffer`() {
        val buffer = BytecodeBuffer()
        Assertions.assertTrue(buffer.isEmpty())

        buffer.add(42)
        Assertions.assertFalse(buffer.isEmpty())

        buffer.clear()
        Assertions.assertTrue(buffer.isEmpty())
    }

    @Test
    fun `capacity returns current capacity`() {
        val buffer = BytecodeBuffer()
        val initialCapacity = buffer.capacity()
        Assertions.assertTrue(initialCapacity > 0)

        // Add elements to potentially trigger growth
        for (i in 0 until initialCapacity + 1) {
            buffer.add(i)
        }

        // Capacity should have grown
        Assertions.assertTrue(buffer.capacity() > initialCapacity)
    }

    @Test
    fun `add stores single value correctly`() {
        val buffer = BytecodeBuffer()
        buffer.add(42)
        Assertions.assertEquals(42, buffer.get(0))
        Assertions.assertEquals(1, buffer.size())

        buffer.add(100)
        Assertions.assertEquals(100, buffer.get(1))
        Assertions.assertEquals(2, buffer.size())
    }

    @Test
    fun `add2 stores two values correctly`() {
        val buffer = BytecodeBuffer()
        buffer.add2(10, 20)

        Assertions.assertEquals(10, buffer.get(0))
        Assertions.assertEquals(20, buffer.get(1))
        Assertions.assertEquals(2, buffer.size())

        buffer.add2(30, 40)
        Assertions.assertEquals(30, buffer.get(2))
        Assertions.assertEquals(40, buffer.get(3))
        Assertions.assertEquals(4, buffer.size())
    }

    @Test
    fun `add3 stores three values correctly`() {
        val buffer = BytecodeBuffer()
        buffer.add3(10, 20, 30)

        Assertions.assertEquals(10, buffer.get(0))
        Assertions.assertEquals(20, buffer.get(1))
        Assertions.assertEquals(30, buffer.get(2))
        Assertions.assertEquals(3, buffer.size())

        buffer.add3(40, 50, 60)
        Assertions.assertEquals(40, buffer.get(3))
        Assertions.assertEquals(50, buffer.get(4))
        Assertions.assertEquals(60, buffer.get(5))
        Assertions.assertEquals(6, buffer.size())
    }

    @Test
    fun `addSlice copies values from another buffer correctly`() {
        val sourceBuffer = BytecodeBuffer()
        sourceBuffer.add(10)
        sourceBuffer.add(20)
        sourceBuffer.add(30)
        sourceBuffer.add(40)
        sourceBuffer.add(50)

        val targetBuffer = BytecodeBuffer()
        targetBuffer.add(1)
        targetBuffer.add(2)

        // Copy slice from index 1, length 3 (values 20, 30, 40)
        targetBuffer.addSlice(sourceBuffer, 1, 3)

        Assertions.assertEquals(5, targetBuffer.size())
        Assertions.assertEquals(1, targetBuffer.get(0))
        Assertions.assertEquals(2, targetBuffer.get(1))
        Assertions.assertEquals(20, targetBuffer.get(2))
        Assertions.assertEquals(30, targetBuffer.get(3))
        Assertions.assertEquals(40, targetBuffer.get(4))
    }

    @Test
    fun `addSlice with zero length does nothing`() {
        val sourceBuffer = BytecodeBuffer()
        sourceBuffer.add(10)
        sourceBuffer.add(20)

        val targetBuffer = BytecodeBuffer()
        targetBuffer.add(1)

        targetBuffer.addSlice(sourceBuffer, 0, 0)

        Assertions.assertEquals(1, targetBuffer.size())
        Assertions.assertEquals(1, targetBuffer.get(0))
    }

    @Test
    fun `get throws IndexOutOfBoundsException for negative index`() {
        val buffer = BytecodeBuffer()
        buffer.add(42)

        val exception = assertThrows<IndexOutOfBoundsException> {
            buffer.get(-1)
        }
        Assertions.assertTrue(exception.message!!.contains("Invalid index -1"))
    }

    @Test
    fun `get throws IndexOutOfBoundsException for index greater than or equal to size`() {
        val buffer = BytecodeBuffer()
        buffer.add(42)

        val exception = assertThrows<IndexOutOfBoundsException> {
            buffer.get(1)
        }
        Assertions.assertTrue(exception.message!!.contains("Invalid index 1"))

        val exception2 = assertThrows<IndexOutOfBoundsException> {
            buffer.get(10)
        }
        Assertions.assertTrue(exception2.message!!.contains("Invalid index 10"))
    }

    @Test
    fun `get throws IndexOutOfBoundsException for empty buffer`() {
        val buffer = BytecodeBuffer()
        val exception = assertThrows<IndexOutOfBoundsException> {
            buffer.get(0)
        }
        Assertions.assertTrue(exception.message!!.contains("Invalid index 0"))
    }

    @Test
    fun `reserve increases size and returns correct index`() {
        val buffer = BytecodeBuffer()
        buffer.add(10)

        val reservedIndex = buffer.reserve()
        Assertions.assertEquals(1, reservedIndex)
        Assertions.assertEquals(2, buffer.size())

        val anotherReservedIndex = buffer.reserve()
        Assertions.assertEquals(2, anotherReservedIndex)
        Assertions.assertEquals(3, buffer.size())
    }

    @Test
    fun `set works correctly with reserved positions`() {
        val buffer = BytecodeBuffer()
        buffer.add(10)

        val reservedIndex = buffer.reserve()
        buffer.set(reservedIndex, 42)

        Assertions.assertEquals(10, buffer.get(0))
        Assertions.assertEquals(42, buffer.get(1))
    }

    @Test
    fun `set throws IndexOutOfBoundsException for invalid indices`() {
        val buffer = BytecodeBuffer()
        buffer.add(10)

        assertThrows<IndexOutOfBoundsException> {
            buffer.set(-1, 42)
        }

        assertThrows<IndexOutOfBoundsException> {
            buffer.set(1, 42)
        }

        assertThrows<IndexOutOfBoundsException> {
            buffer.set(10, 42)
        }
    }

    @Test
    fun `set updates existing values correctly`() {
        val buffer = BytecodeBuffer()
        buffer.add(10)
        buffer.add(20)
        buffer.add(30)

        buffer.set(1, 99)

        Assertions.assertEquals(10, buffer.get(0))
        Assertions.assertEquals(99, buffer.get(1))
        Assertions.assertEquals(30, buffer.get(2))
        Assertions.assertEquals(3, buffer.size())
    }

    @Test
    fun `clear empties the buffer`() {
        val buffer = BytecodeBuffer()
        buffer.add(10)
        buffer.add(20)
        buffer.add(30)
        Assertions.assertEquals(3, buffer.size())
        Assertions.assertFalse(buffer.isEmpty())

        buffer.clear()
        Assertions.assertEquals(0, buffer.size())
        Assertions.assertTrue(buffer.isEmpty())

        // Should be able to add new items starting from index 0
        buffer.add(42)
        Assertions.assertEquals(42, buffer.get(0))
        Assertions.assertEquals(1, buffer.size())
    }

    @Test
    fun `truncate reduces size to specified length`() {
        val buffer = BytecodeBuffer()
        buffer.add(10)
        buffer.add(20)
        buffer.add(30)
        buffer.add(40)
        Assertions.assertEquals(4, buffer.size())

        buffer.truncate(2)
        Assertions.assertEquals(2, buffer.size())
        Assertions.assertEquals(10, buffer.get(0))
        Assertions.assertEquals(20, buffer.get(1))

        // Should throw exception when trying to access truncated items
        assertThrows<IndexOutOfBoundsException> {
            buffer.get(2)
        }

        // Should be able to add new items starting from truncated size
        buffer.add(99)
        Assertions.assertEquals(99, buffer.get(2))
        Assertions.assertEquals(3, buffer.size())
    }

    @Test
    fun `truncate to zero makes buffer empty`() {
        val buffer = BytecodeBuffer()
        buffer.add(10)
        buffer.add(20)

        buffer.truncate(0)
        Assertions.assertEquals(0, buffer.size())
        Assertions.assertTrue(buffer.isEmpty())
    }

    @Test
    fun `truncate with length greater than size throws IllegalArgumentException`() {
        val buffer = BytecodeBuffer()
        buffer.add(10)
        buffer.add(20)

        val exception = assertThrows<IllegalArgumentException> {
            buffer.truncate(5)
        }
        Assertions.assertTrue(exception.message!!.contains("length exceeds number of values"))

        // Buffer should remain unchanged after exception
        Assertions.assertEquals(2, buffer.size())
        Assertions.assertEquals(10, buffer.get(0))
        Assertions.assertEquals(20, buffer.get(1))
    }

    @Test
    fun `truncate allows truncating to current size`() {
        val buffer = BytecodeBuffer()
        buffer.add(10)
        buffer.add(20)

        // Should not change anything
        buffer.truncate(2)
        Assertions.assertEquals(2, buffer.size())
        Assertions.assertEquals(10, buffer.get(0))
        Assertions.assertEquals(20, buffer.get(1))
    }

    @Test
    fun `buffer grows automatically when capacity is exceeded`() {
        val buffer = BytecodeBuffer()
        val initialCapacity = buffer.capacity()

        // Add items beyond initial capacity
        for (i in 0 until initialCapacity + 5) {
            buffer.add(i)
        }

        Assertions.assertEquals(initialCapacity + 5, buffer.size())
        Assertions.assertTrue(buffer.capacity() > initialCapacity)

        // Verify all items are accessible
        for (i in 0 until initialCapacity + 5) {
            Assertions.assertEquals(i, buffer.get(i))
        }
    }

    @Test
    fun `toArray returns defensive copy with correct size`() {
        val buffer = BytecodeBuffer()
        buffer.add(10)
        buffer.add(20)
        buffer.add(30)

        val array = buffer.toArray()
        Assertions.assertEquals(3, array.size)
        Assertions.assertEquals(10, array[0])
        Assertions.assertEquals(20, array[1])
        Assertions.assertEquals(30, array[2])

        // Verify it's a defensive copy by modifying the returned array
        array[0] = 999
        Assertions.assertEquals(10, buffer.get(0)) // Original should be unchanged
    }

    @Test
    fun `toArray returns empty array for empty buffer`() {
        val buffer = BytecodeBuffer()
        val array = buffer.toArray()
        Assertions.assertEquals(0, array.size)
    }

    @Test
    fun `unsafeGetArray returns backing array`() {
        val buffer = BytecodeBuffer()
        buffer.add(10)
        buffer.add(20)

        val array = buffer.unsafeGetArray()

        // Array should contain the items
        Assertions.assertEquals(10, array[0])
        Assertions.assertEquals(20, array[1])

        // Array size should be capacity, not just the number of items
        Assertions.assertTrue(array.size >= buffer.size())
        Assertions.assertEquals(buffer.capacity(), array.size)
    }

    @Test
    fun `toString returns correct string representation`() {
        val buffer = BytecodeBuffer()
        val emptyString = buffer.toString()
        Assertions.assertEquals("BytecodeBuffer(data=[])", emptyString)

        buffer.add(10)
        buffer.add(20)
        buffer.add(30)

        val string = buffer.toString()
        Assertions.assertTrue(string.startsWith("BytecodeBuffer(data=["))
        Assertions.assertTrue(string.contains("10,"))
        Assertions.assertTrue(string.contains("20,"))
        Assertions.assertTrue(string.contains("30,"))
        Assertions.assertTrue(string.endsWith("])"))
    }

    @Test
    fun `equals returns true for identical buffers`() {
        val buffer1 = BytecodeBuffer()
        val buffer2 = BytecodeBuffer()

        // Empty buffers should be equal
        Assertions.assertEquals(buffer1, buffer2)

        // Add same items to both
        buffer1.add(10)
        buffer1.add(20)
        buffer1.add(30)

        buffer2.add(10)
        buffer2.add(20)
        buffer2.add(30)

        Assertions.assertEquals(buffer1, buffer2)
    }

    @Test
    fun `equals returns false for buffers with different content`() {
        val buffer1 = BytecodeBuffer()
        val buffer2 = BytecodeBuffer()

        buffer1.add(10)
        buffer2.add(20)

        Assertions.assertNotEquals(buffer1, buffer2)
    }

    @Test
    fun `equals returns false for buffers with different sizes`() {
        val buffer1 = BytecodeBuffer()
        val buffer2 = BytecodeBuffer()

        buffer1.add(10)
        buffer2.add(10)
        buffer2.add(20)

        Assertions.assertNotEquals(buffer1, buffer2)
    }

    @Test
    fun `equals returns true for same instance`() {
        val buffer = BytecodeBuffer()
        Assertions.assertEquals(buffer, buffer)
    }

    @Test
    fun `equals returns false for null and different types`() {
        val buffer = BytecodeBuffer()
        Assertions.assertNotEquals(buffer, null)
        Assertions.assertNotEquals(buffer, "not a bytecode buffer")
        Assertions.assertNotEquals(buffer, listOf<Int>())
    }

    @Test
    fun `hashCode is consistent with equals`() {
        val buffer1 = BytecodeBuffer()
        val buffer2 = BytecodeBuffer()

        // Empty buffers
        Assertions.assertEquals(buffer1.hashCode(), buffer2.hashCode())

        // Add same content
        buffer1.add(10)
        buffer1.add(20)

        buffer2.add(10)
        buffer2.add(20)

        Assertions.assertEquals(buffer1.hashCode(), buffer2.hashCode())
    }

    @Test
    fun `hashCode differs for different content`() {
        val buffer1 = BytecodeBuffer()
        val buffer2 = BytecodeBuffer()

        buffer1.add(10)
        buffer2.add(20)

        Assertions.assertNotEquals(buffer1.hashCode(), buffer2.hashCode())
    }

    @Test
    fun `growth multiplier is applied correctly`() {
        val buffer = BytecodeBuffer()
        val initialCapacity = buffer.capacity()

        // Add items to force growth
        for (i in 0 until initialCapacity + 1) {
            buffer.add(i)
        }

        // Verify all items are accessible
        for (i in 0 until initialCapacity + 1) {
            Assertions.assertEquals(i, buffer.get(i))
        }

        // The backing array should have grown by GROWTH_MULTIPLIER (which is 2)
        val newCapacity = buffer.capacity()
        Assertions.assertTrue(newCapacity >= (initialCapacity + 1) * 2)
    }

    @Test
    fun `large number of items can be stored and retrieved`() {
        val buffer = BytecodeBuffer()
        val itemCount = 1000

        // Add many items
        for (i in 0 until itemCount) {
            buffer.add(i)
        }

        Assertions.assertEquals(itemCount, buffer.size())

        // Verify all items can be retrieved
        for (i in 0 until itemCount) {
            Assertions.assertEquals(i, buffer.get(i))
        }
    }

    @Test
    fun `operations work correctly after clear`() {
        val buffer = BytecodeBuffer()
        // Add some items
        buffer.add(10)
        buffer.add(20)
        Assertions.assertEquals(2, buffer.size())

        // Clear and verify
        buffer.clear()
        Assertions.assertEquals(0, buffer.size())
        Assertions.assertTrue(buffer.isEmpty())

        // Add new items after clear
        buffer.add(30)
        buffer.add(40)

        Assertions.assertEquals(30, buffer.get(0))
        Assertions.assertEquals(40, buffer.get(1))
        Assertions.assertEquals(2, buffer.size())
    }

    @Test
    fun `operations work correctly after truncate`() {
        val buffer = BytecodeBuffer()
        // Add some items
        buffer.add(10)
        buffer.add(20)
        buffer.add(30)
        buffer.add(40)
        Assertions.assertEquals(4, buffer.size())

        // Truncate to 2
        buffer.truncate(2)
        Assertions.assertEquals(2, buffer.size())

        // Add new items after truncate
        buffer.add(50)
        buffer.add(60)

        Assertions.assertEquals(10, buffer.get(0))
        Assertions.assertEquals(20, buffer.get(1))
        Assertions.assertEquals(50, buffer.get(2))
        Assertions.assertEquals(60, buffer.get(3))
        Assertions.assertEquals(4, buffer.size())
    }

    @Test
    fun `mixed operations work correctly together`() {
        val buffer = BytecodeBuffer()

        // Test add, add2, add3 together
        buffer.add(1)
        buffer.add2(2, 3)
        buffer.add3(4, 5, 6)

        Assertions.assertEquals(6, buffer.size())
        for (i in 0 until 6) {
            Assertions.assertEquals(i + 1, buffer.get(i))
        }

        // Test reserve and set
        val reservedIndex = buffer.reserve()
        buffer.set(reservedIndex, 99)

        Assertions.assertEquals(7, buffer.size())
        Assertions.assertEquals(99, buffer.get(6))

        // Test addSlice
        val sourceBuffer = BytecodeBuffer()
        sourceBuffer.add(100)
        sourceBuffer.add(200)

        buffer.addSlice(sourceBuffer, 0, 2)

        Assertions.assertEquals(9, buffer.size())
        Assertions.assertEquals(100, buffer.get(7))
        Assertions.assertEquals(200, buffer.get(8))
    }

    @ParameterizedTest
    @ValueSource(ints = [0, 1, 2, 5, 10])
    fun `truncate works correctly with valid lengths`(length: Int) {
        val buffer = BytecodeBuffer()

        // Add 10 items
        for (i in 0 until 10) {
            buffer.add(i)
        }

        buffer.truncate(length)
        Assertions.assertEquals(length, buffer.size())

        // Verify remaining items are correct
        for (i in 0 until length) {
            Assertions.assertEquals(i, buffer.get(i))
        }
    }

    @Test
    fun `truncate throws IllegalArgumentException for length exceeding size`() {
        val buffer = BytecodeBuffer()

        // Add 5 items
        for (i in 0 until 5) {
            buffer.add(i)
        }

        // Try to truncate to length greater than size
        val exception = assertThrows<IllegalArgumentException> {
            buffer.truncate(10)
        }
        Assertions.assertTrue(exception.message!!.contains("length exceeds number of values"))

        // Buffer should remain unchanged
        Assertions.assertEquals(5, buffer.size())
        for (i in 0 until 5) {
            Assertions.assertEquals(i, buffer.get(i))
        }
    }

    @Test
    fun `buffer handles negative values correctly`() {
        val buffer = BytecodeBuffer()
        buffer.add(-1)
        buffer.add(-100)
        buffer.add(0)
        buffer.add(100)

        Assertions.assertEquals(-1, buffer.get(0))
        Assertions.assertEquals(-100, buffer.get(1))
        Assertions.assertEquals(0, buffer.get(2))
        Assertions.assertEquals(100, buffer.get(3))
    }

    @Test
    fun `buffer handles maximum and minimum integer values`() {
        val buffer = BytecodeBuffer()
        buffer.add(Int.MAX_VALUE)
        buffer.add(Int.MIN_VALUE)

        Assertions.assertEquals(Int.MAX_VALUE, buffer.get(0))
        Assertions.assertEquals(Int.MIN_VALUE, buffer.get(1))
    }

    @Test
    fun `addSlice handles edge cases correctly`() {
        val sourceBuffer = BytecodeBuffer()
        sourceBuffer.add(10)
        sourceBuffer.add(20)
        sourceBuffer.add(30)

        val targetBuffer = BytecodeBuffer()

        // Add slice from the end
        targetBuffer.addSlice(sourceBuffer, 2, 1)
        Assertions.assertEquals(1, targetBuffer.size())
        Assertions.assertEquals(30, targetBuffer.get(0))

        // Add slice from the beginning
        targetBuffer.addSlice(sourceBuffer, 0, 2)
        Assertions.assertEquals(3, targetBuffer.size())
        Assertions.assertEquals(30, targetBuffer.get(0))
        Assertions.assertEquals(10, targetBuffer.get(1))
        Assertions.assertEquals(20, targetBuffer.get(2))
    }

    @Test
    fun `capacity increases correctly with multiple growth cycles`() {
        val buffer = BytecodeBuffer()
        val initialCapacity = buffer.capacity()

        // Force multiple growth cycles
        val itemsToAdd = initialCapacity * 4
        for (i in 0 until itemsToAdd) {
            buffer.add(i)
        }

        Assertions.assertEquals(itemsToAdd, buffer.size())
        Assertions.assertTrue(buffer.capacity() >= itemsToAdd)

        // Verify all items are still accessible
        for (i in 0 until itemsToAdd) {
            Assertions.assertEquals(i, buffer.get(i))
        }
    }
}
