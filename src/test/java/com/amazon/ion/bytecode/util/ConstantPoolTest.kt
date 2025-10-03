// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.util

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

class ConstantPoolTest {

    @Test
    fun `constructor creates empty constant pool with correct initial capacity`() {
        val pool = ConstantPool(5)
        assertEquals(0, pool.size)
        assertTrue(pool.isEmpty())
    }

    @Test
    fun `size returns correct number of elements`() {
        val constantPool = ConstantPool(10)
        assertEquals(0, constantPool.size)

        constantPool.add("test")
        assertEquals(1, constantPool.size)

        constantPool.add(42)
        assertEquals(2, constantPool.size)
    }

    @Test
    fun `isEmpty returns true for empty pool and false for non-empty pool`() {
        val constantPool = ConstantPool(10)
        assertTrue(constantPool.isEmpty())

        constantPool.add("test")
        assertFalse(constantPool.isEmpty())

        constantPool.clear()
        assertTrue(constantPool.isEmpty())
    }

    @Test
    fun `add returns correct index and stores value`() {
        val constantPool = ConstantPool(10)
        val index1 = constantPool.add("first")
        assertEquals(0, index1)
        assertEquals("first", constantPool.get(0))

        val index2 = constantPool.add("second")
        assertEquals(1, index2)
        assertEquals("second", constantPool.get(1))

        val index3 = constantPool.add(null)
        assertEquals(2, index3)
        assertNull(constantPool.get(2))
    }

    @Test
    fun `add can store various types of objects`() {
        val constantPool = ConstantPool(10)
        val stringIndex = constantPool.add("string")
        val intIndex = constantPool.add(42)
        val listIndex = constantPool.add(listOf(1, 2, 3))
        val nullIndex = constantPool.add(null)

        assertEquals("string", constantPool.get(stringIndex))
        assertEquals(42, constantPool.get(intIndex))
        assertEquals(listOf(1, 2, 3), constantPool.get(listIndex))
        assertNull(constantPool.get(nullIndex))
    }

    @Test
    fun `get throws IndexOutOfBoundsException for negative index`() {
        val constantPool = ConstantPool(10)
        constantPool.add("test")

        val exception = assertThrows<IndexOutOfBoundsException> {
            constantPool.get(-1)
        }
        assertTrue(exception.message!!.contains("Invalid index -1"))
    }

    @Test
    fun `get throws IndexOutOfBoundsException for index greater than or equal to size`() {
        val constantPool = ConstantPool(10)
        constantPool.add("test")

        val exception = assertThrows<IndexOutOfBoundsException> {
            constantPool.get(1)
        }
        assertTrue(exception.message!!.contains("Invalid index 1"))

        val exception2 = assertThrows<IndexOutOfBoundsException> {
            constantPool.get(10)
        }
        assertTrue(exception2.message!!.contains("Invalid index 10"))
    }

    @Test
    fun `get throws IndexOutOfBoundsException for empty pool`() {
        val constantPool = ConstantPool(10)
        val exception = assertThrows<IndexOutOfBoundsException> {
            constantPool.get(0)
        }
        assertTrue(exception.message!!.contains("Invalid index 0"))
    }

    @Test
    fun `clear empties the constant pool`() {
        val constantPool = ConstantPool(10)
        constantPool.add("test1")
        constantPool.add("test2")
        constantPool.add("test3")
        assertEquals(3, constantPool.size)
        assertFalse(constantPool.isEmpty())

        constantPool.clear()
        assertEquals(0, constantPool.size)
        assertTrue(constantPool.isEmpty())

        // Should be able to add new items starting from index 0
        val index = constantPool.add("new item")
        assertEquals(0, index)
        assertEquals("new item", constantPool.get(0))
    }

    @Test
    fun `truncate reduces size to specified length`() {
        val constantPool = ConstantPool(10)
        constantPool.add("item0")
        constantPool.add("item1")
        constantPool.add("item2")
        constantPool.add("item3")
        assertEquals(4, constantPool.size)

        constantPool.truncate(2)
        assertEquals(2, constantPool.size)
        assertEquals("item0", constantPool.get(0))
        assertEquals("item1", constantPool.get(1))

        // Should throw exception when trying to access truncated items
        assertThrows<IndexOutOfBoundsException> {
            constantPool.get(2)
        }

        // Should be able to add new items starting from truncated size
        val newIndex = constantPool.add("new item")
        assertEquals(2, newIndex)
        assertEquals("new item", constantPool.get(2))
    }

    @Test
    fun `truncate to zero makes pool empty`() {
        val constantPool = ConstantPool(10)
        constantPool.add("item1")
        constantPool.add("item2")

        constantPool.truncate(0)
        assertEquals(0, constantPool.size)
        assertTrue(constantPool.isEmpty())
    }

    @Test
    fun `truncate throws exception when length exceeds number of values`() {
        val constantPool = ConstantPool(10)
        constantPool.add("item1")
        constantPool.add("item2")

        val exception = assertThrows<IllegalArgumentException> {
            constantPool.truncate(3)
        }
        assertEquals("length exceeds number of values", exception.message)
    }

    @Test
    fun `truncate allows truncating to current size`() {
        val constantPool = ConstantPool(10)
        constantPool.add("item1")
        constantPool.add("item2")

        // Should not throw exception
        constantPool.truncate(2)
        assertEquals(2, constantPool.size)
    }

    @Test
    fun `pool grows automatically when capacity is exceeded`() {
        val smallPool = ConstantPool(2)

        // Add items beyond initial capacity
        for (i in 0..5) {
            val index = smallPool.add("item$i")
            assertEquals(i, index)
        }

        assertEquals(6, smallPool.size)

        // Verify all items are accessible
        for (i in 0..5) {
            assertEquals("item$i", smallPool.get(i))
        }
    }

    @ParameterizedTest
    @ValueSource(ints = [1, 5, 10, 100, 1000])
    fun `pool handles various initial capacities`(initialCapacity: Int) {
        val pool = ConstantPool(initialCapacity)
        assertTrue(pool.isEmpty())
        assertEquals(0, pool.size)

        // Add one item to verify it works
        val index = pool.add("test")
        assertEquals(0, index)
        assertEquals("test", pool.get(0))
    }

    @Test
    fun `toArray returns defensive copy with correct size`() {
        val constantPool = ConstantPool(10)
        constantPool.add("item1")
        constantPool.add("item2")
        constantPool.add(null)

        val array = constantPool.toArray()
        assertEquals(3, array.size)
        assertEquals("item1", array[0])
        assertEquals("item2", array[1])
        assertNull(array[2])

        // Verify it's a defensive copy by modifying the returned array
        array[0] = "modified"
        assertEquals("item1", constantPool.get(0)) // Original should be unchanged
    }

    @Test
    fun `toArray returns empty array for empty pool`() {
        val constantPool = ConstantPool(10)
        val array = constantPool.toArray()
        assertEquals(0, array.size)
    }

    @Test
    fun `unsafeGetArray returns backing array`() {
        val constantPool = ConstantPool(10)
        constantPool.add("item1")
        constantPool.add("item2")

        val array = constantPool.unsafeGetArray()

        // Array should contain the items
        assertEquals("item1", array[0])
        assertEquals("item2", array[1])

        // If we make changes it should be reflected in the BytecodeBuffer
        // DON'T ACTUALLY DO THIS OUTSIDE OF TEST CODE!
        array[0] = "item3"
        assertEquals("item3", constantPool.get(0))
    }

    @Test
    fun `toString returns correct string representation`() {
        val constantPool = ConstantPool(10)
        val emptyString = constantPool.toString()
        assertEquals("ConstantPool(data=[])", emptyString)

        constantPool.add("test")
        constantPool.add(42)

        val string = constantPool.toString()
        assertEquals("ConstantPool(data=[test,42,])", string)
    }

    @Test
    fun `equals returns true for identical pools`() {
        val pool1 = ConstantPool(5)
        val pool2 = ConstantPool(10) // Different capacity

        // Empty pools should be equal
        assertEquals(pool1, pool2)

        // Add same items to both
        pool1.add("test")
        pool1.add(42)
        pool1.add(null)

        pool2.add("test")
        pool2.add(42)
        pool2.add(null)

        assertEquals(pool1, pool2)
    }

    @Test
    fun `equals returns false for pools with different content`() {
        val pool1 = ConstantPool(5)
        val pool2 = ConstantPool(5)

        pool1.add("test1")
        pool2.add("test2")

        assertNotEquals(pool1, pool2)
    }

    @Test
    fun `equals returns false for pools with different sizes`() {
        val pool1 = ConstantPool(5)
        val pool2 = ConstantPool(5)

        pool1.add("test")
        pool2.add("test")
        pool2.add("extra")

        assertNotEquals(pool1, pool2)
    }

    @Test
    fun `equals returns true for same instance`() {
        val constantPool = ConstantPool(10)
        assertEquals(constantPool, constantPool)
    }

    @Test
    fun `equals returns false for null and different types`() {
        val constantPool = ConstantPool(10)
        assertNotEquals(constantPool, null)
        assertNotEquals(constantPool, "not a constant pool")
        assertNotEquals(constantPool, listOf<Any>())
    }

    @Test
    fun `hashCode is consistent with equals`() {
        val pool1 = ConstantPool(5)
        val pool2 = ConstantPool(10)

        // Empty pools
        assertEquals(pool1.hashCode(), pool2.hashCode())

        // Add same content
        pool1.add("test")
        pool1.add(42)

        pool2.add("test")
        pool2.add(42)

        assertEquals(pool1.hashCode(), pool2.hashCode())
    }

    @Test
    fun `hashCode differs for different content`() {
        val pool1 = ConstantPool(5)
        val pool2 = ConstantPool(5)

        pool1.add("test1")
        pool2.add("test2")

        assertNotEquals(pool1.hashCode(), pool2.hashCode())
    }

    @Test
    fun `constant pool implements AppendableConstantPoolView interface correctly`() {
        val constantPool = ConstantPool(10)
        val view: AppendableConstantPoolView = constantPool

        val index = view.add("test")
        assertEquals(0, index)
        assertEquals("test", view.get(0))
    }

    @Test
    fun `growth multiplier is applied correctly`() {
        // Create a small pool to test growth
        val smallPool = ConstantPool(1)

        // Add items to force growth
        smallPool.add("item1")
        smallPool.add("item2") // This should trigger growth

        // Verify both items are accessible
        assertEquals("item1", smallPool.get(0))
        assertEquals("item2", smallPool.get(1))
        assertEquals(2, smallPool.size)

        // The backing array should have grown by GROWTH_MULTIPLIER
        val backingArray = smallPool.unsafeGetArray()
        assertTrue(backingArray.size >= 2) // Should be at least 2 (1 * GROWTH_MULTIPLIER)
    }

    @Test
    fun `large number of items can be stored and retrieved`() {
        val largePool = ConstantPool(10)
        val itemCount = 1000

        // Add many items
        for (i in 0 until itemCount) {
            val index = largePool.add("item$i")
            assertEquals(i, index)
        }

        assertEquals(itemCount, largePool.size)

        // Verify all items can be retrieved
        for (i in 0 until itemCount) {
            assertEquals("item$i", largePool.get(i))
        }
    }

    @Test
    fun `pool handles null values correctly`() {
        val constantPool = ConstantPool(10)
        val index1 = constantPool.add(null)
        val index2 = constantPool.add("not null")
        val index3 = constantPool.add(null)

        assertEquals(0, index1)
        assertEquals(1, index2)
        assertEquals(2, index3)

        assertNull(constantPool.get(0))
        assertEquals("not null", constantPool.get(1))
        assertNull(constantPool.get(2))

        assertEquals(3, constantPool.size)
    }

    @Test
    fun `operations work correctly after clear`() {
        val constantPool = ConstantPool(10)
        // Add some items
        constantPool.add("item1")
        constantPool.add("item2")
        assertEquals(2, constantPool.size)

        // Clear and verify
        constantPool.clear()
        assertEquals(0, constantPool.size)
        assertTrue(constantPool.isEmpty())

        // Add new items after clear
        val index1 = constantPool.add("new1")
        val index2 = constantPool.add("new2")

        assertEquals(0, index1)
        assertEquals(1, index2)
        assertEquals("new1", constantPool.get(0))
        assertEquals("new2", constantPool.get(1))
        assertEquals(2, constantPool.size)
    }

    @Test
    fun `operations work correctly after truncate`() {
        val constantPool = ConstantPool(10)
        // Add some items
        constantPool.add("item1")
        constantPool.add("item2")
        constantPool.add("item3")
        constantPool.add("item4")
        assertEquals(4, constantPool.size)

        // Truncate to 2
        constantPool.truncate(2)
        assertEquals(2, constantPool.size)

        // Add new items after truncate
        val index1 = constantPool.add("new1")
        val index2 = constantPool.add("new2")

        assertEquals(2, index1)
        assertEquals(3, index2)
        assertEquals("item1", constantPool.get(0))
        assertEquals("item2", constantPool.get(1))
        assertEquals("new1", constantPool.get(2))
        assertEquals("new2", constantPool.get(3))
        assertEquals(4, constantPool.size)
    }
}
