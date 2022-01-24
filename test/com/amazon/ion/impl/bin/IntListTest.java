package com.amazon.ion.impl.bin;

import junit.framework.TestCase;

public class IntListTest extends TestCase {

    public void testSize() {
        IntList intList = new IntList();
        assertEquals(intList.size(), 0);
        intList.add(1);
        assertEquals(intList.size(), 1);
        intList.add(2);
        assertEquals(intList.size(), 2);
        intList.add(3);
        assertEquals(intList.size(), 3);
        intList.clear();
        assertEquals(intList.size(), 0);
    }

    public void testIsEmpty() {
        IntList intList = new IntList();
        assertTrue(intList.isEmpty());
        intList.add(1);
        assertFalse(intList.isEmpty());
    }

    public void testClear() {
        IntList intList = new IntList();
        assertTrue(intList.isEmpty());
        intList.add(1);
        intList.add(2);
        intList.add(3);
        assertFalse(intList.isEmpty());
        intList.clear();
        assertTrue(intList.isEmpty());
    }

    public void testAddAndGet() {
        IntList intList = new IntList();
        intList.add(1);
        intList.add(2);
        intList.add(3);
        assertEquals(intList.get(0), 1);
        assertEquals(intList.get(1), 2);
        assertEquals(intList.get(2), 3);
    }

    public void testGrow() {
        // Create the list with insufficient capacity
        IntList intList = new IntList(1);
        intList.add(1);
        intList.add(2);
        intList.add(3);
        assertEquals(intList.get(0), 1);
        assertEquals(intList.get(1), 2);
        assertEquals(intList.get(2), 3);
    }
}