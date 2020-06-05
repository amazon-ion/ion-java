/*
 * Copyright 2007-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.ion.impl.lite;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import com.amazon.ion.ContainedValueException;
import com.amazon.ion.IonInt;
import com.amazon.ion.IonSequence;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import org.junit.Test;
import com.amazon.ion.system.IonSystemBuilder;

/**
 * All tests related to {@link IonSequenceLite#subList(int, int)}. Extracted to a separate test due to the amount of
 * tests
 */
public abstract class BaseIonSequenceLiteSublistTestCase {

    static final IonSystem SYSTEM = IonSystemBuilder.standard().build();
    static final int[] INTS = new int[]{0, 1, 2, 3, 4, 5, 6};

    protected abstract IonSequence newSequence();

    List<IonValue> newSubList() {
        List<IonValue> seq = newSequence();
        return seq.subList(0, seq.size());
    }

    @Test
    public void sublistSize() {
        final IonSequence sequence = newSequence();
        final List<IonValue> actual = sequence.subList(2, 5);

        assertEquals(3, actual.size());
    }


    @Test
    public void sublistIsEmpty() {
        IonSequence sequence = newSequence();

        assertTrue(sequence.subList(0, 0).isEmpty());
        assertFalse(sequence.subList(0, 1).isEmpty());
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void subListFromIndexLessThanZero() {
        newSequence().subList(-1, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void subListToIndexLessThanFromIndex() {
        newSequence().subList(2, 1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void subListToIndexExceedsSize() {
        IonSequence seq = newSequence();
        // toIndex is exclusive, hence the + 1
        seq.subList(0, seq.size() + 1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void subListOfSubListFromIndexLessThanZero() {
        newSubList().subList(-1, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void subListOfSubListToIndexLessThanFromIndex() {
        newSubList().subList(2, 1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void subListOfSubListToIndexExceedsSize() {
        List<IonValue> seq = newSubList();
        // toIndex is exclusive, hence the + 1
        seq.subList(0, seq.size() + 1);
    }

    @Test
    public void sublistGet() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);

        final IonValue element = sublist.get(0);

        assertEquals(2, ((IonInt) element).intValue());
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void sublistGetOutOfRange() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);

        sublist.get(4);
    }

    @Test
    public void sublistContains() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);

        final IonValue insideSubList = sequence.get(2);
        final IonValue outsideSublist = sequence.get(0);

        assertTrue(sublist.contains(insideSubList));
        assertFalse(sublist.contains(outsideSublist));
    }

    @Test
    public void sublistContainsAll() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);

        final List<IonValue> insideSubList = Arrays.asList(sequence.get(2), sequence.get(3));
        final List<IonValue> outsideSublist = Arrays.asList(sequence.get(0), sequence.get(1));
        final List<IonValue> mixed = Arrays.asList(sequence.get(0), sequence.get(3));

        assertTrue(sublist.containsAll(insideSubList));
        assertFalse(sublist.containsAll(outsideSublist));
        assertFalse(sublist.containsAll(mixed));
    }

    @Test
    public void sublistToArray() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);

        final Object[] array = sublist.toArray();
        assertEquals(3, array.length);
        assertEquals(2, ((IonInt) array[0]).intValue());
        assertEquals(3, ((IonInt) array[1]).intValue());
        assertEquals(4, ((IonInt) array[2]).intValue());
    }

    @Test
    public void sublistToTypedArray() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);

        final IonValue[] array = sublist.toArray(IonValue.EMPTY_ARRAY);

        assertEquals(3, array.length);
        assertEquals(2, ((IonInt) array[0]).intValue());
        assertEquals(3, ((IonInt) array[1]).intValue());
        assertEquals(4, ((IonInt) array[2]).intValue());
    }

    @Test
    public void sublistAdd() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);

        final IonInt value = SYSTEM.newInt(99);
        sublist.add(value);
        assertEquals(4, sublist.size());
        assertEquals(value, sublist.get(3));
    }

    @Test(expected = ContainedValueException.class)
    public void sublistAddSame() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);

        final IonValue value = sequence.get(0);
        sublist.add(value);
    }

    @Test
    public void sublistAddWithIndex() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);

        final IonInt value = SYSTEM.newInt(99);
        sublist.add(0, value);
        assertEquals(4, sublist.size());
        assertEquals(value, sublist.get(0));
        assertEquals(2, ((IonInt) sublist.get(1)).intValue());
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void sublistAddWithIndexOutOfRange() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);

        final IonInt value = SYSTEM.newInt(99);
        sublist.add(4, value);
    }

    @Test
    public void sublistAddAll() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);

        final List<IonInt> values = Arrays.asList(SYSTEM.newInt(100), SYSTEM.newInt(101));
        sublist.addAll(values);
        assertEquals(5, sublist.size());
        assertEquals(values.get(0), sublist.get(3));
        assertEquals(values.get(1), sublist.get(4));
    }

    @Test
    public void sublistAddAllWithIndex() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);

        final List<IonInt> values = Arrays.asList(SYSTEM.newInt(100), SYSTEM.newInt(101));
        sublist.addAll(0, values);
        assertEquals(5, sublist.size());
        assertEquals(values.get(0), sublist.get(0));
        assertEquals(values.get(1), sublist.get(1));

        assertEquals(2, ((IonInt) sublist.get(2)).intValue());
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void sublistAddAllWithIndexOutOfRange() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);

        final List<IonInt> values = Arrays.asList(SYSTEM.newInt(100), SYSTEM.newInt(101));
        sublist.addAll(3, values);
    }

    @Test
    public void sublistRetainAll() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);

        final List<IonValue> toRetain = Collections.singletonList(sublist.get(0));

        assertTrue(sublist.retainAll(toRetain));

        assertEquals(1, sublist.size());
        assertTrue(sublist.contains(toRetain.get(0)));
        assertEquals(5, sequence.size());
    }

    @Test
    public void sublistRetainAllWithDuplicates() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);

        final List<IonValue> toRetain = Arrays.asList(sublist.get(0), sublist.get(0));

        assertTrue(sublist.retainAll(toRetain));

        assertEquals(1, sublist.size());
        assertTrue(sublist.contains(toRetain.get(0)));
        assertEquals(5, sequence.size());
    }

    @Test
    public void sublistClear() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);

        sublist.clear();

        assertEquals(0, sublist.size());
        assertEquals(4, sequence.size());
    }

    @Test
    public void sublistRemoveIndex() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);

        final IonInt ionValue = (IonInt) sublist.remove(0);
        assertEquals(2, sublist.size());
        assertEquals(2, ionValue.intValue());
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void sublistRemoveIndexOutOfRange() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);

        sublist.remove(3);
    }

    @Test
    public void sublistRemoveObject() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);

        final IonInt ionValue = (IonInt) sequence.get(2);
        assertTrue(sublist.remove(ionValue));
        assertEquals(2, sublist.size());
    }

    @Test
    public void sublistRemoveAll() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);

        final List<IonValue> toRemove = Arrays.asList(sequence.get(2), sequence.get(3));
        assertTrue(sublist.removeAll(toRemove));
        assertEquals(1, sublist.size());
        for (IonValue v : toRemove) {
            assertFalse(sublist.contains(v));
        }
    }

    @Test
    public void sublistRemoveAllNotInList() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);

        final List<IonValue> toRemove = Arrays.asList(sequence.get(0), sequence.get(1));
        assertFalse(sublist.removeAll(toRemove));
        assertEquals(3, sublist.size());
    }

    @Test
    public void sublistIndexOf() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);

        assertEquals(0, sublist.indexOf(sequence.get(2)));
        assertEquals(-1, sublist.indexOf(sequence.get(0)));
        assertEquals(-1, sublist.indexOf(SYSTEM.newInt(99)));
    }

    @Test
    public void sublistLastIndexOf() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);

        assertEquals(0, sublist.lastIndexOf(sequence.get(2)));
        assertEquals(-1, sublist.lastIndexOf(sequence.get(0)));
        assertEquals(-1, sublist.lastIndexOf(SYSTEM.newInt(99)));
    }

    @Test
    public void sublistIterator() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);

        final Iterator<IonValue> iterator = sublist.iterator();
        assertTrue(iterator.hasNext());
        assertEquals(sublist.get(0), iterator.next());

        assertTrue(iterator.hasNext());
        assertEquals(sublist.get(1), iterator.next());

        assertTrue(iterator.hasNext());
        assertEquals(sublist.get(2), iterator.next());

        assertFalse(iterator.hasNext());
    }

    @Test
    public void sublistListIterator() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);

        final ListIterator<IonValue> iterator = sublist.listIterator();

        assertFalse(iterator.hasPrevious());
        assertTrue(iterator.hasNext());
        assertEquals(0, iterator.nextIndex());
        assertEquals(-1, iterator.previousIndex());
        assertEquals(sublist.get(0), iterator.next());

        assertTrue(iterator.hasNext());
        assertTrue(iterator.hasPrevious());
        assertEquals(1, iterator.nextIndex());
        assertEquals(0, iterator.previousIndex());
        assertEquals(sublist.get(1), iterator.next());

        assertTrue(iterator.hasNext());
        assertTrue(iterator.hasPrevious());
        assertEquals(2, iterator.nextIndex());
        assertEquals(1, iterator.previousIndex());
        assertEquals(sublist.get(2), iterator.next());

        assertEquals(3, iterator.nextIndex());
        assertEquals(2, iterator.previousIndex());
        assertFalse(iterator.hasNext());
        assertTrue(iterator.hasPrevious());

        assertEquals(sublist.get(2), iterator.previous());
        final IonInt newAddValue = SYSTEM.newInt(200);
        iterator.add(newAddValue);
        assertEquals(newAddValue, sublist.get(2));
        assertTrue(iterator.hasNext());
    }

    @Test
    public void sublistListIteratorWithIndex() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);

        final ListIterator<IonValue> iterator = sublist.listIterator(1);

        assertTrue(iterator.hasNext());
        assertTrue(iterator.hasPrevious());
        assertEquals(1, iterator.nextIndex());
        assertEquals(0, iterator.previousIndex());
        assertEquals(sublist.get(1), iterator.next());

        iterator.previous(); // back to initial state

        assertEquals(sublist.get(0), iterator.previous());
    }

    @Test
    public void sublistSublist() {
        IonSequence sequence = newSequence();

        final List<IonValue> sublist = sequence.subList(1, 5);   // 1,2,3,4
        final List<IonValue> subSublist = sublist.subList(1, 2); // 2

        assertEquals(sublist.get(1), subSublist.get(0));
        assertEquals(sequence.get(2), subSublist.get(0));
    }

    // Concurrent modification tests ---------------------------------------------------------------------------------

    @Test(expected = ConcurrentModificationException.class)
    public void sublistSizeConcurrentModification() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);
        sequence.add(SYSTEM.newInt(99));

        sublist.size();
    }

    @Test(expected = ConcurrentModificationException.class)
    public void sublistIsEmptyConcurrentModification() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);
        sequence.remove(0);

        sublist.isEmpty();
    }

    @Test(expected = ConcurrentModificationException.class)
    public void sublistGetConcurrentModification() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);
        sequence.clear();

        sublist.get(0);
    }

    @Test(expected = ConcurrentModificationException.class)
    public void sublistSetConcurrentModification() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);
        sequence.remove(0);

        final IonInt element = SYSTEM.newInt(99);
        sublist.set(0, element);
    }

    @Test(expected = ConcurrentModificationException.class)
    public void sublistContainsConcurrentModification() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);
        final IonInt value = SYSTEM.newInt(99);
        sequence.add(value);

        sublist.contains(value);
    }

    @Test(expected = ConcurrentModificationException.class)
    public void sublistContainsAllConcurrentModification() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);
        final IonValue value = sequence.remove(0);

        sublist.containsAll(Collections.singletonList(value));
    }

    @Test(expected = ConcurrentModificationException.class)
    public void sublistToArrayConcurrentModification() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);
        sequence.remove(0);

        sublist.toArray();
    }

    @Test(expected = ConcurrentModificationException.class)
    public void sublistToTypedArrayConcurrentModification() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);
        sequence.remove(0);

        sublist.toArray(IonValue.EMPTY_ARRAY);
    }

    @Test(expected = ConcurrentModificationException.class)
    public void sublistAddConcurrentModification() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);
        sequence.remove(0);

        sublist.add(SYSTEM.newInt(0));
    }

    @Test(expected = ConcurrentModificationException.class)
    public void sublistAddWithIndexConcurrentModification() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);
        sequence.remove(0);

        sublist.add(0, SYSTEM.newInt(0));
    }

    @Test(expected = ConcurrentModificationException.class)
    public void sublistAddAllConcurrentModification() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);
        sequence.remove(0);

        sublist.addAll(Arrays.asList(SYSTEM.newInt(100), SYSTEM.newInt(101)));
    }

    @Test(expected = ConcurrentModificationException.class)
    public void sublistAddAllWithIndexConcurrentModification() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);
        sequence.remove(0);

        sublist.addAll(0, Arrays.asList(SYSTEM.newInt(100), SYSTEM.newInt(101)));
    }

    @Test(expected = ConcurrentModificationException.class)
    public void sublistRetainAllConcurrentModification() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);
        sequence.remove(0);

        sublist.retainAll(Arrays.asList(SYSTEM.newInt(100), SYSTEM.newInt(101)));
    }

    @Test(expected = ConcurrentModificationException.class)
    public void sublistClearConcurrentModification() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);
        sequence.retainAll(Collections.emptyList());

        sublist.clear();
    }

    @Test(expected = ConcurrentModificationException.class)
    public void sublistRemoveIndexConcurrentModification() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);
        sequence.remove(0);

        sublist.remove(0);
    }

    @Test(expected = ConcurrentModificationException.class)
    public void sublistRemoveObjectConcurrentModification() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);
        sequence.remove(0);

        sublist.remove(sequence.get(2));
    }

    @Test(expected = ConcurrentModificationException.class)
    public void sublistRemoveAllConcurrentModification() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);
        sequence.remove(0);

        sublist.removeAll(Collections.singletonList(sequence.get(2)));
    }

    @Test(expected = ConcurrentModificationException.class)
    public void sublistIndexOfConcurrentModification() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);
        sequence.remove(0);

        sublist.indexOf(sequence.get(2));
    }

    @Test(expected = ConcurrentModificationException.class)
    public void sublistLastIndexOfConcurrentModification() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);
        sequence.remove(0);

        sublist.lastIndexOf(sequence.get(2));
    }

    @Test(expected = ConcurrentModificationException.class)
    public void sublistIteratorConcurrentModification() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);
        sequence.add(SYSTEM.newInt(99));

        sublist.iterator();
    }

    @Test(expected = ConcurrentModificationException.class)
    public void sublistListIteratorConcurrentModification() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);
        sequence.remove(0);

        sublist.listIterator();
    }

    @Test(expected = ConcurrentModificationException.class)
    public void sublistListIteratorWithIndexConcurrentModification() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);
        sequence.add(SYSTEM.newInt(99));

        sublist.listIterator(1);
    }

    @Test(expected = ConcurrentModificationException.class)
    public void sublistSubListConcurrentModification() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);
        sequence.remove(0);

        sublist.subList(0, 1);
    }
}
