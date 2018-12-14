package software.amazon.ion.impl.lite;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import org.junit.Test;
import software.amazon.ion.ContainedValueException;
import software.amazon.ion.IonInt;
import software.amazon.ion.IonList;
import software.amazon.ion.IonSequence;
import software.amazon.ion.IonSystem;
import software.amazon.ion.IonValue;
import software.amazon.ion.ReadOnlyValueException;
import software.amazon.ion.system.IonSystemBuilder;

public abstract class BaseIonSequenceLiteTestCase {

    protected static final IonSystem SYSTEM = IonSystemBuilder.standard().build();

    protected abstract IonSequence newEmptySequence();

    @Test
    public void retainAll() {
        final IonSequence sequence = newEmptySequence();
        final ArrayList<IonValue> toRetain = new ArrayList<IonValue>();

        final IonValue retainedValue = SYSTEM.newInt(1);
        sequence.add(retainedValue);
        toRetain.add(retainedValue);

        final IonValue toRemoveValue = SYSTEM.newInt(2);
        sequence.add(toRemoveValue);

        assertTrue(sequence.retainAll(toRetain));

        assertEquals(1, sequence.size());
        assertTrue(sequence.contains(retainedValue));
        assertFalse(sequence.contains(toRemoveValue));
    }

    @Test
    public void retainAllUsesReferenceEquality() {
        final IonSequence sequence = newEmptySequence();
        final ArrayList<IonValue> toRetain = new ArrayList<IonValue>();

        final IonValue value = SYSTEM.newInt(1);
        sequence.add(value);

        final IonValue equalValue = SYSTEM.newInt(1);
        toRetain.add(equalValue);

        assertEquals(equalValue, value);
        assertNotSame(equalValue, value);

        assertTrue(sequence.retainAll(toRetain));
        assertEquals(0, sequence.size());
    }

    @Test(expected = ReadOnlyValueException.class)
    public void retainAllReadOnly() {
        final IonSequence sequence = newEmptySequence();
        sequence.makeReadOnly();

        sequence.retainAll(Collections.emptyList());
    }









    @Test
    public void sublistSize() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> actual = parent.subList(2, 5);

        assertEquals(3, actual.size());
    }

    @Test(expected = ConcurrentModificationException.class)
    public void sublistSizeConcurrentModification() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);
        parent.remove(0);

        sublist.size();
    }

    @Test
    public void sublistIsEmpty() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});

        assertTrue(parent.subList(0, 0).isEmpty());
        assertFalse(parent.subList(0, 1).isEmpty());
    }

    @Test(expected = ConcurrentModificationException.class)
    public void sublistIsEmptyConcurrentModification() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);
        parent.remove(0);

        sublist.isEmpty();
    }

    @Test
    public void sublistGet() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);

        final IonValue element = sublist.get(0);

        assertEquals(2, ((IonInt) element).intValue());
    }

    @Test(expected = ConcurrentModificationException.class)
    public void sublistGetConcurrentModification() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);
        parent.remove(0);

        sublist.get(0);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void sublistGetOutOfRange() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);

        sublist.get(5);
    }

    @Test
    public void sublistSet() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);

        final IonInt element = SYSTEM.newInt(99);
        final IonValue previous = sublist.set(0, element);

        assertEquals(2, ((IonInt) previous).intValue());
        assertEquals(element, sublist.get(0));
        assertEquals(element, parent.get(2));
    }

    @Test
    public void sublistSetOnParent() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);

        final IonInt element = SYSTEM.newInt(99);
        final IonValue previous = parent.set(2, element);

        assertEquals(2, ((IonInt) previous).intValue());
        assertEquals(element, sublist.get(0));
        assertEquals(element, parent.get(2));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void sublistSetOutOfRange() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);

        final IonInt element = SYSTEM.newInt(99);
        sublist.set(4, element);
    }

    @Test(expected = ConcurrentModificationException.class)
    public void sublistSetConcurrentModification() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);
        parent.remove(0);

        final IonInt element = SYSTEM.newInt(99);
        sublist.set(0, element);
    }

    @Test
    public void sublistContains() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);

        final IonValue insideSubList = parent.get(2);
        final IonValue outsideSublist = parent.get(0);

        assertTrue(sublist.contains(insideSubList));
        assertFalse(sublist.contains(outsideSublist));
    }

    @Test
    public void sublistContainsAll() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);

        final List<IonValue> insideSubList = Arrays.asList(parent.get(2), parent.get(3));
        final List<IonValue> outsideSublist = Arrays.asList(parent.get(0), parent.get(1));
        final List<IonValue> mixed = Arrays.asList(parent.get(0), parent.get(3));

        assertTrue(sublist.containsAll(insideSubList));
        assertFalse(sublist.containsAll(outsideSublist));
        assertFalse(sublist.containsAll(mixed));
    }

    @Test(expected = ConcurrentModificationException.class)
    public void sublistContainsConcurrentModification() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);
        final IonValue value = parent.remove(0);

        sublist.contains(Collections.singletonList(value));
    }

    @Test
    public void sublistToArray() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);

        final Object[] array = sublist.toArray();
        assertEquals(3, array.length);
        assertEquals(2, ((IonInt) array[0]).intValue());
        assertEquals(3, ((IonInt) array[1]).intValue());
        assertEquals(4, ((IonInt) array[2]).intValue());
    }

    @Test(expected = ConcurrentModificationException.class)
    public void sublistToArrayConcurrentModification() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);
        parent.remove(0);

        sublist.toArray();
    }

    @Test
    public void sublistToTypedArray() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);

        final IonValue[] array = sublist.toArray(IonValue.EMPTY_ARRAY);

        assertEquals(3, array.length);
        assertEquals(2, ((IonInt) array[0]).intValue());
        assertEquals(3, ((IonInt) array[1]).intValue());
        assertEquals(4, ((IonInt) array[2]).intValue());
    }

    @Test(expected = ConcurrentModificationException.class)
    public void sublistToTypedArrayConcurrentModification() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);
        parent.remove(0);

        sublist.toArray(IonValue.EMPTY_ARRAY);
    }

    @Test
    public void sublistAdd() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);

        final IonInt value = SYSTEM.newInt(99);
        sublist.add(value);
        assertEquals(4, sublist.size());
        assertEquals(value, sublist.get(3));
    }

    @Test(expected = ContainedValueException.class)
    public void sublistAddSame() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);

        final IonValue value = parent.get(0);
        sublist.add(value);
    }

    @Test(expected = ConcurrentModificationException.class)
    public void sublistAddConcurrentModification() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);
        parent.remove(0);

        sublist.add(SYSTEM.newInt(0));
    }

    @Test
    public void sublistAddWithIndex() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);

        final IonInt value = SYSTEM.newInt(99);
        sublist.add(0, value);
        assertEquals(4, sublist.size());
        assertEquals(value, sublist.get(0));
        assertEquals(2, ((IonInt) sublist.get(1)).intValue());
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void sublistAddWithIndexOutOfRange() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);

        final IonInt value = SYSTEM.newInt(99);
        sublist.add(4, value);
    }

    @Test(expected = ConcurrentModificationException.class)
    public void sublistAddWithIndexConcurrentModification() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);
        parent.remove(0);

        sublist.add(0, SYSTEM.newInt(0));
    }

    @Test
    public void sublistAddAll() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);

        final List<IonInt> values = Arrays.asList(SYSTEM.newInt(100), SYSTEM.newInt(101));
        sublist.addAll(values);
        assertEquals(5, sublist.size());
        assertEquals(values.get(0), sublist.get(3));
        assertEquals(values.get(1), sublist.get(4));
    }

    @Test
    public void sublistAddAllWithIndex() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);

        final List<IonInt> values = Arrays.asList(SYSTEM.newInt(100), SYSTEM.newInt(101));
        sublist.addAll(0, values);
        assertEquals(5, sublist.size());
        assertEquals(values.get(0), sublist.get(0));
        assertEquals(values.get(1), sublist.get(1));

        assertEquals(2, ((IonInt) sublist.get(2)).intValue());
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void sublistAddAllWithIndexOutOfRange() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);

        final List<IonInt> values = Arrays.asList(SYSTEM.newInt(100), SYSTEM.newInt(101));
        sublist.addAll(3, values);
    }

    @Test(expected = ConcurrentModificationException.class)
    public void sublistAddAllConcurrentModification() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);
        parent.remove(0);

        sublist.addAll(Arrays.asList(SYSTEM.newInt(100), SYSTEM.newInt(101)));
    }

    @Test
    public void sublistRetainAll() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);

        final List<IonValue> toRetain = Collections.singletonList(sublist.get(0));

        assertTrue(sublist.retainAll(toRetain));

        assertEquals(1, sublist.size());
        assertTrue(sublist.contains(toRetain.get(0)));
        assertEquals(5, parent.size());
    }

    @Test
    public void sublistRetainAllWithDuplicates() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);

        final List<IonValue> toRetain = Arrays.asList(sublist.get(0), sublist.get(0));

        assertTrue(sublist.retainAll(toRetain));

        assertEquals(1, sublist.size());
        assertTrue(sublist.contains(toRetain.get(0)));
        assertEquals(5, parent.size());
    }

    @Test(expected = ConcurrentModificationException.class)
    public void sublistRetainAllConcurrentModification() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);
        parent.remove(0);

        sublist.retainAll(Arrays.asList(SYSTEM.newInt(100), SYSTEM.newInt(101)));
    }

    @Test
    public void sublistClear() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);

        sublist.clear();

        assertEquals(0, sublist.size());
        assertEquals(4, parent.size());
    }

    @Test(expected = ConcurrentModificationException.class)
    public void sublistClearConcurrentModification() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);
        parent.remove(0);

        sublist.clear();
    }

    @Test
    public void sublistRemoveIndex() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);

        final IonInt ionValue = (IonInt) sublist.remove(0);
        assertEquals(2, sublist.size());
        assertEquals(2, ionValue.intValue());
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void sublistRemoveIndexOutOfRange() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);

        sublist.remove(3);
    }

    @Test(expected = ConcurrentModificationException.class)
    public void sublistRemoveIndexConcurrentModification() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);
        parent.remove(0);

        sublist.remove(0);
    }

    @Test
    public void sublistRemoveObject() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);

        final IonInt ionValue = (IonInt) parent.get(2);
        assertTrue(sublist.remove(ionValue));
        assertEquals(2, sublist.size());
    }

    @Test(expected = ConcurrentModificationException.class)
    public void sublistRemoveObjectConcurrentModification() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);
        parent.remove(0);

        sublist.remove(parent.get(2));
    }

    @Test
    public void sublistRemoveAll() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);

        final List<IonValue> toRemove = Arrays.asList(parent.get(2), parent.get(3));
        assertTrue(sublist.removeAll(toRemove));
        assertEquals(1, sublist.size());
        for (IonValue v : toRemove) {
            assertFalse(sublist.contains(v));
        }
    }

    @Test
    public void sublistRemoveAllNotInList() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);

        final List<IonValue> toRemove = Arrays.asList(parent.get(0), parent.get(1));
        assertFalse(sublist.removeAll(toRemove));
        assertEquals(3, sublist.size());
    }

    @Test(expected = ConcurrentModificationException.class)
    public void sublistRemoveAllConcurrentModification() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);
        parent.remove(0);

        sublist.removeAll(Collections.singletonList(parent.get(2)));
    }

    @Test
    public void sublistIndexOf() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);

        assertEquals(0, sublist.indexOf(parent.get(2)));
        assertEquals(-1, sublist.indexOf(parent.get(0)));
        assertEquals(-1, sublist.indexOf(SYSTEM.newInt(99)));
    }

    @Test(expected = ConcurrentModificationException.class)
    public void sublistIndexOfConcurrentModification() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);
        parent.remove(0);

        sublist.indexOf(parent.get(2));
    }

    @Test
    public void sublistLastIndexOf() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);

        assertEquals(0, sublist.lastIndexOf(parent.get(2)));
        assertEquals(-1, sublist.lastIndexOf(parent.get(0)));
        assertEquals(-1, sublist.lastIndexOf(SYSTEM.newInt(99)));
    }

    @Test(expected = ConcurrentModificationException.class)
    public void sublistLastIndexOfConcurrentModification() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);
        parent.remove(0);

        sublist.lastIndexOf(parent.get(2));
    }

    @Test(expected = ConcurrentModificationException.class)
    public void sublistIteratorConcurrentModification() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);
        parent.remove(0);

        sublist.iterator();
    }

    @Test
    public void sublistIterator() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);

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
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);

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

        final IonInt newSetValue = SYSTEM.newInt(100);
        iterator.set(newSetValue);
        assertEquals(newSetValue, sublist.get(2));
        assertFalse(iterator.hasNext());

        assertEquals(sublist.get(2), iterator.previous());
        final IonInt newAddValue = SYSTEM.newInt(200);
        iterator.add(newAddValue);
        assertEquals(newAddValue, sublist.get(2));
        assertTrue(iterator.hasNext());
    }

    @Test(expected = ConcurrentModificationException.class)
    public void sublistListIteratorConcurrentModification() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);
        parent.remove(0);

        sublist.listIterator();
    }

    @Test
    public void sublistListIteratorWithIndex() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);

        final ListIterator<IonValue> iterator = sublist.listIterator(1);

        assertTrue(iterator.hasNext());
        assertTrue(iterator.hasPrevious());
        assertEquals(1, iterator.nextIndex());
        assertEquals(0, iterator.previousIndex());
        assertEquals(sublist.get(1), iterator.next());

        iterator.previous(); // back to initial state

        assertEquals(sublist.get(0), iterator.previous());
    }

    @Test(expected = ConcurrentModificationException.class)
    public void sublistListIteratorWithIndexConcurrentModification() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);
        parent.remove(0);

        sublist.listIterator(1);
    }

    @Test
    public void sublistSublist() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(1, 5);    // 1,2,3,4
        final List<IonValue> subSublist = sublist.subList(0, 2); // 1,2

        final IonInt newValue = SYSTEM.newInt(100);
        parent.set(1, newValue);
        assertEquals(newValue, sublist.get(0));
        assertEquals(newValue, subSublist.get(0));
    }
    @Test(expected = ConcurrentModificationException.class)

    public void sublistSubListConcurrentModification() {
        final IonList parent = SYSTEM.newList(new int[]{0, 1, 2, 3, 4, 5, 6});
        final List<IonValue> sublist = parent.subList(2, 5);
        parent.remove(0);

        sublist.subList(0,1);
    }
}