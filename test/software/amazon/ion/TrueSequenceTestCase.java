/*
 * Copyright 2009-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package software.amazon.ion;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;
import software.amazon.ion.ContainedValueException;
import software.amazon.ion.IonDatagram;
import software.amazon.ion.IonInt;
import software.amazon.ion.IonNull;
import software.amazon.ion.IonSequence;
import software.amazon.ion.IonValue;
import software.amazon.ion.ReadOnlyValueException;
import software.amazon.ion.impl.PrivateIonValue;


/**
 * Test cases for "true" sequence types (list and sexp), covering features that
 * are not supported by datagram.
 */
public abstract class TrueSequenceTestCase
    extends SequenceTestCase
{
    /**
     * @return a new null sequence.
     */
    @Override
    protected abstract IonSequence makeNull();


    @Test
    public void testNullSequenceBadAdds()
    {
        IonSequence value = makeNull();
        testBadAdds(value);
    }


    @Test
    public void testNullSequenceBadRemoves()
    {
        IonSequence value = makeNull();
        testBadRemoves(value);
    }


    @Test
    public void testNullSequenceAddAll()
    {
        IonSequence seq = makeNull();

        List<IonValue> list = Collections.emptyList();
        boolean changed = seq.addAll(list);
        assertFalse(changed);
        checkNullSequence(seq);

        testAddAll(seq);
    }


    @Test
    public void testNullSequenceIndexedAddAll()
    {
        IonSequence seq = makeNull();

        List<IonValue> list = Collections.emptyList();
        boolean changed = seq.addAll(0, list);
        assertFalse(changed);
        checkNullSequence(seq);

        testIndexedAddAll(seq);
    }


    /**
     *  TODO amznlabs/ion-java#47  Implement indexed addAll for datagram
     *  Hoist this up to SequencenceTestCase.
     */
    @Test
    public void testIndexedAddAll()
    {
        IonSequence seq = makeEmpty();
        testIndexedAddAll(seq);
    }

    public void testIndexedAddAll(IonSequence seq)
    {
        try
        {
            seq.addAll(0, null);
            fail("expected exception");
        }
        catch (NullPointerException e) { }

        List<IonValue> list = new ArrayList<IonValue>();
        try
        {
            seq.addAll(1, list);
            fail("expected exception");
        }
        catch (IndexOutOfBoundsException e) { }

        boolean changed = seq.addAll(0, list);
        assertFalse(changed);
        assertEquals(0, seq.size());

        IonValue v1 = system().newInt(1);
        list.add(v1);

        changed = seq.addAll(0, list);
        assertTrue(changed);
        assertEquals(1, seq.size());
        assertSame(seq.get(0), v1);
        assertSame(seq, v1.getContainer());

        try
        {
            seq.addAll(0, list);
            fail("expected exception");
        }
        catch (ContainedValueException e) { }

        list.clear();
        IonValue v2 = system().newInt(2);
        IonValue v3 = system().newInt(3);
        list.add(v2);
        list.add(v3);

        seq.addAll(1, list);
        assertEquals(3, seq.size());
        assertSame(seq.get(0), v1);
        assertSame(seq.get(1), v2);
        assertSame(seq.get(2), v3);

        list.clear();
        IonValue v4 = system().newInt(4);
        list.add(v4);

        seq.addAll(1, list);
        assertEquals(4, seq.size());
        assertSame(seq.get(0), v1);
        assertSame(seq.get(1), v4);
        assertSame(seq.get(2), v2);
        assertSame(seq.get(3), v3);
    }


    /**
     *  TODO amznlabs/ion-java#50 Implement set for datagram
     *  Hoist this up to SequencenceTestCase.
     */
    @Test
    public void testSet()
    {
        IonSequence s = wrapAndParse("e0", "e1", "e3");

        set(s, 0);
        set(s, 2);
        set(s, 1);

        assertEquals(wrapAndParse("0", "1", "2"), s);
    }

    @Test
    public void testSetInsideDatagramForcesEncode()
    {
        IonSequence s = wrapAndParse("e0", "'''text'''", "e3");
        IonDatagram d = system().newDatagram(s);
        byte[] bytes = d.getBytes();

        set(s, 1);

        byte[] newBytes = d.getBytes();
        assertTrue("encoded data should change",
                   bytes.length != newBytes.length);
    }


    private void set(IonSequence s, int index)
    {
        IonValue origElement = s.get(index);
        IonValue newElement = system().newInt(index);

        IonSequence expectedElements = s.clone();
        expectedElements.remove(index);
        expectedElements.add(index, newElement.clone());

        IonValue removed = s.set(index, newElement);

        assertSame(newElement, s.get(index));
        assertSame(s, newElement.getContainer());
        assertSame(origElement, removed);
        assertEquals(null, removed.getContainer());

        assertEquals(index, ((PrivateIonValue)newElement).getElementId());

        assertEquals(expectedElements, s);
    }


    /**
     *  TODO amznlabs/ion-java#50 Implement set for datagram
     *  Hoist this up to SequencenceTestCase.
     */
    @Test
    public void testSetNullElement()
    {
        IonSequence s = wrapAndParse("e0");
        testSetThrows(s, 0, null, NullPointerException.class);
    }


    /**
     *  TODO amznlabs/ion-java#50 Implement set for datagram
     *  Hoist this up to SequencenceTestCase.
     */
    @Test
    public void testSetContainedValue()
    {
        IonSequence s0 = wrapAndParse("e0");
        IonSequence s1 = wrapAndParse("e1");

        testSetThrows(s0, 0, s1.get(0), ContainedValueException.class);
    }


    /**
     *  TODO amznlabs/ion-java#50 Implement set for datagram
     *  Hoist this up to SequencenceTestCase.
     */
    @Test
    public void testSetDatagram()
    {
        IonSequence s = wrapAndParse("e0");
        IonDatagram dg = loader().load("hi");
        testSetThrows(s, 0, dg, IllegalArgumentException.class);
    }


    /**
     *  TODO amznlabs/ion-java#50 Implement set for datagram
     *  Hoist this up to SequencenceTestCase.
     */
    @Test
    public void testSetReadOnlyChild()
    {
        IonSequence s = wrapAndParse("e0");
        IonValue v = system().newNull();
        v.makeReadOnly();
        testSetThrows(s, 0, v, ReadOnlyValueException.class);
    }


    /**
     *  TODO amznlabs/ion-java#50 Implement set for datagram
     *  Hoist this up to SequencenceTestCase.
     */
    @Test
    public void testSetReadOnlyContainer()
    {
        IonSequence s = wrapAndParse("e0");
        s.makeReadOnly();
        IonValue v = system().newNull();
        testSetThrows(s, 0, v, ReadOnlyValueException.class);
    }


    /**
     *  TODO amznlabs/ion-java#50 Implement set for datagram
     *  Hoist this up to SequencenceTestCase.
     */
    @Test
    public void testSetOutOfBounds()
    {
        IonSequence s = makeNull();
        if (s != null)
        {
            testSetOutOfBounds(s, -1);
            testSetOutOfBounds(s, 0);
            testSetOutOfBounds(s, 1);
        }
        // else we're testing datagram

        s = makeEmpty();
        testSetOutOfBounds(s, -1);
        testSetOutOfBounds(s, 0);
        testSetOutOfBounds(s, 1);

        s.add().newInt(3498);
        testSetOutOfBounds(s, -1);
        testSetOutOfBounds(s, 1);
        testSetOutOfBounds(s, 2);
    }

    private void testSetOutOfBounds(IonSequence s, int index)
    {
        IonValue v = system().newNull();
        testSetThrows(s, index, v, IndexOutOfBoundsException.class);
    }

    private <T extends RuntimeException>
    void testSetThrows(IonSequence s, int index, IonValue v,
                       Class<T> exceptionType)
        throws RuntimeException
    {
        IonSequence orig = s.clone();
        assertEquals(orig.isNullValue(), s.isNullValue());
        try {
            s.set(index, v);
            fail("expected " + exceptionType.getSimpleName());
        }
        catch (RuntimeException e)
        {
            if (! exceptionType.isAssignableFrom(e.getClass()))
            {
                throw e;
            }
        }
        assertEquals(orig, s);
    }


    @Test
    public void testNullSequenceContains()
    {
        IonNull nullValue1 = system().newNull();

        IonSequence seq = makeNull();
        assertFalse(seq.contains(nullValue1));
        try
        {
            seq.contains(null);
            fail("expected exception");
        }
        catch (NullPointerException e) { }
        try
        {
            seq.contains(new Integer(0));
            fail("expected exception");
        }
        catch (ClassCastException e) { }
    }


    @Test
    public void testNullSequenceContainsAll()
    {
        IonNull nullValue = system().newNull();
        IonInt  intValue  = system().newInt(1);

        List<Object> empty = new ArrayList<Object>();
        List<Object> hasJavaNull = Arrays.asList((Object)null);
        List<Integer> hasJavaInt = Arrays.asList(new Integer(0));
        List<Object> hasNull = Arrays.asList((Object)nullValue);
        List<Object> hasNullAndInt = Arrays.asList((Object)intValue,
                                                   (Object)nullValue);

        IonSequence seq = makeNull();
        assertTrue(seq.containsAll(empty));
        assertFalse(seq.containsAll(hasNull));
        assertFalse(seq.containsAll(hasNullAndInt));
        try
        {
            seq.containsAll(null);
            fail("expected exception");
        }
        catch (NullPointerException e) { }
        try
        {
            seq.containsAll(hasJavaNull);
            fail("expected exception");
        }
        catch (NullPointerException e) { }
        try
        {
            seq.containsAll(hasJavaInt);
            fail("expected exception");
        }
        catch (ClassCastException e) { }
    }


    @Test
    public void testNullSequenceRemoveAll()
    {
        IonNull nullValue1 = system().newNull();
        IonNull nullValue2 = system().newNull();
        IonInt  intValue1  = system().newInt(1);

        List<Object> empty = new ArrayList<Object>();
        List<Object> hasJavaNull = Arrays.asList((Object)null);
        List<Integer> hasJavaInt = Arrays.asList(new Integer(0));
        List<Object> hasNull1 = Arrays.asList((Object)nullValue1);
        List<Object> hasNull2AndInt = Arrays.asList((Object)intValue1,
                                                    (Object)nullValue2);

        IonSequence seq = makeNull();
        assertFalse(seq.removeAll(empty));
        assertFalse(seq.removeAll(hasNull1));
        assertFalse(seq.removeAll(hasNull2AndInt));
        try
        {
            seq.removeAll(null);
            fail("expected exception");
        }
        catch (NullPointerException e) { }
        try
        {
            seq.removeAll(hasJavaNull);
            fail("expected exception");
        }
        catch (NullPointerException e) { }
        try
        {
            seq.removeAll(hasJavaInt);
            fail("expected exception");
        }
        catch (ClassCastException e) { }
    }


    @Test
    public void testNullSequenceRetainAll()
    {
        IonNull nullValue1 = system().newNull();
        IonNull nullValue2 = system().newNull();
        IonInt  intValue1  = system().newInt(1);

        List<Object> empty = new ArrayList<Object>();
//        List<Object> hasJavaNull = Arrays.asList((Object)null);
//        List<Integer> hasJavaInt = Arrays.asList(new Integer(0));
        List<Object> hasNull1 = Arrays.asList((Object)nullValue1);
        List<Object> hasNull2AndInt = Arrays.asList((Object)intValue1,
                                                    (Object)nullValue2);

        IonSequence seq = makeNull();
        assertFalse(seq.retainAll(empty));
        assertFalse(seq.retainAll(hasNull1));
        assertFalse(seq.retainAll(hasNull2AndInt));
//        try
//        {
//            seq.retainAll(null);
//            fail("expected exception");
//        }
//        catch (NullPointerException e) { }
//        try
//        {
//            seq.retainAll(hasJavaNull);
//            fail("expected exception");
//        }
//        catch (NullPointerException e) { }
//        try
//        {
//            seq.retainAll(hasJavaInt);
//            fail("expected exception");
//        }
//        catch (ClassCastException e) { }
    }


    @Test
    public void testNullSequenceToArray()
    {
        IonSequence seq = makeNull();
        assertEquals(0, seq.toArray().length);

        Object[] objArray = new Object[0];
        assertSame(objArray, seq.toArray(objArray));

        objArray = new Object[2];
        objArray[0] = new Object();
        assertSame(objArray, seq.toArray(objArray));
        assertNull(objArray[0]);

        try
        {
            seq.toArray(null);
            fail("expected exception");
        }
        catch (NullPointerException e) { }

        // This may be required by spec, it's ambiguous.
//        Integer[] intArray = new Integer[0];
//        try
//        {
//            seq.toArray(intArray);
//            fail("expected exception");
//        }
//        catch (ArrayStoreException e) { }
    }


    @Test
    public void testNullSequenceIndexOf()
    {
        IonSequence s = makeNull();
        try {
            s.indexOf(null);
            fail("expected exception");
        }
        catch (NullPointerException e) { }
        try {
            s.indexOf(new Integer(0));
            fail("expected exception");
        }
        catch (ClassCastException e) { }

        IonNull nullValue1 = system().newNull();
        assertEquals(-1, s.indexOf(nullValue1));
    }

    @Test
    public void testMakeNullRemovesChildsContainer()
    {
        IonValue val = system().newString("test");
        IonSequence seq = newSequence(val);
        seq.makeNull();
        assertNull("Removed value should have null container",
                   val.getContainer());
    }
}
