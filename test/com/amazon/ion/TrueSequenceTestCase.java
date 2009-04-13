// Copyright (c) 2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 *
 */
public abstract class TrueSequenceTestCase
    extends SequenceTestCase
{
    /**
     * @return a new null sequence.
     */
    @Override
    protected abstract IonSequence makeNull();


    public void testNullSequenceBadAdds()
    {
        IonSequence value = makeNull();
        testBadAdds(value);
    }


    public void testNullSequenceBadRemoves()
    {
        IonSequence value = makeNull();
        testBadRemoves(value);
    }


    public void testNullSequenceAddAll()
    {
        IonSequence seq = makeNull();
        testAddAll(seq);
    }


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

    public void testMakeNullRemovesChildsContainer()
    {
        IonValue val = system().newString("test");
        IonSequence seq = newSequence(val);
        seq.makeNull();
        assertNull("Removed value should have null container",
                   val.getContainer());
    }
}
