package com.amazon.ion.impl.lite;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.amazon.ion.IonSequence;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.system.IonSystemBuilder;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(JUnitParamsRunner.class)
public class IonSequenceLiteSubListViewEqualityTests {
    static final IonSystem SYSTEM = IonSystemBuilder.standard().build();
    private static List<Integer> INTS = Arrays.asList(0, 1, 2, 3, 4, 5, 6);
    private static List<Integer> OTHER_INTS = Arrays.asList(0, 1, 2, 33, 4, 5, 6);
    private static List<IonValue> ARRAY_LIST = newArrayList(INTS);
    private static List<IonValue> OTHER_ARRAY_LIST = newArrayList(OTHER_INTS);

    private interface SublistMaker {
        List<IonValue> makeSublist(List<IonValue> seq);
    }

    private static void runSubListEquivalenceTests(List<IonValue> seq, SublistMaker maker) {
        testSublistEquivalence(seq, maker);
        testSublistNonEquivalence(seq, maker);
    }

    // makes a sublist of seq and asserts its equivalence to sublists of all sequence types and to a sublist
    // derived from ARRAY_LIST
    private static void testSublistEquivalence(List<IonValue> seq, SublistMaker maker) {
        List<IonValue> subList = maker.makeSublist(seq);
        for(IonSequence otherSeq : getTestSequences()) {
            List<IonValue> otherSubList = maker.makeSublist(otherSeq);
            assertEquals("subLists should be equivalent", subList, otherSubList);
            assertEquals("subLists should have the same hashCode", subList.hashCode(), otherSubList.hashCode());
        }

        List<IonValue> ararySubList = maker.makeSublist(ARRAY_LIST);
        assertEquals("subList should be equivalent to a sublist of ARRAY_LIST",
                ararySubList, subList);

        assertEquals("subList should have the same hash code as a sublist of ARRAY_LIST",
                ararySubList.hashCode(), subList.hashCode());
    }

    // makes a sublist of seq and asserts its non-equivalence to the same subList range of the "other" sequence
    // and to a sublist derived from OTHER_ARRAY_LIST
    private static void testSublistNonEquivalence(List<IonValue> seq, SublistMaker maker) {
        // A hash collision is unlikely but possible so we do not assert that the hashCodes are different here.

        List<IonValue> subList = maker.makeSublist(seq);
        for(IonSequence otherSeq : getOtherTestSequences()) {
            List<IonValue> otherSubList = maker.makeSublist(otherSeq);
            assertNotEquals("subLists should *not* be equivalent", subList, otherSubList);
        }

        assertNotEquals("subList should *not* be equivalent to a sublist of OTHER_ARRAY_LIST",
                maker.makeSublist(OTHER_ARRAY_LIST), subList);
    }

    private static IonSequence[] getParameters(List<Integer> ints) {
        return new IonSequence[] {
            populateSequence(SYSTEM.newList(), ints),
            populateSequence(SYSTEM.newSexp(), ints),
            populateSequence(SYSTEM.newDatagram(), ints)
        };
    }


    @Test
    @Parameters(method = "getTestSequences")
    public void fullSubList(IonSequence seq) {
        SublistMaker maker = new SublistMaker() {
            @Override
            public List<IonValue> makeSublist(List<IonValue> seq) {
                return seq.subList(0, seq.size());
            }
        };
        runSubListEquivalenceTests(seq, maker);

        // The sublist should also be equivalent to the ARRAY_LIST
        List<IonValue> subList = maker.makeSublist(seq);
        assertEquals("The full sublist should be equivalent to the ArrayList<IonValue",
                ARRAY_LIST, subList);

        assertNotEquals("The full sublist should *not* be equivalent to the *other* ArrayList<IonValue",
                OTHER_ARRAY_LIST, subList);
    }

    @Test
    @Parameters(method = "getTestSequences")
    public void fullSubListOfFullSubList(IonSequence seq) {
        SublistMaker maker = new SublistMaker() {
            @Override
            public List<IonValue> makeSublist(List<IonValue> seq) {
                return seq.subList(0, seq.size()).subList(0, seq.size());
            }
        };
        runSubListEquivalenceTests(seq, maker);
    }

    @Test
    @Parameters(method = "getTestSequences")
    public void partialSubList(IonSequence seq) {
        SublistMaker maker = new SublistMaker() {
            @Override
            public List<IonValue> makeSublist(List<IonValue> seq) {
                return seq.subList(1, seq.size() - 1);
            }
        };
        runSubListEquivalenceTests(seq, maker);
    }

    @Test
    @Parameters(method = "getTestSequences")
    public void partialSubListOfPartialSubList(IonSequence seq) {
        SublistMaker maker = new SublistMaker() {
            @Override
            public List<IonValue> makeSublist(List<IonValue> seq) {
                return seq.subList(1, seq.size() - 1).subList(1, seq.size() - 2);
            }
        };
        runSubListEquivalenceTests(seq, maker);
    }

    @Test
    @Parameters(method = "getTestSequences")
    public void sequenceEquivalenceToSubList(IonSequence seq) {
        List<IonValue> subList = seq.subList(0, seq.size());

        // Note the asymmetry.  This is expected. See IonSequence.subList javadoc.
        assertEquals("the sublist should be equivalent to the sequence", subList, seq);
        assertNotEquals("the sequence should *not* be equivalent to the subList", seq, subList);
    }

    @Test
    @Parameters(method = "getTestSequences")
    public void subListNotEquivalentToNull(IonSequence seq) {
        List<IonValue> subList = seq.subList(0, seq.size());
        assertNotEquals("subList should not be equivalent to null", subList, null);
    }

    @Test
    @Parameters(method = "getTestSequences")
    public void subListEquivalentToSelf(IonSequence seq) {
        List<IonValue> subList = seq.subList(0, seq.size());
        assertEquals("subList should be equivalent to itself", subList, subList);
    }

    public static IonSequence[] getTestSequences() {
        return getParameters(INTS);
    }

    public static IonSequence[] getOtherTestSequences() {
        return getParameters(OTHER_INTS);
    }

    private static IonSequence populateSequence(IonSequence seq, List<Integer> ints) {
        for(int i : ints) {
            seq.add(SYSTEM.newInt(i));
        }
        return seq;
    }

    private static ArrayList<IonValue> newArrayList(List<Integer> ints) {
        ArrayList<IonValue> values = new ArrayList<IonValue>();
        for(int i : ints) {
            values.add(SYSTEM.newInt(i));
        }
        return values;
    }
}
