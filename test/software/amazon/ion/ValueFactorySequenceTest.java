/*
 * Copyright 2011-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import org.junit.Before;
import org.junit.Test;
import software.amazon.ion.ContainedValueException;
import software.amazon.ion.IonList;
import software.amazon.ion.IonSequence;
import software.amazon.ion.IonSexp;
import software.amazon.ion.IonValue;
import software.amazon.ion.ValueFactory;
import software.amazon.ion.junit.Injected.Inject;

public class ValueFactorySequenceTest
    extends IonTestCase
{
    private static abstract class SequenceMaker<S extends IonSequence>
    {
        ValueFactory factory;
        abstract S newSeq(IonSequence child);
        abstract S newSeq(IonValue... children);
    }

    private static class ListMaker extends SequenceMaker<IonList>
    {
        @Override public String toString() { return "LIST"; }

        @Override
        IonList newSeq(IonSequence child)
        {
            return factory.newList(child);
        }

        @Override
        public IonList newSeq(IonValue... children)
        {
            return factory.newList(children);
        }
    }

    private static class SexpMaker extends SequenceMaker<IonSexp>
    {
        @Override public String toString() { return "SEXP"; }

        @Override
        IonSexp newSeq(IonSequence child)
        {
            return factory.newSexp(child);
        }

        @Override
        public IonSexp newSeq(IonValue... children)
        {
            return factory.newSexp(children);
        }
    }

    @Inject("maker")
    public static final SequenceMaker[] MAKERS =
    { new ListMaker(), new SexpMaker() };


    protected SequenceMaker<?> myMaker;

    public void setMaker(SequenceMaker maker)
    {
        myMaker = maker;
    }

    @Before
    public void initMaker()
    {
        myMaker.factory = system();
    }

    //========================================================================

    @Test
    public void testNewWithNoParams()
    {
        IonSequence s = myMaker.newSeq();
        assertFalse(s.isNullValue());
        assertTrue(s.isEmpty());
    }

    @Test
    public void testNewWithNullArray()
    {
        IonSequence s = myMaker.newSeq((IonValue[])null);
        assertTrue(s.isNullValue());
    }

    @Test(expected = NullPointerException.class)
    public void testNewWithNullChild()
    {
        myMaker.newSeq(system().newInt(1), null);
    }

    @SuppressWarnings("cast")
    @Test(expected = IllegalArgumentException.class)
    public void testNewWithDatagramSequenceChild()
    {
        myMaker.newSeq((IonSequence) system().newDatagram());
    }

    @Test(expected = NullPointerException.class)
    public void testNewWithNullSequenceChild()
    {
        myMaker.newSeq((IonSequence) null);
    }

    @Test
    public void testNewWithRealSequenceChild()
    {
        IonSequence inner = myMaker.newSeq(system().newInt(1));
        IonSequence outer = myMaker.newSeq(inner);
        assertSame(inner, outer.get(0));
    }

    @Test(expected = ContainedValueException.class)
    public void testNewWithContainedSequenceChild()
    {
        IonSequence inner = system().newEmptyList().add().newEmptyList();
        myMaker.newSeq(inner);
    }
}
