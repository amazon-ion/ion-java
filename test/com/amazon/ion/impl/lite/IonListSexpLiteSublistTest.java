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

import java.util.List;
import java.util.ListIterator;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import com.amazon.ion.IonInt;
import com.amazon.ion.IonSequence;
import com.amazon.ion.IonValue;
import com.amazon.ion.impl.lite.BaseIonSequenceLiteSublistTestCase;
import com.amazon.ion.junit.Injected;
import com.amazon.ion.junit.Injected.Inject;

/**
 * Sublist tests for list and sexp
 */
@RunWith(Injected.class)
public class IonListSexpLiteSublistTest extends BaseIonSequenceLiteSublistTestCase {

    interface IonSequenceProvider {

        IonSequence newSequence();
    }

    @Inject("provider")
    public static final IonSequenceProvider[] providers = new IonSequenceProvider[]{
        new IonSequenceProvider() {
            public IonSequence newSequence() {
                return SYSTEM.newList(INTS);
            }
        },
        new IonSequenceProvider() {
            public IonSequence newSequence() {
                return SYSTEM.newSexp(INTS);
            }
        },
    };

    private IonSequenceProvider provider;

    public void setProvider(final IonSequenceProvider provider) {
        this.provider = provider;
    }

    protected IonSequence newSequence() {
        return provider.newSequence();
    }

    @Test
    public void sublistSet() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);

        final IonInt element = SYSTEM.newInt(99);
        final IonValue previous = sublist.set(0, element);

        assertEquals(2, ((IonInt) previous).intValue());
        assertEquals(element, sublist.get(0));
        assertEquals(element, sequence.get(2));
    }

    @Test
    public void sublistSetOnParent() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);

        final IonInt element = SYSTEM.newInt(99);
        final IonValue previous = sequence.set(2, element);

        assertEquals(2, ((IonInt) previous).intValue());
        assertEquals(element, sublist.get(0));
        assertEquals(element, sequence.get(2));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void sublistSetOutOfRange() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);

        final IonInt element = SYSTEM.newInt(99);
        sublist.set(4, element);
    }

    @Test
    public void sublistSublistChangingParentValues() {
        IonSequence sequence = newSequence();

        final List<IonValue> sublist = sequence.subList(1, 5);    // 1,2,3,4
        final List<IonValue> subSublist = sublist.subList(0, 2);  // 1,2

        final IonInt newValue = SYSTEM.newInt(100);
        sequence.set(1, newValue);
        assertEquals(newValue, sublist.get(0));
        assertEquals(newValue, subSublist.get(0));
    }

    @Test
    public void sublistListIteratorWithSet() {
        final IonSequence sequence = newSequence();
        final List<IonValue> sublist = sequence.subList(2, 5);

        final ListIterator<IonValue> iterator = sublist.listIterator();

        iterator.next();

        final IonInt newSetValue = SYSTEM.newInt(100);
        iterator.set(newSetValue);
        assertEquals(newSetValue, sublist.get(0));
    }
}
