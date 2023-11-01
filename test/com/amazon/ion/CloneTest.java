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

package com.amazon.ion;

import com.amazon.ion.impl._Private_IonSystem;
import com.amazon.ion.junit.IonAssert;
import com.amazon.ion.system.IonSystemBuilder;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.amazon.ion.impl._Private_Utils.newSymbolToken;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CloneTest
{

    public static final _Private_IonSystem PRIVATE_ION_SYSTEM = (_Private_IonSystem) IonSystemBuilder.standard().build();

    private static <V extends IonValue> V assertSelfCloneable(V original)
    {
        V clone = (V)original.clone();
        return assertEqualsAndNotSame(original, clone);
    }

    private static <V extends IonValue> V assertSystemCloneable(V original)
    {
        V clone = (V)(system().clone(original));
        return assertEqualsAndNotSame(original, clone);
    }

    private static <V extends IonValue> V assertEqualsAndNotSame(V expected, V actual)
    {
        IonAssert.assertIonEquals(expected, actual);
        assertThat(actual, not(sameInstance(expected)));
        return actual;
    }

    @Test
    public void testIonValueCloneWithUnknownSymbolText()
    {
        SymbolToken tok = newSymbolToken(99);
        IonSymbol original = system().newSymbol(tok);

        UnknownSymbolException use = assertThrows(UnknownSymbolException.class, original::clone);
        assertThat(use.getMessage(), containsString("$99"));
    }

    @Test
    public void testValueFactoryCloneWithUnknownSymbolText()
    {
        SymbolToken tok = newSymbolToken(99);
        IonSymbol original = system().newSymbol(tok);

        UnknownSymbolException use = assertThrows(UnknownSymbolException.class, () -> system().clone(original));
        assertThat(use.getMessage(), containsString("$99"));
    }

    @Test
    public void testDifferentValueFactoryCloneWithUnknownSymbolText()
    {
        IonSystem otherSystem = IonSystemBuilder.standard().build();

        SymbolToken tok = newSymbolToken(99);
        IonSymbol original = system().newSymbol(tok);

        // TODO amazon-ion/ion-java/issues/30 An UnknownSymbolException is expected here, but
        // it isn't thrown.
        IonSymbol copy = otherSystem.clone(original);

        // If we don't fail we should at least retain the SID.
        assertEquals(99, copy.symbolValue().getSid());
    }

    @Test
    public void testIonValueCloneWithUnknownAnnotationText()
    {
        SymbolToken tok = newSymbolToken(99);
        IonInt original = system().newInt(5);
        original.setTypeAnnotationSymbols(tok);

        assertSelfCloneable(original);
    }

    @Test
    public void testValueFactoryCloneWithUnknownAnnotationText()
    {
        SymbolToken tok = newSymbolToken(99);
        IonInt original = system().newInt(5);
        original.setTypeAnnotationSymbols(tok);

        assertSystemCloneable(original);
    }

    @Test
    public void testDifferentValueFactoryCloneWithUnknownAnnotationText()
    {
        IonSystem otherSystem = IonSystemBuilder.standard().build();

        SymbolToken tok = newSymbolToken(99);
        IonInt original = system().newInt(5);
        original.setTypeAnnotationSymbols(tok);

        // TODO amazon-ion/ion-java/issues/30 An UnknownSymbolException is expected here, but
        // it isn't thrown.
        IonInt copy = otherSystem.clone(original);

        // If we don't fail we should at least retain the SID.
        assertEquals(99, copy.getTypeAnnotationSymbols()[0].getSid());
    }


    @Test
    public void testIonValueCloneWithUnknownFieldNameText()
    {
        SymbolToken tok = newSymbolToken(99);
        IonStruct original = system().newEmptyStruct();
        IonValue child = system().newNull();
        original.add(tok, child);

        // This works since the cloned child doesn't retain its field name.
        child.clone();

        UnknownSymbolException use = assertThrows(UnknownSymbolException.class, original::clone);
        assertThat(use.getMessage(), containsString("$99"));
    }

    @Test
    public void testValueFactoryCloneWithUnknownFieldNameText()
    {
        SymbolToken tok = newSymbolToken(99);
        IonStruct original = system().newEmptyStruct();
        IonValue child = system().newNull();
        original.add(tok, child);

        // This works since the cloned child doesn't retain its field name.
        assertSystemCloneable(child);

        UnknownSymbolException use = assertThrows(UnknownSymbolException.class, () -> system().clone(original));
        assertThat(use.getMessage(), containsString("$99"));
    }

    @Test
    public void testDifferentValueFactoryCloneWithUnknownFieldNameText()
    {
        IonSystem otherSystem = IonSystemBuilder.standard().build();

        SymbolToken tok = newSymbolToken(99);
        IonStruct original = system().newEmptyStruct();
        IonValue child = system().newNull();
        original.add(tok, child);

        // This works since the cloned child doesn't retain its field name.
        otherSystem.clone(child);

        // TODO amazon-ion/ion-java/issues/30 An UnknownSymbolException is expected here, but
        // it isn't thrown.
        IonStruct copy = otherSystem.clone(original);

        // If we don't fail we should at least retain the SID.
        assertEquals(99, copy.iterator().next().getFieldNameSymbol().getSid());
    }

    @Test
    public void cloneEmptyContainer()
    {
        IonStruct original = system().newEmptyStruct();

        IonStruct clone = assertSelfCloneable(original);
        assertThat(clone, emptyIterable());
    }

    @Test
    public void cloneEmptyNestedContainer()
    {
        IonList original = system().newEmptyList();
        original.add().newEmptyStruct();

        assertSelfCloneable(original);
    }

    @Test
    public void cloneBasicStruct()
    {
        IonStruct original = system().newEmptyStruct();
        original.add("foo").newString("bar");

        assertSelfCloneable(original);
    }

    @Test
    public void cloneNestedContainer()
    {
        IonStruct original = system().newEmptyStruct();
        original.add("foo").newEmptyList().add().newString("bar");

        assertSelfCloneable(original);
    }

    @Test
    public void cloneMultipleElements()
    {
        IonList original = system().newList(new int[] {1, 2, 3});

        assertSelfCloneable(original);
    }

    @Test
    public void cloneDatagram() {
        IonDatagram original = system().newDatagram();
        original.add().newList(new int[] {1, 2, 3});
        original.add().newList(new int[] {4, 5, 6});

        assertSelfCloneable(original);
    }

    @Test
    public void cloneValueWithEmptySpaceInItsAnnotationsArray() {
        IonInt original = system().newInt(123);
        original.addTypeAnnotation("abc"); // The annotation array grows to length 1
        original.addTypeAnnotation("def"); // The annotation array grows to length 2
        original.addTypeAnnotation("ghi"); // The annotation array grows to length 4, leaving a null at index 3

        assertSelfCloneable(original);
    }

    // TODO consider adding a general assertion for modify-after-clone. For now, the following test verifies that
    //  modifying a cloned value does not modify the original in a specific case.
    @Test
    public void modifyAfterCloneDoesNotChangeOriginal() {
        IonDatagram original = system().newDatagram();
        original.add().newList(new int[] {1, 2, 3});
        original.add().newList(new int[] {4, 5, 6});

        IonDatagram clone = original.clone();
        assertEqualsAndNotSame(original, clone);

        IonList clonedList1 = (IonList) clone.get(0);
        clonedList1.add().newInt(4);
        IonList clonedList2 = (IonList) clone.get(1);
        clonedList2.remove(0);

        assertNotEquals(original, clone);
    }

    @Test
    public void readOnlyIonStructMultithreadedTest() {
        // See: https://github.com/amazon-ion/ion-java/issues/629
        String ionStr = "{a:1,b:2,c:3,d:4,e:5,f:6}";

        IonStruct ionValue = (IonStruct)system().singleValue(ionStr);
        ionValue.makeReadOnly();

        for (int i=0; i<100; i++) {
            IonStruct clone = ionValue.clone();
            clone.makeReadOnly();

            List<CompletableFuture<Void>> waiting = new ArrayList<>();
            for (int j = 0; j < 4; j++) {
                waiting.add(CompletableFuture.runAsync(() -> {
                    for (int k = 0; k <= 100; k++) {
                        assertNotNull(clone.get("a"));
                    }
                }));
            }
            waiting.forEach(CompletableFuture::join);
        }
    }

    /**
     * @return the singleton IonSystem
     */
    private static _Private_IonSystem system()
    {
        return PRIVATE_ION_SYSTEM;
    }
}
