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
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

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
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    /**
     * Verifies that a read-only struct without nested values can be accessed via multiple threads concurrently.
     * @param beforeMakingReadOnly logic to be applied to each struct before it is made read-only.
     */
    private void testReadOnlyIonStructMultithreadedAccess(Function<IonStruct, IonStruct> beforeMakingReadOnly) {
        // See: https://github.com/amazon-ion/ion-java/issues/629
        String ionStr = "{a:1,b:2,c:3,d:4,e:5,f:6}";

        IonStruct struct = beforeMakingReadOnly.apply((IonStruct) system().singleValue(ionStr));
        struct.makeReadOnly();

        int numberOfTasks = 2;
        List<CompletableFuture<Void>> waiting = new ArrayList<>();
        for (int task = 0; task < numberOfTasks; task++) {
            waiting.add(CompletableFuture.runAsync(() -> assertNotNull(struct.get("a"))));
        }
        waiting.forEach(CompletableFuture::join);
    }

    @RepeatedTest(20)
    public void readOnlyIonStructMultithreadedAccessSucceeds() {
        testReadOnlyIonStructMultithreadedAccess(s -> s);
    }

    @RepeatedTest(20)
    public void readOnlyClonedIonStructMultithreadedAccessSucceeds() {
        testReadOnlyIonStructMultithreadedAccess(IonStruct::clone);
    }

    /**
     * Creates a new IonStruct with several children, including two large nested structs. The large children will
     * take longer to clone and sequentially access, giving more time for race conditions to surface in multithreaded
     * contexts if the code is insufficiently protected.
     * @return a new IonStruct.
     */
    private IonStruct createIonStructWithLargeChildStructs() {
        IonSystem ionSystem = system();
        IonStruct attribute = ionSystem.newEmptyStruct();
        attribute.put("a", ionSystem.newSymbol("foo"));
        attribute.put("b", ionSystem.newSymbol("foo"));
        attribute.put("c", ionSystem.newSymbol("foo"));
        IonStruct struct = ionSystem.newEmptyStruct();
        struct.put("a", ionSystem.newSymbol("foo"));
        struct.put("b", ionSystem.newSymbol("foo"));
        struct.put("c", ionSystem.newSymbol("foo"));
        struct.put("number", ionSystem.newDecimal(1e-2));
        IonStruct child1 = ionSystem.newEmptyStruct();
        for (int i = 0; i < 8; i++) {
            child1.put("field" + i, attribute.clone());
        }
        struct.put("child1", child1);
        IonStruct child2 = ionSystem.newEmptyStruct();
        for (int i = 0; i < 1000; i++) {
            child2.put("beforeTarget" + i, attribute.clone());
        }
        child2.put("target", attribute);
        for (int i = 0; i < 400; i++) {
            child2.put("afterTarget" + i, attribute.clone());
        }
        struct.put("child2", child2);
        return struct;
    }

    /**
     * Verifies that a read-only struct's large nested struct can be accessed via multiple threads concurrently.
     * @param prepareStruct logic to be applied to the struct. The returned struct's "child2" struct must be read-only.
     */
    private void testReadOnlyIonStructMultithreadedNestedAccess(Function<IonStruct, IonStruct> prepareStruct) {
        IonStruct struct = prepareStruct.apply(createIonStructWithLargeChildStructs());
        int numberOfTasks = 2;
        AtomicInteger success = new AtomicInteger();
        AtomicInteger error = new AtomicInteger();
        List<CompletableFuture<Void>> tasks = new ArrayList<>();

        for (int task = 0; task < numberOfTasks; task++) {
            tasks.add(CompletableFuture.runAsync(
                () -> {
                    IonStruct child2 = (IonStruct) struct.get("child2");
                    String value = child2 == null || child2.get("target") == null
                        ? null
                        : ((IonStruct) child2.get("target")).get("a").toString();

                    if (value == null) {
                        error.getAndIncrement();
                    }
                    success.getAndIncrement();
                }
            ));
        }
        tasks.forEach(CompletableFuture::join);

        assertTrue(success.get() > 0);
        assertEquals(0, error.get());
    }

    @RepeatedTest(20)
    public void readOnlyIonStructMultithreadedNestedAccessSucceeds() {
        testReadOnlyIonStructMultithreadedNestedAccess(s -> {
            s.makeReadOnly();
            return s;
        });
    }

    @RepeatedTest(20)
    public void readOnlyClonedIonStructMultithreadedNestedAccessSucceeds() {
        testReadOnlyIonStructMultithreadedNestedAccess(s -> {
            IonStruct clone = s.clone();
            clone.makeReadOnly();
            return clone;
        });
    }

    @RepeatedTest(20)
    public void readOnlyNestedIonStructMultithreadedAccessSucceeds() {
        testReadOnlyIonStructMultithreadedNestedAccess(s -> {
            s.get("child2").makeReadOnly();
            return s;
        });
    }

    @RepeatedTest(20)
    public void readOnlyClonedNestedIonStructMultithreadedAccessSucceeds() {
        testReadOnlyIonStructMultithreadedNestedAccess(s -> {
            IonStruct clone = s.clone();
            clone.get("child2").makeReadOnly();
            return clone;
        });
    }

    /**
     * @return the singleton IonSystem
     */
    private static _Private_IonSystem system()
    {
        return PRIVATE_ION_SYSTEM;
    }
}
