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

import static com.amazon.ion.impl._Private_Utils.newSymbolToken;

import com.amazon.ion.system.SimpleCatalog;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;


public class CloneTest
    extends IonTestCase
{
    @Rule
    public ExpectedException thrown = ExpectedException.none();


    @Test
    public void testIonValueCloneWithUnknownSymbolText()
    {
        SymbolToken tok = newSymbolToken(99);
        IonSymbol original = system().newSymbol(tok);

        thrown.expect(UnknownSymbolException.class);
        thrown.expectMessage("$99");
        original.clone();
    }

    @Test
    public void testValueFactoryCloneWithUnknownSymbolText()
    {
        SymbolToken tok = newSymbolToken(99);
        IonSymbol original = system().newSymbol(tok);

        thrown.expect(UnknownSymbolException.class);
        thrown.expectMessage("$99");
        system().clone(original);
    }

    @Test
    public void testDifferentValueFactoryCloneWithUnknownSymbolText()
    {
        IonSystem otherSystem = newSystem(new SimpleCatalog());

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

        IonInt actual = original.clone();
        assertEquals(original, actual);
    }

    @Test
    public void testValueFactoryCloneWithUnknownAnnotationText()
    {
        SymbolToken tok = newSymbolToken(99);
        IonInt original = system().newInt(5);
        original.setTypeAnnotationSymbols(tok);

        IonInt actual = system().clone(original);
        assertEquals(original, actual);
    }

    @Test
    public void testDifferentValueFactoryCloneWithUnknownAnnotationText()
    {
        IonSystem otherSystem = newSystem(new SimpleCatalog());

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

        thrown.expect(UnknownSymbolException.class);
        thrown.expectMessage("$99");
        original.clone();
    }

    @Test
    public void testValueFactoryCloneWithUnknownFieldNameText()
    {
        SymbolToken tok = newSymbolToken(99);
        IonStruct original = system().newEmptyStruct();
        IonValue child = system().newNull();
        original.add(tok, child);

        // This works since the cloned child doesn't retain its field name.
        system().clone(child);

        thrown.expect(UnknownSymbolException.class);
        thrown.expectMessage("$99");
        system().clone(original);
    }

    @Test
    public void testDifferentValueFactoryCloneWithUnknownFieldNameText()
    {
        IonSystem otherSystem = newSystem(new SimpleCatalog());

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
        IonStruct clone = original.clone();
        assertTrue(clone.isEmpty());
        assertEquals(original, clone);
    }

    @Test
    public void cloneEmptyNestedContainer()
    {
        IonList original = system().newEmptyList();
        original.add().newEmptyStruct();
        IonList clone = original.clone();
        assertEquals(original, clone);
    }

    @Test
    public void cloneBasicStruct()
    {
        IonStruct original = system().newEmptyStruct();
        original.add("foo").newString("bar");
        IonStruct clone = original.clone();
        assertEquals(original, clone);
    }

    @Test
    public void cloneNestedContainer()
    {
        IonStruct original = system().newEmptyStruct();
        original.add("foo").newEmptyList().add().newString("bar");
        IonStruct clone = original.clone();
        assertEquals(original, clone);
    }

    @Test
    public void cloneMultipleElements()
    {
        IonList original = system().newList(new int[] {1, 2, 3});
        IonList clone = original.clone();
        assertEquals(original, clone);
    }

    @Test
    public void cloneDatagram() {
        IonDatagram original = system().newDatagram();
        original.add().newList(new int[] {1, 2, 3});
        original.add().newList(new int[] {4, 5, 6});
        IonDatagram clone = original.clone();
        assertEquals(original, clone);
    }

    @Test
    public void cloneValueWithEmptySpaceInItsAnnotationsArray() {
        IonInt original = system().newInt(123);
        original.addTypeAnnotation("abc"); // The annotation array grows to length 1
        original.addTypeAnnotation("def"); // The annotation array grows to length 2
        original.addTypeAnnotation("ghi"); // The annotation array grows to length 4, leaving a null at index 3
        IonInt clone = original.clone();
        assertEquals(original, clone);
    }
}
