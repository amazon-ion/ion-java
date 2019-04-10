/*
 * Copyright 2010-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.ion.impl.lite;

import com.amazon.ion.IonTestCase;
import com.amazon.ion.IonValue;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.UnknownSymbolException;
import com.amazon.ion.impl._Private_Utils;
import org.junit.Test;

/**
 * Tests to ensure that the current emerged retention behaviors for SID's across the IonValueLite DOM are correctly
 * preserved with the symbol ID flag / state management. As of time of authoring the observed behavior for SID
 * management is as follows:
 *
 * -------------------------------------------------------------------------------------------------------
 * |                     | fieldName                 | annotations               | value (IonSymbolLite) |
 * |                     | text + SID | SID only     | text + SID | SID only     | text + SID | SID only |
 * -------------------------------------------------------------------------------------------------------
 * | <init>              | N/A        | N/A          | N/A        | N/A          | clear      | retain   |
 * | clone               | clear      | resolve/fail | clear      | retain       | clear      | clear (?)|
 * | clearSymbolIDValues | clear      | retain       | clear      | clear [null] | clear      | retain   |
 * | set                 | retain     | retain       | retain     | retain       | N/A        | N/A      |
 * | get                 | ephemeral  | ephemeral    | ephemeral  | N/A          | ephemeral  | memoizes |
 * -------------------------------------------------------------------------------------------------------
 *
 */
public class SIDPresentLifecycleTest extends IonTestCase {

    @Test
    public void testFieldsWithSID() {
        IonStringLite value = (IonStringLite) system().newString("Test Value");
        // a new value should create without SID presence as it has no annotations
        assertFalse(value._isSymbolIdPresent());
        value.setFieldNameSymbol(_Private_Utils.newSymbolToken("Symbol-With-Sid", 12));
        // setting a field with a SID means that SID Present now must be true
        assertTrue(value._isSymbolIdPresent());

        // test clone of the value results in no SID present
        assertFalse(value.clone()._isSymbolIdPresent());

        // verify detaching from the container clears the SID present flag
        value.detachFromContainer();
        assertFalse(value._isSymbolIdPresent());

        //verify that detaching still retains symbols - as SID is still present
        value.setFieldNameSymbol(_Private_Utils.newSymbolToken(13));
        assertTrue(value._isSymbolIdPresent());
        value.clearSymbolIDValues();
        // NOTE: SID is still present as clearSymbolIDValues doesn't clear SID-only tokens
        assertEquals(13, value.getFieldId());
        assertTrue(value._isSymbolIdPresent());
    }

    @Test
    public void testAnnotationsWithSIDs() {
        IonStringLite value = (IonStringLite) system().newString("Test Value 2");
        // a new value should create without any SID's as it has no annotations
        assertFalse(value._isSymbolIdPresent());
        value.setTypeAnnotationSymbols(
                _Private_Utils.newSymbolToken("Symbol-Without-SID", -1),
                _Private_Utils.newSymbolToken("Symbol-With-SID", 99));
        // setting an annotation with a SID means that SID present now must be true
        assertTrue(value._isSymbolIdPresent());

        // test clone of the value (given no *unresolvable* SID's) results in no SID's present
        assertFalse(value.clone()._isSymbolIdPresent());

        // ensure that setting annotations that don't have SIDs doesn't change the SID present status (need to wait for
        // an explicit action such as detatch from container or clone).
        value.setTypeAnnotationSymbols(_Private_Utils.newSymbolToken("Symbol-Without-Sid", -1));
        assertTrue(value._isSymbolIdPresent());

        // verify detaching from the container clears the SID Present flag
        value.detachFromContainer();
        assertFalse(value._isSymbolIdPresent());

        // verify that where SID context preserves through the clone due to SID context not being known (believed a bug)
        // that the SID present flag is *retained*
        value.setTypeAnnotationSymbols(_Private_Utils.newSymbolToken(101));
        assertTrue(value.clone()._isSymbolIdPresent());
    }

    @Test
    public void testPropagationAcrossGraph() {
        IonStructLite struct = (IonStructLite) system().newEmptyStruct();
        assertFalse(struct._isSymbolIdPresent());

        // 1. test addition of child by field WITHOUT SID causes no propagation
        IonStringLite keyValue1 = (IonStringLite) system().newString("Foo");
        struct.add(_Private_Utils.newSymbolToken("field_1", -1), keyValue1);
        assertFalse(struct._isSymbolIdPresent());
        assertFalse(keyValue1._isSymbolIdPresent());

        // 2. test addition of child by field WITH SID but also with field text causes no propagation
        // (struct only takes field name if present and strips SID)
        IonStringLite keyValue2 = (IonStringLite) system().newString("Bar");
        struct.add(_Private_Utils.newSymbolToken("field_2", 87), keyValue2);
        assertFalse(struct._isSymbolIdPresent());
        assertFalse(keyValue1._isSymbolIdPresent());
        assertFalse(keyValue2._isSymbolIdPresent());

        // 3. test addition of child by field with SID ONLY causes propagation due to retention of field SID
        IonStringLite keyValue3 = (IonStringLite) system().newString("Car");
        struct.add(_Private_Utils.newSymbolToken(76), keyValue3);
        assertTrue(struct._isSymbolIdPresent());
        assertFalse(keyValue1._isSymbolIdPresent());
        assertFalse(keyValue2._isSymbolIdPresent());
        assertTrue(keyValue3._isSymbolIdPresent());

        // 4. test explicitly annotating a field propagates
        IonStructLite struct2 = (IonStructLite) system().newEmptyStruct();
        IonStringLite keyValue4 = (IonStringLite) system().newString("But");
        struct2.add("field_1", keyValue4);
        keyValue4.setTypeAnnotationSymbols(_Private_Utils.newSymbolToken("Lah", 13));
        assertTrue(struct2._isSymbolIdPresent());

        // 5. test clone propagates clearing of status. NOTE - clone with an unresolvable SID for a field ID fails
        try {
            struct.clone();
            fail("clone was expected NOT to succeed due to behavior at time of writing where unresolvable SID's fail");
        } catch (UnknownSymbolException use) {
            // this is expected??! until someone fixed the TO DO in IonValueLite#getFieldName
        }

        // remove the field without SID such that the clone can be enacted.
        struct.remove_child(2);

        IonStructLite clonedStruct = struct.clone();
        assertFalse(clonedStruct._isSymbolIdPresent());
        for (IonValue value : clonedStruct) {
            assertFalse(((IonValueLite) value)._isSymbolIdPresent());
        }

        // 6. ensure clearing symbols propogates from root to impacted leaves
        // add back in field ID (SID) only child before clearing to test SID retained
        struct.add(_Private_Utils.newSymbolToken(76), keyValue3);
        struct.clearSymbolIDValues();
        assertTrue(struct._isSymbolIdPresent());
        assertFalse(keyValue1._isSymbolIdPresent());
        assertFalse(keyValue2._isSymbolIdPresent());
        assertTrue(keyValue3._isSymbolIdPresent());

        // 7. ensure that nested SID-only annotation clone behavior propogates to cloned root.
        IonStructLite outer = (IonStructLite) system().newEmptyStruct();
        IonListLite middle = (IonListLite) system().newEmptyList();
        IonIntLite inner = (IonIntLite) system().newInt(10);
        middle.add(inner);
        outer.put("foo", middle);
        // now add a SID only annotation to inner
        inner.setTypeAnnotationSymbols(_Private_Utils.newSymbolToken(99));
        // verify that all components are signaling SID present
        assertTrue(outer._isSymbolIdPresent());
        assertTrue(middle._isSymbolIdPresent());
        assertTrue(inner._isSymbolIdPresent());
        // conduct a clone and verify all cloned components have retained SID
        IonStructLite clonedOuter = outer.clone();
        assertTrue(clonedOuter._isSymbolIdPresent());
        IonListLite clonedMiddle = (IonListLite) outer.get("foo");
        assertTrue(clonedMiddle._isSymbolIdPresent());
        assertTrue(clonedMiddle.get_child(0)._isSymbolIdPresent());
    }

    @Test
    public void testSymbolValueLifecyle() {
        // test creation from SymbolToken
        IonSymbolLite symbolLite = (IonSymbolLite) system().newSymbol(_Private_Utils.newSymbolToken("version", 5));
        // SID isn't present as it is stripped if there is text present
        assertFalse(symbolLite._isSymbolIdPresent());

        IonListLite container1 = (IonListLite) system().newEmptyList();
        container1.add(symbolLite);
        // test propagation of context when symbol context is added
        assertEquals(5, symbolLite.getSymbolId());
        // as the getSymbolId memoizes SID into the entity SID present must be set
        assertTrue(symbolLite._isSymbolIdPresent());
        // check memoization propagates to container
        assertTrue(container1._isSymbolIdPresent());
        container1.clearSymbolIDValues();
        assertFalse(container1._isSymbolIdPresent());
        assertFalse(symbolLite._isSymbolIdPresent());

        IonSymbolLite symbolSIDOnly = (IonSymbolLite) system().newSymbol(_Private_Utils.newSymbolToken(321));
        // SID Present is preserved when only SID is present
        assertTrue(symbolSIDOnly._isSymbolIdPresent());
        IonListLite container2 = (IonListLite) system().newEmptyList();
        container2.add(symbolSIDOnly);
        assertTrue(container2._isSymbolIdPresent());
        // clear symbol ID values won't clear the symbolSIDOnly SID value - therefore container2 must still remain
        // marked as having a SID present
        container2.clearSymbolIDValues();
        assertTrue(container2._isSymbolIdPresent());
        assertTrue(symbolSIDOnly._isSymbolIdPresent());
    }

    @Test
    public void testSIDRetentionAcrossObjectHierarchy() {
        // field ID's can be retained on Container - so ensure these are retained.
        IonStructLite outer = (IonStructLite) system().newEmptyStruct();
        IonListLite middle = (IonListLite) system().newEmptyList();
        IonStringLite inner = (IonStringLite) system().newString("A");
        outer.add(_Private_Utils.newSymbolToken(321), middle);
        middle.add(inner);
        inner.setTypeAnnotationSymbols(_Private_Utils.newSymbolToken("B", 123));
        assertTrue(outer._isSymbolIdPresent());
        assertTrue(middle._isSymbolIdPresent());
        assertTrue(inner._isSymbolIdPresent());
        outer.clearSymbolIDValues();
        assertTrue(outer._isSymbolIdPresent());
        assertTrue(middle._isSymbolIdPresent());
        assertFalse(inner._isSymbolIdPresent());

        // field ID's can be retained on IonSymbolLite - so ensure these are retained as well.
        IonStructLite container = (IonStructLite) system().newEmptyStruct();
        IonSymbolLite symbol = (IonSymbolLite) system().newSymbol(_Private_Utils.newSymbolToken("Foo", 123));
        container.add(_Private_Utils.newSymbolToken(321), symbol);
        container.clearSymbolIDValues();
        assertEquals(SymbolTable.UNKNOWN_SYMBOL_ID, symbol.getSymbolId());
        assertEquals("Foo", symbol.stringValue());
        assertEquals(321, symbol.getFieldId());
        assertTrue(symbol._isSymbolIdPresent());

        assertTrue(container._isSymbolIdPresent());
    }
}
