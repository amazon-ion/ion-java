// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.lite;

import com.amazon.ion.IonTestCase;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.impl._Private_Utils;
import org.junit.Test;

public class IonValueLiteCloneTest extends IonTestCase {

    @Test
    public void cloneWithEmptyAnnotationArray() {
        IonValueLite value = (IonValueLite) system().newInt(1);
        value.setTypeAnnotationSymbols();
        assertSame(SymbolToken.EMPTY_ARRAY, value._annotations);
        assertEmptyClone(value);
    }

    @Test
    public void cloneAfterClearingAnnotations() {
        IonValueLite value = (IonValueLite) system().newInt(1);
        value.addTypeAnnotation("first");
        value.addTypeAnnotation("second");
        value.addTypeAnnotation("third");
        value.clearTypeAnnotations();
        assertTrue(value._annotations.length > 0);
        assertEmptyClone(value);
    }

    @Test
    public void cloneAfterRemovingLastAnnotation() {
        IonValueLite value = (IonValueLite) system().newInt(1);
        value.addTypeAnnotation("first");
        value.removeTypeAnnotation("first");
        assertTrue(value._annotations.length > 0);
        assertEmptyClone(value);
    }

    @Test
    public void cloneWithoutAnnotationArray() {
        IonValueLite value = (IonValueLite) system().newInt(1);
        IonValueLite clone = (IonValueLite) value.clone();
        assertNull(value._annotations);
        assertNull(clone._annotations);
        clone.addTypeAnnotation("clone");
        assertFalse(value.hasTypeAnnotation("clone"));
    }

    @Test
    public void cloneWithAnnotationsDoesNotShareArray() {
        IonValueLite value = (IonValueLite) system().newInt(1);
        value.addTypeAnnotation("first");
        value.addTypeAnnotation("second");
        value.addTypeAnnotation("third");
        IonValueLite clone = (IonValueLite) value.clone();
        assertNotSame(value._annotations, clone._annotations);
        assertArrayEquals(value.getTypeAnnotations(), clone.getTypeAnnotations());
        clone.removeTypeAnnotation("second");
        value.addTypeAnnotation("original");
        assertTrue(value.hasTypeAnnotation("second"));
        assertFalse(clone.hasTypeAnnotation("original"));
    }

    @Test
    public void cloneStopsAtFirstNullAnnotation() {
        IonValueLite value = (IonValueLite) system().newInt(1);
        value.setTypeAnnotationSymbols(
            _Private_Utils.newSymbolToken("first", -1),
            null,
            _Private_Utils.newSymbolToken("ignored", -1));
        IonValueLite clone = (IonValueLite) value.clone();
        clone.addTypeAnnotation("second");
        assertArrayEquals(new String[] {"first", "second"}, clone.getTypeAnnotations());
        assertArrayEquals(new String[] {"first"}, value.getTypeAnnotations());
    }

    private void assertEmptyClone(IonValueLite value) {
        IonValueLite clone = (IonValueLite) value.clone();
        assertSame(SymbolToken.EMPTY_ARRAY, clone._annotations);
        assertEquals(0, clone.getTypeAnnotations().length);
        clone.addTypeAnnotation("clone");
        value.addTypeAnnotation("original");
        assertFalse(value.hasTypeAnnotation("clone"));
        assertFalse(clone.hasTypeAnnotation("original"));
    }
}
