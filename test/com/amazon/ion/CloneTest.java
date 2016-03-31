// Copyright (c) 2007-2015 Amazon.com, Inc.  All rights reserved.

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

        // TODO ION-339 An UnknownSymbolException is expected here, but
        // it isn't thrown.
        IonSymbol copy = otherSystem.clone(original);

        // If we don't fail we should at least retain the SID.
        assertEquals(99, copy.symbolValue().getSid());
    }

    static final boolean DEFECTIVE_CLONE_OF_UNKNOWN_ANNOTATION_TEXT = true;

    @Test
    public void testIonValueCloneWithUnknownAnnotationText()
    {
        SymbolToken tok = newSymbolToken(99);
        IonInt original = system().newInt(5);
        original.setTypeAnnotationSymbols(tok);

        if (! DEFECTIVE_CLONE_OF_UNKNOWN_ANNOTATION_TEXT) {
            thrown.expect(UnknownSymbolException.class);
            thrown.expectMessage("$99");
        }

        IonInt actual = original.clone();
        if (DEFECTIVE_CLONE_OF_UNKNOWN_ANNOTATION_TEXT) {
            assertEquals(original, actual);
        }
    }

    @Test
    public void testValueFactoryCloneWithUnknownAnnotationText()
    {
        SymbolToken tok = newSymbolToken(99);
        IonInt original = system().newInt(5);
        original.setTypeAnnotationSymbols(tok);

        if (! DEFECTIVE_CLONE_OF_UNKNOWN_ANNOTATION_TEXT) {
            thrown.expect(UnknownSymbolException.class);
            thrown.expectMessage("$99");
        }

        IonInt actual = system().clone(original);
        if (DEFECTIVE_CLONE_OF_UNKNOWN_ANNOTATION_TEXT) {
            assertEquals(original, actual);
        }
    }

    @Test
    public void testDifferentValueFactoryCloneWithUnknownAnnotationText()
    {
        IonSystem otherSystem = newSystem(new SimpleCatalog());

        SymbolToken tok = newSymbolToken(99);
        IonInt original = system().newInt(5);
        original.setTypeAnnotationSymbols(tok);

        // TODO ION-339 An UnknownSymbolException is expected here, but
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

        // TODO ION-339 An UnknownSymbolException is expected here, but
        // it isn't thrown.
        IonStruct copy = otherSystem.clone(original);

        // If we don't fail we should at least retain the SID.
        assertEquals(99, copy.iterator().next().getFieldNameSymbol().getSid());
    }
}
