// Copyright (c) 2007-2015 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import static com.amazon.ion.impl._Private_Utils.newSymbolToken;

import com.amazon.ion.system.SimpleCatalog;
import org.junit.Test;


public class CloneTest
    extends IonTestCase
{
    @Test
    public void testIonValueCloneWithUnknownSymbolText()
    {
        SymbolToken tok = newSymbolToken(99);
        IonSymbol original = system().newSymbol(tok);
        try {
            original.clone();
            fail("Expected UnknownSymbolException");
        }
        catch (UnknownSymbolException e) { }
    }

    @Test
    public void testValueFactoryCloneWithUnknownSymbolText()
    {
        SymbolToken tok = newSymbolToken(99);
        IonSymbol original = system().newSymbol(tok);
        try {
            system().clone(original);
            fail("Expected UnknownSymbolException");
        }
        catch (UnknownSymbolException e) { }
    }

    @Test
    public void testDifferentValueFactoryCloneWithUnknownSymbolText()
    {
        for (DomType domType : DomType.values())
        {
            SymbolToken tok = newSymbolToken(99);
            IonSymbol original = system().newSymbol(tok);
            // TODO ION-339 An UnknownSymbolException is expected here, but
            // it isn't thrown.
            IonSystem otherSystem = newSystem(new SimpleCatalog(), domType);
            IonSymbol copy = otherSystem.clone(original);
            // If we don't fail we should at least retain the SID.
            assertEquals(99, copy.symbolValue().getSid());
        }
    }


    @Test
    public void testIonValueCloneWithUnknownAnnotationText()
    {
        SymbolToken tok = newSymbolToken(99);
        IonInt original = system().newInt(5);
        original.setTypeAnnotationSymbols(tok);
        try {
            IonInt copy = original.clone();
//            fail("Expected UnknownSymbolException"); // TODO
            // If we don't fail we should at least retain the SID.
            assertEquals(99, copy.getTypeAnnotationSymbols()[0].getSid());
        }
        catch (UnknownSymbolException e) { }
    }

    @Test
    public void testValueFactoryCloneWithUnknownAnnotationText()
    {
        SymbolToken tok = newSymbolToken(99);
        IonInt original = system().newInt(5);
        original.setTypeAnnotationSymbols(tok);
        try {
            IonInt copy = system().clone(original);
//            fail("Expected UnknownSymbolException"); // TODO
            // If we don't fail we should at least retain the SID.
            assertEquals(99, copy.getTypeAnnotationSymbols()[0].getSid());
        }
        catch (UnknownSymbolException e) { }
    }

    @Test
    public void testDifferentValueFactoryCloneWithUnknownAnnotationText()
    {
        for (DomType domType : DomType.values())
        {
            SymbolToken tok = newSymbolToken(99);
            IonInt original = system().newInt(5);
            original.setTypeAnnotationSymbols(tok);
            // TODO ION-339 An UnknownSymbolException is expected here, but
            // it isn't thrown.
            IonSystem otherSystem = newSystem(new SimpleCatalog(), domType);
            IonInt copy = otherSystem.clone(original);
            // If we don't fail we should at least retain the SID.
            assertEquals(99, copy.getTypeAnnotationSymbols()[0].getSid());
        }
    }


    @Test
    public void testIonValueCloneWithUnknownFieldNameText()
    {
        SymbolToken tok = newSymbolToken(99);
        IonStruct original = system().newEmptyStruct();
        original.add(tok, system().newNull());
        try {
            original.clone();
            fail("Expected UnknownSymbolException");
        }
        catch (UnknownSymbolException e) { }
    }

    @Test
    public void testValueFactoryCloneWithUnknownFieldNameText()
    {
        SymbolToken tok = newSymbolToken(99);
        IonStruct original = system().newEmptyStruct();
        original.add(tok, system().newNull());
        try {
            system().clone(original);
            fail("Expected UnknownSymbolException");
        }
        catch (UnknownSymbolException e) { }
    }

    @Test
    public void testDifferentValueFactoryCloneWithUnknownFieldNameText()
    {
        for (DomType domType : DomType.values())
        {
            SymbolToken tok = newSymbolToken(99);
            IonStruct original = system().newEmptyStruct();
            original.add(tok, system().newNull());
            // TODO ION-339 An UnknownSymbolException is expected here, but
            // it isn't thrown.
            IonSystem otherSystem = newSystem(new SimpleCatalog(), domType);
            IonStruct copy = otherSystem.clone(original);
            // If we don't fail we should at least retain the SID.
            assertEquals(99, copy.iterator().next().getFieldNameSymbol().getSid());
        }
    }
}
