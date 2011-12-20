// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import static com.amazon.ion.junit.IonAssert.assertAnnotations;

import com.amazon.ion.junit.IonAssert;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 */
public class IonValueTest
    extends IonTestCase
{
    private final String ann = "ann";
    private final String ben = "ben";

    @Test
    public void testGetFieldNameSymbol()
    {
        IonValue v = system().newNull();
        assertEquals(null, v.getFieldNameSymbol());

        v.makeReadOnly();
        assertEquals(null, v.getFieldNameSymbol());
    }


    @Test
    public void testGetTypeAnnotationsImmutability()
    {
        IonValue v = system().newNull();
        v.setTypeAnnotations(ann);

        String[] anns = v.getTypeAnnotations();
        assertEquals(ann, anns[0]);
        anns[0] = ben;

        anns = v.getTypeAnnotations();
        assertEquals(ann, anns[0]);
    }

    @Test
    public void testGetTypeAnnotationSymbols()
    {
        IonValue v = system().newNull();
        InternedSymbol[] anns = v.getTypeAnnotationSymbols();
        assertArrayEquals(InternedSymbol.EMPTY_ARRAY, anns);

        v.setTypeAnnotations(ann, ben);
        anns = v.getTypeAnnotationSymbols();
        assertEquals(ann, anns[0].getText());
        assertEquals(ben, anns[1].getText());
    }

    @Test
    public void testGetTypeAnnotationSymbolsImmutability()
    {
        IonValue v = system().newNull();
        v.setTypeAnnotations(ann);

        InternedSymbol[] anns = v.getTypeAnnotationSymbols();
        assertEquals(ann, anns[0].getText());
        anns[0] = new FakeInternedSymbol("ben", SymbolTable.UNKNOWN_SYMBOL_ID);

        InternedSymbol[] anns2 = v.getTypeAnnotationSymbols();
        assertEquals(ann, anns2[0].getText());
    }

    @Test
    public void testHasTypeAnnotation()
    {
        IonValue v = system().newNull();
        assertFalse(v.hasTypeAnnotation(null));
        assertFalse(v.hasTypeAnnotation(""));
        assertFalse(v.hasTypeAnnotation(ann));

        v.addTypeAnnotation(ann);
        assertFalse(v.hasTypeAnnotation(null));
        assertFalse(v.hasTypeAnnotation(""));
        assertTrue(v.hasTypeAnnotation(ann));
    }

    @Test(expected = ReadOnlyValueException.class)
    public void testSetTypeAnnotationsOnReadOnlyValue()
    {
        IonValue v = system().newNull();
        v.makeReadOnly();
        v.setTypeAnnotations(ann);
    }

    @Test
    public void testSetTypeAnnotations()
    {
        IonValue v = system().newNull();
        assertAnnotations(v);

        v.setTypeAnnotations(ann);
        assertAnnotations(v, ann);

        v.setTypeAnnotations(ann, ben);
        assertAnnotations(v, ann, ben);

        v.setTypeAnnotations(ann, ann);  // allow duplicates
        assertAnnotations(v, ann, ann);

        v.setTypeAnnotations(ben);       // shortening the list
        assertAnnotations(v, ben);

        v.setTypeAnnotations();
        assertAnnotations(v);

        v.setTypeAnnotations(ann);
        v.setTypeAnnotations((String[]) null);
        assertAnnotations(v);
    }

    @Test
    public void testSetTypeAnnotationInterning()
    {
        SymbolTable systemSymtab = system().getSystemSymbolTable();
        InternedSymbol nameSym = systemSymtab.find(SystemSymbols.NAME);

        String nameOrig = nameSym.getText();
        String nameCopy = new String(nameOrig);
        assertNotSame(nameOrig, nameCopy);

        IonValue v = system().newNull();
        v.setTypeAnnotations(nameCopy);

        // TODO ION-267 fails because v doesn't have any symbol table at all
//        assertSame(nameOrig, v.getTypeAnnotations()[0]);
//        assertSame(nameOrig, v.getTypeAnnotationSymbols()[0].getText());
    }

    @Test(expected = EmptySymbolException.class)
    public void testSetTypeAnnotationsWithNullString()
    {
        IonValue v = system().newNull();
        v.setTypeAnnotations(ann, null);
    }

    @Test(expected = EmptySymbolException.class)
    public void testSetTypeAnnotationsWithEmptyString()
    {
        IonValue v = system().newNull();
        v.setTypeAnnotations(ann, "");
    }

    @Test
    public void testSetTypeAnnotationsThenModifyingThem()
    {
        String[] anns = { ann, ben };
        IonValue v = system().newNull();
        v.setTypeAnnotations(anns);

        // Modifying the array we passed shouldn't affect the value
        anns[1] = ann;
        assertAnnotations(v, ann, ben);
    }

    //------------------------------------------------------------------------

    @Test(expected = ReadOnlyValueException.class)
    public void testAddTypeAnnotationsOnReadOnlyValue()
    {
        IonValue v = system().newNull();
        v.makeReadOnly();
        v.addTypeAnnotation(ann);
    }

    @Test @Ignore
    public void testAddAnnotationDuplicate()
    {
        IonValue v = system().newNull();
        assertAnnotations(v);

        v.addTypeAnnotation(ann);
        assertAnnotations(v, ann);

        v.addTypeAnnotation(ann);
        assertAnnotations(v, ann, ann);
    }

    @Test
    public void testCloneAndRetainDuplicateAnnotations()
    {
        IonStruct v = system().newNullStruct();
        v.setTypeAnnotations(ann, ann);

        IonStruct v2 = v.cloneAndRetain(ann);
        assertAnnotations(v2, ann, ann);

        IonStruct v3 = v.cloneAndRemove(ben);
        assertAnnotations(v3, ann, ann);
    }


    @Test
    public void testRemoveTypeAnnotation()
    {
        IonValue v = system().singleValue("null");
        v.removeTypeAnnotation(ann);
        assertAnnotations(v);

        v = system().singleValue("ann::null");
        v.removeTypeAnnotation(ben);
        assertAnnotations(v, ann);
        v.removeTypeAnnotation(null);
        assertAnnotations(v, ann);
        v.removeTypeAnnotation("");
        assertAnnotations(v, ann);

        v.removeTypeAnnotation(ann);
        assertAnnotations(v);

        v = system().singleValue("ann::ben::cam::null");
        v.removeTypeAnnotation(ben);
        assertAnnotations(v, ann, "cam");
        v.removeTypeAnnotation(ben);
        assertAnnotations(v, ann, "cam");
        v.removeTypeAnnotation("cam");
        assertAnnotations(v, ann);
        v.removeTypeAnnotation(ann);
        assertAnnotations(v);
    }


    @Test
    public void testRemoveTypeAnnotationNull()
    {
        IonValue v = system().newNull();
        v.removeTypeAnnotation(null);
        v.removeTypeAnnotation("");

        v.addTypeAnnotation(ann);
        v.removeTypeAnnotation(null);
        v.removeTypeAnnotation("");
        assertTrue(v.hasTypeAnnotation(ann));
    }

    /** Trap for ION-144 */
    @Test
    public void testRemoveDuplicateAnnotation()
    {
        IonValue v = system().singleValue("ann::ben::ann::null");
        assertAnnotations(v, ann, ben, ann);
        v.removeTypeAnnotation(ann);
        assertAnnotations(v, ben, ann);
    }

    @Test
    public void testDetachWithUnknownAnnotation()
    {
        IonList list = (IonList) system().singleValue("[$99::null]");
        IonValue child = list.get(0);
        child.removeFromContainer();

        IonAssert.assertIonEquals(system().singleValue("$99::null"), child);
    }

}
