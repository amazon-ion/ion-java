// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import static com.amazon.ion.junit.IonAssert.assertAnnotations;

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


    /** Trap for ION-173 ION-144 */
    @Test @Ignore
    public void testRemoveDuplicateAnnotation()
    {
        IonValue v = system().singleValue("ann::ann::null");
        assertAnnotations(v, ann, ann);
    }
}
