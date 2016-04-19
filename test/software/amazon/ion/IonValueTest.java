/*
 * Copyright 2011-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.ion;

import static java.lang.String.format;
import static software.amazon.ion.junit.IonAssert.assertAnnotations;

import org.junit.Ignore;
import org.junit.Test;
import software.amazon.ion.EmptySymbolException;
import software.amazon.ion.IonList;
import software.amazon.ion.IonStruct;
import software.amazon.ion.IonSystem;
import software.amazon.ion.IonValue;
import software.amazon.ion.ReadOnlyValueException;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.SymbolToken;
import software.amazon.ion.SystemSymbols;
import software.amazon.ion.junit.IonAssert;
import software.amazon.ion.system.IonTextWriterBuilder;
import software.amazon.ion.system.IonWriterBuilder.InitialIvmHandling;

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
        SymbolToken[] anns = v.getTypeAnnotationSymbols();
        assertArrayEquals(SymbolToken.EMPTY_ARRAY, anns);

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

        SymbolToken[] anns = v.getTypeAnnotationSymbols();
        assertEquals(ann, anns[0].getText());
        anns[0] = new FakeSymbolToken("ben", SymbolTable.UNKNOWN_SYMBOL_ID);

        SymbolToken[] anns2 = v.getTypeAnnotationSymbols();
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
        SymbolToken nameSym = systemSymtab.find(SystemSymbols.NAME);

        String nameOrig = nameSym.getText();
        String nameCopy = new String(nameOrig);
        assertNotSame(nameOrig, nameCopy);

        IonValue v = system().newNull();
        v.setTypeAnnotations(nameCopy);

        // TODO amznlabs/ion-java#21 fails because v doesn't have any symbol table at all
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


    @Test
    public void testCustomToString()
        throws Exception
    {
        IonValue v = system().singleValue("[hello,a::12]");

        assertEquals("[hello,a::12]",
                     v.toString(IonTextWriterBuilder.standard()));
        assertEquals("[\"hello\",12]",
                     v.toString(IonTextWriterBuilder.json()));
        assertEquals("$ion_1_0 [hello,a::12]",
                     v.toString(IonTextWriterBuilder.standard()
                                    .withInitialIvmHandling(InitialIvmHandling.ENSURE)));

        // TODO amznlabs/ion-java#57 determine if these really should be platform independent newlines
        final String pretty = format("%n[%n  hello,%n  a::12%n]");
        assertEquals(pretty,
                     v.toString(IonTextWriterBuilder.pretty()));
        assertEquals(pretty,
                     v.toPrettyString());
    }


    @Test
    public void testCloningSystemLookingValue()
    {
        IonList list = (IonList) oneValue("[$ion_1_0]");
        IonValue v = list.get(0);

        // Try using the same system.
        IonValue v2 = system().clone(v);
        IonAssert.assertIonEquals(v, v2);

        IonSystem system2 = newSystem(null);
        v2 = system2.clone(v);
        IonAssert.assertIonEquals(v, v2);
    }
}
