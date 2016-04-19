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

package software.amazon.ion.impl;

import static software.amazon.ion.Symtabs.FRED_MAX_IDS;
import static software.amazon.ion.Symtabs.LOCAL_SYMBOLS_ABC;
import static software.amazon.ion.Symtabs.makeLocalSymtab;
import static software.amazon.ion.impl.PrivateUtils.EMPTY_STRING_ARRAY;
import static software.amazon.ion.impl.PrivateUtils.copyLocalSymbolTable;

import org.junit.Test;
import software.amazon.ion.IonException;
import software.amazon.ion.IonTestCase;
import software.amazon.ion.SubstituteSymbolTableException;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.SymbolToken;
import software.amazon.ion.Symtabs;
import software.amazon.ion.impl.LocalSymbolTable;
import software.amazon.ion.impl.SubstituteSymbolTable;

public class LocalSymbolTableTest
    extends IonTestCase
{
    private static final String A = "a";
    private static final String OTHER_A = new String(A); // Force a new instance

    private static final SymbolTable ST_FRED_V2 =
        Symtabs.CATALOG.getTable("fred", 2);
    private static final SymbolTable ST_GINGER_V1 =
        Symtabs.CATALOG.getTable("ginger", 1);


    //-------------------------------------------------------------------------
    // intern()

    public void internKnownText(SymbolTable st)
    {
        // Existing symbol from imports
        String fredSym = ST_FRED_V2.findKnownSymbol(3);
        SymbolToken tok = st.intern(new String(fredSym));
        assertSame(fredSym, tok.getText());
        assertSame(st.getSystemSymbolTable().getMaxId() + 3, tok.getSid());

        String gingerSym = ST_GINGER_V1.findKnownSymbol(1);
        tok = st.intern(new String(gingerSym));
        assertSame(gingerSym, tok.getText());
        assertSame(st.getSystemSymbolTable().getMaxId() + ST_FRED_V2.getMaxId() + 1,
                   tok.getSid());

        // Existing local symbol
        tok = st.intern(OTHER_A);
        assertSame(A, tok.getText());
        assertEquals(st.getImportedMaxId() + 1, tok.getSid());
    }

    @Test
    public void testInternKnownText()
    {
        SymbolTable st = makeLocalSymtab(system(), LOCAL_SYMBOLS_ABC,
                                         ST_FRED_V2, ST_GINGER_V1);
        internKnownText(st);
    }

    @Test
    public void testInternUnknownText()
    {
        SymbolTable st = makeLocalSymtab(system(), LOCAL_SYMBOLS_ABC,
                                         ST_FRED_V2, ST_GINGER_V1);

        String D = "d";
        checkUnknownSymbol(D, st);
        SymbolToken tok = st.intern(D);
        assertSame(D, tok.getText());
        assertEquals(st.getImportedMaxId() + 4, tok.getSid());

        tok = st.intern(new String(D)); // Force a new instance
        assertSame(D, tok.getText());
        assertEquals(st.getImportedMaxId() + 4, tok.getSid());
    }

    @Test
    public void testInternKnownTextWhenReadOnly()
    {
        SymbolTable st = makeLocalSymtab(system(), LOCAL_SYMBOLS_ABC,
                                         ST_FRED_V2, ST_GINGER_V1);
        st.makeReadOnly();
        internKnownText(st);
    }

    @Test(expected = IonException.class)
    public void testInternUnknownTextWhenReadOnly()
    {
        SymbolTable st = makeLocalSymtab(system(), LOCAL_SYMBOLS_ABC,
                                         ST_FRED_V2, ST_GINGER_V1);
        st.makeReadOnly();
        st.intern("d");
    }


    @Test(expected = NullPointerException.class)
    public void testInternNull()
    {
        SymbolTable st = makeLocalSymtab(system(), LOCAL_SYMBOLS_ABC);
        st.intern(null);
    }

    //-------------------------------------------------------------------------
    // Tests for _Private_DmsdkUtils.makeCopy(SymbolTable)

    @Test
    public void testCopyLSTWithSubstitutedImports()
    {
        SubstituteSymbolTable subFred2 =
            new SubstituteSymbolTable("fred", 2, FRED_MAX_IDS[2]);
        Symtabs.register("fred", 1, catalog());

        SymbolTable localSymtab =
            makeLocalSymtab(system(), EMPTY_STRING_ARRAY, subFred2);

        SymbolTable[] imports = localSymtab.getImportedTables();
        assertTrue(imports[0].isSubstitute());

        myExpectedException.expect(SubstituteSymbolTableException.class);
        copyLocalSymbolTable(localSymtab); // method under test
    }

    @Test
    public void testCopyLSTOnSystemSymtab()
    {
        SymbolTable systemSymtab = system().getSystemSymbolTable();

        myExpectedException.expect(IllegalArgumentException.class);
        copyLocalSymbolTable(systemSymtab); // method under test
    }

    @Test
    public void testCopyLSTOnImport()
    {
        SymbolTable fred1 = Symtabs.register("fred", 1, catalog());

        myExpectedException.expect(IllegalArgumentException.class);
        copyLocalSymbolTable(fred1); // method under test
    }

    @Test
    public void testCopyLSTOnSubstitutedImport()
    {
        SubstituteSymbolTable subFred2 =
            new SubstituteSymbolTable("fred", 2, FRED_MAX_IDS[2]);

        myExpectedException.expect(IllegalArgumentException.class);
        copyLocalSymbolTable(subFred2); // method under test
    }

    @Test
    public void testCopyLST()
    {
        SymbolTable orig = makeLocalSymtab(system(), LOCAL_SYMBOLS_ABC);
        SymbolTable copy = copyLocalSymbolTable(orig); // method under test

        assertNotSame(orig, copy);

        assertTrue(copy.isLocalTable());

        int systemMaxId = orig.getSystemSymbolTable().getMaxId();
        checkSymbol("a", systemMaxId + 1, copy.find("a"));
        checkSymbol("b", systemMaxId + 2, copy.find("b"));
        checkSymbol("c", systemMaxId + 3, copy.find("c"));
    }

    @Test
    public void testCopyLSTThenAddSymbols()
    {
        SymbolTable orig = makeLocalSymtab(system(), LOCAL_SYMBOLS_ABC);
        SymbolTable copy = copyLocalSymbolTable(orig); // method under test

        // interning in copy doesn't modify orig
        assertNull(orig.find("amazon"));
        copy.intern("amazon");
        assertNull(orig.find("amazon"));

        // interning in orig doesn't modify copy
        assertNull(copy.find("dotcom"));
        orig.intern("dotcom");
        assertNull(copy.find("dotcom"));
    }

    @Test
    public void testCopyLSTWithImports()
    {
        SymbolTable fred1 = Symtabs.register("fred", 1, catalog());
        SymbolTable orig = makeLocalSymtab(system(), LOCAL_SYMBOLS_ABC, fred1);

        SymbolTable copy = copyLocalSymbolTable(orig);  // method under test

        SymbolTable[] origImports = orig.getImportedTables();
        SymbolTable[] copyImports = copy.getImportedTables();

        assertNotSame(origImports, copyImports);

        // check that the imported SymbolTables point to the same refs.
        assertArrayContentsSame(origImports, copyImports);
    }

    @Test
    public void testCopyLSTNotReadOnly()
    {
        SymbolTable orig = makeLocalSymtab(system(), LOCAL_SYMBOLS_ABC);

        // original is not read only
        assertFalse(orig.isReadOnly());
        SymbolTable copy = copyLocalSymbolTable(orig); // method under test
        assertFalse(copy.isReadOnly());

        // original is read only
        orig.makeReadOnly();
        copy = copyLocalSymbolTable(orig); // method under test
        assertFalse(copy.isReadOnly());
    }

    @Test
    public void testCopyLSTIsSymtabsExtends()
    {
        SymbolTable orig = makeLocalSymtab(system(), LOCAL_SYMBOLS_ABC);
        SymbolTable copy = copyLocalSymbolTable(orig);  // method under test

        assertTrue(((LocalSymbolTable) copy).symtabExtends(orig));
        assertTrue(((LocalSymbolTable) orig).symtabExtends(copy));
    }


    //-------------------------------------------------------------------------
    // find()


    public void testFindSymbolToken(SymbolTable st)
    {
        // Existing symbol from imports
        String fredSym = ST_FRED_V2.findKnownSymbol(3);
        SymbolToken tok = st.find(new String(fredSym));
        assertSame(fredSym, tok.getText());
        assertSame(st.getSystemSymbolTable().getMaxId() + 3, tok.getSid());

        String gingerSym = ST_GINGER_V1.findKnownSymbol(1);
        tok = st.find(new String(gingerSym));
        assertSame(gingerSym, tok.getText());
        assertSame(st.getSystemSymbolTable().getMaxId() + ST_FRED_V2.getMaxId() + 1,
                   tok.getSid());

        // Existing local symbol
        tok = st.find(OTHER_A);
        assertSame(A, tok.getText());
        assertEquals(st.getImportedMaxId() + 1, tok.getSid());

        // Non-existing symbol
        assertEquals(null, st.find("not there"));
    }

    @Test
    public void testFindSymbolToken()
    {
        SymbolTable st = makeLocalSymtab(system(), LOCAL_SYMBOLS_ABC,
                                         ST_FRED_V2, ST_GINGER_V1);
        testFindSymbolToken(st);
    }

    @Test
    public void testFindSymbolTokenWhenReadOnly()
    {
        SymbolTable st = makeLocalSymtab(system(), LOCAL_SYMBOLS_ABC,
                                         ST_FRED_V2, ST_GINGER_V1);
        st.makeReadOnly();
        testFindSymbolToken(st);
    }


    @Test(expected = NullPointerException.class)
    public void testFindSymbolTokenNull()
    {
        SymbolTable st = makeLocalSymtab(system(), LOCAL_SYMBOLS_ABC);
        st.find(null);
    }

    @Test
    public void testVersion()
    {
        SymbolTable st = makeLocalSymtab(system(), LOCAL_SYMBOLS_ABC);
        assertEquals(0, st.getVersion());
    }
}
