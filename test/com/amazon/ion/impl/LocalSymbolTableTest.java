// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.SymbolTable.UNKNOWN_SYMBOL_ID;

import com.amazon.ion.InternedSymbol;
import com.amazon.ion.IonException;
import com.amazon.ion.IonTestCase;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.Symtabs;
import org.junit.Test;

/**
 *
 */
public class LocalSymbolTableTest
    extends IonTestCase
{
    private static final String A = "a";
    private static final String OTHER_A = new String(A); // Force a new instance

    private static final SymbolTable ST_FRED_V2 =
        Symtabs.CATALOG.getTable("fred", 2);
    private static final SymbolTable ST_GINGER_V1 =
        Symtabs.CATALOG.getTable("ginger", 1);



    private SymbolTable makeAbcTable(SymbolTable... imports)
    {
        SymbolTable st = system().newLocalSymbolTable(imports);
        st.addSymbol(A);
        st.addSymbol("b");
        st.addSymbol("c");
        return st;
    }


    //-------------------------------------------------------------------------
    // intern()

    public void internKnownText(SymbolTable st)
    {
        // Existing symbol from imports
        String fredSym = ST_FRED_V2.findKnownSymbol(3);
        InternedSymbol is = st.intern(new String(fredSym));
        assertSame(fredSym, is.getText());
        assertSame(st.getSystemSymbolTable().getMaxId() + 3, is.getId());

        String gingerSym = ST_GINGER_V1.findKnownSymbol(1);
        is = st.intern(new String(gingerSym));
        assertSame(gingerSym, is.getText());
        assertSame(st.getSystemSymbolTable().getMaxId() + ST_FRED_V2.getMaxId() + 1,
                   is.getId());

        // Existing local symbol
        is = st.intern(OTHER_A);
        assertSame(A, is.getText());
        assertEquals(st.getImportedMaxId() + 1, is.getId());
    }

    @Test
    public void testInternKnownText()
    {
        SymbolTable st = makeAbcTable(ST_FRED_V2, ST_GINGER_V1);
        internKnownText(st);
    }

    @Test
    public void testInternUnknownText()
    {
        SymbolTable st = makeAbcTable(ST_FRED_V2, ST_GINGER_V1);

        String D = "d";
        assertEquals(UNKNOWN_SYMBOL_ID, st.findSymbol(D));
        InternedSymbol is = st.intern(D);
        assertSame(D, is.getText());
        assertEquals(st.getImportedMaxId() + 4, is.getId());

        is = st.intern(new String(D)); // Force a new instance
        assertSame(D, is.getText());
        assertEquals(st.getImportedMaxId() + 4, is.getId());
    }

    @Test
    public void testInternKnownTextWhenReadOnly()
    {
        SymbolTable st = makeAbcTable(ST_FRED_V2, ST_GINGER_V1);
        st.makeReadOnly();
        internKnownText(st);
    }

    @Test(expected = IonException.class)
    public void testInternUnknownTextWhenReadOnly()
    {
        SymbolTable st = makeAbcTable(ST_FRED_V2, ST_GINGER_V1);
        st.makeReadOnly();
        st.intern("d");
    }


    @Test(expected = NullPointerException.class)
    public void testInternNull()
    {
        SymbolTable st = makeAbcTable();
        st.intern(null);
    }


    //-------------------------------------------------------------------------
    // findInternedSymbol()


    public void testFindInternedSymbol(SymbolTable st)
    {
        // Existing symbol from imports
        String fredSym = ST_FRED_V2.findKnownSymbol(3);
        InternedSymbol is = st.find(new String(fredSym));
        assertSame(fredSym, is.getText());
        assertSame(st.getSystemSymbolTable().getMaxId() + 3, is.getId());

        String gingerSym = ST_GINGER_V1.findKnownSymbol(1);
        is = st.find(new String(gingerSym));
        assertSame(gingerSym, is.getText());
        assertSame(st.getSystemSymbolTable().getMaxId() + ST_FRED_V2.getMaxId() + 1,
                   is.getId());

        // Existing local symbol
        is = st.find(OTHER_A);
        assertSame(A, is.getText());
        assertEquals(st.getImportedMaxId() + 1, is.getId());

        // Non-existing symbol
        assertEquals(null, st.find("not there"));
    }

    @Test
    public void testFindInternedSymbol()
    {
        SymbolTable st = makeAbcTable(ST_FRED_V2, ST_GINGER_V1);
        testFindInternedSymbol(st);
    }

    @Test
    public void testFindInternedSymbolWhenReadOnly()
    {
        SymbolTable st = makeAbcTable(ST_FRED_V2, ST_GINGER_V1);
        st.makeReadOnly();
        testFindInternedSymbol(st);
    }


    @Test(expected = NullPointerException.class)
    public void testFindInternSymbolNull()
    {
        SymbolTable st = makeAbcTable();
        st.find(null);
    }
}
