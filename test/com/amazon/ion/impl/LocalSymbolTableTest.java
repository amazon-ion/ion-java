// Copyright (c) 2011-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonException;
import com.amazon.ion.IonTestCase;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
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
        SymbolTable st = makeAbcTable(ST_FRED_V2, ST_GINGER_V1);
        internKnownText(st);
    }

    @Test
    public void testInternUnknownText()
    {
        SymbolTable st = makeAbcTable(ST_FRED_V2, ST_GINGER_V1);

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
        SymbolTable st = makeAbcTable(ST_FRED_V2, ST_GINGER_V1);
        testFindSymbolToken(st);
    }

    @Test
    public void testFindSymbolTokenWhenReadOnly()
    {
        SymbolTable st = makeAbcTable(ST_FRED_V2, ST_GINGER_V1);
        st.makeReadOnly();
        testFindSymbolToken(st);
    }


    @Test(expected = NullPointerException.class)
    public void testFindSymbolTokenNull()
    {
        SymbolTable st = makeAbcTable();
        st.find(null);
    }
}
