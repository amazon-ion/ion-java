// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.Symtabs.sharedSymtabStruct;
import static com.amazon.ion.impl.IonImplUtils.EMPTY_STRING_ARRAY;
import static com.amazon.ion.impl.IonImplUtils.stringIterator;
import static com.amazon.ion.impl.SymbolTableTest.checkSharedTable;

import com.amazon.ion.InternedSymbol;
import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonTestCase;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SharedSymtabMaker;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.Symtabs;
import com.amazon.ion.SystemSymbols;
import com.amazon.ion.junit.Injected.Inject;
import java.io.IOException;
import org.junit.Test;


/**
 * @see SymbolTableTest
 */
public class SharedSymbolTableTest
    extends IonTestCase
{
    private static final String A = "a";
    private static final String OTHER_A = new String(A); // Force a new instance


    SharedSymtabMaker myMaker;

    public void setMaker(SharedSymtabMaker maker)
    {
        myMaker = maker;
    }

    @Inject("maker")
    public static final SharedSymtabMaker[] MAKERS =
        SharedSymtabMaker.values();


    private SymbolTable makeAbcTable()
    {
        String[] syms = { A, "b", "c" };
        SymbolTable st = myMaker.newSharedSymtab(system(), "ST", 1, syms);
        return st;
    }


    //-------------------------------------------------------------------------

    @Test
    public void testBasicSharedSymtabCreation()
    {
        String[] syms = { "a", "b", "c" };
        SymbolTable st = myMaker.newSharedSymtab(system(), "ST", 1, syms);
        checkSharedTable("ST", 1, syms, st);
    }

    @Test
    public void testDomSharedSymbolTable()
    {
        // JIRA ION-72
        String[] symbols = { "hello" };
        IonStruct struct = sharedSymtabStruct(system(), "foobar", 1, symbols);
        final SymbolTable table =  myMaker.newSharedSymtab(system(), struct);

        checkSharedTable("foobar", 1, new String[]{ "hello" }, table);
    }

    //-------------------------------------------------------------------------
    // Testing name field

    @Test
    public void testMalformedName()
    {
        testMalformedName(null);     // missing field
        testMalformedName(" \"\" "); // empty string
        testMalformedName("null.string");
        testMalformedName("null");
        testMalformedName("a_symbol");
        testMalformedName("159");
    }

    public void testMalformedName(String nameValue)
    {
        IonStruct s = sharedSymtabStruct(system(), "dummy", 1, "x");
        putParsedValue(s, SystemSymbols.NAME, nameValue);

        try
        {
            myMaker.newSharedSymtab(system(), s);
            fail("Expected exception");
        }
        catch (IonException e) {
            assertTrue(e.getMessage().contains("'name'"));
        }
    }


    //-------------------------------------------------------------------------
    // Testing version field

    @Test
    public void testMalformedVersion()
    {
        testMalformedVersion(null);
        testMalformedVersion("-1");
        testMalformedVersion("0");

        testMalformedVersion("null.int");
        testMalformedVersion("null");
        testMalformedVersion("a_symbol");
        testMalformedVersion("2.0");
    }

    public void testMalformedVersion(String versionValue)
    {
        IonStruct s = sharedSymtabStruct(system(), "ST", 1, "x", "y");
        putParsedValue(s, SystemSymbols.VERSION, versionValue);

        SymbolTable st = myMaker.newSharedSymtab(system(), s);
        checkSharedTable("ST", 1, new String[]{ "x", "y" }, st);
    }


    //-------------------------------------------------------------------------
    // Testing symbols field

    @Test
    public void testMalformedSymbols()
    {
        testMalformedSymbols(null);
        testMalformedSymbols("[]");
        testMalformedSymbols("null.list");
        testMalformedSymbols("{}");
        testMalformedSymbols("null.struct");
        testMalformedSymbols("null");
        testMalformedSymbols("a_symbol");
        testMalformedSymbols("100");
    }

    public void testMalformedSymbols(String symbolValue)
    {
        IonStruct s = sharedSymtabStruct(system(), "ST", 5);
        putParsedValue(s, SystemSymbols.SYMBOLS, symbolValue);

        SymbolTable st = myMaker.newSharedSymtab(system(), s);
        checkSharedTable("ST", 5, EMPTY_STRING_ARRAY, st);
    }


    @Test
    public void testMalformedSymbolEntry()
    {
        testMalformedSymbolEntry(" \"\" ");      // empty string
        testMalformedSymbolEntry("null.string");
        testMalformedSymbolEntry("null");
        testMalformedSymbolEntry("a_symbol");
        testMalformedSymbolEntry("100");
        testMalformedSymbolEntry("['''whee''']");
    }

    public void testMalformedSymbolEntry(String symbolValue)
    {
        IonStruct s = sharedSymtabStruct(system(), "ST", 5);
        IonValue entry = system().singleValue(symbolValue);
        s.put(SystemSymbols.SYMBOLS).newList(entry);

        SymbolTable st = myMaker.newSharedSymtab(system(), s);
        checkSharedTable("ST", 5, new String[]{ null }, st);

        assertEquals(1, st.getMaxId());
        assertEquals(null, st.findKnownSymbol(1));
        assertEquals("$1", st.findSymbol(1));
    }


    /**
     * We need to retain duplicate symbols in a shared symtab, because there
     * may be data encoded non-canonically that uses the higher sid.  If we
     * remove those duplicates then we can't decode such data.
     */
    @Test
    public void testSharedSymtabWithDuplicates()
    {
        SymbolTable st =
            myMaker.newSharedSymtab(system(), "ST", 1, "a", "b", "a", "c");

        checkSharedTable("ST", 1, new String[]{"a", "b", "a", "c"},
                         st);

        assertEquals(1, st.findSymbol("a"));  // lowest sid wins
        assertEquals(4, st.findSymbol("c"));

        // Now extend it
        catalog().putTable(st);
        SymbolTable st2 =
            system().newSharedSymbolTable("ST", 2, stringIterator("x"));

        assertEquals(1, st2.findSymbol("a"));  // lowest sid wins
        assertEquals(4, st2.findSymbol("c"));
        assertEquals(5, st2.findSymbol("x"));
    }

    // TODO test imports in shared symtabs

    /**
     * We need to normalize invalid values in a shared symtab, because there
     * may be data encoded non-canonically that uses the higher sid.  If we
     * remove those values then we can't decode such data.
     */
    @Test
    public void testSharedSymtabWithBadSymbolValues()
    {
        String symtab =
            SymbolTableTest.SharedSymbolTablePrefix +
            "{" +
            "  name:'''ST''', version:1," +
            "  symbols:['''a''', null, \"\", '''c''', 12]" +
            "}";

        SymbolTable st =
            myMaker.newSharedSymtab(system(), system().newReader(symtab));
        checkSharedTable("ST", 1, new String[]{"a", null, null, "c", null},
                         st);

        assertEquals(4, st.findSymbol("c"));
    }


    public void roundTrip(String serializedSymbolTable)
        throws IOException
    {
        IonReader reader = system().newReader(serializedSymbolTable);
        SymbolTable stFromReader = myMaker.newSharedSymtab(system(), reader);
        assertTrue(stFromReader.isSharedTable());

        StringBuilder buf = new StringBuilder();
        IonWriter out = system().newTextWriter(buf);
        stFromReader.writeTo(out);
        reader = system().newReader(buf.toString());
        SymbolTable reloaded = myMaker.newSharedSymtab(system(), reader);

        Symtabs.assertEqualSymtabs(stFromReader, reloaded);
    }

    @Test
    public void testSharedSymtabRoundTrip()
        throws Exception
    {
        for (String symtab : Symtabs.FRED_SERIALIZED)
        {
            if (symtab != null) roundTrip(symtab);
        }
        for (String symtab : Symtabs.GINGER_SERIALIZED)
        {
            if (symtab != null) roundTrip(symtab);
        }
    }


    //-------------------------------------------------------------------------
    // intern()

    @Test
    public void testInternKnownText()
    {
        assertNotSame(A, OTHER_A);

        SymbolTable st = makeAbcTable();

        InternedSymbol is = st.intern(OTHER_A);
        assertSame(A, is.getText());
        assertEquals(st.getImportedMaxId() + 1, is.getId());
    }

    @Test(expected = NullPointerException.class)
    public void testInternNull()
    {
        SymbolTable st = makeAbcTable();
        st.intern(null);
    }


    @Test(expected = IonException.class)
    public void testInternUnknownText()
    {
        SymbolTable st = makeAbcTable();
        st.intern("d");
    }


    //-------------------------------------------------------------------------
    // findInternedSymbol()

    @Test
    public void testFindInternedSymbol()
    {
        SymbolTable st = makeAbcTable();

        InternedSymbol is = st.find(OTHER_A);
        assertSame(A, is.getText());
        assertEquals(st.getImportedMaxId() + 1, is.getId());

        is = st.find("not there");
        assertNull(is);
    }

    @Test(expected = NullPointerException.class)
    public void testFindInternSymbolNull()
    {
        SymbolTable st = makeAbcTable();
        st.find(null);
    }
}
