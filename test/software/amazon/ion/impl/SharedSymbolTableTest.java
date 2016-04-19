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

import static software.amazon.ion.Symtabs.sharedSymtabStruct;
import static software.amazon.ion.impl.PrivateUtils.EMPTY_STRING_ARRAY;
import static software.amazon.ion.impl.PrivateUtils.stringIterator;
import static software.amazon.ion.impl.SymbolTableTest.checkSharedTable;

import java.io.IOException;
import org.junit.Test;
import software.amazon.ion.IonException;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonStruct;
import software.amazon.ion.IonTestCase;
import software.amazon.ion.IonValue;
import software.amazon.ion.IonWriter;
import software.amazon.ion.SharedSymtabMaker;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.SymbolToken;
import software.amazon.ion.Symtabs;
import software.amazon.ion.SystemSymbols;
import software.amazon.ion.junit.Injected.Inject;


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
        testMalformedSymbols(null);                     // null
        testMalformedSymbols("true");                   // boolean
        testMalformedSymbols("100");                    // integer
        testMalformedSymbols("0.123");                  // decimal
        testMalformedSymbols("-0.12e4");                // float
        testMalformedSymbols("2013-05-09");             // timestamp
        testMalformedSymbols("\"string\"");             // string
        testMalformedSymbols("a_symbol");               // symbol
        testMalformedSymbols("{{MTIz}}");               // blob
        testMalformedSymbols("{{'''clob_content'''}}"); // clob
        testMalformedSymbols("{a:123}");                // struct
        testMalformedSymbols("[]");                     // empty list
        testMalformedSymbols("(a b c)");                // sexp
        testMalformedSymbols("null.struct");            // null.struct
        testMalformedSymbols("null.list");              // null.list
        testMalformedSymbols("null.sexp");              // null.sexp
        testMalformedSymbols("null");                   // string
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
        testMalformedSymbolEntry("null");                        // null
        testMalformedSymbolEntry("true");                        // boolean
        testMalformedSymbolEntry("100");                         // integer
        testMalformedSymbolEntry("0.123");                       // decimal
        testMalformedSymbolEntry("-0.12e4");                     // float
        testMalformedSymbolEntry("2013-05-09");                  // timestamp
        testMalformedSymbolEntry("\"\"");                        // empty string
        testMalformedSymbolEntry("a_symbol");                    // symbol
        testMalformedSymbolEntry("{{MTIz}}");                    // blob
        testMalformedSymbolEntry("{{'''clob_content'''}}");      // clob
        testMalformedSymbolEntry("{a:123}");                     // struct
        testMalformedSymbolEntry("[a, b, c]");                   // list
        testMalformedSymbolEntry("(a b c)");                     // sexp
        testMalformedSymbolEntry("null.string");                 // null.string
        testMalformedSymbolEntry("['''whee''']");                // string nested inside list
    }

    public void testMalformedSymbolEntry(String symbolValue)
    {
        IonStruct s = sharedSymtabStruct(system(), "ST", 5);
        IonValue entry = system().singleValue(symbolValue);
        s.put(SystemSymbols.SYMBOLS).newList(entry);

        SymbolTable st = myMaker.newSharedSymtab(system(), s);
        checkSharedTable("ST", 5, new String[]{ null }, st);

        assertEquals(1, st.getMaxId());
        checkUnknownSymbol(1, st);
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

        checkSymbol("a", 1, st);  // lowest sid wins
        checkSymbol("b", 2, st);
        checkSymbol("a", 3, /* dupe */ true, st);
        checkSymbol("c", 4, st);

        // Now extend it
        catalog().putTable(st);
        SymbolTable st2 =
            system().newSharedSymbolTable("ST", 2, stringIterator("x"));

        checkSymbol("a", 1, st2);  // lowest sid wins
        checkSymbol("c", 4, st2);
        checkSymbol("x", 5, st2);

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

        checkSymbol("c", 4, st);
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

        SymbolToken tok = st.intern(OTHER_A);
        assertSame(A, tok.getText());
        assertEquals(st.getImportedMaxId() + 1, tok.getSid());
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
    // find()

    @Test
    public void testFindSymbolToken()
    {
        SymbolTable st = makeAbcTable();

        SymbolToken tok = st.find(OTHER_A);
        assertSame(A, tok.getText());
        assertEquals(st.getImportedMaxId() + 1, tok.getSid());

        tok = st.find("not there");
        assertNull(tok);
    }

    @Test(expected = NullPointerException.class)
    public void testFindSymbolTokenNull()
    {
        SymbolTable st = makeAbcTable();
        st.find(null);
    }
}
