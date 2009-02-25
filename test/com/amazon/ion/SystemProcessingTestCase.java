// Copyright (c) 2008-2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import static com.amazon.ion.Symtabs.LocalSymbolTablePrefix;
import static com.amazon.ion.SystemSymbolTable.ION_1_0;
import static com.amazon.ion.SystemSymbolTable.ION_1_0_MAX_ID;

import com.amazon.ion.impl.SymbolTableTest;
import com.amazon.ion.system.SimpleCatalog;



/**
 *
 */
public abstract class SystemProcessingTestCase
    extends IonTestCase
{
    protected abstract void prepare(String text)
        throws Exception;

    protected abstract boolean processingBinary();

    protected abstract void startIteration()
        throws Exception;

    protected final void startIteration(String text)
        throws Exception
    {
        prepare(text);
        startIteration();
    }

    protected abstract void startSystemIteration()
        throws Exception;

    protected abstract void nextValue()
        throws Exception;

    protected abstract SymbolTable currentSymtab()
        throws Exception;

    protected abstract void checkAnnotation(String expected)
        throws Exception;

    protected abstract void checkType(IonType expected)
        throws Exception;

    protected abstract void checkString(String expected)
        throws Exception;

    protected abstract void checkSymbol(String expected)
        throws Exception;

    protected abstract void checkSymbol(String expected, int expectedSid)
        throws Exception;

    /**
     * Checks a symbol that's defined in a missing symbol table.
     */
    protected abstract void checkMissingSymbol(String expected, int expectedSid)
        throws Exception;

    protected abstract void checkInt(long expected)
        throws Exception;

    protected abstract void checkDecimal(double expected)
        throws Exception;

    protected abstract void checkFloat(double expected)
        throws Exception;

    protected abstract void checkTimestamp(String expected)
        throws Exception;

    protected abstract void checkEof()
        throws Exception;


    //=========================================================================

    //=========================================================================

    public void testLocalTableResetting()
        throws Exception
    {
        String text = "bar foo $ion_1_0 1 far boo";

        prepare(text);
        startIteration();

        nextValue();
        checkSymbol("bar");

        SymbolTable table1 = currentSymtab();
        checkLocalTable(table1);

        nextValue();
        checkSymbol("foo");
        assertSame(table1, currentSymtab());

        // The symbol table changes here

        nextValue();
        checkInt(1);

        nextValue();
        checkSymbol("far");

        SymbolTable table2 = currentSymtab();
        checkLocalTable(table2);
        assertNotSame(table1, table2);

        nextValue();
        checkSymbol("boo");
        assertSame(table2, currentSymtab());
    }

    public void testTrivialLocalTableResetting()
        throws Exception
    {
        String text = "1 $ion_1_0 2";

        startIteration(text);

        nextValue();
        checkInt(1);

        SymbolTable table1 = currentSymtab();
        checkTrivialLocalTable(table1);

        nextValue();
        checkInt(2);

        SymbolTable table2 = currentSymtab();
        checkTrivialLocalTable(table2);
        if (table1.isLocalTable() || table2.isLocalTable())
        {
            assertNotSame(table1, table2);
        }
        assertEquals(SystemSymbolTable.ION_1_0_MAX_ID, table2.getMaxId());
    }

    public void testLocalTableReplacement()
        throws Exception
    {
        String text =
            "$ion_symbol_table::{" +
            "  symbols:[ \"foo\", \"bar\" ]," +
            "}\n" +
            "bar foo\n" +
            "$ion_symbol_table::{" +
            "  symbols:[ \"bar\" ]," +
            "}\n" +
            "bar foo";

        startIteration(text);

        nextValue();
        checkSymbol("bar", ION_1_0_MAX_ID + 2);

        SymbolTable table1 = currentSymtab();
        checkLocalTable(table1);

        nextValue();
        checkSymbol("foo", ION_1_0_MAX_ID + 1);

        // Symtab changes here...

        nextValue();
        checkSymbol("bar", ION_1_0_MAX_ID + 1);

        SymbolTable table2 = currentSymtab();
        checkLocalTable(table2);
        assertNotSame(table1, table2);
        assertTrue(ION_1_0_MAX_ID + 1 <= table2.getMaxId());
        assertTrue(ION_1_0_MAX_ID + 2 >= table2.getMaxId());

        nextValue();
        checkSymbol("foo", ION_1_0_MAX_ID + 2);
        assertEquals(ION_1_0_MAX_ID + 2, table2.getMaxId());
        assertSame(table2, currentSymtab());
    }

    public void testTrivialLocalTableReplacement()
        throws Exception
    {
        String text =
            "$ion_symbol_table::{" +
            "}\n" +
            "1\n" +
            "$ion_symbol_table::{" +
            "}\n" +
            "2";

        startIteration(text);

        nextValue();
        checkInt(1);

        SymbolTable table1 = currentSymtab();
        checkLocalTable(table1);

        nextValue();
        checkInt(2);

        SymbolTable table2 = currentSymtab();
        checkLocalTable(table2);
        assertNotSame(table1, table2);
        assertEquals(ION_1_0_MAX_ID, table2.getMaxId());
    }


    public void testLocalSymtabWithOpenContent()
        throws Exception
    {
        String data = "$ion_symbol_table::{open:33,symbols:[\"a\",\"b\"]} b";

        startIteration(data);
        nextValue();
        checkSymbol("b", 11);

        if (false)
        {
            // No such promises at the moment...
            SymbolTable symtab = currentSymtab();
            IonStruct symtabStruct = symtab.getIonRepresentation();
            checkInt(33, symtabStruct.get("open"));
        }
    }


    /**
     * Import v2 but catalog has v1.
     */
    public void testLocalTableWithLesserImport()
        throws Exception
    {
        final int fred1id = ION_1_0_MAX_ID + 1;
        final int fred2id = ION_1_0_MAX_ID + 2;
        final int fred3id = ION_1_0_MAX_ID + 3;

        final int local = ION_1_0_MAX_ID + Symtabs.FRED_MAX_IDS[2];
        final int local1id = local + 1;
        final int local2id = local + 2;

        SimpleCatalog catalog = (SimpleCatalog) system().getCatalog();
        Symtabs.register("fred", 1, catalog);
        Symtabs.register("fred", 2, catalog);


        String text =
            LocalSymbolTablePrefix +
            "{" +
            "  imports:[{name:\"fred\", version:2, " +
            "            max_id:" + Symtabs.FRED_MAX_IDS[2] + "}]," +
            "}\n" +
            "local1 local2 fred_1 fred_2 fred_3";

        prepare(text);

        // Remove the imported table
        assertNotNull(catalog.removeTable("fred", 2));

        startIteration();
        nextValue();
        checkSymbol("local1", local1id);
        nextValue();
        checkSymbol("local2", local2id);
        nextValue();
        checkSymbol("fred_1", fred1id);
        nextValue();
        checkSymbol("fred_2", fred2id);
        nextValue();
        checkMissingSymbol("fred_3", (processingBinary() ? fred3id : local+3));
        checkEof();
    }

    /**
     * Import v2 but catalog has v3.
     */
    public void testLocalTableWithGreaterImport()
        throws Exception
    {
        final int fred1id = ION_1_0_MAX_ID + 1;
        final int fred2id = ION_1_0_MAX_ID + 2;
        final int fred3id = ION_1_0_MAX_ID + 3;

        final int local = ION_1_0_MAX_ID + Symtabs.FRED_MAX_IDS[2];
        final int local1id = local + 1;
        final int local2id = local + 2;
        final int local3id = local + 3;

        SimpleCatalog catalog = (SimpleCatalog) system().getCatalog();
        Symtabs.register("fred", 1, catalog);
        Symtabs.register("fred", 2, catalog);
        SymbolTable fredV3 = Symtabs.register("fred", 3, catalog);

        // Make sure our syms don't overlap.
        assertTrue(fredV3.findSymbol("fred_5") != local3id);

        // fred_5 is not in table version 2, so it gets local symbol
        // fred_2 is missing from version 3
        String text =
            LocalSymbolTablePrefix +
            "{" +
            "  imports:[{name:\"fred\", version:2, " +
            "            max_id:" + Symtabs.FRED_MAX_IDS[2] + "}]" +
            "} " +
            "local1 local2 fred_1 fred_2 fred_3 fred_5";

        prepare(text);

        // Remove the imported table before decoding the binary.
        assertNotNull(catalog.removeTable("fred", 2));

        startIteration();
        nextValue();
        checkSymbol("local1", local1id);
        nextValue();
        checkSymbol("local2", local2id);
        nextValue();
        checkSymbol("fred_1", fred1id);
        nextValue();
        checkMissingSymbol("fred_2", (processingBinary() ? fred2id : local+3));
        nextValue();
        checkSymbol("fred_3", fred3id);
        nextValue();
        checkSymbol("fred_5", (processingBinary() ? local+3 : local+4));
        checkEof();
    }


    public void testSharedTableNotAddedToCatalog()
        throws Exception
    {
        String text =
            SystemSymbolTable.ION_1_0 + " " +
            SymbolTableTest.IMPORTED_1_SERIALIZED +
            " 'imported 1'";
        assertNull(system().getCatalog().getTable("imported"));

        startIteration(text);
        nextValue();
        checkType(IonType.STRUCT);
        checkAnnotation(SystemSymbolTable.ION_SHARED_SYMBOL_TABLE);

        assertNull(system().getCatalog().getTable("imported"));

        nextValue();
        checkSymbol("imported 1");
    }

    public void testObsoleteSharedTableFormat()
        throws Exception
    {
        String text =
            "$ion_symbol_table::{ name:'''test''', symbols:['''x'''] }" +
            "346";

        startIteration(text);
        nextValue();
        checkInt(346);

        assertNull(system().getCatalog().getTable("test"));
    }


    /**
     * Parse Ion string data and ensure it matches expected text.
     */
    protected void testString(String expectedValue, String ionData)
        throws Exception
    {
        testString(expectedValue, ionData, ionData);
    }

    /**
     * Parse Ion string data and ensure it matches expected text.
     */
    protected void testString(String expectedValue,
                              String expectedRendering,
                              String ionData)
        throws Exception
    {
        startIteration(ionData);
        nextValue();
        checkString(expectedValue);
        checkEof();
    }

    public void testUnicodeCharacters()
        throws Exception
    {
        String ionData = "\"\\0\"";
        testString("\0", ionData);

        ionData = "\"\\x01\"";
        testString("\01", ionData);

        // U+007F is a control character
        ionData = "\"\\x7f\"";
        testString("\u007f", ionData);

        ionData = "\"\\xff\"";
        testString("\u00ff", ionData);

        ionData = "\"\\" + "u0110\""; // Carefully avoid Java escape
        testString("\u0110", ionData);

        ionData = "\"\\" + "uffff\""; // Carefully avoid Java escape
        testString("\uffff", ionData);

        ionData = "\"\\" + "U0001d110\""; // Carefully avoid Java escape
//        testString("\ud834\udd10", ionData); // FIXME enable test case

        // The largest legal code point
        ionData = "\"\\" + "U0010ffff\""; // Carefully avoid Java escape
//        testString("\udbff\udfff", ionData); // FIXME enable test case
    }


    public void testQuotesInLongStrings()
        throws Exception
    {
        testString("'", "\"'\"", "'''\\''''");
        testString("x''y", "\"x''y\"", "'''x''y'''");
        testString("x'''y", "\"x'''y\"", "'''x''\\'y'''");
        testString("x\"y", "\"x\\\"y\"", "'''x\"y'''");
        testString("x\"\"y", "\"x\\\"\\\"y\"", "'''x\"\"y'''");
    }

    // TODO similar tests on clob

    public void XXXtestPosInt() // TODO rework?
        throws Exception
    {
        startIteration("+1");
        nextValue();
        checkSymbol("+");
        nextValue();
        checkInt(1);
        checkEof();
    }

    public void XXXtestPosDecimal() // TODO rework?
        throws Exception
    {
        startIteration("+123d0");
        nextValue();
        checkSymbol("+");
        nextValue();
        checkDecimal(123D);
        checkEof();
    }

    public void XXXtestPosFloat() // TODO rework?
        throws Exception
    {
        startIteration("+123e0");
        nextValue();
        checkSymbol("+");
        nextValue();
        checkFloat(123D);
        checkEof();
    }

    public void XXXtestPosTimestamp() // TODO rework?
        throws Exception
    {
        startIteration("+2009-02-18");
        nextValue();
        checkSymbol("+");
        nextValue();
        checkTimestamp("2009-02-18");
        checkEof();
    }

    public void XXXtestSpecialFloats() // FIXME enable test case
        throws Exception
    {
        startIteration("nan +inf -inf");
        nextValue();
        checkFloat(Double.NaN);
        nextValue();
        checkFloat(Double.POSITIVE_INFINITY);
        nextValue();
        checkFloat(Double.NEGATIVE_INFINITY);
        checkEof();
    }


    //=========================================================================

    public void testSystemIterationShowsIvm()
        throws Exception
    {
        String text = ION_1_0;

        prepare(text);
        startSystemIteration();
        nextValue();
        checkSymbol(ION_1_0, SystemSymbolTable.ION_1_0_SID);
        SymbolTable st = currentSymtab();
        assertTrue(st.isSystemTable());
        assertEquals(ION_1_0, st.getIonVersionId());
        checkEof();
    }
}
