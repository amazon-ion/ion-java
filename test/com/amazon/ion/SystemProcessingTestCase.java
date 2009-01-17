/*
 * Copyright (c) 2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;

import static com.amazon.ion.Symtabs.LocalSymbolTablePrefix;
import static com.amazon.ion.SystemSymbolTable.ION_1_0_MAX_ID;

import com.amazon.ion.impl.SymbolTableTest;
import com.amazon.ion.system.SimpleCatalog;



/**
 *
 */
public abstract class SystemProcessingTestCase
    extends IonTestCase
{
    // Subclasses must override EITHER startIteration(String) OR
    // prepare(String) and startIteration()
    protected void prepare(String text)
        throws Exception
    {
        startIteration();
    }

    protected abstract boolean processingBinary();

    protected void startIteration()
        throws Exception
    {
    }

    @Deprecated
    protected void startIteration(String text)
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

    protected abstract void checkEof()
        throws Exception;


    //=========================================================================

    public void testLocalTableResetting()
        throws Exception
    {
        String text = "bar foo $ion_1_0 1 bar foo";

        startIteration(text);

        nextValue();
        checkSymbol("bar");

        SymbolTable table1 = currentSymtab();
        checkLocalTable(table1);

        nextValue();
        checkSymbol("foo");
        assertSame(table1, currentSymtab());

        // Symbol table changes here

        nextValue();
        checkInt(1);

        nextValue();
        checkSymbol("bar");

        SymbolTable table2 = currentSymtab();
        checkLocalTable(table2);
        assertNotSame(table1, table2);

        nextValue();
        checkSymbol("foo");
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
            "  symbols:{ $100:\"foo\"," +
            "            $101:\"bar\"}," +
            "}\n" +
            "bar foo\n" +
            "$ion_symbol_table::{" +
            "  symbols:{ $13:\"foo\"}," +
            "}\n" +
            "bar foo";

        startIteration(text);

        nextValue();
        checkSymbol("bar", 101);

        SymbolTable table1 = currentSymtab();
        checkLocalTable(table1);

        nextValue();
        checkSymbol("foo", 100);

        nextValue();
        checkSymbol("bar", 14);

        SymbolTable table2 = currentSymtab();
        checkLocalTable(table2);
        assertNotSame(table1, table2);
        assertEquals(14, table2.getMaxId());

        nextValue();
        checkSymbol("foo", 13);
        assertEquals(14, table2.getMaxId());
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
        assertEquals(SystemSymbolTable.ION_1_0_MAX_ID, table2.getMaxId());
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
            "            max_id:" + Symtabs.FRED_MAX_IDS[2] + "}]," + // FIXME remove
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

        // We can't load the original text because it doesn't have max_id
        // and the table isn't in the catalog.
//        badValue(text);
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

        // fred5 is not in table version 2, so it gets local symbol
        String text =
            LocalSymbolTablePrefix +
            "{" +
            "  imports:[{name:\"fred\", version:2, " +
            "            max_id:" + Symtabs.FRED_MAX_IDS[2] + "}]," + // FIXME remove
            "}\n" +
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

        // We can't load the original text because it doesn't have max_id
        // and the table isn't in the catalog.
//        badValue(text);  FIXME re-enable
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

    //=========================================================================

    public void testSystemIterationShowsIvm()
        throws Exception
    {
        String text = SystemSymbolTable.ION_1_0;

        prepare(text);
        startSystemIteration();
        nextValue();
        checkSymbol(SystemSymbolTable.ION_1_0, SystemSymbolTable.ION_1_0_SID);
        checkEof();
    }
}
