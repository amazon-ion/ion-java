/*
 * Copyright (c) 2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;

import com.amazon.ion.impl.SymbolTableTest;



/**
 *
 */
public abstract class SystemProcessingTestCase
    extends IonTestCase
{

    protected abstract void startIteration(String text)
        throws Exception;

    protected abstract void nextUserValue()
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

    protected abstract void checkInt(long expected)
        throws Exception;

    //=========================================================================

    public void testLocalTableResetting()
        throws Exception
    {
        String text = "bar foo $ion_1_0 1 bar foo";

        startIteration(text);

        nextUserValue();
        checkSymbol("bar");

        SymbolTable table1 = currentSymtab();
        checkLocalTable(table1);

        nextUserValue();
        checkSymbol("foo");
        assertSame(table1, currentSymtab());

        // Symbol table changes here

        nextUserValue();
        checkInt(1);

        nextUserValue();
        checkSymbol("bar");

        SymbolTable table2 = currentSymtab();
        checkLocalTable(table2);
        assertNotSame(table1, table2);

        nextUserValue();
        checkSymbol("foo");
        assertSame(table2, currentSymtab());
    }

    public void testTrivialLocalTableResetting()
        throws Exception
    {
        String text = "1 $ion_1_0 2";

        startIteration(text);

        nextUserValue();
        checkInt(1);

        SymbolTable table1 = currentSymtab();
        checkTrivialLocalTable(table1);

        nextUserValue();
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

        nextUserValue();
        checkSymbol("bar", 101);

        SymbolTable table1 = currentSymtab();
        checkLocalTable(table1);

        nextUserValue();
        checkSymbol("foo", 100);

        nextUserValue();
        checkSymbol("bar", 14);

        SymbolTable table2 = currentSymtab();
        checkLocalTable(table2);
        assertNotSame(table1, table2);
        assertEquals(14, table2.getMaxId());

        nextUserValue();
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

        nextUserValue();
        checkInt(1);

        SymbolTable table1 = currentSymtab();
        checkLocalTable(table1);

        nextUserValue();
        checkInt(2);

        SymbolTable table2 = currentSymtab();
        checkLocalTable(table2);
        assertNotSame(table1, table2);
        assertEquals(SystemSymbolTable.ION_1_0_MAX_ID, table2.getMaxId());
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
        nextUserValue();
        checkType(IonType.STRUCT);
        checkAnnotation(SystemSymbolTable.ION_SHARED_SYMBOL_TABLE);

        assertNull(system().getCatalog().getTable("imported"));

        nextUserValue();
        checkSymbol("imported 1");
    }

    public void testObsoleteSharedTableFormat()
        throws Exception
    {
        String text =
            "$ion_symbol_table::{ name:'''test''', symbols:['''x'''] }" +
            "346";

        startIteration(text);
        nextUserValue();
        checkInt(346);

        assertNull(system().getCatalog().getTable("test"));
    }
}
