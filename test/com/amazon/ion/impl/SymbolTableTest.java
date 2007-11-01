/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.impl;

import static com.amazon.ion.SystemSymbolTable.ION_1_0;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonException;
import com.amazon.ion.IonInt;
import com.amazon.ion.IonList;
import com.amazon.ion.IonLoader;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSexp;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonTestCase;
import com.amazon.ion.IonValue;
import com.amazon.ion.LocalSymbolTable;
import com.amazon.ion.StaticSymbolTable;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.system.SimpleCatalog;
import java.util.Iterator;

/**
 *
 */
public class SymbolTableTest
    extends IonTestCase
{
    public final int ION_1_0_MAX_ID =
        system().getSystemSymbolTable(ION_1_0).getMaxId();

    public final static int IMPORTED_1_MAX_ID = 2;
    public final static int IMPORTED_2_MAX_ID = 4;
    public final static int IMPORTED_3_MAX_ID = 5;


    public StaticSymbolTable registerImportedV1()
    {
        IonSystem system = system();
        String importingText =
            "$ion_1_0\n" +
            "$ion_symbol_table::{" +
            "  name:'''imported''', version:1," +
            "  symbols:{" +
            "    $1:'''imported 1'''," +
            "    $2:'''imported 2'''," +
            "  }" +
            "}\n" +
            "null";
        oneValue(importingText);

        StaticSymbolTable importedTable =
            system.getCatalog().getTable("imported", 1);
        assertEquals(IMPORTED_1_MAX_ID, importedTable.getMaxId());

        return importedTable;
    }

    public StaticSymbolTable registerImportedV2()
    {
        IonSystem system = system();
        String importingText =
            "$ion_1_0\n" +
            "$ion_symbol_table::{" +
            "  name:'''imported''', version:2," +
            "  symbols:{" +
            "    $1:'''imported 1'''," +
            "    $2:'''imported 2'''," +
            "    $3:'''fred3'''," +
            "    $4:'''fred4'''," +
            "  }" +
            "}\n" +
            "null";
        loader().loadText(importingText);

        StaticSymbolTable importedTable =
            system.getCatalog().getTable("imported", 2);
        assertEquals(IMPORTED_2_MAX_ID, importedTable.getMaxId());

        return importedTable;
    }

    public StaticSymbolTable registerImportedV3()
    {
        IonSystem system = system();
        String importingText =
            "$ion_1_0\n" +
            "$ion_symbol_table::{" +
            "  name:'''imported''', version:3," +
            "  symbols:{" +
            "    $1:'''imported 1'''," +
            // Removed symbol     imported 2
            "    $3:'''fred3'''," +
            "    $4:'''fred4'''," +
            "    $5:'''fred5'''," +
            "  }" +
            "}\n" +
            "null";
        oneValue(importingText);

        StaticSymbolTable importedTable =
            system.getCatalog().getTable("imported", 3);
        assertEquals(IMPORTED_3_MAX_ID, importedTable.getMaxId());

        return importedTable;
    }


    //=========================================================================
    // Test cases

    public void testLocalTable()
    {
        String text =
            "$ion_1_0::{" +
            "  symbols:{ $100:\"foo\"," +
            "            $101:\"bar\"}," +
            "}\n" +
            "null";

        LocalSymbolTable symbolTable = oneValue(text).getSymbolTable();

        assertEquals(2, symbolTable.size());
        checkSymbol("foo", 100, symbolTable);
        checkSymbol("bar", 101, symbolTable);

        assertEquals(-1, symbolTable.findSymbol("not there"));
        assertEquals("$33", symbolTable.findSymbol(33));
    }


    public void testLocalTableReplacement()
    {
        String text =
            "$ion_1_0::{" +
            "  symbols:{ $100:\"foo\"," +
            "            $101:\"bar\"}," +
            "}\n" +
            "bar foo\n" +
            "$ion_1_0::{" +
            "  symbols:{ $13:\"foo\"}," +
            "}\n" +
            "bar foo";

        IonReader scanner = system().newTextReader(text);

        IonValue value = scanner.next();
        checkSymbol("bar", 101, value);

        LocalSymbolTable table1 = scanner.getLocalSymbolTable();

        value = scanner.next();
        checkSymbol("foo", 100, value);

        value = scanner.next();
        checkSymbol("bar", 14, value);

        LocalSymbolTable table2 = scanner.getLocalSymbolTable();
        assertNotSame(table1, table2);
        assertEquals(14, table2.getMaxId());

        value = scanner.next();
        checkSymbol("foo", 13, value);
        assertEquals(14, table2.getMaxId());

        assertSame(table2, scanner.getLocalSymbolTable());
    }


    public void testLocalTableResetting()
        throws Exception
    {
        String text =
            "$ion_1_0::{" +
            "  symbols:{ $100:\"foo\"," +
            "            $101:\"bar\"}," +
            "}\n" +
            "bar foo\n" +
            "$ion_1_0\n" +
            "1 bar foo";

        IonReader scanner = system().newTextReader(text);
        testLocalTableResetting(scanner);

        IonLoader loader = system().newLoader();
        IonDatagram datagram = loader.loadText(text);

        testLocalTableResetting(datagram.iterator());

        // FIXME this fails because the (second) local table isn't encoded.
        datagram = loader.load(datagram.toBytes());
        testLocalTableResetting(datagram.iterator());
    }


    public void testLocalTableResetting(Iterator<IonValue> values)
    {
        IonReader scanner = ((values instanceof IonReader)
                              ? (IonReader) values
                              : null);

        IonValue value = values.next();
        checkSymbol("bar", 101, value);

        SymbolTable table1 = value.getSymbolTable();
        if (scanner != null) {
            assertSame(table1, scanner.getLocalSymbolTable());
        }

        value = values.next();
        checkSymbol("foo", 100, value);
        assertSame(table1, value.getSymbolTable());
        if (scanner != null) {
            assertSame(table1, scanner.getLocalSymbolTable());
        }

        // Symbol table changes here

        // Make sure the symtab doesn't change until we call next()
        assertTrue(values.hasNext());
        if (scanner != null) {
            assertSame(table1, scanner.getLocalSymbolTable());
        }

        value = values.next();
        checkInt(1, value);

        value = values.next();
        checkSymbol("bar", value);

        SymbolTable table2 = value.getSymbolTable();
        assertNotSame(table1, table2);
//        assertEquals(14, table2.getMaxId());  // We don't know the new sid

        value = values.next();
        checkSymbol("foo", value);
//        assertEquals(14, table2.getMaxId());

        assertSame(table2, value.getSymbolTable());
    }


    public void testStaticTable()
        throws Exception
    {
        String text =
            "$ion_symbol_table::{" +
            "  name: \"test table\"," +
            "  version: 1,\n" +
            "  symbols:{ $100:\"foo\"," +
            "            $101:\"bar\"}," +
            "  max_id:101," +
            "}\n" +
            "null";

        IonReader scanner = system().newTextReader(text);
        testStaticTable(scanner);

        IonDatagram datagram = loader().loadText(text);
        testStaticTable(datagram.iterator());

        datagram = loader().load(datagram.toBytes());
        testStaticTable(datagram.iterator());
    }


    public void testStaticTable(Iterator<IonValue> values)
    {
        SymbolTable symbolTable = values.next().getSymbolTable();

        // Nothing should be added to the local table.
        assertEquals(0, symbolTable.size());

        symbolTable = system().getCatalog().getTable("test table");

        assertEquals(2, symbolTable.size());
        checkSymbol("foo", 100, symbolTable);
        checkSymbol("bar", 101, symbolTable);
        assertEquals(101, symbolTable.getMaxId());

        assertEquals(-1, symbolTable.findSymbol("not there"));
        assertEquals("$33", symbolTable.findSymbol(33));
    }



    public void testHidingSystemSymbolId()
    {
        int nameSid =
            system().getSystemSymbolTable("$ion_1_0").findSymbol("name");

        String importingText =
            "$ion_1_0::{" +
            "  symbols:{" +
            "    $" + nameSid + ":'''shadow'''," +
            "    $25:'''local25'''," +
            "  }," +
            "}\n" +
            "null";

        IonReader scanner = system().newTextReader(importingText);
        try {
            scanner.next();
            fail("Expected IonException");
        }
        catch (IonException e) { /* ok */ }
    }


    public void testShadowing()
    {
        int shadowId = ION_1_0_MAX_ID + 1;

        String importingText =
            "$ion_1_0\n" +
            "$ion_symbol_table::{" +
            "  name:'''imported''', version:1," +
            "  symbols:{" +
            "    $1:'''imported 1'''," +
            "    $2:'''imported 2'''," +
            "  }" +
            "}\n" +
            "$ion_1_0::{" +
            "  imports:[{name:'''imported''', version:1, max_id:2}]," +
            "  symbols:{" +
            "    $" + shadowId + ":'''shadow'''," +
            "    $25:'''outer 25'''," +
            "  }," +
            "}\n" +
            "'outer 25'\n" +
            "'imported 1'\n" +
            "shadow\n" +
            "$" + shadowId;

        IonReader scanner = system().newTextReader(importingText);

        IonValue value = scanner.next();
        LocalSymbolTable symtab = value.getSymbolTable();
        assertEquals(ION_1_0_MAX_ID, symtab.getSystemSymbolTable().getMaxId());

        checkSymbol("outer 25", 25, value);

        assertNull(symtab.findKnownSymbol(26));

        value = scanner.next();
        checkSymbol("imported 1", 26, value);

        value = scanner.next();
        checkSymbol("shadow", shadowId, value);

        // Here the input text is $NNN  but it comes back correctly.
        value = scanner.next();
        checkSymbol("shadow", shadowId, value);
    }


    public void testParsingLocalTableWithImport()
    {
        SymbolTable importedTable = registerImportedV1();

        String text =
            "$ion_1_0::{" +
            "  symbols:{ $100:\"dates\",\n" +
            "            $101:\"whenDate\"},\n" +
            "  imports:[{name:'''imported''',version:1}],\n" +
            "}\n" +
            "null";
        IonDatagram dg = system().newLoader().loadText(text);

        LocalSymbolTable symbolTable = dg.get(0).getSymbolTable();
        SymbolTable used = symbolTable.getImportedTable("imported");
        assertSame(importedTable, used);

        // Check that the encoded table has max_id on import
        byte[] binary = dg.toBytes();
        dg = system().newLoader().load(binary);
        IonStruct symtabStruct = (IonStruct) dg.systemGet(0);
        IonList imports = (IonList) symtabStruct.get("imports");
        IonStruct importStruct = (IonStruct) imports.get(0);
        checkString("imported", importStruct.get("name"));
        checkInt(IMPORTED_1_MAX_ID, importStruct.get("max_id"));
    }


    public void testLocalTableWithMissingImport()
    {
        final int import1id = ION_1_0_MAX_ID + 1;
        final int import2id = ION_1_0_MAX_ID + 2;

        final int local1id = ION_1_0_MAX_ID + IMPORTED_1_MAX_ID + 1;
        final int local2id = local1id + 1;

        SymbolTable importedTable = registerImportedV1();

        String text =
            "$ion_1_0::{" +
            "  symbols:{ $" + local1id + ":\"local1\"," +
            "            $" + local2id + ":\"local2\"}," +
            "  imports:[{name:\"imported\", version:1," +
            "            max_id:" + IMPORTED_1_MAX_ID + "}]," +
            "}\n" +
            "local1 local2 'imported 1' 'imported 2'";
        byte[] binary = encode(text);

        // Remove the imported table before decoding the binary.
        SimpleCatalog catalog = (SimpleCatalog) system().getCatalog();
        assertSame(importedTable, catalog.removeTable("imported", 1));

        IonDatagram dg = system().newLoader().load(binary);
        checkSymbol("local1", local1id, dg.get(0));
        checkSymbol("local2", local2id, dg.get(1));
        checkSymbol("$" + import1id, import1id, dg.get(2));
        checkSymbol("$" + import2id, import2id, dg.get(3));

        LocalSymbolTable st = dg.get(3).getSymbolTable();
        assertNull(st.getImportedTable("imported"));

        assertEquals(-1, st.findSymbol("imported 2"));
        assertEquals(-1, st.findSymbol("unknown"));
    }


    /**
     * Import v2 but catalog has v1.
     */
    public void testLocalTableWithLesserImport()
    {
        final int import1id = ION_1_0_MAX_ID + 1;
        final int import2id = ION_1_0_MAX_ID + 2;
        final int fred3id   = ION_1_0_MAX_ID + 3;

        final int local1id = ION_1_0_MAX_ID + IMPORTED_2_MAX_ID + 1;
        final int local2id = local1id + 1;

        registerImportedV1();
        registerImportedV2();

        String text =
            "$ion_1_0::{" +
            "  symbols:{ $" + local1id + ":\"local1\"," +
            "            $" + local2id + ":\"local2\"}," +
            "  imports:[{name:\"imported\", version:2,}]," +
            "}\n" +
            "local1 local2 'imported 1' 'imported 2' fred3";
        byte[] binary = encode(text);

        // Remove the imported table before decoding the binary.
        SimpleCatalog catalog = (SimpleCatalog) system().getCatalog();
        assertNotNull(catalog.removeTable("imported", 2));

        IonDatagram dg = loader().load(binary);
        checkSymbol("local1", local1id, dg.get(0));
        checkSymbol("local2", local2id, dg.get(1));
        checkSymbol("imported 1", import1id, dg.get(2));
        checkSymbol("imported 2", import2id, dg.get(3));
        checkSymbol("$" + fred3id, fred3id, dg.get(4));

        // We can't load the original text because it doesn't have max_id
        badValue(text);
    }

    /**
     * Import v2 but catalog has v3.
     */
    public void testLocalTableWithGreaterImport()
    {
        final int import1id = ION_1_0_MAX_ID + 1;
        final int import2id = ION_1_0_MAX_ID + 2;
        final int fred3id   = ION_1_0_MAX_ID + 3;

        final int local1id = ION_1_0_MAX_ID + IMPORTED_2_MAX_ID + 1;
        final int local2id = local1id + 1;
        final int local3id = local2id + 1;

        registerImportedV1();
        registerImportedV2();
        StaticSymbolTable i3 = registerImportedV3();

        // Make sure our syms don't overlap.
        assertTrue(i3.findSymbol("fred5") != local3id);

        // fred5 is not in table version 2, so it gets local symbol
        String text =
            "$ion_1_0::{" +
            "  imports:[{name:\"imported\", version:2,}]," +
            "}\n" +
            "local1 local2 'imported 1' 'imported 2' fred3 fred5";
        byte[] binary = encode(text);

        // Remove the imported table before decoding the binary.
        SimpleCatalog catalog = (SimpleCatalog) system().getCatalog();
        assertNotNull(catalog.removeTable("imported", 2));

        IonDatagram dg = loader().load(binary);
        checkSymbol("local1", local1id, dg.get(0));
        checkSymbol("local2", local2id, dg.get(1));
        checkSymbol("imported 1", import1id, dg.get(2));
        checkSymbol("$" + import2id, import2id, dg.get(3));
        checkSymbol("fred3", fred3id, dg.get(4));
        checkSymbol("fred5", local3id, dg.get(5));

        // We can't load the original text because it doesn't have max_id
        badValue(text);
    }


    // TODO test empty imports:[]
    // TODO test non-array imports
    // TODO test getUsedTable(null)
    // TODO test getImportedTable(null)

    public void testLocalWithBadMaxId()
    {
        String text =
            "$ion_1_0::{" +
            "  imports:[{name:\"imported\", version:1," +
            "            max_id:buggy" +                   // max_id not an int
            "}]," +
            "}\n" +
            "null";
        badValue(text);

        text =
            "$ion_1_0::{" +
            "  imports:[{name:\"imported\", version:1}]," +
            // No max_id, and table is not in catalog.
            "}\n" +
            "null";
        badValue(text);


        text =
            "$ion_1_0::{" +
            "  imports:[{name:\"imported\", version:1," +
            "            max_id:0" +                     // max_id not positive
            "}]," +
            "}\n" +
            "null";
        badValue(text);

        registerImportedV1();
        badValue(text);
    }


    public void testStaticTableMissingName()
    {
        String text =
            "$ion_symbol_table::{" +
            "  version:1," +
            "  symbols:{ $100:\"x\" }\n" +
            "}";
        badValue(text);
    }


    public void testStaticTableMissingVersion()
    {
        String text =
            "$ion_symbol_table::{" +
            "  name:\"test\"," +
            "  symbols:{ $100:\"x\" }\n" +
            "}\n" +
            "null";
        oneValue(text);
        StaticSymbolTable symbolTable =
            system().getCatalog().getTable("test");

        // Version defaults to 1
        assertEquals(1, symbolTable.getVersion());
    }


    public void testInvalidName()
    {
        // Name must be a string
        String text =
            "$ion_symbol_table::{" +
            "  name:test," +                  // Here's the bug
            "  version:1," +
            "  symbols:{ $100:\"x\" }\n" +
            "}\n" +
            "null";
        badValue(text);

        // Name must be a non-empty
        text =
            "$ion_symbol_table::{" +
            "  name:\"\"," +                  // Here's the bug
            "  version:1," +
            "  symbols:{ $100:\"x\" }\n" +
            "}\n" +
            "null";
        badValue(text);

        // Name must be a non-null
        text =
            "$ion_symbol_table::{" +
            "  name:null.string," +                  // Here's the bug
            "  version:1," +
            "  symbols:{ $100:\"x\" }\n" +
            "}\n" +
            "null";
        badValue(text);
    }

    public void testVersionNotInt()
    {
        String text =
            "$ion_symbol_table::{" +
            "  name:\"test\"," +
            "  version:'1'," +                  // Here's the bug
            "  symbols:{ $100:\"x\" }\n" +
            "}\n" +
            "null";
        badValue(text);
    }

    public void testNullVersion()
    {
        String text =
            "$ion_symbol_table::{" +
            "  name:\"test\"," +
            "  version:null," +                  // Here's the bug
            "  symbols:{ $100:\"x\" }\n" +
            "}\n" +
            "null";
        badValue(text);

        text =
            "$ion_symbol_table::{" +
            "  name:\"test\"," +
            "  version:null.int," +                  // Here's the bug
            "  symbols:{ $100:\"x\" }\n" +
            "}\n" +
            "null";
        badValue(text);

        text =
            "$ion_symbol_table::{" +
            "  name:\"test\"," +
            "  version:null.bool," +                  // Here's the bug
            "  symbols:{ $100:\"x\" }\n" +
            "}\n" +
            "null";
        badValue(text);
    }

    public void testSymbolsFieldNotStruct()
    {
        String text =
            "$ion_symbol_table::{" +
            "  name:test," +
            "  version:1," +
            "  symbols:[[$100,\"x\"]]\n" +
            "}\n" +
            "null";
        badValue(text);

    }

    public void testSymbolNotString()
    {
        String text =
            "$ion_symbol_table::{" +
            "  name:\"test\", version:1," +
            "  symbols:{ $100:dates }\n" +
            "}\n" +
            "null";
        badValue(text);

        text =
            "$ion_symbol_table::{" +
            "  name:\"test\", version:1," +
            "  symbols:{ $100:100 }\n" +
            "}\n" +
            "null";
        badValue(text);
    }

    public void testSystemIdOnNonStruct()
    {
        String text = "$ion_1_0::12";
        IonInt v = (IonInt) oneValue(text);
        checkInt(12, v);
    }

    public void testSymbolTableOnNonStruct()
    {
        String text = "$ion_symbol_table::12";
        IonInt v = (IonInt) oneValue(text);
        checkInt(12, v);
    }

    public void testNestedSystemId()
    {
        String text = "($ion_1_0)";
        IonSexp v = oneSexp(text);
        checkSymbol(ION_1_0, v.get(0));
    }
}
