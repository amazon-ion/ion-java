// Copyright (c) 2007-2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.SystemSymbolTable.ION;
import static com.amazon.ion.SystemSymbolTable.ION_1_0;
import static com.amazon.ion.SystemSymbolTable.ION_1_0_MAX_ID;
import static com.amazon.ion.SystemSymbolTable.ION_1_0_SID;
import static com.amazon.ion.SystemSymbolTable.ION_SYMBOL_TABLE;
import static com.amazon.ion.impl.UnifiedSymbolTable.ION_SHARED_SYMBOL_TABLE;

import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonException;
import com.amazon.ion.IonInt;
import com.amazon.ion.IonList;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSexp;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonTestCase;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.Symtabs;
import com.amazon.ion.system.SimpleCatalog;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class SymbolTableTest
    extends IonTestCase
{
    public static final String LocalSymbolTablePrefix = ION_SYMBOL_TABLE + "::";
    public static final String SharedSymbolTablePrefix = ION_SHARED_SYMBOL_TABLE + "::";


    public final static int IMPORTED_1_MAX_ID = 2;
    public final static int IMPORTED_2_MAX_ID = 4;
    public final static int IMPORTED_3_MAX_ID = 5;

    public final static String IMPORTED_1_SERIALIZED =
        SharedSymbolTablePrefix +
        "{" +
        "  name:'''imported''', version:1," +
        "  symbols:['''imported 1''', '''imported 2''']" +
        "}";



    public SymbolTable registerImportedV1()
    {
        SymbolTable shared = registerSharedSymtab(IMPORTED_1_SERIALIZED);
        assertEquals(IMPORTED_1_MAX_ID, shared.getMaxId());

        SymbolTable importedTable =
            system().getCatalog().getTable("imported", 1);
        assertSame(shared, importedTable);

        return importedTable;
    }

    public SymbolTable registerImportedV2()
    {
        IonSystem system = system();
        String importingText =
            SharedSymbolTablePrefix +
            "{" +
            "  name:'''imported''', version:2," +
            "  symbols:[" +
            "    '''imported 1'''," +
            "    '''imported 2'''," +
            "    '''fred3'''," +
            "    '''fred4'''," +
            "  ]" +
            "}";
        SymbolTable shared = registerSharedSymtab(importingText);
        assertEquals(IMPORTED_2_MAX_ID, shared.getMaxId());

        SymbolTable importedTable =
            system.getCatalog().getTable("imported", 2);
        assertSame(shared, importedTable);

        return importedTable;
    }

    public SymbolTable registerImportedV3()
    {
        IonSystem system = system();
        String importingText =
            SharedSymbolTablePrefix +
            "{" +
            "  name:'''imported''', version:3," +
            "  symbols:[" +
            "    '''imported 1'''," +
            "    null," +            // Removed 'imported 2'
            "    '''fred3'''," +
            "    '''fred4'''," +
            "    '''fred5'''," +
            "  ]" +
            "}";
        SymbolTable shared = registerSharedSymtab(importingText);
        assertEquals(IMPORTED_3_MAX_ID, shared.getMaxId());

        SymbolTable importedTable =
            system.getCatalog().getTable("imported", 3);
        assertSame(shared, importedTable);

        return importedTable;
    }


    //=========================================================================
    // Test cases

    public void testInitialSystemSymtab()
    {
        final SymbolTable systemTable = system().getSystemSymbolTable(ION_1_0);
        assertEquals(ION, systemTable.getName());

        String text = "0";
        IonValue v = oneValue(text);
        SymbolTable st = v.getSymbolTable();
        assertSame(systemTable, st.getSystemSymbolTable());

        IonDatagram dg = loader().load(text);
        IonSymbol sysId = (IonSymbol) dg.systemGet(0);
        checkSymbol(ION_1_0, ION_1_0_SID, sysId);
        assertSame(systemTable, sysId.getSymbolTable());

        v = dg.get(0);
        st = v.getSymbolTable();
        assertSame(systemTable, st.getSystemSymbolTable());
    }

    public void testLocalTable()
    {
        String text =
            LocalSymbolTablePrefix +
            "{" +
            "  symbols:[ \"foo\", \"bar\"]," +
            "}\n" +
            "null";

        SymbolTable symbolTable = oneValue(text).getSymbolTable();
        checkLocalTable(symbolTable);

        checkSymbol("foo", ION_1_0_MAX_ID + 1, symbolTable);
        checkSymbol("bar", ION_1_0_MAX_ID + 2, symbolTable);

        assertEquals(-1, symbolTable.findSymbol("not there"));
        assertEquals("$33", symbolTable.findSymbol(33));
    }

    public void testImportsFollowSymbols()
    {
        registerImportedV1();

        final int import1id = ION_1_0_MAX_ID + 1;
        final int local1id = ION_1_0_MAX_ID + IMPORTED_1_MAX_ID + 1;
        final int local2id = local1id + 1;

        String importingText =
            "$ion_1_0 "+
            LocalSymbolTablePrefix +
            "{" +
            "  symbols:[ '''local1''' ]," +
            "  imports:[{name:'''imported''', version:1, max_id:2}]," +
            "}\n" +
            "local2\n" +  // This symbol is added to end of locals
            "local1\n" +
            "'imported 1'";

        Iterator<IonValue> scanner = system().iterate(importingText);

        IonValue value = scanner.next();
        SymbolTable symtab = value.getSymbolTable();
        checkLocalTable(symtab);
        checkSymbol("local2", local2id, value);

        value = scanner.next();
        checkSymbol("local1", local1id, value);

        value = scanner.next();
        checkSymbol("imported 1", import1id, value);
    }


    /**
     * Attempts to override system symbols are ignored.
     */
    public void testOverridingSystemSymbolId()
    {
        int nameSid =
            system().getSystemSymbolTable("$ion_1_0").findSymbol("name");

        String importingText =
            LocalSymbolTablePrefix +
            "{" +
            "  symbols:[ '''name''' ]," +
            "}\n" +
            "null";

        Iterator<IonValue> scanner = system().iterate(importingText);
        IonValue v = scanner.next();
        SymbolTable symtab = v.getSymbolTable();
        assertTrue(symtab.isLocalTable());
        assertEquals(nameSid, symtab.findSymbol("name"));
    }


    public void testOverridingImportedSymbolId()
    {
        registerImportedV1();

        final int import1id = ION_1_0_MAX_ID + 1;

        String importingText =
            "$ion_1_0 "+
            LocalSymbolTablePrefix +
            "{" +
            "  imports:[{name:'''imported''', version:1, max_id:2}]," +
            "  symbols:[ '''imported 1''' ]," +
            "}\n" +
            "'imported 1'\n" +
            "$" + import1id;

        Iterator<IonValue> scanner = system().iterate(importingText);

        IonValue value = scanner.next();
        SymbolTable symtab = value.getSymbolTable();
        checkLocalTable(symtab);
        checkSymbol("imported 1", import1id, value);

        // Here the input text is $NNN  but it comes back correctly.
        value = scanner.next();
        checkSymbol("imported 1", import1id, value);
    }


    public void XXXtestInjectingMaxIdIntoImport() // TODO implement
    {
        SymbolTable importedTable = registerImportedV1();

        String text =
            LocalSymbolTablePrefix +
            "{" +
            "  imports:[{name:'''imported''',version:1}],\n" +
            "}\n" +
            "null";
        IonDatagram dg = loader().load(text);

        SymbolTable symbolTable = dg.get(0).getSymbolTable();
        checkLocalTable(symbolTable);

        SymbolTable[] imported = symbolTable.getImportedTables();
        assertEquals(1, imported.length);
        assertSame(importedTable, imported[0]);

        // Check that the encoded table has max_id on import
        byte[] binary = dg.getBytes();
        dg = loader().load(binary);
        IonStruct symtabStruct = (IonStruct) dg.systemGet(1);
        IonList imports = (IonList) symtabStruct.get("imports");
        IonStruct importStruct = (IonStruct) imports.get(0);
        checkString("imported", importStruct.get("name"));

        IonValue maxIdValue = importStruct.get("max_id");
        assertNotNull("max_id wasn't injected into import", maxIdValue);
        checkInt(IMPORTED_1_MAX_ID, maxIdValue);
    }


    public void testLocalTableWithMissingImport()
    {
        // Use a big symtab to get beyond any default allocation within the
        // dummy symtab.  This was done to trap a bug in UnifiedSymbolTable.
        ArrayList<String> syms = new ArrayList<String>();
        int maxId = 50;
        for (int i = 1; i <= maxId; i++)
        {
            syms.add("S" + i);
        }

        SymbolTable table =
            system().newSharedSymbolTable("T", 1, syms.iterator());

        SimpleCatalog catalog = (SimpleCatalog) system().getCatalog();
        catalog.putTable(table);

        final int import1id = ION_1_0_MAX_ID + 1;
        final int import2id = ION_1_0_MAX_ID + 2;

        final int local1id = ION_1_0_MAX_ID + maxId + 1;
        final int local2id = local1id + 1;


        String text =
            LocalSymbolTablePrefix +
            "{" +
            "  symbols:[ \"local1\", \"local2\" ]," +
            "  imports:[{name:'''T''', version:1," +
            "            max_id:" + maxId + "}]," +
            "}\n" +
            "local1 local2 S1 S2";
        byte[] binary = encode(text);

        // Remove the imported table before decoding the binary.
        assertSame(table, catalog.removeTable("T", 1));

        IonDatagram dg = loader().load(binary);
        checkSymbol("local1", local1id, dg.get(0));
        checkSymbol("local2", local2id, dg.get(1));
        checkSymbol("$" + import1id, import1id, dg.get(2));
        checkSymbol("$" + import2id, import2id, dg.get(3));

        SymbolTable st = dg.get(3).getSymbolTable();
        checkLocalTable(st);

        SymbolTable dummy = findImportedTable(st, "T");
        assertEquals(1, dummy.getVersion());
        assertEquals(maxId, dummy.getMaxId());
        assertEquals(-1, dummy.findSymbol("S1"));
        assertEquals(-1, dummy.findSymbol("S2"));

        assertEquals(-1, st.findSymbol("S1"));
        assertEquals(-1, st.findSymbol("S2"));
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
            LocalSymbolTablePrefix +
            "{" +
            "  imports:[{name:\"imported\", version:2, " +
            "            max_id:" + IMPORTED_2_MAX_ID + "}]," +
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
        SymbolTable i3 = registerImportedV3();

        // Make sure our syms don't overlap.
        assertTrue(i3.findSymbol("fred5") != local3id);

        // fred5 is not in table version 2, so it gets local symbol
        String text =
            LocalSymbolTablePrefix +
            "{" +
            "  imports:[{name:\"imported\", version:2, " +
            "            max_id:" + IMPORTED_2_MAX_ID + "}]," +
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
    }

    public void testRepeatedImport()
    {
        SymbolTable importedV1 = registerImportedV1();
        SymbolTable importedV2 = registerImportedV2();

        String text =
            LocalSymbolTablePrefix +
            "{" +
            "  imports:[{name:\"imported\", version:1}," +
            "           {name:\"imported\", version:2}]," +
            "}\n" +
            "null";

        IonValue v = oneValue(text);
        SymbolTable symbolTable = v.getSymbolTable();
        SymbolTable[] importedTables = symbolTable.getImportedTables();
        assertEquals(2, importedTables.length);
        assertSame(importedV1, importedTables[0]);
        assertSame(importedV2, importedTables[1]);
        assertEquals(ION_1_0_MAX_ID + IMPORTED_1_MAX_ID + IMPORTED_2_MAX_ID,
                     symbolTable.getMaxId());

        assertEquals(10, symbolTable.findSymbol("imported 1"));
        assertEquals(11, symbolTable.findSymbol("imported 2"));
        assertEquals(14, symbolTable.findSymbol("fred3"));
        assertEquals(15, symbolTable.findSymbol("fred4"));

        // Gaps left by second copies of 'imported 1' and 'imported 2'
        assertNull(symbolTable.findKnownSymbol(12));
        assertNull(symbolTable.findKnownSymbol(13));
    }



    public void testMalformedImportsField()
    {
        testMalformedImportsField("[]");
        testMalformedImportsField("null.list");
        testMalformedImportsField("null");
        testMalformedImportsField("'''hello'''");
        testMalformedImportsField("imports");
        testMalformedImportsField("a_symbol");


        testMalformedImportsField("[{}]");
        testMalformedImportsField("[null.struct]");
        testMalformedImportsField("[null]");
        testMalformedImportsField("[1009]");
        testMalformedImportsField("[a_symbol]");
    }

    private void testMalformedImportsField(String value)
    {
        String text =
            LocalSymbolTablePrefix +
            "{" +
            "  imports:" + value + "," +
            "  symbols:['''local''']" +
            "}\n" +
            "null";
        IonValue v = oneValue(text);
        SymbolTable symbolTable = v.getSymbolTable();
        assertEquals(0, symbolTable.getImportedTables().length);
        assertEquals(ION_1_0_MAX_ID + 1, symbolTable.findSymbol("local"));
        assertEquals(ION_1_0_MAX_ID + 1, symbolTable.getMaxId());
    }


    public void testImportWithMalformedName()
    {
        SymbolTable importedV1 = registerImportedV1();

        testImportWithMalformedName(importedV1, null);      // missing field
        testImportWithMalformedName(importedV1, " \"\" ");  // empty string
        testImportWithMalformedName(importedV1, "null.string");
        testImportWithMalformedName(importedV1, "null");
        testImportWithMalformedName(importedV1, "'syms'");  // symbol
        testImportWithMalformedName(importedV1, "123");

        // Cannot import system symtab
        testImportWithMalformedName(importedV1, "'''$ion'''");
    }

    private void testImportWithMalformedName(SymbolTable importedV1,
                                             String name)
    {
        String text =
            LocalSymbolTablePrefix +
            "{" +
            "  imports:[{name:\"imported\", version:1}," +
            "           {version:1," + fieldText("name", name) + "}]," +
            "}\n" +
            "null";

        IonValue v = oneValue(text);
        SymbolTable symbolTable = v.getSymbolTable();
        SymbolTable[] importedTables = symbolTable.getImportedTables();
        assertEquals(1, importedTables.length);
        assertSame(importedV1, importedTables[0]);
        assertEquals(ION_1_0_MAX_ID + IMPORTED_1_MAX_ID,
                     symbolTable.getMaxId());
    }


    // TODO test getUsedTable(null)
    // TODO test getImportedTable(null)

    public void testImportWithZeroMaxId()
    {
        SymbolTable importedV1 = registerImportedV1();

        String text =
            LocalSymbolTablePrefix +
            "{" +
            "  imports:[{name:\"imported\", version:1, max_id:0}]," +
            "  symbols:['''local''']" +
            "}\n" +
            "null";
        IonValue v = oneValue(text);
        SymbolTable symbolTable = v.getSymbolTable();
        assertSame(importedV1, symbolTable.getImportedTables()[0]);
        assertEquals(ION_1_0_MAX_ID + 1, symbolTable.findSymbol("local"));
    }

    public void testImportWithBadMaxId()
    {
        SymbolTable importedV1 = registerImportedV1();

        testImportWithBadMaxId(importedV1, "null.int");
        testImportWithBadMaxId(importedV1, "null");
        testImportWithBadMaxId(importedV1, "not_an_int");
//        testImportWithBadMaxId(importedV1, "0");  Zero isn't bad, its zero!
        testImportWithBadMaxId(importedV1, "-1");
        testImportWithBadMaxId(importedV1, "-2223");

        String text =
            LocalSymbolTablePrefix +
            "{" +
            "  imports:[{name:\"imported\", version:1}]," +
            "}\n" +
            "null";
        IonValue v = oneValue(text);
        assertSame(importedV1, v.getSymbolTable().getImportedTables()[0]);

        SimpleCatalog catalog = (SimpleCatalog)system().getCatalog();
        catalog.removeTable(importedV1.getName(), importedV1.getVersion());
        badValue(text);
    }

    public void testImportWithBadMaxId(SymbolTable expected, String maxIdText)
    {
        String text =
            LocalSymbolTablePrefix +
            "{" +
            "  imports:[{name:\"imported\", version:1," +
            "            max_id:" + maxIdText +
            "  }]," +
            "}\n" +
            "null";
        IonValue v = oneValue(text);
        assertSame(expected, v.getSymbolTable().getImportedTables()[0]);
    }


    //-------------------------------------------------------------------------
    // Local symtab creation

    public void testEmptyLocalSymtabCreation()
    {
        SymbolTable st = system().newLocalSymbolTable();
        checkEmptyLocalSymtab(st);

        st = system().newLocalSymbolTable((SymbolTable[])null);
        checkEmptyLocalSymtab(st);

        st = system().newLocalSymbolTable(new SymbolTable[0]);
        checkEmptyLocalSymtab(st);


        SymbolTable systemTable = system().getSystemSymbolTable();

        st = system().newLocalSymbolTable(systemTable);
        checkEmptyLocalSymtab(st);

        st = system().newLocalSymbolTable(new SymbolTable[]{ systemTable });
        checkEmptyLocalSymtab(st);
    }

    public void checkEmptyLocalSymtab(SymbolTable st)
    {
        SymbolTable systemTable = system().getSystemSymbolTable();

        checkLocalTable(st);
        assertSame(systemTable, st.getSystemSymbolTable());
        assertEquals(systemTable.getMaxId(), st.getMaxId());
        assertEquals(systemTable.getMaxId(), st.getImportedMaxId());
        assertEquals(0, st.getImportedTables().length);
    }

    public void testBasicLocalSymtabCreation()
    {
        SymbolTable systemTable = system().getSystemSymbolTable();
        SymbolTable fred1 = Symtabs.CATALOG.getTable("fred", 1);

        SymbolTable st = system().newLocalSymbolTable(systemTable, fred1);
        final int importedMaxId = systemTable.getMaxId() + fred1.getMaxId();

        checkLocalTable(st);
        assertSame(systemTable, st.getSystemSymbolTable());
        assertEquals(importedMaxId, st.getMaxId());
        assertEquals(importedMaxId, st.getImportedMaxId());
        assertEquals(1, st.getImportedTables().length);
        assertSame(fred1, st.getImportedTables()[0]);

        st = system().newLocalSymbolTable(new SymbolTable[]{ systemTable });
        checkEmptyLocalSymtab(st);
    }

    public void testBadLocalSymtabCreation()
    {
        SymbolTable systemTable = system().getSystemSymbolTable();
        SymbolTable fred1 = Symtabs.CATALOG.getTable("fred", 1);

        try
        {
            system().newLocalSymbolTable((SymbolTable)null);
        }
        catch (NullPointerException e) { }

        try
        {
            system().newLocalSymbolTable(fred1, systemTable);
        }
        catch (IllegalArgumentException e) { }

        try
        {
            system().newLocalSymbolTable(fred1, null);
        }
        catch (NullPointerException e) { }

        try
        {
            system().newLocalSymbolTable(fred1,
                                         system().newLocalSymbolTable());
        }
        catch (IllegalArgumentException e) { }

    }

    //-------------------------------------------------------------------------
    // Shared symtab creation

    public void testSystemNewSharedSymtab()
        throws Exception
    {
        for (String symtab : Symtabs.FRED_SERIALIZED)
        {
            if (symtab != null) testSystemNewSharedSymtab(symtab);
        }
        for (String symtab : Symtabs.GINGER_SERIALIZED)
        {
            if (symtab != null) testSystemNewSharedSymtab(symtab);
        }
    }

    public void testSystemNewSharedSymtab(String serializedSymbolTable)
        throws IOException
    {
      IonReader reader = system().newReader(serializedSymbolTable);
      SymbolTable stFromReader = system().newSharedSymbolTable(reader);
      assertTrue(stFromReader.isSharedTable());

      IonStruct struct = (IonStruct) oneValue(serializedSymbolTable);
      SymbolTable stFromValue = system().newSharedSymbolTable(struct);
      assertTrue(stFromValue.isSharedTable());

      Symtabs.assertEqualSymtabs(stFromReader, stFromValue);

      // Try a bit of round-trip action
      StringBuilder buf = new StringBuilder();
      IonWriter out = system().newTextWriter(buf);
      stFromReader.writeTo(out);
      reader = system().newReader(buf.toString());
      SymbolTable reloaded = system().newSharedSymbolTable(reader);

      Symtabs.assertEqualSymtabs(stFromReader, reloaded);
    }


    public void testBasicSharedSymtabCreation()
    {
        String[] syms = { "a", null, "b" };
        SymbolTable st =
            system().newSharedSymbolTable("ST", 1,
                                          Arrays.asList(syms).iterator());
        checkSharedTable("ST", 1, new String[]{"a", "b"}, st);


        // Now create version two
        catalog().putTable(st);

        String[] syms2 = { "c", "a" };
        SymbolTable st2 =
            system().newSharedSymbolTable("ST", 2,
                                          Arrays.asList(syms2).iterator());
        checkSharedTable("ST", 2, new String[]{"a", "b", "c"}, st2);
    }

    public void testEmptySharedSymtabCreation()
    {
        String[] noStrings = new String[0];

        SymbolTable st = system().newSharedSymbolTable("ST", 1, null);
        checkSharedTable("ST", 1, noStrings, st);

        st = system().newSharedSymbolTable("ST", 1,
                                           Arrays.asList(noStrings).iterator());
        checkSharedTable("ST", 1, noStrings, st);
    }

    public void testSharedSymtabCreationWithImports()
    {
        SymbolTable fred1   = Symtabs.CATALOG.getTable("fred", 1);
        SymbolTable ginger1 = Symtabs.CATALOG.getTable("ginger", 1);

        String[] syms = { "a", "fred_1", "b" };
        UnifiedSymbolTable st =
            system().newSharedSymbolTable("ST", 1,
                                          Arrays.asList(syms).iterator(),
                                          fred1);
        checkSharedTable("ST", 1,
                         new String[]{"fred_1", "fred_2", "a", "b"},
                         st);


        // Again, with two imports

        st = system().newSharedSymbolTable("ST", 1,
                                           Arrays.asList(syms).iterator(),
                                           fred1, ginger1);
        checkSharedTable("ST", 1,
                         new String[]{"fred_1", "fred_2", "g1", "g2", "a", "b"},
                         st);
    }

    public void testBadSharedSymtabCreation()
    {
        String[] syms = { "a" };
        List<String> symList = Arrays.asList(syms);

        try
        {
            // Prior version doesn't exist
            system().newSharedSymbolTable("ST", 2, symList.iterator());
            fail("expected exception");
        }
        catch (IonException e) { }

        try
        {
            system().newSharedSymbolTable(null, 1, symList.iterator());
            fail("expected exception");
        }
        catch (IllegalArgumentException e) { }

        try
        {
            system().newSharedSymbolTable("", 1, symList.iterator());
            fail("expected exception");
        }
        catch (IllegalArgumentException e) { }

        try
        {
            system().newSharedSymbolTable("ST", 0, symList.iterator());
            fail("expected exception");
        }
        catch (IllegalArgumentException e) { }

        try
        {
            system().newSharedSymbolTable("ST", -1, symList.iterator());
            fail("expected exception");
        }
        catch (IllegalArgumentException e) { }
    }

    // TODO test import that has null sid
    // TODO test insertion of empty symbol

    public void checkSharedTable(String name, int version,
                                 String[] expectedSymbols,
                                 SymbolTable actual)
    {
        assertTrue(actual.isSharedTable());
        assertFalse(actual.isSystemTable());
        assertEquals(name, actual.getName());
        assertEquals(version, actual.getVersion());
        assertEquals(0, ((UnifiedSymbolTable)actual).getImportedMaxId());

        assertEquals("symbol count",
                     expectedSymbols.length, actual.getMaxId());

        for (int i = 0; i < expectedSymbols.length; i++)
        {
            int sid = i+1;
            assertEquals("sid " + sid,
                         expectedSymbols[i], actual.findKnownSymbol(sid));
        }
    }


    //-------------------------------------------------------------------------
    // Testing name field

    public void testMalformedSymtabName()
    {
        testMalformedSymtabName(null);     // missing field
        testMalformedSymtabName(" \"\" "); // empty string
        testMalformedSymtabName("null.string");
        testMalformedSymtabName("null");
        testMalformedSymtabName("a_symbol");
        testMalformedSymtabName("159");
    }

    public void testMalformedSymtabName(String nameText)
    {
        String text =
            SharedSymbolTablePrefix +
            "{" +
            "  version:1," +
            "  symbols:[\"x\"]," +
            fieldText("name", nameText) +
            "}";
        try
        {
            loadSharedSymtab(text);
            fail("Expected exception");
        }
        catch (IonException e) {
            assertTrue(e.getMessage().contains("'name'"));
        }
    }


    //-------------------------------------------------------------------------
    // Testing version field

    public void testSharedTableMissingVersion()
    {
        String text =
            SharedSymbolTablePrefix +
            "{" +
            "  name:\"test\"," +
            "  symbols:[ \"x\" ]\n" +
            "}";
        SymbolTable symbolTable = loadSharedSymtab(text);

        // Version defaults to 1
        assertEquals(1, symbolTable.getVersion());
    }

    public void testMalformedVersionField()
    {
        testMalformedVersionField("-1");
        testMalformedVersionField("0");

        testMalformedVersionField("null.int");
        testMalformedVersionField("null");
        testMalformedVersionField("a_symbol");
        testMalformedVersionField("2.0");
    }

    public void testMalformedVersionField(String versionValue)
    {
        String text =
            SharedSymbolTablePrefix +
            "{" +
            "  name:\"test\"," +
            "  version:" + versionValue + "," +
            "  symbols:[\"x\", \"y\"]\n" +
            "}";

        SymbolTable table = registerSharedSymtab(text);
        assertEquals("test", table.getName());
        assertEquals(1, table.getVersion());
        assertEquals(2, table.getMaxId());

        text =
            LocalSymbolTablePrefix +
            "{ imports:[{ name:\"test\"," +
            "             version:" + versionValue + "}]" +
            "}\n" +
            "y";

        IonValue v = oneValue(text);
        checkSymbol("y", ION_1_0_MAX_ID + 2, v);
        assertSame(table, v.getSymbolTable().getImportedTables()[0]);
    }


    //-------------------------------------------------------------------------
    // Testing symbols field

    public void testMalformedSymbolsField()
    {
        testMalformedSymbolsField("[]");
        testMalformedSymbolsField("null.list");
        testMalformedSymbolsField("{}");
        testMalformedSymbolsField("null.struct");
        testMalformedSymbolsField("null");
        testMalformedSymbolsField("a_symbol");
        testMalformedSymbolsField("100");
    }

    public void testMalformedSymbolsField(String symbolValue)
    {
        String text =
            SharedSymbolTablePrefix +
            "{" +
            "  symbols:" + symbolValue + "," +   // Keep symbols first
            "  name:\"test\", version:5," +
            "}";
        SymbolTable table = registerSharedSymtab(text);
        assertEquals("test", table.getName());
        assertEquals(5, table.getVersion());
        assertEquals(0, table.getMaxId());

        text =
            LocalSymbolTablePrefix +
            "{symbols:" + symbolValue + "} " +
            "null";
        IonValue v = oneValue(text);
        table = v.getSymbolTable();
        assertTrue(table.isLocalTable());
        assertEquals(ION_1_0_MAX_ID, table.getMaxId());
    }

    public void testMalformedSymbolDeclarations()
    {
        testMalformedSymbolDeclaration(" \"\" ");      // empty string
        testMalformedSymbolDeclaration("null.string");
        testMalformedSymbolDeclaration("null");
        testMalformedSymbolDeclaration("a_symbol");
        testMalformedSymbolDeclaration("100");
        testMalformedSymbolDeclaration("['''whee''']");
    }

    public void testMalformedSymbolDeclaration(String symbolValue)
    {
        String text =
            SharedSymbolTablePrefix +
            "{" +
            "  name:\"test\", version:1," +
            "  symbols:[" + symbolValue + "]" +
            "}";
        SymbolTable table = registerSharedSymtab(text);
        assertEquals(1, table.getMaxId());
        assertEquals(null, table.findKnownSymbol(1));
        assertEquals("$1", table.findSymbol(1));


        text =
            LocalSymbolTablePrefix +
            "{symbols:[" + symbolValue + "]} " +
            "null";
        IonValue v = oneValue(text);
        table = v.getSymbolTable();
        assertTrue(table.isLocalTable());
        assertEquals(ION_1_0_MAX_ID + 1, table.getMaxId());
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


    static byte[] openFileForBuffer(Object arg) {
        FileInputStream is = null;
        byte[] buf = null;

        if (arg instanceof String) {
            String filename = (String)arg;
            File f = new File(filename);
            if (!f.canRead()) {
                throw new IllegalArgumentException("can't read the file " + filename);
            }
            try {
                is =  new FileInputStream(f);
                if (f.length() < 1 || f.length() > Integer.MAX_VALUE) {
                    throw new IllegalArgumentException("file is too long to load into a buffer: " + filename + " len = "+f.length());
                }
                int len = (int)f.length();
                buf = new byte[len];
                try {
                    if (is.read(buf) != len) {
                        throw new IOException ("failed to read file into buffer: " + filename);
                    }
                }
                catch (IOException e) {
                    throw new IllegalArgumentException(e);
                }
            }
            catch (FileNotFoundException e){
                throw new IllegalArgumentException("can't read the file " + filename);
            }
        }
        else {
            throw new IllegalArgumentException("string routines need a filename");
        }
        return buf;
    }


    private static String fieldText(String fieldName, String value)
    {
        if (value == null) return "";
        assert value.length() > 0;

        return fieldName + ':' + value;
    }

}
